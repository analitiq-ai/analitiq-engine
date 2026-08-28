"""How a failing write is attributed, per record and per chunk.

Two loops decide what the ack reports: one request per record, or one per
chunk. They attribute a failure differently on purpose -- a record can fail
alone, a chunk takes the unsent tail with it -- and one of them has a
documented shortfall that must not be quietly changed while the code moves.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pytest

from cdk.api import GenericAPIConnector
from cdk.types import AckStatus, Cursor, FailureCategory, SchemaSpec, WriteMode

from .fakes import FakeResponse, FakeSession, runtime_with

pytestmark = pytest.mark.unit


def _document(
    *,
    properties: dict[str, Any] | None = None,
    body: Any = None,
    batching: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    block: dict[str, Any] = {
        "request": {
            "method": "POST",
            "path": "/items",
            "body": body
            or (
                {"items": {"from_input": "records"}}
                if batching
                else {"item": {"from_input": "record"}}
            ),
        },
        "input": {
            "schema": {
                "type": "object",
                "properties": properties
                or {
                    "id": {
                        "type": "integer",
                        "native_type": "integer",
                        "arrow_type": "Int64",
                    }
                },
            }
        },
    }
    if batching:
        block["batching"] = batching
    if params:
        block["params"] = params
    if response:
        block["response"] = response
    return {
        "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
        "endpoint_id": "items",
        "operations": {"write": {"insert": block}},
    }


async def _connected(
    session: FakeSession, document: dict[str, Any]
) -> GenericAPIConnector:
    connector = GenericAPIConnector()
    connector.set_stream_endpoints({"items": document})
    await connector.connect(runtime_with(session))
    accepted = await connector.configure_schema(
        SchemaSpec(
            stream_id="items",
            version=1,
            write_mode=WriteMode.WRITE_MODE_INSERT,
            ack_timeout_seconds=30,
        )
    )
    assert accepted, connector.last_schema_rejection
    return connector


async def _write(connector: GenericAPIConnector, batch: pa.RecordBatch) -> Any:
    return await connector.write_batch(
        run_id="run-1",
        stream_id="items",
        batch_seq=1,
        record_batch=batch,
        record_ids=[f"r{i}" for i in range(batch.num_rows)],
        cursor=Cursor(b"c"),
        emitted_at=datetime(2026, 7, 31, tzinfo=timezone.utc),
    )


def _ids(rows: int) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([{"id": i} for i in range(rows)])


@pytest.mark.asyncio
class TestOneRequestPerRecord:
    async def test_a_deterministic_rejection_fails_only_that_record(self) -> None:
        session = FakeSession(
            [
                FakeResponse(status=422, body={}),
                FakeResponse(body={}),
                FakeResponse(body={}),
            ]
        )
        connector = await _connected(session, _document())
        result = await _write(connector, _ids(3))
        assert result.records_written == 2
        assert result.failed_record_ids == ("r0",)
        assert len(session.calls) == 3

    async def test_a_retryable_failure_mid_batch_discards_what_landed(self) -> None:
        # The documented shortfall (plan item 4.21): the base's outer catch
        # cannot see the local written counter, so the engine re-sends the
        # records that already landed. Streams with a declared idempotency
        # key are protected; the rest are classified at-least-once for
        # exactly this reason. Pinned so it is changed deliberately, in one
        # place, rather than drifting during a move.
        session = FakeSession(
            [FakeResponse(body={})] + [FakeResponse(status=429, body={})] * 3
        )
        connector = await _connected(session, _document())
        result = await _write(connector, _ids(3))
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.records_written == 0

    async def test_a_body_that_cannot_be_built_fails_only_its_record(self) -> None:
        # Body construction is data-dependent -- a record field can feed a
        # derived function -- so one unbuildable record is not a reason to
        # fail the records around it.
        document = _document(
            properties={
                "id": {
                    "type": "integer",
                    "native_type": "integer",
                    "arrow_type": "Int64",
                },
                "tag": {
                    "type": "string",
                    "native_type": "string",
                    "arrow_type": "Utf8",
                },
            },
            body={
                "item": {"from_input": "record.id"},
                "label": {
                    "function": "lookup",
                    "input": {"from_input": "record.tag"},
                    "map": {"known": "ok"},
                },
            },
        )
        session = FakeSession([FakeResponse(body={})])
        connector = await _connected(session, document)
        batch = pa.RecordBatch.from_pylist(
            [{"id": 0, "tag": ["not", "a", "scalar"]}, {"id": 1, "tag": None}],
            schema=pa.schema(
                [pa.field("id", pa.int64()), pa.field("tag", pa.list_(pa.string()))]
            ),
        )
        result = await _write(connector, batch)
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 1
        assert result.failed_record_ids == ("r0",)


@pytest.mark.asyncio
class TestOneRequestPerChunk:
    async def test_a_failing_chunk_takes_the_unsent_tail_with_it(self) -> None:
        # Every record past the failed chunk would land only to be re-sent
        # when the engine replays the dead-lettered batch.
        session = FakeSession(
            [FakeResponse(body={}), FakeResponse(status=422, body={})]
        )
        connector = await _connected(session, _document(batching={"max_records": 2}))
        result = await _write(connector, _ids(6))
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 2
        assert result.failed_record_ids == ("r2", "r3", "r4", "r5")
        assert len(session.calls) == 2

    async def test_a_retryable_first_chunk_asks_for_the_batch_again(self) -> None:
        # Nothing landed, so a retry cannot duplicate.
        session = FakeSession([FakeResponse(status=503, body={}) for _ in range(3)])
        connector = await _connected(session, _document(batching={"max_records": 2}))
        result = await _write(connector, _ids(4))
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.records_written == 0

    async def test_an_unbuildable_chunk_attributes_from_its_offset(self) -> None:
        # Unbuildable at the chunk, not at the parse: a function handed a
        # type it cannot take is a defect the document carries fine and the
        # body build meets on every chunk.
        document = _document(
            batching={"max_records": 2},
            body={
                "items": {"from_input": "records"},
                "tag": {"function": "base64_encode", "input": {"literal": 5}},
            },
        )
        session = FakeSession()
        connector = await _connected(session, document)
        result = await _write(connector, _ids(4))
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 0
        assert result.failed_record_ids == ("r0", "r1", "r2", "r3")


@pytest.mark.asyncio
class TestJsonTypedFields:
    async def test_a_json_column_is_decoded_before_the_body_is_built(self) -> None:
        # The wire carries a Json-typed field as an encoded string. Sending
        # it through would put a quoted string where the endpoint declared a
        # nested object.
        document = _document(
            properties={
                "id": {
                    "type": "integer",
                    "native_type": "integer",
                    "arrow_type": "Int64",
                },
                "payload": {
                    "type": "object",
                    "native_type": "json",
                    "arrow_type": "Json",
                },
            }
        )
        session = FakeSession([FakeResponse(body={})])
        connector = await _connected(session, document)
        batch = pa.RecordBatch.from_pylist([{"id": 1, "payload": '{"a": [1, 2]}'}])
        result = await _write(connector, batch)
        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert session.calls[0]["data"] == b'{"item":{"id":1,"payload":{"a":[1,2]}}}'

    async def test_a_malformed_json_column_fails_the_record(self) -> None:
        document = _document(
            properties={
                "id": {
                    "type": "integer",
                    "native_type": "integer",
                    "arrow_type": "Int64",
                },
                "payload": {
                    "type": "object",
                    "native_type": "json",
                    "arrow_type": "Json",
                },
            }
        )
        session = FakeSession()
        connector = await _connected(session, document)
        batch = pa.RecordBatch.from_pylist([{"id": 1, "payload": "{not json"}])
        result = await _write(connector, batch)
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert session.calls == []


@pytest.mark.asyncio
class TestDeclaredWriteParams:
    async def test_a_from_param_node_binds_a_declared_default(self) -> None:
        document = _document(
            params={
                "tenant": {
                    "in": "body",
                    "type": "string",
                    "required": True,
                    "default": {"literal": "acme"},
                }
            },
            body={
                "item": {"from_input": "record"},
                "tenant": {"from_param": "tenant"},
            },
        )
        session = FakeSession([FakeResponse(body={})])
        connector = await _connected(session, document)
        await _write(connector, _ids(1))
        assert b'"tenant":"acme"' in session.calls[0]["data"]

    async def test_a_param_that_resolves_to_nothing_drops_its_field(self) -> None:
        # A declared param whose default resolves to nothing binds None,
        # and an unresolved value omits its field rather than going onto
        # the wire as null.
        document = _document(
            params={
                "tenant": {
                    "in": "body",
                    "type": "string",
                    "required": False,
                    "default": {"ref": "connection.parameters.absent"},
                }
            },
            body={
                "item": {"from_input": "record"},
                "tenant": {"from_param": "tenant"},
            },
        )
        session = FakeSession([FakeResponse(body={})])
        connector = await _connected(session, document)
        await _write(connector, _ids(1))
        assert b"tenant" not in session.calls[0]["data"]


#: A provider that answers 200 and lists what it rejected in the body.
_ERRORS_DECLARED: dict[str, Any] = {
    "success_when": {"empty": {"ref": "response.body.errors"}},
    "error": {
        "code": {"ref": "response.body.error.code"},
        "message": {"ref": "response.body.error.message"},
    },
}


@pytest.mark.asyncio
class TestDeclaredResponse:
    async def test_a_false_success_when_fails_the_record_with_its_error(self) -> None:
        # The transport said 200; the body said no. The record is rejected
        # by the provider, so it is the record's failure and deterministic.
        session = FakeSession(
            [
                FakeResponse(body={"errors": []}),
                FakeResponse(
                    body={
                        "errors": [{"row": 0}],
                        "error": {"code": "DUP", "message": "already exists"},
                    }
                ),
                FakeResponse(body={"errors": []}),
            ]
        )
        connector = await _connected(session, _document(response=_ERRORS_DECLARED))
        result = await _write(connector, _ids(3))
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert (
            result.failure_category == FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED
        )
        assert result.records_written == 2
        assert result.failed_record_ids == ("r1",)
        assert "code='DUP'" in result.failure_summary
        assert "message='already exists'" in result.failure_summary
        assert len(session.calls) == 3

    async def test_a_true_success_when_acks_the_batch(self) -> None:
        session = FakeSession([FakeResponse(body={"errors": []})] * 2)
        connector = await _connected(session, _document(response=_ERRORS_DECLARED))
        result = await _write(connector, _ids(2))
        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.records_written == 2

    async def test_a_failed_chunk_is_reported_whole_with_the_unsent_tail(self) -> None:
        # No per-item id extraction exists in the contract, so which
        # records of the chunk the provider skipped is unknown: the chunk
        # fails as one, and the tail is not sent.
        session = FakeSession(
            [
                FakeResponse(body={"errors": []}),
                FakeResponse(body={"errors": [{"row": 1}]}),
            ]
        )
        connector = await _connected(
            session, _document(batching={"max_records": 2}, response=_ERRORS_DECLARED)
        )
        result = await _write(connector, _ids(6))
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert (
            result.failure_category == FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED
        )
        assert result.records_written == 2
        assert result.failed_record_ids == ("r2", "r3", "r4", "r5")
        assert len(session.calls) == 2

    async def test_affected_records_must_match_what_the_request_carried(self) -> None:
        session = FakeSession(
            [FakeResponse(body={"n": 2}), FakeResponse(body={"n": 1})]
        )
        connector = await _connected(
            session,
            _document(
                batching={"max_records": 2},
                response={"affected_records": {"ref": "response.body.n"}},
            ),
        )
        result = await _write(connector, _ids(4))
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 2
        assert result.failed_record_ids == ("r2", "r3")
        assert "affected_records resolved to 1" in result.failure_summary
        assert "carrying 2 records" in result.failure_summary

    async def test_a_matching_affected_records_count_acks(self) -> None:
        session = FakeSession([FakeResponse(body={"n": 2})])
        connector = await _connected(
            session,
            _document(
                batching={"max_records": 2},
                response={"affected_records": {"ref": "response.body.n"}},
            ),
        )
        result = await _write(connector, _ids(2))
        assert result.status == AckStatus.ACK_STATUS_SUCCESS

    async def test_status_and_headers_are_in_the_response_scope(self) -> None:
        # HTTP/2 lowercases header names; the declared `Location` must
        # still find it, as it would on the client's own header map.
        session = FakeSession(
            [
                FakeResponse(status=201, body={}, headers={"location": "/items/1"}),
                FakeResponse(status=200, body={}),
            ]
        )
        connector = await _connected(
            session,
            _document(
                response={
                    "success_when": {
                        "and": [
                            {"eq": [{"ref": "response.status"}, 201]},
                            {"exists": {"ref": "response.headers.Location"}},
                        ]
                    }
                }
            ),
        )
        result = await _write(connector, _ids(2))
        assert result.records_written == 1
        assert result.failed_record_ids == ("r1",)

    async def test_generated_keys_and_metadata_surface_on_the_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        session = FakeSession([FakeResponse(body={"id": 42, "rev": "v7"})])
        connector = await _connected(
            session,
            _document(
                response={
                    "success_when": {"exists": {"ref": "response.body.id"}},
                    "generated_keys": {"ref": "response.body.id"},
                    "metadata": {"revision": {"ref": "response.body.rev"}},
                }
            ),
        )
        with caplog.at_level("INFO", logger="cdk.api.generic"):
            result = await _write(connector, _ids(1))
        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert "generated_keys=42 metadata={'revision': 'v7'}" in caplog.text

    async def test_success_when_can_read_a_declared_metadata_key(self) -> None:
        session = FakeSession([FakeResponse(body={"state": "rejected"})])
        connector = await _connected(
            session,
            _document(
                response={
                    "success_when": {"eq": [{"ref": "response.metadata.state"}, "ok"]},
                    "metadata": {"state": {"ref": "response.body.state"}},
                }
            ),
        )
        result = await _write(connector, _ids(1))
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.failed_record_ids == ("r0",)
        assert "no error extraction declared" in result.failure_summary

    async def test_a_response_block_that_cannot_be_read_is_a_config_defect(
        self,
    ) -> None:
        # The provider accepted the request; the declaration cannot read the
        # answer. Whether the record landed is unknowable, so it is reported
        # failed with the rest, and the configuration owns the fix.
        session = FakeSession([FakeResponse(body={})])
        connector = await _connected(
            session,
            _document(
                response={
                    "affected_records": {
                        "function": "base64_encode",
                        "input": {"literal": 5},
                    }
                }
            ),
        )
        result = await _write(connector, _ids(2))
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.failure_category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT
        assert result.records_written == 0
        assert result.failed_record_ids == ("r0", "r1")
        assert "write response could not be read" in result.failure_summary
        assert len(session.calls) == 1
