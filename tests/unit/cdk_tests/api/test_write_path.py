"""The write role, driven end to end against a scripted session."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pytest

from cdk.api import GenericAPIConnector
from cdk.types import AckStatus, Cursor, RetrySemantics, SchemaSpec, WriteMode

from .fakes import BASE_URL, FakeResponse, FakeSession, runtime_with, sent_query

pytestmark = pytest.mark.unit


def _document(
    *,
    mode: str = "insert",
    body: Any = None,
    batching: dict[str, Any] | None = None,
    idempotency: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
    query: dict[str, Any] | None = None,
    content_type: str | None = None,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "method": "POST",
        "path": "/items",
        "body": body
        or (
            {"items": {"from_input": "records"}}
            if batching
            else {"item": {"from_input": "record"}}
        ),
    }
    if headers is not None:
        request["headers"] = headers
    if query is not None:
        request["query"] = query
    if content_type is not None:
        request["content_type"] = content_type
    block: dict[str, Any] = {
        "request": request,
        "input": {
            "schema": {
                "type": "object",
                "properties": {
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
    if idempotency:
        block["idempotency"] = idempotency
    if mode == "upsert":
        block["conflict_keys"] = ["id"]
    return {
        "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
        "endpoint_id": "items",
        "operations": {"write": {mode: block}},
    }


def _batch(rows: int = 2) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([{"id": i} for i in range(rows)])


async def _connected(
    session: FakeSession,
    document: dict[str, Any],
    *,
    mode: WriteMode = WriteMode.WRITE_MODE_INSERT,
    error_map: dict[str, Any] | None = None,
) -> GenericAPIConnector:
    connector = GenericAPIConnector()
    connector.set_stream_endpoints({"items": document})
    await connector.connect(runtime_with(session, error_map=error_map))
    accepted = await connector.configure_schema(
        SchemaSpec(
            stream_id="items", version=1, write_mode=mode, ack_timeout_seconds=30
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


@pytest.mark.asyncio
class TestWriting:
    async def test_each_record_is_one_request_without_a_batching_block(self) -> None:
        session = FakeSession([FakeResponse(body={}), FakeResponse(body={})])
        connector = await _connected(session, _document())
        result = await _write(connector, _batch(2))
        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.records_written == 2
        assert len(session.calls) == 2
        assert session.calls[0]["url"] == f"{BASE_URL}/items"

    async def test_a_batching_block_sends_chunks(self) -> None:
        session = FakeSession([FakeResponse(body={}), FakeResponse(body={})])
        connector = await _connected(session, _document(batching={"max_records": 2}))
        result = await _write(connector, _batch(3))
        assert result.records_written == 3
        assert len(session.calls) == 2
        assert session.calls[0]["data"] == b'{"items":[{"id":0},{"id":1}]}'

    async def test_an_empty_batch_succeeds_and_still_advances_the_cursor(self) -> None:
        session = FakeSession()
        connector = await _connected(session, _document())
        result = await _write(connector, _batch(0))
        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.committed_cursor == Cursor(b"c")
        assert session.calls == []

    async def test_the_declared_idempotency_key_lands_where_it_was_declared(
        self,
    ) -> None:
        session = FakeSession([FakeResponse(body={})])
        connector = await _connected(
            session,
            _document(idempotency={"in": "header", "name": "Idempotency-Key"}),
        )
        await _write(connector, _batch(1))
        assert session.calls[0]["headers"]["Idempotency-Key"] == "r0"

    async def test_upsert_keys_on_the_records_content(self) -> None:
        # A changed row must get a new key, or the provider's replay cache
        # swallows a legitimate update.
        session = FakeSession([FakeResponse(body={})])
        connector = await _connected(
            session,
            _document(
                mode="upsert",
                idempotency={"in": "header", "name": "Idempotency-Key"},
            ),
            mode=WriteMode.WRITE_MODE_UPSERT,
        )
        await _write(connector, _batch(1))
        assert session.calls[0]["headers"]["Idempotency-Key"] != "r0"


@pytest.mark.asyncio
class TestTheDeclaredRequestReachesTheWire:
    """What the write role used to drop: every declared header and query entry."""

    async def test_the_declared_query_reaches_the_wire(self) -> None:
        session = FakeSession([FakeResponse(body={})])
        connector = await _connected(
            session, _document(query={"dry_run": {"literal": "false"}})
        )
        await _write(connector, _batch(1))
        assert sent_query(session.calls[0]) == {"dry_run": "false"}

    async def test_the_declared_headers_reach_the_wire_on_a_chunked_write(self) -> None:
        # The chunked path takes no per-record extra headers, so a plan that
        # did not carry them sent none at all.
        session = FakeSession([FakeResponse(body={})])
        connector = await _connected(
            session,
            _document(
                batching={"max_records": 5}, headers={"X-Tenant": {"literal": "acme"}}
            ),
        )
        await _write(connector, _batch(2))
        assert session.calls[0]["headers"]["X-Tenant"] == "acme"


@pytest.mark.asyncio
class TestReadiness:
    async def test_an_unconnected_connector_refuses_before_writing(self) -> None:
        connector = GenericAPIConnector()
        result = await _write(connector, _batch(1))
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert "not connected" in result.failure_summary

    async def test_an_unconfigured_stream_refuses_before_writing(self) -> None:
        connector = GenericAPIConnector()
        await connector.connect(runtime_with(FakeSession()))
        result = await _write(connector, _batch(1))
        assert "Schema not configured" in result.failure_summary

    async def test_a_document_the_worker_never_registered_is_rejected(self) -> None:
        connector = GenericAPIConnector()
        await connector.connect(runtime_with(FakeSession()))
        accepted = await connector.configure_schema(
            SchemaSpec(
                stream_id="items",
                version=1,
                write_mode=WriteMode.WRITE_MODE_INSERT,
                ack_timeout_seconds=30,
            )
        )
        assert accepted is False
        assert "no preloaded endpoint document" in connector.last_schema_rejection


@pytest.mark.asyncio
class TestVerdicts:
    async def test_a_deterministic_rejection_fails_the_batch_fatally(self) -> None:
        session = FakeSession([FakeResponse(status=422, body={"error": "bad"})])
        connector = await _connected(session, _document())
        result = await _write(connector, _batch(1))
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.failed_record_ids == ("r0",)

    async def test_a_retryable_failure_asks_for_the_whole_batch_again(self) -> None:
        session = FakeSession([FakeResponse(status=429, body={}) for _ in range(3)])
        connector = await _connected(session, _document())
        result = await _write(connector, _batch(1))
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.records_written == 0

    async def test_a_declared_category_rides_the_ack(self) -> None:
        session = FakeSession([FakeResponse(status=400, body={})])
        connector = await _connected(
            session, _document(), error_map={"http": {"400": "auth"}}
        )
        result = await _write(connector, _batch(1))
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.failure_category.name == "FAILURE_CATEGORY_CONFIG_DEFECT"

    async def test_a_partial_write_reports_what_landed(self) -> None:
        # The records that did land are already written, so the whole batch
        # must not be retried.
        session = FakeSession(
            [FakeResponse(body={}), FakeResponse(status=422, body={})]
        )
        connector = await _connected(session, _document())
        result = await _write(connector, _batch(2))
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 1
        assert result.failed_record_ids == ("r1",)

    async def test_a_form_encoding_failure_is_one_record_not_the_whole_batch(
        self,
    ) -> None:
        """Encoding fails for one record's data, so it fails one record.

        A form body carrying a container is that record's defect. Raising
        it from inside the send -- outside the per-record catch, in a block
        that expects only transport errors -- escaped the loop, so the
        outer path reported zero written even though earlier records had
        landed, and a replay sent them a second time.
        """
        session = FakeSession([FakeResponse(body={}), FakeResponse(body={})])
        connector = await _connected(
            session,
            _document(
                body={"payload": {"from_input": "record"}},
                content_type="application/x-www-form-urlencoded",
            ),
        )
        # `record` is the whole row, so every record's body field is a
        # mapping -- the shape a form cannot carry.
        result = await _write(connector, _batch(2))
        assert result.records_written == 0
        assert result.failed_record_ids == ("r0", "r1")
        assert session.calls == []


@pytest.mark.asyncio
class TestCapabilities:
    async def test_upsert_is_advertised_from_the_document(self) -> None:
        connector = GenericAPIConnector()
        connector.set_stream_endpoints({"items": _document(mode="upsert")})
        assert connector.supports_upsert is True

    async def test_upsert_is_not_advertised_without_a_declared_block(self) -> None:
        connector = GenericAPIConnector()
        connector.set_stream_endpoints({"items": _document()})
        assert connector.supports_upsert is False

    async def test_bulk_load_follows_the_batching_declaration(self) -> None:
        connector = GenericAPIConnector()
        connector.set_stream_endpoints(
            {"items": _document(batching={"max_records": 5})}
        )
        assert connector.supports_bulk_load is True

    async def test_transactions_are_never_advertised(self) -> None:
        assert GenericAPIConnector().supports_transactions is False

    async def test_the_retry_verdict_is_per_stream(self) -> None:
        session = FakeSession()
        connector = await _connected(
            session,
            _document(idempotency={"in": "header", "name": "Idempotency-Key"}),
        )
        verdict = connector.retry_semantics("items")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE

    async def test_an_unconfigured_stream_claims_only_at_least_once(self) -> None:
        verdict = GenericAPIConnector().retry_semantics("unknown")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE


@pytest.mark.asyncio
class TestHealthCheck:
    async def test_an_answering_api_is_healthy(self) -> None:
        session = FakeSession([FakeResponse(status=404, body={})])
        connector = await _connected(session, _document())
        # A 404 on the base URL still means the API answered.
        assert await connector.health_check() is True

    async def test_a_server_error_is_not(self) -> None:
        # The probe goes through the same retry client as every request, so
        # a retryable status is re-attempted before it is answered.
        session = FakeSession([FakeResponse(status=500, body={}) for _ in range(3)])
        connector = await _connected(session, _document())
        assert await connector.health_check() is False

    async def test_an_unconnected_connector_is_not(self) -> None:
        assert await GenericAPIConnector().health_check() is False
