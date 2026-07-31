"""The read role, driven end to end against a scripted session.

The three silent behaviour changes this move risked all show up here: the
page size binding (one resolver, or ``runtime.batch_size`` resolves to
nothing), the status boundary, and the declared category surviving to the
raised error.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from cdk.api import GenericAPIConnector
from cdk.exceptions import ReadError, TransientReadError

from .fakes import (
    BASE_URL,
    FakeCheckpoint,
    FakeResponse,
    FakeSession,
    endpoint_document,
    runtime_with,
    stream_source,
)

pytestmark = pytest.mark.unit


async def _read(
    session: FakeSession,
    document: dict[str, Any],
    *,
    source: dict[str, Any] | None = None,
    checkpoint: FakeCheckpoint | None = None,
    batch_size: int = 100,
    error_map: dict[str, Any] | None = None,
) -> list[Any]:
    connector = GenericAPIConnector()
    runtime = runtime_with(session, error_map=error_map)
    batches = []
    async for batch in connector.read_batches(
        runtime,
        {"endpoint_document": document, "stream_source": source or stream_source()},
        checkpoint=checkpoint or FakeCheckpoint(),
        stream_name="items",
        batch_size=batch_size,
    ):
        batches.append(batch)
    return batches


def _rows(count: int, start: int = 0) -> dict[str, Any]:
    return {
        "records": [{"id": start + i, "name": f"n{start + i}"} for i in range(count)]
    }


_OFFSET = {
    "type": "offset",
    "offset": {
        "param": "skip",
        "initial": 0,
        "increment_by": {"ref": "response.record_count"},
    },
    "limit": {"param": "limit", "default": {"ref": "runtime.batch_size"}, "max": 25},
    "stop_when": {"empty": {"ref": "response.body.records"}},
}

_PAGINATION_PARAMS = {
    "skip": {
        "in": "query",
        "type": "integer",
        "required": False,
        "controlled_by": "pagination",
    },
    "limit": {
        "in": "query",
        "type": "integer",
        "required": False,
        "controlled_by": "pagination",
    },
}


@pytest.mark.asyncio
class TestOnePage:
    async def test_a_single_request_yields_one_arrow_batch(self) -> None:
        session = FakeSession([FakeResponse(body=_rows(2))])
        batches = await _read(session, endpoint_document())
        assert len(batches) == 1
        assert batches[0].num_rows == 2
        assert batches[0].column("id").to_pylist() == [0, 1]

    async def test_the_url_is_the_base_plus_the_declared_path(self) -> None:
        session = FakeSession([FakeResponse(body=_rows(1))])
        await _read(session, endpoint_document())
        assert session.calls[0]["url"] == f"{BASE_URL}/items"

    async def test_an_endpoint_with_no_pagination_stops_after_its_page(self) -> None:
        # The unpaginated read runs on the same loop as every other, so the
        # empty-page rule and the yield are written once.
        session = FakeSession([FakeResponse(body=_rows(2))])
        assert len(await _read(session, endpoint_document())) == 1
        assert len(session.calls) == 1

    async def test_a_202_is_read_as_the_success_it_is(self) -> None:
        # The read path used to fail a whole stream on any status but 200.
        session = FakeSession([FakeResponse(status=202, body=_rows(2))])
        batches = await _read(session, endpoint_document())
        assert batches[0].num_rows == 2


@pytest.mark.asyncio
class TestPageSizeBinding:
    async def test_the_provider_cap_clamps_the_engines_batch_size(self) -> None:
        session = FakeSession(
            [FakeResponse(body=_rows(1)), FakeResponse(body=_rows(0))]
        )
        await _read(
            session,
            endpoint_document(pagination=_OFFSET, params=_PAGINATION_PARAMS),
            batch_size=100,
        )
        assert session.calls[0]["params"]["limit"] == 25

    async def test_the_runtime_batch_size_reference_resolves(self) -> None:
        # It resolves only from the read's own resolver: a second one built
        # anywhere else leaves it unresolvable, and the read silently runs
        # at the wrong page size.
        pagination = {
            **_OFFSET,
            "limit": {"param": "limit", "default": {"ref": "runtime.batch_size"}},
        }
        session = FakeSession(
            [FakeResponse(body=_rows(1)), FakeResponse(body=_rows(0))]
        )
        await _read(
            session,
            endpoint_document(pagination=pagination, params=_PAGINATION_PARAMS),
            batch_size=7,
        )
        assert session.calls[0]["params"]["limit"] == 7

    async def test_an_authored_literal_beats_the_engines_batch_size(self) -> None:
        pagination = {
            **_OFFSET,
            "limit": {"param": "limit", "default": {"literal": 5}},
        }
        session = FakeSession(
            [FakeResponse(body=_rows(1)), FakeResponse(body=_rows(0))]
        )
        await _read(
            session,
            endpoint_document(pagination=pagination, params=_PAGINATION_PARAMS),
            batch_size=100,
        )
        assert session.calls[0]["params"]["limit"] == 5

    async def test_a_page_size_that_is_not_a_positive_integer_fails_first(self) -> None:
        pagination = {
            **_OFFSET,
            "limit": {"param": "limit", "default": {"literal": 0}},
        }
        session = FakeSession()
        with pytest.raises(ReadError, match="limit.default"):
            await _read(
                session,
                endpoint_document(pagination=pagination, params=_PAGINATION_PARAMS),
            )
        assert session.calls == []


@pytest.mark.asyncio
class TestPaging:
    async def test_the_offset_advances_by_the_pages_own_record_count(self) -> None:
        session = FakeSession(
            [
                FakeResponse(body=_rows(3)),
                FakeResponse(body=_rows(2, start=3)),
                FakeResponse(body=_rows(0)),
            ]
        )
        batches = await _read(
            session, endpoint_document(pagination=_OFFSET, params=_PAGINATION_PARAMS)
        )
        assert [call["params"]["skip"] for call in session.calls] == [0, 3, 5]
        assert [batch.num_rows for batch in batches] == [3, 2]

    async def test_a_short_page_does_not_end_the_traversal(self) -> None:
        # Providers return short pages for filtering and per-request caps;
        # stopping there silently truncated the read.
        session = FakeSession(
            [FakeResponse(body=_rows(1)), FakeResponse(body=_rows(0))]
        )
        batches = await _read(
            session,
            endpoint_document(pagination=_OFFSET, params=_PAGINATION_PARAMS),
            batch_size=100,
        )
        assert len(batches) == 1
        assert len(session.calls) == 2

    async def test_the_declared_stop_condition_ends_the_loop(self) -> None:
        pagination = {
            **_OFFSET,
            "stop_when": {"eq": [{"ref": "response.body.has_more"}, False]},
        }
        body = {**_rows(2), "has_more": False}
        session = FakeSession([FakeResponse(body=body)])
        batches = await _read(
            session, endpoint_document(pagination=pagination, params=_PAGINATION_PARAMS)
        )
        assert len(batches) == 1
        assert len(session.calls) == 1

    async def test_a_link_page_leaving_the_origin_is_refused(self) -> None:
        pagination = {
            "type": "link",
            "link": {"next_url": {"ref": "response.body.next"}},
            "stop_when": {"missing": {"ref": "response.body.next"}},
        }
        body = {**_rows(2), "next": "https://evil.test/steal"}
        session = FakeSession([FakeResponse(body=body)])
        with pytest.raises(ReadError, match="leaves the connection's origin"):
            await _read(session, endpoint_document(pagination=pagination))

    async def test_a_relative_link_continues_from_the_current_page(self) -> None:
        pagination = {
            "type": "link",
            "link": {"next_url": {"ref": "response.body.next"}},
            "stop_when": {"missing": {"ref": "response.body.next"}},
        }
        session = FakeSession(
            [
                FakeResponse(body={**_rows(1), "next": "?page=2"}),
                FakeResponse(body=_rows(0)),
            ]
        )
        await _read(session, endpoint_document(pagination=pagination))
        assert session.calls[1]["url"] == f"{BASE_URL}/items?page=2"


@pytest.mark.asyncio
class TestIncremental:
    def _document(self) -> dict[str, Any]:
        return endpoint_document(
            params={
                "since": {
                    "in": "query",
                    "type": "string",
                    "required": False,
                    "controlled_by": "replication",
                }
            },
            replication={
                "supported_methods": ["full_refresh", "incremental"],
                "cursor_mappings": [
                    {"cursor_field": "id", "param": "since", "operator": "gte"}
                ],
            },
        )

    async def test_the_stored_cursor_binds_to_its_declared_param(self) -> None:
        session = FakeSession([FakeResponse(body=_rows(1))])
        await _read(
            session,
            self._document(),
            source=stream_source(
                method="incremental", cursor_field="id", safety_window=60
            ),
            checkpoint=FakeCheckpoint({"cursor": "2026-07-31T12:00:00Z"}),
        )
        assert session.calls[0]["params"]["since"] == "2026-07-31T11:59:00Z"

    async def test_the_cursor_advances_from_each_pages_last_record(self) -> None:
        checkpoint = FakeCheckpoint({"cursor": "1"})
        session = FakeSession([FakeResponse(body=_rows(3))])
        await _read(
            session,
            self._document(),
            source=stream_source(
                method="incremental", cursor_field="id", safety_window=0
            ),
            checkpoint=checkpoint,
        )
        assert checkpoint.saved == [{"cursor": 2}]

    async def test_a_missing_safety_window_is_a_wiring_defect(self) -> None:
        # The engine fills it before the config reaches a connector.
        # Inventing a default here is how three copies of the number
        # appeared.
        session = FakeSession()
        with pytest.raises(ReadError, match="safety_window_seconds"):
            await _read(
                session,
                self._document(),
                source=stream_source(method="incremental", cursor_field="id"),
                checkpoint=FakeCheckpoint({"cursor": "1"}),
            )

    async def test_no_prior_cursor_reads_everything(self) -> None:
        session = FakeSession([FakeResponse(body=_rows(1))])
        await _read(
            session,
            self._document(),
            source=stream_source(
                method="incremental", cursor_field="id", safety_window=60
            ),
            checkpoint=FakeCheckpoint(None),
        )
        assert "since" not in session.calls[0]["params"]


@pytest.mark.asyncio
class TestFailures:
    async def test_a_deterministic_status_fails_the_stream(self) -> None:
        session = FakeSession([FakeResponse(status=404, body={"error": "nope"})])
        with pytest.raises(ReadError):
            await _read(session, endpoint_document())

    async def test_a_server_error_is_retryable(self) -> None:
        # 501 is outside the transport's re-attempt set, so it is answered
        # once -- and the verdict still says a retry could clear it.
        session = FakeSession([FakeResponse(status=501, body={})])
        with pytest.raises(TransientReadError):
            await _read(session, endpoint_document())

    async def test_a_declared_category_rides_the_raised_error(self) -> None:
        # Without it the classification degrades to the type ladder and the
        # engine reports a re-derived code.
        session = FakeSession([FakeResponse(status=404, body={})])
        with pytest.raises(ReadError) as caught:
            await _read(
                session, endpoint_document(), error_map={"http": {"404": "config"}}
            )
        assert caught.value.declared_category == "config"

    async def test_a_declared_retryable_status_beats_the_built_in_rule(self) -> None:
        # A declared retryable status also joins the transport's own retry
        # set, so the attempts are spent before the verdict is reached.
        session = FakeSession([FakeResponse(status=400, body={}) for _ in range(3)])
        with pytest.raises(TransientReadError) as caught:
            await _read(
                session,
                endpoint_document(),
                error_map={"http": {"400": "rate_limited"}},
            )
        assert caught.value.declared_category == "rate_limited"

    async def test_a_records_ref_that_addresses_nothing_fails_loud(self) -> None:
        # Answering zero records would end the traversal at page one and
        # report a truncated read as a complete one.
        session = FakeSession([FakeResponse(body={"data": [{"id": 1}]})])
        with pytest.raises(ReadError, match="records.ref"):
            await _read(session, endpoint_document())

    async def test_a_missing_endpoint_document_fails_loud(self) -> None:
        connector = GenericAPIConnector()
        with pytest.raises(ReadError, match="endpoint_document"):
            async for _ in connector.read_batches(
                runtime_with(FakeSession()),
                {"stream_source": stream_source()},
                checkpoint=FakeCheckpoint(),
                stream_name="items",
            ):
                pass


@pytest.mark.asyncio
class TestDecimalPrecision:
    async def test_a_fractional_value_survives_into_arrow(self) -> None:
        document = endpoint_document()
        properties = document["operations"]["read"]["response"]["schema"]["properties"]
        properties["records"]["items"]["properties"]["amount"] = {
            "type": "number",
            "arrow_type": "Decimal128(18, 2)",
        }
        session = FakeSession(
            [FakeResponse(text='{"records": [{"id": 1, "name": "a", "amount": 1.50}]}')]
        )
        batches = await _read(session, document)
        assert batches[0].column("amount").to_pylist() == [Decimal("1.50")]

    async def test_a_fractional_body_value_goes_as_its_exact_string(self) -> None:
        # The read path used to narrow body decimals to float, defeating
        # the lossless parse that exists so a keyset key survives.
        document = endpoint_document(
            request={
                "method": "POST",
                "path": "/items",
                "body": {"after": {"from_param": "since"}},
            },
            params={
                "since": {
                    "in": "body",
                    "type": "number",
                    "required": False,
                    "controlled_by": "pagination",
                }
            },
            pagination={
                "type": "keyset",
                "keyset": {"param": "since", "order_by_field": "amount"},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        session = FakeSession(
            [
                # Raw text, so the trailing zero reaches the decoder the way
                # a provider sends it.
                FakeResponse(text='{"records": [{"id": 1, "amount": 1.50}]}'),
                FakeResponse(body=_rows(0)),
            ]
        )
        await _read(session, document)
        assert session.calls[1]["data"] == b'{"after":"1.50"}'


@pytest.mark.asyncio
class TestLifecycle:
    async def test_the_runtime_is_released_even_when_the_read_fails(self) -> None:
        session = FakeSession([FakeResponse(status=500, body={}) for _ in range(3)])
        runtime = runtime_with(session)
        connector = GenericAPIConnector()
        with pytest.raises(TransientReadError):
            async for _ in connector.read_batches(
                runtime,
                {
                    "endpoint_document": endpoint_document(),
                    "stream_source": stream_source(),
                },
                checkpoint=FakeCheckpoint(),
                stream_name="items",
            ):
                pass
        assert session.closed is True
        assert connector._connected is False
