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
from cdk.api.page_loop import PaginationStrategy
from cdk.api.read_setup import build_read_strategy
from cdk.api.request import ParamTable
from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.exceptions import ReadError, TransientReadError
from cdk.resolver import ResolutionContext, Resolver

from .fakes import (
    BASE_URL,
    FakeCheckpoint,
    FakeResponse,
    FakeSession,
    endpoint_document,
    runtime_with,
    sent_query,
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

#: The request block those params reach the wire through. The contract
#: requires every declared param to be referenced by exactly one binding,
#: in the map its ``in`` names, so a paginated read declares the query keys
#: as well -- the param name is the endpoint's internal handle and never
#: goes out on its own.
_PAGINATION_REQUEST = {
    "method": "GET",
    "path": "/items",
    "query": {"skip": {"from_param": "skip"}, "limit": {"from_param": "limit"}},
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
class TestTheRequestTheContractDescribes:
    """The three binding maps, which are the whole route to the wire.

    ``request.path_params``, ``request.headers`` and ``request.query`` are
    the only things that put a value on a request: the key is the wire name
    and the declared param behind it is the endpoint's internal handle. The
    read once joined the path with its ``{name}`` braces intact, never sent
    ``request.headers`` at all, and then -- having been given the maps --
    also emitted every param under its own name, sending each value twice.
    """

    async def test_a_path_placeholder_is_substituted_before_the_url_is_joined(
        self,
    ) -> None:
        session = FakeSession([FakeResponse(body=_rows(1))])
        await _read(
            session,
            endpoint_document(
                request={
                    "method": "GET",
                    "path": "/items/{id}",
                    "path_params": {"id": {"from_param": "id"}},
                },
                params={
                    "id": {
                        "in": "path",
                        "type": "string",
                        "required": True,
                        "default": {"literal": "a/b"},
                    }
                },
            ),
        )
        # Encoded as one segment: the value crosses a trust boundary, and a
        # slash in it would otherwise rewrite the URL's structure.
        assert session.calls[0]["url"] == f"{BASE_URL}/items/a%2Fb"
        assert sent_query(session.calls[0]) == {}

    async def test_a_declared_header_reaches_the_wire(self) -> None:
        session = FakeSession([FakeResponse(body=_rows(1))])
        await _read(
            session,
            endpoint_document(
                request={
                    "method": "GET",
                    "path": "/items",
                    "headers": {"X-Tenant": {"from_param": "tenant"}},
                },
                params={
                    "tenant": {
                        "in": "query",
                        "type": "string",
                        "required": False,
                        "default": {"literal": "acme"},
                    }
                },
            ),
        )
        assert session.calls[0]["headers"]["X-Tenant"] == "acme"

    async def test_a_header_derived_from_a_param_reaches_the_wire(self) -> None:
        # A header value is bound then resolved, in that order. Resolving
        # the raw declaration instead puts the binding node in the
        # function's input position and fails the read with "input must
        # resolve to a scalar; got dict" -- against a connector that works.
        session = FakeSession([FakeResponse(body=_rows(1))])
        await _read(
            session,
            endpoint_document(
                request={
                    "method": "GET",
                    "path": "/items",
                    "headers": {
                        "X-Format": {
                            "function": "lookup",
                            "input": {"from_param": "fmt"},
                            "map": {"json": "application/json"},
                        }
                    },
                },
                params={
                    "fmt": {
                        "in": "header",
                        "type": "string",
                        "required": False,
                        "default": {"literal": "json"},
                    }
                },
            ),
        )
        assert session.calls[0]["headers"]["X-Format"] == "application/json"

    async def test_a_stream_filter_naming_no_declared_param_is_refused(self) -> None:
        # The filter's value used to sit in the param table with nothing
        # bound to it: the read issued no filtered param at all and
        # returned the whole collection, reporting success. A filter that
        # does not filter is a correctness failure, so the stream fails
        # before its first request.
        session = FakeSession([FakeResponse(body=_rows(1))])
        with pytest.raises(ReadError, match="customer_number"):
            await _read(
                session,
                endpoint_document(
                    request={
                        "method": "GET",
                        "path": "/items",
                        "query": {"customerNumber": {"from_param": "cn"}},
                    },
                    params={"cn": {"in": "query", "type": "string", "required": False}},
                ),
                source=stream_source(
                    filters=[
                        {"field": "customer_number", "operator": "eq", "value": "C-1"}
                    ]
                ),
            )
        assert session.calls == []

    async def test_a_declared_header_shadowing_the_connection_is_refused(
        self,
    ) -> None:
        # request.headers is the whole header map an endpoint can declare,
        # so its keys are the names that reach the wire. A key the
        # connection's transport already sends can only shadow it -- here,
        # replace the connection's credential with the endpoint's.
        session = FakeSession()
        session.headers["Authorization"] = "Bearer connection"
        with pytest.raises(ReadError, match="request.headers declares"):
            await _read(
                session,
                endpoint_document(
                    request={
                        "method": "GET",
                        "path": "/items",
                        "headers": {"Authorization": {"from_param": "token"}},
                    },
                    params={
                        "token": {
                            "in": "header",
                            "type": "string",
                            "required": True,
                            "default": {"literal": "Bearer attacker"},
                        }
                    },
                ),
            )
        assert session.calls == []

    async def test_a_param_bound_under_a_harmless_key_is_not_refused(self) -> None:
        # The mirror image, and the reason the rule reads the key rather
        # than the param name: a param CALLED Authorization that lands
        # under X-Legacy-Auth shadows nothing, and refusing it would fail a
        # working endpoint.
        session = FakeSession([FakeResponse(body=_rows(1))])
        session.headers["Authorization"] = "Bearer connection"
        await _read(
            session,
            endpoint_document(
                request={
                    "method": "GET",
                    "path": "/items",
                    "headers": {"X-Legacy-Auth": {"from_param": "Authorization"}},
                },
                params={
                    "Authorization": {
                        "in": "header",
                        "type": "string",
                        "required": True,
                        "default": {"literal": "legacy"},
                    }
                },
            ),
        )
        assert session.calls[0]["headers"] == {"X-Legacy-Auth": "legacy"}

    async def test_a_query_key_named_ref_is_sent_as_a_parameter(self) -> None:
        # "ref" is a real query parameter name. Resolving the whole map as
        # one node reads the key as an expression marker, and the endpoint
        # fails with an error no caller classifies.
        session = FakeSession([FakeResponse(body=_rows(1))])
        await _read(
            session,
            endpoint_document(
                request={
                    "method": "GET",
                    "path": "/items",
                    "query": {"ref": {"literal": "main"}},
                }
            ),
        )
        assert sent_query(session.calls[0])["ref"] == "main"

    async def test_a_path_placeholder_binding_to_an_empty_value_is_refused(
        self,
    ) -> None:
        # "/items/" addresses the whole collection: the read would fetch
        # every record and report success.
        session = FakeSession()
        with pytest.raises(ReadError, match=r"\{id\}"):
            await _read(
                session,
                endpoint_document(
                    request={
                        "method": "GET",
                        "path": "/items/{id}",
                        "path_params": {"id": {"from_param": "id"}},
                    },
                    params={
                        "id": {
                            "in": "path",
                            "type": "string",
                            "required": True,
                            "default": {"literal": ""},
                        }
                    },
                ),
            )
        assert session.calls == []

    async def test_a_path_value_is_encoded_as_exactly_one_segment(self) -> None:
        # The engine's half of why `url_encode` in a path_params binding is
        # a contract error (RULE-ENDP-027): the engine already does this, so
        # a binding that encodes too would send a%252Fb. Pinned from the
        # wire, because it is the encoding -- not the refusal -- that is the
        # engine's to keep.
        session = FakeSession([FakeResponse(body=_rows(0))])
        await _read(
            session,
            endpoint_document(
                request={
                    "method": "GET",
                    "path": "/items/{id}",
                    "path_params": {"id": {"from_param": "id"}},
                },
                params={
                    "id": {
                        "in": "path",
                        "type": "string",
                        "required": True,
                        "default": {"literal": "a/b"},
                    }
                },
            ),
        )
        assert session.calls[0]["url"].endswith("/items/a%2Fb")

    async def test_a_read_removing_a_transport_header_is_refused(self) -> None:
        # The connection's defaults live on the shared session; nothing in
        # the request build can delete one, so it is refused rather than
        # silently ignored.
        session = FakeSession()
        with pytest.raises(ReadError, match="headers_remove"):
            await _read(
                session,
                endpoint_document(
                    request={
                        "method": "GET",
                        "path": "/items",
                        "headers_remove": ["Authorization"],
                    }
                ),
            )
        assert session.calls == []

    async def test_a_path_param_the_pagination_loop_owns_is_refused(self) -> None:
        # The path is substituted once per read, so page one's value would
        # be frozen into the URL and the read would fetch it forever.
        session = FakeSession()
        with pytest.raises(ReadError, match="pagination loop owns"):
            await _read(
                session,
                endpoint_document(
                    request={
                        "method": "GET",
                        "path": "/items/{skip}",
                        "path_params": {"skip": {"from_param": "skip"}},
                        "query": {"limit": {"from_param": "limit"}},
                    },
                    params={
                        **_PAGINATION_PARAMS,
                        "skip": {**_PAGINATION_PARAMS["skip"], "in": "path"},
                    },
                    pagination=_OFFSET,
                ),
            )
        assert session.calls == []

    async def test_a_path_param_the_replication_loop_owns_is_refused(self) -> None:
        # The same refusal as the pagination case, for the same reason: the
        # path is substituted once and the loop that owns the value has not
        # run. A stored cursor happens to fill this one in, which is what
        # makes it worth refusing -- the first run of the stream has no
        # cursor and a full-refresh run never asks for one, so the document
        # reads or fails depending on what is in the checkpoint. Refusing at
        # plan time gives every run the same answer, and names the real
        # problem instead of blaming a correct binding for an empty segment.
        session = FakeSession([FakeResponse(body=_rows(1))])
        with pytest.raises(ReadError, match="replication loop owns"):
            await _read(
                session,
                endpoint_document(
                    request={
                        "method": "GET",
                        "path": "/items/{since}",
                        "path_params": {"since": {"from_param": "since"}},
                    },
                    params={
                        "since": {
                            "in": "path",
                            "type": "string",
                            "required": True,
                            "controlled_by": "replication",
                        }
                    },
                    replication={
                        "supported_methods": ["full_refresh", "incremental"],
                        "cursor_mappings": [
                            {"cursor_field": "id", "param": "since", "operator": "gte"}
                        ],
                    },
                ),
                source=stream_source(
                    method="incremental", cursor_field="id", safety_window=60
                ),
                checkpoint=FakeCheckpoint({"cursor": "2026-07-31T12:00:00Z"}),
            )
        assert session.calls == []


#: One authoring defect per request block, each raising a different thing
#: out of the resolver: conflicting expression markers and an unknown
#: derived function raise ``TransportSpecError``, a scope that does not
#: exist raises ``KeyError``, a function handed the wrong type raises
#: ``TypeError``. The read used to catch ``ValueError`` at both the path
#: substitution and the request build, so three of the four escaped the
#: connector as raw builtins the worker classified by accident.
_DECLARATION_DEFECTS = {
    "a header with conflicting markers": {
        "headers": {"X-T": {"ref": "connection.parameters.a", "template": "b"}}
    },
    "a header reading a scope that does not exist": {
        "headers": {"X-T": {"ref": "nosuchscope.a"}}
    },
    "a query value calling an unregistered function": {
        "query": {"q": {"function": "nope", "input": {"literal": 1}}}
    },
    "a query value handing a function the wrong type": {
        "query": {"q": {"function": "base64_encode", "input": {"literal": 5}}}
    },
    "a path binding reading a scope that does not exist": {
        "path": "/items/{id}",
        "path_params": {"id": {"ref": "nosuchscope.id"}},
    },
    "a path binding calling an unregistered function": {
        "path": "/items/{id}",
        "path_params": {"id": {"function": "nope", "input": {"literal": 1}}},
    },
}

#: The same class one step earlier: a param's declared default is resolved
#: before any binding map is read, so a defect there escaped ahead of every
#: catch site the read has.
_DEFECTIVE_PARAM = {
    "tag": {
        "in": "query",
        "type": "string",
        "required": False,
        "default": {"ref": "nosuchscope.tag"},
    }
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect", sorted(_DECLARATION_DEFECTS), ids=sorted(_DECLARATION_DEFECTS)
)
class TestADeclarationDefectReachesTheCallerAsOneError:
    """Every way a declaration fails to resolve leaves the read as ReadError.

    The worker fails a stream deterministically on ``ReadError`` and would
    retry a bare ``KeyError`` or ``TypeError`` to exhaustion. Classifying
    at the resolution boundary is what makes one ``except`` at each caller
    correct; enumerating the resolver's exception types at each catch site
    instead left a different one uncaught at each site.
    """

    async def test_the_read_fails_with_a_read_error(self, defect: str) -> None:
        block = {"method": "GET", "path": "/items", **_DECLARATION_DEFECTS[defect]}
        session = FakeSession([FakeResponse(body=_rows(1))])
        with pytest.raises(ReadError):
            await _read(session, endpoint_document(request=block))

    async def test_nothing_reaches_the_provider(self, defect: str) -> None:
        # A request the endpoint could not describe must not go out at all.
        block = {"method": "GET", "path": "/items", **_DECLARATION_DEFECTS[defect]}
        session = FakeSession([FakeResponse(body=_rows(1))])
        with pytest.raises(ReadError):
            await _read(session, endpoint_document(request=block))
        assert session.calls == []


@pytest.mark.asyncio
class TestAParamDefaultIsADeclarationLikeAnyOther:
    async def test_a_defective_default_fails_the_read_with_a_read_error(self) -> None:
        session = FakeSession([FakeResponse(body=_rows(1))])
        with pytest.raises(ReadError):
            await _read(
                session,
                endpoint_document(
                    request={
                        "method": "GET",
                        "path": "/items",
                        "query": {"tag": {"from_param": "tag"}},
                    },
                    params=_DEFECTIVE_PARAM,
                ),
            )
        assert session.calls == []


@pytest.mark.asyncio
class TestPageSizeBinding:
    async def test_the_provider_cap_clamps_the_engines_batch_size(self) -> None:
        session = FakeSession(
            [FakeResponse(body=_rows(1)), FakeResponse(body=_rows(0))]
        )
        await _read(
            session,
            endpoint_document(
                pagination=_OFFSET,
                params=_PAGINATION_PARAMS,
                request=_PAGINATION_REQUEST,
            ),
            batch_size=100,
        )
        assert sent_query(session.calls[0])["limit"] == 25

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
            endpoint_document(
                pagination=pagination,
                params=_PAGINATION_PARAMS,
                request=_PAGINATION_REQUEST,
            ),
            batch_size=7,
        )
        assert sent_query(session.calls[0])["limit"] == 7

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
            endpoint_document(
                pagination=pagination,
                params=_PAGINATION_PARAMS,
                request=_PAGINATION_REQUEST,
            ),
            batch_size=100,
        )
        assert sent_query(session.calls[0])["limit"] == 5

    async def test_without_a_limit_block_no_page_size_is_sent(self) -> None:
        # The engine binds what the document declares. Guessing a page-size
        # param would send a name the provider never described.
        pagination = {key: value for key, value in _OFFSET.items() if key != "limit"}
        session = FakeSession(
            [FakeResponse(body=_rows(1)), FakeResponse(body=_rows(0))]
        )
        await _read(
            session,
            endpoint_document(
                pagination=pagination,
                params=_PAGINATION_PARAMS,
                request=_PAGINATION_REQUEST,
            ),
            batch_size=10,
        )
        assert "limit" not in sent_query(session.calls[0])

    async def test_a_page_size_that_is_not_a_positive_integer_fails_first(self) -> None:
        pagination = {
            **_OFFSET,
            "limit": {"param": "limit", "default": {"literal": 0}},
        }
        session = FakeSession()
        with pytest.raises(ReadError, match="limit.default"):
            await _read(
                session,
                endpoint_document(
                    pagination=pagination,
                    params=_PAGINATION_PARAMS,
                    request=_PAGINATION_REQUEST,
                ),
            )
        assert session.calls == []

    async def test_a_page_size_json_can_spell_but_python_cannot_narrow(self) -> None:
        """``1e400`` parses to infinity, and ``int()`` of it overflows.

        A number a JSON document can carry, so it is an authoring defect
        like any other: it has to leave as the read error the worker
        classifies as non-retryable, not as the raw ``OverflowError`` that
        would tear the read down unclassified.
        """
        pagination = {
            **_OFFSET,
            "limit": {"param": "limit", "default": {"literal": float("inf")}},
        }
        session = FakeSession()
        with pytest.raises(ReadError, match="limit.default"):
            await _read(
                session,
                endpoint_document(
                    pagination=pagination,
                    params=_PAGINATION_PARAMS,
                    request=_PAGINATION_REQUEST,
                ),
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
            session,
            endpoint_document(
                pagination=_OFFSET,
                params=_PAGINATION_PARAMS,
                request=_PAGINATION_REQUEST,
            ),
        )
        assert [sent_query(call)["skip"] for call in session.calls] == [0, 3, 5]
        assert [batch.num_rows for batch in batches] == [3, 2]

    async def test_a_short_page_does_not_end_the_traversal(self) -> None:
        # Providers return short pages for filtering and per-request caps;
        # stopping there silently truncated the read.
        session = FakeSession(
            [FakeResponse(body=_rows(1)), FakeResponse(body=_rows(0))]
        )
        batches = await _read(
            session,
            endpoint_document(
                pagination=_OFFSET,
                params=_PAGINATION_PARAMS,
                request=_PAGINATION_REQUEST,
            ),
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
            session,
            endpoint_document(
                pagination=pagination,
                params=_PAGINATION_PARAMS,
                request=_PAGINATION_REQUEST,
            ),
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
        with pytest.raises(ReadError, match="leaves its transport's origin"):
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
            request={
                "method": "GET",
                "path": "/items",
                "query": {"since": {"from_param": "since"}},
            },
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
        assert sent_query(session.calls[0])["since"] == "2026-07-31T11:59:00Z"

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
        assert "since" not in sent_query(session.calls[0])


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

    def _keyset_body_document(self) -> dict[str, Any]:
        return endpoint_document(
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

    async def test_a_keyset_key_goes_back_as_the_number_it_arrived_as(self) -> None:
        # The lossless parse makes it a Decimal; a body schema typing the
        # key as a number rejects a quoted string, so it goes as a number.
        session = FakeSession(
            [
                # Raw text, so the trailing zero reaches the decoder the way
                # a provider sends it.
                FakeResponse(text='{"records": [{"id": 1, "amount": 1.50}]}'),
                FakeResponse(body=_rows(0)),
            ]
        )
        await _read(session, self._keyset_body_document())
        assert session.calls[1]["data"] == b'{"after":1.5}'

    async def test_a_key_float_cannot_hold_is_refused_rather_than_rounded(self) -> None:
        # Rounding a continuation token silently moves the position the
        # next page resumes from, so records are skipped or repeated with
        # nothing to show for it. JSON has no wider number.
        session = FakeSession(
            [
                FakeResponse(
                    text='{"records": [{"id": 1, "amount": 1.2345678901234567890}]}'
                )
            ]
        )
        with pytest.raises(ReadError, match="without losing digits"):
            await _read(session, self._keyset_body_document())


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


@pytest.mark.asyncio
class TestAFollowedLinkReplacesTheWholeRequest:
    """The contract's link rule, which the params half alone does not express.

    A next URL carries the provider's own query and is meant to be followed
    verbatim. Suppressing only the params still rebuilds the declared body,
    which either re-sends a filter the link already applied or fails on a
    body whose expressions no longer resolve.
    """

    def _document(self) -> dict[str, Any]:
        return endpoint_document(
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.body.next"}},
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
            params={
                "since": {
                    "in": "body",
                    "type": "string",
                    "required": False,
                    "default": "2024-01-01",
                },
                "tenant": {
                    "in": "header",
                    "type": "string",
                    "required": False,
                    "default": {"literal": "acme"},
                },
            },
            request={
                "method": "POST",
                "path": "/items",
                "headers": {"X-Tenant": {"from_param": "tenant"}},
                "body": {"filter": {"from_param": "since"}},
            },
        )

    async def test_the_first_request_carries_the_declared_body(self) -> None:
        session = FakeSession([FakeResponse(body=_rows(0))])
        await _read(session, self._document())
        assert session.calls[0]["data"] == b'{"filter":"2024-01-01"}'

    async def test_a_followed_page_carries_no_body(self) -> None:
        session = FakeSession(
            [
                FakeResponse(body={**_rows(1), "next": "?page=2"}),
                FakeResponse(body=_rows(0)),
            ]
        )
        await _read(session, self._document())
        assert session.calls[1]["data"] is None
        assert sent_query(session.calls[1]) == {}
        # The next URL replaces the request, not the connection: a page-two
        # request that drops the endpoint's headers is a different request
        # from the one page one certified.
        assert session.calls[1]["headers"]["X-Tenant"] == "acme"


@pytest.mark.asyncio
class TestAPaginationValueKeepsItsJsonType:
    async def test_a_decimal_token_goes_into_a_body_as_a_number(self) -> None:
        # The lossless response parse turns a fractional token into a
        # Decimal. In a body it must go back as the number the provider
        # sent -- a body schema typing the field as a number rejects a
        # quoted string, and the whole stream dies after page one.
        document = endpoint_document(
            pagination={
                "type": "cursor",
                "cursor": {
                    "param": "after",
                    "next_cursor": {"ref": "response.body.next"},
                },
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
            params={
                "after": {
                    "in": "body",
                    "type": "number",
                    "required": False,
                    "controlled_by": "pagination",
                }
            },
            request={
                "method": "POST",
                "path": "/items",
                "body": {"after": {"from_param": "after"}},
            },
        )
        session = FakeSession(
            [
                # Raw text: the token has to reach the lossless decoder as a
                # JSON number, which is where the Decimal comes from.
                FakeResponse(
                    text='{"records": [{"id": 1, "name": "a"}], "next": 12.5}'
                ),
                FakeResponse(body=_rows(0)),
            ]
        )
        await _read(session, document)
        assert session.calls[1]["data"] == b'{"after":12.5}'


@pytest.mark.asyncio
class TestTheDeclaredMediaType:
    """``request.content_type`` selects the encoding AND the header sent."""

    @staticmethod
    def _document(**request_over: Any) -> dict[str, Any]:
        return endpoint_document(
            request={
                "method": "POST",
                "path": "/items",
                "body": {"grant_type": {"literal": "client_credentials"}},
                **request_over,
            }
        )

    async def test_a_body_defaults_to_json(self) -> None:
        session = FakeSession([FakeResponse(body=_rows(0))])
        await _read(session, self._document())
        assert session.calls[0]["data"] == b'{"grant_type":"client_credentials"}'
        assert session.calls[0]["headers"]["Content-Type"] == "application/json"

    async def test_a_form_endpoint_sends_form_bytes_under_its_own_header(self) -> None:
        # The field exists because this is common for REST POST bodies, and
        # before it the engine sent JSON regardless and the provider refused.
        session = FakeSession([FakeResponse(body=_rows(0))])
        await _read(
            session,
            self._document(content_type="application/x-www-form-urlencoded"),
        )
        assert session.calls[0]["data"] == b"grant_type=client_credentials"
        assert (
            session.calls[0]["headers"]["Content-Type"]
            == "application/x-www-form-urlencoded"
        )

    async def test_declared_parameters_reach_the_wire_verbatim(self) -> None:
        # The parameters describe the bytes, so they select nothing -- but
        # what the author declared is what is sent.
        session = FakeSession([FakeResponse(body=_rows(0))])
        await _read(
            session, self._document(content_type="application/json; charset=utf-8")
        )
        assert (
            session.calls[0]["headers"]["Content-Type"]
            == "application/json; charset=utf-8"
        )

    async def test_a_media_type_the_engine_cannot_encode_fails_before_sending(
        self,
    ) -> None:
        session = FakeSession()
        with pytest.raises(ReadError, match="cannot encode"):
            await _read(session, self._document(content_type="application/xml"))
        assert session.calls == []


class TestBuildingTheAdapterTouchesNothingItWasGiven:
    """The page size reaches the wire without rewriting the caller's table.

    ``RequestBuilder`` holds the same ``ParamTable`` object the adapter is
    built from, so a build that wrote the page size into it made the value
    depend on which of the two calls happened first. It rides the
    ``PageRequest`` instead, which is what the builder binds from anyway.
    """

    @staticmethod
    def _strategy(table: ParamTable) -> PaginationStrategy:
        return build_read_strategy(
            {
                "type": "offset",
                "offset": {"param": "offset", "initial": 0, "increment_by": 25},
                "limit": {"param": "limit", "default": {"literal": 25}},
            },
            table=table,
            resolver=Resolver(
                ResolutionContext(runtime={"batch_size": 100}),
                functions=DEFAULT_FUNCTIONS,
            ),
            url=f"{BASE_URL}/items",
            origin=BASE_URL,
            batch_size=100,
        )

    def test_the_page_size_is_on_the_first_request(self) -> None:
        first = self._strategy(ParamTable(values={"tenant": "acme"})).first()
        assert first.params == {"tenant": "acme", "offset": 0, "limit": 25}

    def test_the_page_size_is_not_written_into_the_callers_table(self) -> None:
        table = ParamTable(values={"tenant": "acme"})
        self._strategy(table)
        assert table.values == {"tenant": "acme"}
