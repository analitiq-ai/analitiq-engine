"""The param table and the request built from it, for both roles."""

from __future__ import annotations

from typing import Any

import pytest

from cdk.api.request import ParamTable, RequestBuilder, build_write_body
from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.resolver import ResolutionContext, Resolver

pytestmark = pytest.mark.unit


def _resolver(**runtime: Any) -> Resolver:
    return Resolver(
        ResolutionContext(
            connection={
                "parameters": {"profile": 42},
                "selections": {},
                "discovered": {},
            },
            runtime={"connection_id": "test-conn", **runtime},
        ),
        functions=DEFAULT_FUNCTIONS,
    )


class TestReadParamTable:
    def test_declared_defaults_resolve_through_the_grammar(self) -> None:
        table = ParamTable.for_read(
            {
                "profile": {
                    "in": "query",
                    "type": "integer",
                    "required": True,
                    "default": {"ref": "connection.parameters.profile"},
                }
            },
            _resolver(),
        )
        assert table.values == {"profile": 42}

    def test_a_controlled_param_is_left_to_its_loop(self) -> None:
        # A resolved default would be overwritten on the first page anyway
        # -- or survive as a stale value the loop never touched.
        table = ParamTable.for_read(
            {
                "limit": {
                    "in": "query",
                    "type": "integer",
                    "required": False,
                    "controlled_by": "pagination",
                    "default": {"literal": 5},
                }
            },
            _resolver(),
        )
        assert table.values == {}

    def test_the_placement_is_read_from_the_contract_key(self) -> None:
        # The raw key is "in"; the model spells it "location", and reading
        # that name finds nothing and puts every param in the query string.
        table = ParamTable.for_read(
            {"payload": {"in": "body", "type": "string", "required": False}},
            _resolver(),
        )
        assert table.placements == {"payload": "body"}

    def test_a_stream_filter_overrides_a_declared_default(self) -> None:
        table = ParamTable.for_read(
            {
                "status": {
                    "in": "query",
                    "type": "string",
                    "required": False,
                    "default": {"literal": "all"},
                }
            },
            _resolver(),
            filters=[{"field": "status", "value": "open"}],
        )
        assert table.values == {"status": "open"}

    def test_an_unresolved_default_omits_its_param(self, caplog) -> None:
        with caplog.at_level("WARNING"):
            table = ParamTable.for_read(
                {
                    "who": {
                        "in": "query",
                        "type": "string",
                        "required": False,
                        "default": {"ref": "connection.parameters.absent"},
                    }
                },
                _resolver(),
            )
        assert table.values == {}
        assert "parameter omitted" in caplog.text


class TestRequestBuilder:
    def test_body_placed_params_stay_out_of_the_query(self) -> None:
        table = ParamTable.for_read(
            {
                "q": {"in": "query", "type": "string", "required": False},
                "payload": {"in": "body", "type": "string", "required": False},
            },
            _resolver(),
        )
        builder = RequestBuilder(
            table, raw_body=None, resolver=_resolver(), endpoint="/items"
        )
        query, body = builder.for_page({"q": "x", "payload": "y"})
        assert query == {"q": "x"}
        assert body is None

    def test_the_body_binds_the_pages_own_param_values(self) -> None:
        # Built per page: a body-paginated endpoint must see what the loop
        # set, not the values frozen at the first request.
        table = ParamTable.for_read(
            {"offset": {"in": "body", "type": "integer", "required": False}},
            _resolver(),
        )
        builder = RequestBuilder(
            table,
            raw_body={"page": {"from_param": "offset"}},
            resolver=_resolver(),
            endpoint="/items",
        )
        _, first = builder.for_page({"offset": 0})
        _, second = builder.for_page({"offset": 100})
        assert (first, second) == ({"page": 0}, {"page": 100})

    def test_a_body_that_resolves_away_entirely_is_refused(self) -> None:
        # Reading with a null body sends a request the endpoint did not
        # describe; the read path never checked this before.
        builder = RequestBuilder(
            ParamTable(),
            raw_body={"ref": "connection.parameters.absent"},
            resolver=_resolver(),
            endpoint="/items",
        )
        with pytest.raises(ValueError, match="resolved to nothing"):
            builder.for_page({})


class TestWriteBody:
    def test_without_a_spec_the_record_is_the_body(self) -> None:
        record = {"id": 1}
        assert (
            build_write_body(
                body_spec=None,
                endpoint="/items",
                params={},
                resolver=_resolver(),
                record=record,
            )
            is record
        )

    def test_a_spec_binds_params_and_the_record(self) -> None:
        body = build_write_body(
            body_spec={
                "profile": {"from_param": "profile"},
                "item": {"from_input": "record"},
            },
            endpoint="/items",
            params={"profile": 42},
            resolver=_resolver(),
            record={"id": 1},
        )
        assert body == {"profile": 42, "item": {"id": 1}}

    def test_a_batched_spec_binds_the_whole_chunk(self) -> None:
        body = build_write_body(
            body_spec={"items": {"from_input": "records"}},
            endpoint="/items",
            params={},
            resolver=_resolver(),
            records=[{"id": 1}, {"id": 2}],
        )
        assert body == {"items": [{"id": 1}, {"id": 2}]}

    def test_a_spec_that_resolves_away_entirely_is_refused(self) -> None:
        with pytest.raises(ValueError, match="resolved to nothing"):
            build_write_body(
                body_spec={"ref": "connection.parameters.absent"},
                endpoint="/items",
                params={},
                resolver=_resolver(),
                record={"id": 1},
            )
