"""The param table and the request built from it, for both roles."""

from __future__ import annotations

from typing import Any

import pytest

from cdk.api.request import (
    ParamTable,
    RequestBuilder,
    bind_request_values,
    build_write_body,
    request_block_problem,
    substitute_path,
)
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

    def test_a_function_form_default_resolves(self) -> None:
        # The whole expression grammar is in scope for a default, not just
        # literal/ref/template -- a function form once fell through and the
        # raw expression dict went on the wire.
        import base64

        table = ParamTable.for_read(
            {
                "auth": {
                    "in": "query",
                    "type": "string",
                    "required": True,
                    "default": {
                        "function": "base64_encode",
                        "input": {"ref": "connection.parameters.token"},
                    },
                }
            },
            Resolver(
                ResolutionContext(connection={"parameters": {"token": "tok-123"}}),
                functions=DEFAULT_FUNCTIONS,
            ),
        )
        assert table.values == {"auth": base64.b64encode(b"tok-123").decode("ascii")}

    def test_a_template_default_with_a_missing_placeholder_stays_partial(self) -> None:
        # Templates resolve leniently: the absent placeholder renders empty
        # and the partially-resolved value still goes out, which is not the
        # same rule as a bare ref omitting its param.
        table = ParamTable.for_read(
            {
                "scope": {
                    "in": "query",
                    "type": "string",
                    "required": False,
                    "default": {
                        "template": (
                            "${connection.parameters.org}/"
                            "${connection.parameters.gone}"
                        )
                    },
                }
            },
            Resolver(ResolutionContext(connection={"parameters": {"org": "acme"}})),
        )
        assert table.values == {"scope": "acme/"}

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
        prepared = builder.for_page({"q": "x", "payload": "y"})
        assert prepared.query == {"q": "x"}
        assert prepared.body is None

    def test_a_path_placed_param_stays_off_the_query_string(self) -> None:
        # The path substitution already sent it; repeating it as ?id=<value>
        # is a parameter the endpoint never described.
        table = ParamTable.for_read(
            {"id": {"in": "path", "type": "string", "required": True}},
            _resolver(),
        )
        builder = RequestBuilder(
            table, raw_body=None, resolver=_resolver(), endpoint="/items/{id}"
        )
        assert builder.for_page({"id": "abc"}).query == {}

    def test_a_header_placed_param_lands_in_the_headers(self) -> None:
        table = ParamTable.for_read(
            {"X-Tenant": {"in": "header", "type": "string", "required": True}},
            _resolver(),
        )
        builder = RequestBuilder(
            table, raw_body=None, resolver=_resolver(), endpoint="/items"
        )
        prepared = builder.for_page({"X-Tenant": "acme"})
        assert prepared.headers == {"X-Tenant": "acme"}
        assert prepared.query == {}

    def test_the_declared_query_map_lands_under_its_own_key(self) -> None:
        # request.query is not a rename map: the key is what goes on the
        # wire, and it need not be a declared param's name.
        table = ParamTable.for_read(
            {"limit": {"in": "query", "type": "integer", "required": False}},
            _resolver(),
        )
        builder = RequestBuilder(
            table,
            raw_body=None,
            resolver=_resolver(),
            endpoint="/items",
            declared_query={"page[limit]": {"from_param": "limit"}},
        )
        assert builder.for_page({"limit": 25}).query["page[limit]"] == 25

    def test_a_declared_header_binds_a_param(self) -> None:
        table = ParamTable.for_read(
            {"tenant": {"in": "query", "type": "string", "required": False}},
            _resolver(),
        )
        builder = RequestBuilder(
            table,
            raw_body=None,
            resolver=_resolver(),
            endpoint="/items",
            declared_headers={"X-Tenant": {"from_param": "tenant"}},
        )
        assert builder.for_page({"tenant": "acme"}).headers == {"X-Tenant": "acme"}

    def test_a_continuation_page_keeps_the_endpoint_headers_and_sends_no_body(
        self,
    ) -> None:
        # A next URL replaces the request, not the connection: the headers
        # say how this connection talks to the provider.
        table = ParamTable.for_read(
            {"tenant": {"in": "query", "type": "string", "required": False}},
            _resolver(),
            filters=[{"field": "tenant", "value": "acme"}],
        )
        builder = RequestBuilder(
            table,
            raw_body={"filter": {"literal": "x"}},
            resolver=_resolver(),
            endpoint="/items",
            declared_headers={"X-Tenant": {"from_param": "tenant"}},
        )
        prepared = builder.for_page({}, sends_declared_body=False)
        assert prepared.headers == {"X-Tenant": "acme"}
        assert prepared.body is None
        assert prepared.query == {}

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
        first = builder.for_page({"offset": 0}).body
        second = builder.for_page({"offset": 100}).body
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


class TestABindingKeyIsAName:
    """A key in a binding map names a header, a query param or a placeholder.

    Resolving the map as one value puts it in a value position, where the
    grammar's four expression markers are keywords. ``ref`` is a real query
    parameter name, so that mis-read breaks a working endpoint outright --
    and raises past the ``except ValueError`` both roles classify with.
    """

    @pytest.mark.parametrize("marker", ["ref", "template", "literal", "function"])
    def test_a_key_named_after_an_expression_marker_is_still_a_name(
        self, marker: str
    ) -> None:
        assert bind_request_values(
            {marker: {"literal": "main"}},
            params={},
            resolver=_resolver(),
            block="query",
            endpoint="/items",
        ) == {marker: "main"}

    def test_a_path_placeholder_may_be_named_ref(self) -> None:
        bound = bind_request_values(
            {"ref": {"literal": "main"}},
            params={},
            resolver=_resolver(),
            block="path_params",
            endpoint="/contents",
        )
        assert (
            substitute_path("/contents/{ref}", bound, endpoint="/contents")
            == "/contents/main"
        )

    def test_an_expression_that_resolves_to_nothing_still_omits_its_key(self) -> None:
        # The per-request policy survives the per-value resolution: an
        # unresolved expression drops its key rather than going out raw.
        assert (
            bind_request_values(
                {"tenant": {"ref": "connection.parameters.absent"}},
                params={},
                resolver=_resolver(),
                block="query",
                endpoint="/items",
            )
            == {}
        )


class TestRequestBlockRefusals:
    """One rule over the merged header map, and one over the path bindings."""

    def _table(self, **declared: Any) -> ParamTable:
        return ParamTable.for_read(declared, _resolver())

    def test_a_declared_header_the_connection_owns_is_refused(self) -> None:
        problem = request_block_problem(
            {"headers": {"Authorization": {"literal": "Bearer x"}}},
            reserved_headers=frozenset({"authorization"}),
        )
        assert problem is not None and "request.headers declares" in problem

    def test_a_header_placed_param_the_connection_owns_is_refused(self) -> None:
        # Byte-identical intent, so the same verdict -- and this is the route
        # that reaches the wire, because the params fold in after the
        # declared map.
        table = self._table(
            Authorization={
                "in": "header",
                "type": "string",
                "required": True,
                "default": {"literal": "Bearer attacker"},
            }
        )
        problem = request_block_problem(
            {"headers": {"X-Auth": {"from_param": "Authorization"}}},
            reserved_headers=frozenset({"authorization"}),
            header_params=table.header_params(),
        )
        assert problem is not None
        assert "Authorization" in problem and "in: header" in problem

    def test_a_header_placed_param_conflicting_on_content_type_is_refused(
        self,
    ) -> None:
        table = self._table(
            **{
                "Content-Type": {
                    "in": "header",
                    "type": "string",
                    "required": True,
                    "default": {"literal": "application/xml"},
                }
            }
        )
        problem = request_block_problem(
            {}, reserved_headers=frozenset(), header_params=table.header_params()
        )
        assert problem is not None and "application/json" in problem

    def test_a_header_placed_content_type_matching_the_engine_is_permitted(
        self,
    ) -> None:
        table = self._table(
            **{
                "Content-Type": {
                    "in": "header",
                    "type": "string",
                    "required": True,
                    "default": {"literal": "application/json"},
                }
            }
        )
        assert (
            request_block_problem(
                {}, reserved_headers=frozenset(), header_params=table.header_params()
            )
            is None
        )

    def test_a_path_binding_that_encodes_the_value_itself_is_refused(self) -> None:
        problem = request_block_problem(
            {
                "path": "/Contact/{id}",
                "path_params": {
                    "id": {"function": "url_encode", "input": {"from_param": "id"}}
                },
            },
            reserved_headers=frozenset(),
        )
        assert problem is not None and "url_encode" in problem

    def test_a_plain_path_binding_is_permitted(self) -> None:
        assert (
            request_block_problem(
                {
                    "path": "/Contact/{id}",
                    "path_params": {"id": {"from_param": "id"}},
                },
                reserved_headers=frozenset(),
            )
            is None
        )


class TestPathSubstitution:
    def test_a_placeholder_takes_its_bound_value(self) -> None:
        assert (
            substitute_path("/Contact/{id}", {"id": 42}, endpoint="items")
            == "/Contact/42"
        )

    def test_a_path_with_no_placeholder_is_untouched(self) -> None:
        assert substitute_path("/items", {}, endpoint="items") == "/items"

    def test_a_value_carrying_a_separator_is_encoded_as_one_segment(self) -> None:
        # The value crosses a trust boundary: unencoded, "a/b" adds a path
        # segment and "?x" starts a query the endpoint never declared.
        assert (
            substitute_path("/Contact/{id}", {"id": "a/b?c"}, endpoint="items")
            == "/Contact/a%2Fb%3Fc"
        )

    def test_an_unbound_placeholder_is_refused(self) -> None:
        # A URL with braces in it is answered 200 by many providers.
        with pytest.raises(ValueError, match=r"\{id\}"):
            substitute_path("/Contact/{id}", {}, endpoint="items")

    def test_a_placeholder_resolving_to_nothing_is_refused(self) -> None:
        with pytest.raises(ValueError, match=r"\{id\}"):
            substitute_path("/Contact/{id}", {"id": None}, endpoint="items")

    def test_a_placeholder_resolving_to_an_empty_string_is_refused(self) -> None:
        # "/Contact/" addresses the whole collection: a read fetches every
        # record instead of one, and a PUT or PATCH targets all of them.
        # url_encode answers "" for an unbound input, so this is reachable
        # without anyone declaring an empty value.
        with pytest.raises(ValueError, match=r"\{id\}"):
            substitute_path("/Contact/{id}", {"id": ""}, endpoint="items")


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
