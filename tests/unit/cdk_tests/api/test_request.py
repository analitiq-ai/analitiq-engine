"""The param table and the request built from it, for both roles."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
from analitiq.contracts.endpoints import Pagination, Param, ReadRequest
from analitiq.contracts.stream import Filter
from pydantic import TypeAdapter

from cdk.api.exceptions import RequestSpecError
from cdk.api.request import (
    ParamTable,
    RequestBuilder,
    bind_query_and_headers,
    bind_request_values,
    build_write_body,
    request_block_problem,
    substitute_path,
)
from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.resolver import ResolutionContext, Resolver

pytestmark = pytest.mark.unit

#: Both are discriminated unions, so which branch a block is comes out of
#: the parse rather than out of a key the test sniffed for.
_READ_REQUEST: TypeAdapter[Any] = TypeAdapter(ReadRequest)
_PAGINATION: TypeAdapter[Any] = TypeAdapter(Pagination)


def _filters(*declared: Mapping[str, Any]) -> list[Filter]:
    """The stream's filters, parsed as the read path hands them over.

    Same reason as ``_params``: the read reads ``field``/``value`` off the
    contract model, so a test passing dicts would prove nothing about what
    the read does with a real stream's filters.
    """
    return [Filter.model_validate(f) for f in declared]


def _params(declared: Mapping[str, Any]) -> dict[str, Param]:
    """The operation's declared params, parsed as the caller receives them.

    Parsed rather than handed over as dicts because that is the difference
    the whole module turns on: a param's placement is ``location`` on the
    model and ``in`` on the wire, and a test writing the wire name against
    code reading the attribute would agree with itself and with nothing
    else.
    """
    return {name: Param.model_validate(spec) for name, spec in declared.items()}


def _request(block: Mapping[str, Any]) -> Any:
    """One declared request block, parsed as an operation's is.

    ``method`` and ``path`` are required of every request the contract
    accepts, and defaulted here so each test writes only the binding map it
    is about. A block carrying a body names its own method: only the POST
    branch of the read request declares one.
    """
    return _READ_REQUEST.validate_python({"method": "GET", "path": "/items", **block})


def _pagination(block: Mapping[str, Any]) -> Any:
    """One declared pagination block, parsed as a read operation's is."""
    return _PAGINATION.validate_python(block)


def _resolver(**runtime: Any) -> Resolver:
    return Resolver(
        ResolutionContext(
            connection={
                "parameters": {
                    "profile": 42,
                    "media_type": "application/json",
                    "other_type": "xml",
                },
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
            _params(
                {
                    "profile": {
                        "in": "query",
                        "type": "integer",
                        "required": True,
                        "default": {"ref": "connection.parameters.profile"},
                    }
                }
            ),
            _resolver(),
            endpoint="/items",
        )
        assert table.values == {"profile": 42}

    def test_a_function_form_default_resolves(self) -> None:
        # The whole expression grammar is in scope for a default, not just
        # literal/ref/template -- a function form once fell through and the
        # raw expression dict went on the wire.
        import base64

        table = ParamTable.for_read(
            _params(
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
                }
            ),
            Resolver(
                ResolutionContext(connection={"parameters": {"token": "tok-123"}}),
                functions=DEFAULT_FUNCTIONS,
            ),
            endpoint="/items",
        )
        assert table.values == {"auth": base64.b64encode(b"tok-123").decode("ascii")}

    def test_a_template_default_with_a_missing_placeholder_stays_partial(self) -> None:
        # Templates resolve leniently: the absent placeholder renders empty
        # and the partially-resolved value still goes out, which is not the
        # same rule as a bare ref omitting its param.
        table = ParamTable.for_read(
            _params(
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
                }
            ),
            Resolver(ResolutionContext(connection={"parameters": {"org": "acme"}})),
            endpoint="/items",
        )
        assert table.values == {"scope": "acme/"}

    def test_a_controlled_param_is_left_to_its_loop(self) -> None:
        # A resolved default would be overwritten on the first page anyway
        # -- or survive as a stale value the loop never touched.
        table = ParamTable.for_read(
            _params(
                {
                    "limit": {
                        "in": "query",
                        "type": "integer",
                        "required": False,
                        "controlled_by": "pagination",
                        "default": {"literal": 5},
                    }
                }
            ),
            _resolver(),
            endpoint="/items",
        )
        assert table.values == {}

    def test_a_stream_filter_overrides_a_declared_default(self) -> None:
        table = ParamTable.for_read(
            _params(
                {
                    "status": {
                        "in": "query",
                        "type": "string",
                        "required": False,
                        "operators": ["eq"],
                        "default": {"literal": "all"},
                    }
                }
            ),
            _resolver(),
            filters=_filters({"field": "status", "operator": "eq", "value": "open"}),
            endpoint="/items",
        )
        assert table.values == {"status": "open"}

    def test_a_filter_the_param_does_not_declare_the_operator_for_is_refused(
        self,
    ) -> None:
        # The operator has no rendering on this transport: the value goes
        # out bound to a query key either way, so `gt` and `lt` built the
        # identical request and one of the two streams was always wrong.
        # The endpoint declares what comparing the param means; the stream
        # is held to it.
        with pytest.raises(RequestSpecError, match="'gt'"):
            ParamTable.for_read(
                _params(
                    {
                        "amount": {
                            "in": "query",
                            "type": "number",
                            "required": False,
                            "operators": ["eq"],
                        }
                    }
                ),
                _resolver(),
                filters=_filters({"field": "amount", "operator": "gt", "value": 100}),
                endpoint="/items",
            )

    def test_a_filter_on_a_param_declaring_no_operators_is_refused(self) -> None:
        # Absence of `operators` is how the endpoint says the param is not
        # stream-filterable, not an empty permission to send anything.
        with pytest.raises(RequestSpecError, match="not stream-filterable"):
            ParamTable.for_read(
                _params(
                    {"region": {"in": "query", "type": "string", "required": False}}
                ),
                _resolver(),
                filters=_filters({"field": "region", "operator": "eq", "value": "eu"}),
                endpoint="/items",
            )

    def test_a_filter_on_a_loop_owned_param_is_refused(self) -> None:
        # The contract forbids a `controlled_by` param from declaring
        # `operators`, so a filter aimed at one the pagination loop owns
        # lands on the same refusal instead of being overwritten on page
        # one -- a narrowing the stream declared and never got.
        with pytest.raises(RequestSpecError, match="not stream-filterable"):
            ParamTable.for_read(
                _params(
                    {
                        "cursor": {
                            "in": "query",
                            "type": "string",
                            "required": False,
                            "controlled_by": "pagination",
                        }
                    }
                ),
                _resolver(),
                filters=_filters({"field": "cursor", "operator": "eq", "value": "abc"}),
                endpoint="/items",
            )

    def test_a_filter_carrying_no_value_is_refused(self) -> None:
        # The contract cannot tell an omitted `value` from an explicit null
        # for a non-unary operator, so this shape validates. Dropping it
        # silently is the whole-collection read the refusals above exist
        # to prevent.
        with pytest.raises(RequestSpecError, match="no value"):
            ParamTable.for_read(
                _params(
                    {
                        "status": {
                            "in": "query",
                            "type": "string",
                            "required": False,
                            "operators": ["eq"],
                        }
                    }
                ),
                _resolver(),
                filters=_filters({"field": "status", "operator": "eq"}),
                endpoint="/items",
            )

    def test_a_filter_naming_no_declared_param_is_refused(self) -> None:
        # A filter reaches the provider as a declared param bound by a
        # request map. One naming no param narrows nothing: the value sat
        # in the table, no binding read it, and the stream read the whole
        # collection while reporting success.
        with pytest.raises(RequestSpecError, match="customer_number"):
            ParamTable.for_read(
                _params({"cn": {"in": "query", "type": "string", "required": False}}),
                _resolver(),
                filters=_filters(
                    {"field": "customer_number", "operator": "eq", "value": "C-1"}
                ),
                endpoint="/items",
            )

    def test_an_unresolved_default_omits_its_param(self, caplog) -> None:
        with caplog.at_level("WARNING"):
            table = ParamTable.for_read(
                _params(
                    {
                        "who": {
                            "in": "query",
                            "type": "string",
                            "required": False,
                            "default": {"ref": "connection.parameters.absent"},
                        }
                    }
                ),
                _resolver(),
                endpoint="/items",
            )
        assert table.values == {}
        assert "parameter omitted" in caplog.text


class TestRequestBuilder:
    def test_a_declared_param_reaches_the_wire_only_through_its_binding(self) -> None:
        # The contract requires every declared param to be referenced by
        # exactly one binding, in the map its ``in`` names, so the param
        # name is the endpoint's internal handle and the binding key is the
        # wire name. Emitting the param name as well sent every value
        # twice, the second time under a name the provider never declared
        # -- which for a secret-valued param put a credential on the query
        # string.
        table = ParamTable.for_read(
            _params(
                {
                    "api_key": {
                        "in": "query",
                        "type": "string",
                        "required": True,
                        "default": {"literal": "s3cret"},
                    },
                    "tenant": {"in": "header", "type": "string", "required": True},
                    "id": {"in": "path", "type": "string", "required": True},
                    "payload": {"in": "body", "type": "string", "required": False},
                }
            ),
            _resolver(),
            endpoint="/items",
        )
        builder = RequestBuilder(
            table,
            raw_body={"data": {"from_param": "payload"}},
            resolver=_resolver(),
            endpoint="/items/{id}",
            declared_query={"key": {"from_param": "api_key"}},
            declared_headers={"X-Tenant": {"from_param": "tenant"}},
        )
        prepared = builder.for_page({"tenant": "acme", "id": "abc", "payload": "y"})
        assert prepared.query == {"key": "s3cret"}
        assert prepared.headers == {"X-Tenant": "acme"}
        assert prepared.body == {"data": "y"}

    def test_the_declared_query_map_lands_under_its_own_key(self) -> None:
        # request.query is not a rename map: the key is what goes on the
        # wire, and it need not be a declared param's name.
        table = ParamTable.for_read(
            _params({"limit": {"in": "query", "type": "integer", "required": False}}),
            _resolver(),
            endpoint="/items",
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
            _params({"tenant": {"in": "query", "type": "string", "required": False}}),
            _resolver(),
            endpoint="/items",
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
            _params(
                {
                    "tenant": {
                        "in": "query",
                        "type": "string",
                        "required": False,
                        "operators": ["eq"],
                    }
                }
            ),
            _resolver(),
            filters=_filters({"field": "tenant", "operator": "eq", "value": "acme"}),
            endpoint="/items",
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

    def test_a_required_param_that_resolved_to_nothing_refuses_the_page(
        self,
    ) -> None:
        # The page would otherwise go out without it and the provider would
        # answer the collection this param was narrowing -- a 200 the loop
        # accepts and commits.
        table = ParamTable.for_read(
            _params({"since": {"in": "query", "type": "string", "required": True}}),
            _resolver(),
            endpoint="/items",
        )
        builder = RequestBuilder(
            table,
            raw_body=None,
            resolver=_resolver(),
            endpoint="/items",
            declared_query={"since": {"from_param": "since"}},
        )
        with pytest.raises(RequestSpecError, match="declared required"):
            builder.for_page({})

    def test_a_loop_owned_required_param_does_not_refuse_page_one(self) -> None:
        # Page one of a cursor scheme carries no cursor by construction. The
        # loop owns when its param is in flight, so holding the author's
        # `required` against it would refuse the first page of every correct
        # document.
        table = ParamTable.for_read(
            _params(
                {
                    "page_token": {
                        "in": "query",
                        "type": "string",
                        "required": True,
                        "controlled_by": "pagination",
                    }
                }
            ),
            _resolver(),
            endpoint="/items",
        )
        builder = RequestBuilder(
            table,
            raw_body=None,
            resolver=_resolver(),
            endpoint="/items",
            declared_query={"token": {"from_param": "page_token"}},
        )
        assert builder.for_page({}).query == {}
        assert builder.for_page({"page_token": "t-2"}).query == {"token": "t-2"}

    def test_a_page_value_outside_its_declared_range_refuses_the_page(self) -> None:
        # The check runs per page because this is the only point at which the
        # loop's own values exist: a table-build check would judge the
        # defaults and let the paging limit through unjudged.
        table = ParamTable.for_read(
            _params(
                {
                    "limit": {
                        "in": "query",
                        "type": "integer",
                        "required": False,
                        "controlled_by": "pagination",
                        "maximum": 100,
                    }
                }
            ),
            _resolver(),
            endpoint="/items",
        )
        builder = RequestBuilder(
            table,
            raw_body=None,
            resolver=_resolver(),
            endpoint="/items",
            declared_query={"page[limit]": {"from_param": "limit"}},
        )
        assert builder.for_page({"limit": 100}).query == {"page[limit]": 100}
        with pytest.raises(RequestSpecError, match="maximum=100"):
            builder.for_page({"limit": 500})

    def test_a_continuation_carrying_params_still_sends_no_query(self) -> None:
        # The continuation URL carries its own query, and a param reaches
        # the wire only through a binding that names it. A page's params
        # going onto the query string raw would be a second sink with its
        # own spelling rules: internal names, no bindings, no refusals.
        table = ParamTable.for_read(
            _params({"limit": {"in": "query", "type": "integer", "required": False}}),
            _resolver(),
            endpoint="/items",
        )
        builder = RequestBuilder(
            table,
            raw_body=None,
            resolver=_resolver(),
            endpoint="/items",
            declared_query={"page[limit]": {"from_param": "limit"}},
        )
        prepared = builder.for_page({"limit": 25}, sends_declared_body=False)
        assert prepared.query == {}

    def test_the_body_binds_the_pages_own_param_values(self) -> None:
        # Built per page: a body-paginated endpoint must see what the loop
        # set, not the values frozen at the first request.
        table = ParamTable.for_read(
            _params({"offset": {"in": "body", "type": "integer", "required": False}}),
            _resolver(),
            endpoint="/items",
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
        with pytest.raises(RequestSpecError, match="resolved to nothing"):
            builder.for_page({})


class TestABindingKeyIsAName:
    """A key in a binding map names a header, a query param or a placeholder.

    Resolving the map as one value puts it in a value position, where the
    grammar's four expression markers are keywords. ``ref`` is a real query
    parameter name, so that mis-read breaks a working endpoint outright.
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

    def test_a_dropped_key_is_warned_about_once_and_by_name(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # An optional param nothing filled in is the ordinary case, not a
        # defect: the key is dropped and a line says which. Resolving each
        # value on its own put every one of them in a top-level position,
        # where the resolver logged a second line naming nothing -- so a
        # read with six optional params paged a thousand times wrote 12,000
        # lines about a connector behaving exactly as authored.
        with caplog.at_level("WARNING"):
            bind_request_values(
                {
                    "tenant": {"from_param": "tenant_id"},
                    "region": {"from_param": "region_code"},
                },
                params={},
                resolver=_resolver(),
                block="query",
                endpoint="/items",
            )
        messages = [
            record.getMessage()
            for record in caplog.records
            if record.levelname == "WARNING"
        ]
        assert len(messages) == 2
        assert all("dropping" in message for message in messages)
        assert any("'tenant'" in message for message in messages)
        assert any("'region'" in message for message in messages)


class TestEndpointHeadersAreWireSafe:
    """An endpoint's headers take a different route to the wire than the
    transport's, and the HTTP client judges both the same way."""

    def test_a_value_carrying_a_line_break_is_refused(self) -> None:
        with pytest.raises(RequestSpecError, match="no HTTP client will send"):
            bind_query_and_headers(
                params={},
                declared_query=None,
                declared_headers={"X-Trace": {"literal": "a\r\nInjected: b"}},
                resolver=_resolver(),
                endpoint="/items",
            )

    def test_a_name_ending_in_a_line_break_is_refused(self) -> None:
        # `$` in a Python regex also matches before a trailing newline, so a
        # `match()` anchor passed exactly the request-splitting shape this
        # refusal exists for.
        with pytest.raises(RequestSpecError, match="not an HTTP token"):
            bind_query_and_headers(
                params={},
                declared_query=None,
                declared_headers={"X-Trace\n": {"literal": "x"}},
                resolver=_resolver(),
                endpoint="/items",
            )

    def test_a_name_that_is_not_a_token_is_refused(self) -> None:
        with pytest.raises(RequestSpecError, match="not an HTTP token"):
            bind_query_and_headers(
                params={},
                declared_query=None,
                declared_headers={"Bad Name": {"literal": "x"}},
                resolver=_resolver(),
                endpoint="/items",
            )

    @pytest.mark.parametrize("char", ["\x0b", "\x1f", "\x7f"])
    def test_any_forbidden_control_character_is_refused(self, char: str) -> None:
        # Not just CR/LF/NUL: the client rejects the whole control range
        # except horizontal tab when it serialises the request.
        with pytest.raises(RequestSpecError, match="no HTTP client will send"):
            bind_query_and_headers(
                params={},
                declared_query=None,
                declared_headers={"X-Trace": {"literal": f"a{char}b"}},
                resolver=_resolver(),
                endpoint="/items",
            )

    def test_horizontal_tab_stays_legal_in_a_value(self) -> None:
        # Tab IS legal whitespace in a field value; refusing it would fail a
        # connector the client sends fine.
        _, headers = bind_query_and_headers(
            params={},
            declared_query=None,
            declared_headers={"X-Trace": {"literal": "a\tb"}},
            resolver=_resolver(),
            endpoint="/items",
        )
        assert headers == {"X-Trace": "a\tb"}

    def test_an_ordinary_header_still_passes(self) -> None:
        _, headers = bind_query_and_headers(
            params={},
            declared_query=None,
            declared_headers={"X-Trace": {"literal": "abc"}},
            resolver=_resolver(),
            endpoint="/items",
        )
        assert headers == {"X-Trace": "abc"}


class TestSendableValues:
    """What a bound value may be by the time it reaches the wire.

    One sink normalizes every bound request value, so the JSON spelling of
    a boolean, the refusal of a declared null and the refusal of a raw
    container hold identically for headers, query and path bindings.
    """

    def test_a_json_boolean_goes_out_in_its_json_spelling(self) -> None:
        bound = bind_request_values(
            {"include_archived": False, "active": True},
            params={},
            resolver=_resolver(),
            block="query",
            endpoint="/items",
        )
        assert bound == {"include_archived": "false", "active": "true"}

    def test_a_bare_declared_null_is_refused_loud(self) -> None:
        # Static, so it names a key nothing can ever send on any
        # connection -- unlike an expression resolving to nothing, which
        # is a per-connection fact the omit rule drops.
        with pytest.raises(RequestSpecError, match="declares null"):
            bind_request_values(
                {"cursor": None},
                params={},
                resolver=_resolver(),
                block="query",
                endpoint="/items",
            )

    def test_an_expression_resolving_to_nothing_is_still_omitted(self) -> None:
        # The distinction the null refusal must not swallow: a literal
        # null IS an expression resolving to nothing, and keeps the
        # per-request omit behavior.
        assert (
            bind_request_values(
                {"cursor": {"literal": None}},
                params={},
                resolver=_resolver(),
                block="query",
                endpoint="/items",
            )
            == {}
        )

    def test_a_container_value_is_refused_loud(self) -> None:
        with pytest.raises(RequestSpecError, match="must be a scalar"):
            bind_request_values(
                {"filter": {"literal": {"status": "open"}}},
                params={},
                resolver=_resolver(),
                block="query",
                endpoint="/items",
            )


class TestNeverFillableScopeRefusals:
    """A secrets/auth read anywhere in the operation is refused at the boundary.

    Request-time resolution omits an unresolved value rather than failing,
    so without this refusal a param default or pagination value reading
    ``secrets.*`` vanishes from every request with only a log line -- run
    green, credential-less requests.
    """

    def test_a_param_default_reading_a_secret_is_refused(self) -> None:
        problem = request_block_problem(
            _request({"query": {"key": {"from_param": "api_key"}}}),
            endpoint="items",
            reserved_headers=frozenset(),
            resolver=_resolver(),
            declared_params=_params(
                {
                    "api_key": {
                        "in": "query",
                        "type": "string",
                        "required": True,
                        "default": {"ref": "secrets.api_key"},
                    }
                }
            ),
        )
        assert problem is not None
        assert "'secrets.api_key'" in problem
        assert "request-time resolution never supplies" in problem

    def test_a_pagination_value_reading_a_secret_is_refused(self) -> None:
        problem = request_block_problem(
            _request({}),
            endpoint="items",
            reserved_headers=frozenset(),
            resolver=_resolver(),
            pagination=_pagination(
                {
                    "type": "offset",
                    "offset": {"param": "skip", "initial": 0, "increment_by": 1},
                    "limit": {
                        "param": "limit",
                        "default": {"ref": "secrets.page_size"},
                    },
                    "stop_when": {"empty": {"ref": "response.body.records"}},
                }
            ),
        )
        assert problem is not None
        assert "'secrets.page_size'" in problem

    @pytest.mark.parametrize(
        "block",
        [
            {
                "type": "offset",
                "offset": {"param": "skip", "initial": 0, "increment_by": 1},
                "limit": {"param": "limit", "default": {"ref": "response.body.size"}},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
            {
                "type": "page",
                "page": {
                    "param": "page",
                    "initial": 1,
                    "increment_by": {"ref": "response.body.size"},
                },
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        ],
        ids=["limit.default", "page.increment_by"],
    )
    def test_a_pre_page_value_reading_the_response_is_refused(self, block) -> None:
        """The pagination block is the one place ``response.*`` is fillable.

        Both PRE_PAGE_VALUE_PATHS entries are the exception: they resolve
        before a page exists, so a response read there resolves to nothing
        on every run. ``resolve_page_size`` answers that by warning and
        taking the engine's batch size, and ``_Page`` by keeping a stride
        nobody authored -- green runs paginating at a size the document did
        not ask for.
        """
        problem = request_block_problem(
            _request({}),
            endpoint="items",
            reserved_headers=frozenset(),
            resolver=_resolver(),
            pagination=_pagination(block),
        )
        assert problem is not None
        assert "'response.body.size'" in problem

    def test_a_per_page_value_reading_the_response_is_not_refused(self) -> None:
        # The contrast that makes the entries above mean something: the
        # same expression one field over is resolved against the page just
        # served, so the page loop supplies it.
        assert (
            request_block_problem(
                _request({}),
                endpoint="items",
                reserved_headers=frozenset(),
                resolver=_resolver(),
                pagination=_pagination(
                    {
                        "type": "offset",
                        "offset": {
                            "param": "skip",
                            "initial": 0,
                            "increment_by": {"ref": "response.body.size"},
                        },
                        "stop_when": {"empty": {"ref": "response.body.records"}},
                    }
                ),
            )
            is None
        )

    @pytest.mark.parametrize("subtree", ["parameters", "selections", "discovered"])
    def test_a_subtree_root_is_supplied_not_merely_its_children(
        self, subtree: str
    ) -> None:
        """``request_resolver`` puts each subtree in scope as a whole mapping.

        A prefix test alone answers no for the root it is a prefix of --
        ``connection.parameters`` does not start with
        ``connection.parameters.`` -- so a body that IS the subtree was
        reported as reading something no run supplies, while the resolver
        hands it over and the serializer sends it.
        """
        assert (
            request_block_problem(
                _request(
                    {
                        "method": "POST",
                        "body": {"ref": f"connection.{subtree}"},
                    }
                ),
                endpoint="items",
                reserved_headers=frozenset(),
                resolver=_resolver(),
            )
            is None
        )

    def test_a_subtree_root_in_a_scalar_slot_is_still_refused_where_it_lands(
        self,
    ) -> None:
        """Supplied is not the same question as sendable.

        The phase guard says the resolver fills it; the request build says a
        query value must be a scalar. Two rules, one for each question --
        conflating them is what made the guard lie about supply.
        """
        assert (
            request_block_problem(
                _request({"query": {"q": {"ref": "connection.parameters"}}}),
                endpoint="items",
                reserved_headers=frozenset(),
                resolver=_resolver(),
            )
            is None
        )
        with pytest.raises(RequestSpecError, match="must be a scalar"):
            bind_query_and_headers(
                params={},
                declared_query={"q": {"ref": "connection.parameters"}},
                declared_headers=None,
                resolver=_resolver(),
                endpoint="/items",
            )

    @pytest.mark.parametrize("wire_key", ["ref", "template", "literal", "function"])
    def test_a_wire_key_named_after_a_marker_cannot_hide_its_siblings(
        self, wire_key: str
    ) -> None:
        """The map's KEYS are wire names; only its values are expressions.

        Scanning the whole map puts the map itself in a value position, so
        a query parameter genuinely named ``ref`` reads as an expression
        marker and the scan answers about that key alone -- never seeing
        the ``api_key`` beside it. Request-time resolution then drops the
        credential silently, on every request, which is the exact failure
        this guard exists to prevent.
        """
        problem = request_block_problem(
            _request(
                {
                    "query": {
                        wire_key: {"literal": "v"},
                        "api_key": {"ref": "secrets.api_key"},
                    }
                }
            ),
            endpoint="items",
            reserved_headers=frozenset(),
            resolver=_resolver(),
        )
        assert problem is not None
        assert "'secrets.api_key'" in problem

    def test_a_param_named_after_a_marker_cannot_hide_another_default(self) -> None:
        """Same shadowing, one map over: params are keyed by author names too."""
        problem = request_block_problem(
            _request({"query": {"k": {"from_param": "api_key"}}}),
            endpoint="items",
            reserved_headers=frozenset(),
            resolver=_resolver(),
            declared_params=_params(
                {
                    "ref": {"in": "query", "type": "string", "required": False},
                    "api_key": {
                        "in": "query",
                        "type": "string",
                        "required": True,
                        "default": {"ref": "secrets.api_key"},
                    },
                }
            ),
        )
        assert problem is not None
        assert "'secrets.api_key'" in problem

    def test_a_runtime_key_typo_is_refused_not_prefix_matched(self) -> None:
        # The engine passes exactly connection_id and batch_size; a bare
        # `runtime.` prefix match would give `runtime.batchsize` the
        # warn-and-omit fate this walk exists to refuse.
        problem = request_block_problem(
            _request({"query": {"limit": {"ref": "runtime.batchsize"}}}),
            endpoint="items",
            reserved_headers=frozenset(),
            resolver=_resolver(),
        )
        assert problem is not None
        assert "'runtime.batchsize'" in problem

    def test_the_supplied_runtime_keys_are_not_refused(self) -> None:
        # The supplied set is the RESOLVER's, per phase: this one carries
        # batch_size the way the read role's does.
        assert (
            request_block_problem(
                _request({"query": {"limit": {"ref": "runtime.batch_size"}}}),
                endpoint="items",
                reserved_headers=frozenset(),
                resolver=_resolver(batch_size=37),
            )
            is None
        )

    def test_a_runtime_key_another_phase_supplies_is_refused(self) -> None:
        # The same read against a resolver built WITHOUT batch_size -- the
        # write role's shape -- is a value that never arrives on this phase.
        problem = request_block_problem(
            _request({"query": {"limit": {"ref": "runtime.batch_size"}}}),
            endpoint="items",
            reserved_headers=frozenset(),
            resolver=_resolver(),
        )
        assert problem is not None
        assert "'runtime.batch_size'" in problem

    def test_a_connection_read_in_a_param_default_is_not_refused(self) -> None:
        # The refusal is about the phase, not about deferral in general:
        # request-time resolution DOES supply the connection document.
        assert (
            request_block_problem(
                _request({"query": {"key": {"from_param": "api_key"}}}),
                endpoint="items",
                reserved_headers=frozenset(),
                resolver=_resolver(),
                declared_params=_params(
                    {
                        "api_key": {
                            "in": "query",
                            "type": "string",
                            "required": True,
                            "default": {"ref": "connection.parameters.api_key"},
                        }
                    }
                ),
            )
            is None
        )


class TestRequestBlockRefusals:
    """One rule over the declared header map, and one over the path bindings."""

    def test_a_declared_header_the_connection_owns_is_refused(self) -> None:
        problem = request_block_problem(
            _request({"headers": {"Authorization": {"literal": "Bearer x"}}}),
            endpoint="items",
            reserved_headers=frozenset({"authorization"}),
            resolver=_resolver(),
        )
        assert problem is not None and "request.headers declares" in problem

    def test_a_param_bound_under_a_permitted_key_is_permitted(self) -> None:
        # The rule judges the WIRE names, which are the keys of
        # request.headers. A param named 'Authorization' bound to
        # 'X-Legacy-Auth' sends only 'X-Legacy-Auth', so refusing it would
        # refuse an endpoint that shadows nothing.
        assert (
            request_block_problem(
                _request(
                    {"headers": {"X-Legacy-Auth": {"from_param": "Authorization"}}}
                ),
                endpoint="items",
                reserved_headers=frozenset({"authorization"}),
                resolver=_resolver(),
            )
            is None
        )

    def test_a_param_bound_under_a_reserved_key_is_refused(self) -> None:
        # The mirror image: an innocuous param name under a reserved key
        # still shadows the connection's header, because the key is what
        # the provider sees.
        problem = request_block_problem(
            _request({"headers": {"Authorization": {"from_param": "tok"}}}),
            endpoint="items",
            reserved_headers=frozenset({"authorization"}),
            resolver=_resolver(),
        )
        assert problem is not None and "Authorization" in problem

    def test_a_plain_path_binding_is_permitted(self) -> None:
        assert (
            request_block_problem(
                _request(
                    {
                        "path": "/Contact/{id}",
                        "path_params": {"id": {"from_param": "id"}},
                    }
                ),
                endpoint="items",
                reserved_headers=frozenset(),
                resolver=_resolver(),
            )
            is None
        )

    @pytest.mark.parametrize("loop", ["pagination", "replication"])
    def test_a_placeholder_bound_to_a_loop_owned_param_is_refused(
        self, loop: str
    ) -> None:
        # The path is substituted once, before either loop has produced a
        # value. Pagination would freeze page one into the URL; replication
        # has no cursor on the first run and none at all under full
        # refresh, so the read failed on the URL it built and blamed a
        # correct binding -- then worked on the next run.
        problem = request_block_problem(
            _request(
                {
                    "path": "/items/{since}",
                    "path_params": {"since": {"from_param": "since"}},
                }
            ),
            endpoint="items",
            reserved_headers=frozenset(),
            resolver=_resolver(),
            controlled_by={"since": loop},
        )
        assert problem is not None
        assert f"{loop} loop owns" in problem


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
        with pytest.raises(RequestSpecError, match=r"\{id\}"):
            substitute_path("/Contact/{id}", {}, endpoint="items")

    def test_a_placeholder_resolving_to_nothing_is_refused(self) -> None:
        with pytest.raises(RequestSpecError, match=r"\{id\}"):
            substitute_path("/Contact/{id}", {"id": None}, endpoint="items")

    def test_a_placeholder_resolving_to_an_empty_string_is_refused(self) -> None:
        # "/Contact/" addresses the whole collection: a read fetches every
        # record instead of one, and a PUT or PATCH targets all of them.
        # url_encode answers "" for an unbound input, so this is reachable
        # without anyone declaring an empty value.
        with pytest.raises(RequestSpecError, match=r"\{id\}"):
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
        with pytest.raises(RequestSpecError, match="resolved to nothing"):
            build_write_body(
                body_spec={"ref": "connection.parameters.absent"},
                endpoint="/items",
                params={},
                resolver=_resolver(),
                record={"id": 1},
            )

    @pytest.mark.parametrize(
        "body_spec",
        [
            {"x": {"ref": "connection.parameters.a", "template": "b"}},
            {"x": {"ref": "nope.a"}},
            {"x": {"function": "nope", "input": 1}},
            {"x": {"from_param": "p", "extra": 1}},
        ],
        ids=["two markers", "unknown scope", "unknown function", "binding siblings"],
    )
    def test_every_defect_in_a_spec_leaves_as_one_classified_error(
        self, body_spec: dict[str, Any]
    ) -> None:
        # These four used to leave as TransportSpecError, KeyError,
        # TransportSpecError and ValueError. The write's catch sites
        # classified the ValueError as a rejected record and let the others
        # fail the whole batch, so what went wrong inside the body decided
        # what the failure MEANT. The read's body build has answered one
        # class all along; this is the same body, built for the other role.
        with pytest.raises(RequestSpecError, match="request.body for endpoint"):
            build_write_body(
                body_spec=body_spec,
                endpoint="/items",
                params={},
                resolver=_resolver(),
                record={"id": 1},
            )
