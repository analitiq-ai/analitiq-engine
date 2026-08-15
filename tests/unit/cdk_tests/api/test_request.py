"""The param table and the request built from it, for both roles."""

from __future__ import annotations

from typing import Any

import pytest

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

    def test_a_filter_naming_no_declared_param_is_refused(self) -> None:
        # A filter reaches the provider as a declared param bound by a
        # request map. One naming no param narrows nothing: the value sat
        # in the table, no binding read it, and the stream read the whole
        # collection while reporting success.
        with pytest.raises(RequestSpecError, match="customer_number"):
            ParamTable.for_read(
                {"cn": {"in": "query", "type": "string", "required": False}},
                _resolver(),
                filters=[{"field": "customer_number", "value": "C-1"}],
            )

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
    def test_a_declared_param_reaches_the_wire_only_through_its_binding(self) -> None:
        # The contract requires every declared param to be referenced by
        # exactly one binding, in the map its ``in`` names, so the param
        # name is the endpoint's internal handle and the binding key is the
        # wire name. Emitting the param name as well sent every value
        # twice, the second time under a name the provider never declared
        # -- which for a secret-valued param put a credential on the query
        # string.
        table = ParamTable.for_read(
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
            },
            _resolver(),
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

    def test_a_continuation_carrying_params_still_sends_no_query(self) -> None:
        # The continuation URL carries its own query, and a param reaches
        # the wire only through a binding that names it. A page's params
        # going onto the query string raw would be a second sink with its
        # own spelling rules: internal names, no bindings, no refusals.
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
        prepared = builder.for_page({"limit": 25}, sends_declared_body=False)
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
            {"query": {"key": {"from_param": "api_key"}}},
            reserved_headers=frozenset(),
            resolver=_resolver(),
            params={},
            declared_params={
                "api_key": {
                    "in": "query",
                    "type": "string",
                    "required": True,
                    "default": {"ref": "secrets.api_key"},
                }
            },
        )
        assert problem is not None
        assert "'secrets.api_key'" in problem
        assert "request-time resolution never supplies" in problem

    def test_a_pagination_value_reading_a_secret_is_refused(self) -> None:
        problem = request_block_problem(
            {},
            reserved_headers=frozenset(),
            resolver=_resolver(),
            params={},
            pagination={
                "type": "offset",
                "limit": {
                    "param": "limit",
                    "default": {"ref": "secrets.page_size"},
                },
            },
        )
        assert problem is not None
        assert "'secrets.page_size'" in problem

    def test_a_runtime_key_typo_is_refused_not_prefix_matched(self) -> None:
        # The engine passes exactly connection_id and batch_size; a bare
        # `runtime.` prefix match would give `runtime.batchsize` the
        # warn-and-omit fate this walk exists to refuse.
        problem = request_block_problem(
            {"query": {"limit": {"ref": "runtime.batchsize"}}},
            reserved_headers=frozenset(),
            resolver=_resolver(),
            params={},
        )
        assert problem is not None
        assert "'runtime.batchsize'" in problem

    def test_the_supplied_runtime_keys_are_not_refused(self) -> None:
        # The supplied set is the RESOLVER's, per phase: this one carries
        # batch_size the way the read role's does.
        assert (
            request_block_problem(
                {"query": {"limit": {"ref": "runtime.batch_size"}}},
                reserved_headers=frozenset(),
                resolver=_resolver(batch_size=37),
                params={},
            )
            is None
        )

    def test_a_runtime_key_another_phase_supplies_is_refused(self) -> None:
        # The same read against a resolver built WITHOUT batch_size -- the
        # write role's shape -- is a value that never arrives on this phase.
        problem = request_block_problem(
            {"query": {"limit": {"ref": "runtime.batch_size"}}},
            reserved_headers=frozenset(),
            resolver=_resolver(),
            params={},
        )
        assert problem is not None
        assert "'runtime.batch_size'" in problem

    def test_a_connection_read_in_a_param_default_is_not_refused(self) -> None:
        # The refusal is about the phase, not about deferral in general:
        # request-time resolution DOES supply the connection document.
        assert (
            request_block_problem(
                {"query": {"key": {"from_param": "api_key"}}},
                reserved_headers=frozenset(),
                resolver=_resolver(),
                params={},
                declared_params={
                    "api_key": {
                        "in": "query",
                        "type": "string",
                        "required": True,
                        "default": {"ref": "connection.parameters.api_key"},
                    }
                },
            )
            is None
        )


class TestRequestBlockRefusals:
    """One rule over the declared header map, and one over the path bindings."""

    def test_a_controlled_param_bound_under_content_type_is_refused(self) -> None:
        # The value judge sees the pre-checkpoint table, where a loop-owned
        # param is absent -- so without the static refusal a resumed
        # incremental read sends the stored cursor as the media type, and
        # fails only once a checkpoint exists.
        problem = request_block_problem(
            {"headers": {"Content-Type": {"from_param": "cursor"}}},
            reserved_headers=frozenset(),
            resolver=_resolver(),
            params={},
            controlled_by={"cursor": "replication"},
        )
        assert problem is not None
        assert "'cursor'" in problem
        assert "replication" in problem

    def test_a_controlled_param_wrapped_in_a_function_is_refused(self) -> None:
        # A binding is a binding at any depth. Before the first checkpoint
        # the lookup input resolves to nothing and the header is dropped, so
        # the value judge sees nothing to refuse; after one, the loop binds
        # the param, the lookup falls to its `safe` value and a non-JSON
        # media type ships with a JSON body. The contract permits the shape
        # -- a header-placed param may be `controlled_by` -- so this is the
        # only thing standing between it and the wire.
        problem = request_block_problem(
            {
                "headers": {
                    "Content-Type": {
                        "function": "lookup",
                        "input": {"from_param": "cursor"},
                        "map": {"1": "application/json"},
                        "safe": "text/plain",
                    }
                }
            },
            reserved_headers=frozenset(),
            resolver=_resolver(),
            params={},
            controlled_by={"cursor": "replication"},
        )
        assert problem is not None
        assert "'cursor'" in problem
        assert "replication" in problem

    def test_a_binding_spelled_inside_a_literal_is_data(self) -> None:
        # The resolver hands a literal back untouched, so nothing here is a
        # binding the loop ever fills. This value is still refused -- it is
        # not the media type the engine sends -- but as the wrong VALUE it
        # is, not as a loop-controlled binding it is not. The distinction
        # matters where the literal holds a working media type, which the
        # walk must not read as a binding at all.
        problem = request_block_problem(
            {"headers": {"Content-Type": {"literal": {"from_param": "cursor"}}}},
            reserved_headers=frozenset(),
            resolver=_resolver(),
            params={},
            controlled_by={"cursor": "replication"},
        )
        assert problem is not None
        assert "the engine owns that header" in problem
        assert "replication" not in problem, "a literal payload is data"

    def test_a_declared_content_length_is_refused(self) -> None:
        # The body is encoded after the headers are built, so no declared
        # length can be right -- and aiohttp sends the declared one rather
        # than recomputing, truncating the body the provider reads.
        problem = request_block_problem(
            {"headers": {"Content-Length": {"literal": "0"}}},
            reserved_headers=frozenset(),
            resolver=_resolver(),
            params={},
        )
        assert problem is not None
        assert "the engine fills itself" in problem

    def test_a_declared_header_the_connection_owns_is_refused(self) -> None:
        problem = request_block_problem(
            {"headers": {"Authorization": {"literal": "Bearer x"}}},
            reserved_headers=frozenset({"authorization"}),
            resolver=_resolver(),
            params={},
        )
        assert problem is not None and "request.headers declares" in problem

    def test_a_param_bound_under_a_permitted_key_is_permitted(self) -> None:
        # The rule judges the WIRE names, which are the keys of
        # request.headers. A param named 'Authorization' bound to
        # 'X-Legacy-Auth' sends only 'X-Legacy-Auth', so refusing it would
        # refuse an endpoint that shadows nothing.
        assert (
            request_block_problem(
                {"headers": {"X-Legacy-Auth": {"from_param": "Authorization"}}},
                reserved_headers=frozenset({"authorization"}),
                resolver=_resolver(),
                params={},
            )
            is None
        )

    def test_a_param_bound_under_a_reserved_key_is_refused(self) -> None:
        # The mirror image: an innocuous param name under a reserved key
        # still shadows the connection's header, because the key is what
        # the provider sees.
        problem = request_block_problem(
            {"headers": {"Authorization": {"from_param": "tok"}}},
            reserved_headers=frozenset({"authorization"}),
            resolver=_resolver(),
            params={},
        )
        assert problem is not None and "Authorization" in problem

    def test_a_path_binding_that_encodes_the_value_itself_is_refused(self) -> None:
        problem = request_block_problem(
            {
                "path": "/Contact/{id}",
                "path_params": {
                    "id": {"function": "url_encode", "input": {"from_param": "id"}}
                },
            },
            reserved_headers=frozenset(),
            resolver=_resolver(),
            params={},
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
                resolver=_resolver(),
                params={},
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
            {
                "path": "/items/{since}",
                "path_params": {"since": {"from_param": "since"}},
            },
            reserved_headers=frozenset(),
            resolver=_resolver(),
            params={},
            controlled_by={"since": loop},
        )
        assert problem is not None
        assert f"{loop} loop owns" in problem


class TestDeclaredContentType:
    """The engine's own header, judged by the value it would send.

    The contract lets a header value be a literal or an expression, so the
    same media type has several spellings. Reading the declaration instead
    of the value refuses all but one of them, and tells the author to
    declare what they already declared.
    """

    @staticmethod
    def _problem(value: Any, params: dict[str, Any] | None = None) -> str | None:
        return request_block_problem(
            {"headers": {"Content-Type": value}},
            reserved_headers=frozenset({"content-type"}),
            resolver=_resolver(),
            params=params or {},
        )

    @pytest.mark.parametrize(
        "declared",
        [
            "application/json",
            {"literal": "application/json"},
            {"template": "application/json"},
            {"ref": "connection.parameters.media_type"},
            {
                "function": "lookup",
                "input": {"ref": "connection.parameters.profile"},
                "map": {"42": "application/json"},
            },
        ],
        ids=["plain", "literal", "template", "ref", "function"],
    )
    def test_every_spelling_of_the_engines_own_value_is_permitted(
        self, declared: Any
    ) -> None:
        assert self._problem(declared) is None

    @pytest.mark.parametrize(
        "declared",
        [
            "application/xml",
            {"literal": "application/xml"},
            {"template": "application/${connection.parameters.other_type}"},
        ],
        ids=["plain", "literal", "template"],
    )
    def test_a_conflicting_value_is_refused_whatever_its_spelling(
        self, declared: Any
    ) -> None:
        problem = self._problem(declared)
        assert problem is not None
        # The refusal names what would go out, so the author can see which
        # of the two values is theirs.
        assert "application/xml" in problem

    def test_a_value_that_resolves_to_nothing_is_not_judged(self) -> None:
        # bind_request_values drops the key, so no Content-Type reaches the
        # wire and there is no collision to refuse.
        assert self._problem({"ref": "connection.parameters.absent"}) is None

    def test_a_conflicting_value_the_param_table_supplies_is_refused(self) -> None:
        # The rule runs after the param table is built, so a {from_param}
        # binding has its value here. Deferring it let an endpoint send
        # 'Content-Type: text/xml' while the engine serialised JSON --
        # judged nowhere, because configure_schema accepted the document and
        # the request went out.
        problem = self._problem({"from_param": "ct"}, {"ct": "text/xml"})
        assert problem is not None and "text/xml" in problem

    def test_the_engines_own_value_from_the_param_table_is_permitted(self) -> None:
        assert self._problem({"from_param": "ct"}, {"ct": "application/json"}) is None

    def test_a_binding_whose_param_has_no_value_is_not_judged(self) -> None:
        # An unbound param binds None, which bind_request_values drops: no
        # Content-Type reaches the wire, so there is no collision.
        assert self._problem({"from_param": "ct"}) is None

    def test_a_function_over_a_binding_is_judged_by_what_it_sends(self) -> None:
        # Binding comes FIRST in the request build, so the function sees a
        # scalar. Resolving the raw declaration reads the binding node as
        # the function's input and refuses a connector that sends exactly
        # the engine's own value.
        assert (
            self._problem(
                {
                    "function": "lookup",
                    "input": {"from_param": "fmt"},
                    "map": {"json": "application/json"},
                },
                {"fmt": "json"},
            )
            is None
        )

    def test_a_malformed_expression_leaves_as_one_classified_error(self) -> None:
        with pytest.raises(RequestSpecError, match="request.headers.Content-Type"):
            self._problem({"ref": "connection.parameters.x", "template": "y"})


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
