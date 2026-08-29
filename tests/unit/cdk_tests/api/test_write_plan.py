"""The write plan, built from documents the contract actually validated.

Every document here is authored in wire form and parsed by
``ApiEndpointDoc``, so the plan is built from the same object production
builds it from. A hand-written dict would let an alias mistake ship green:
the test author writes ``in``, the plan reads ``location``, and neither
notices while idempotency silently promises exactly-once and never sends
the key.
"""

from __future__ import annotations

from typing import Any

import pytest
from analitiq.contracts.endpoints import (
    ApiEndpointDoc,
    Batching,
    Idempotency,
    WriteOperation,
)
from pydantic import ValidationError

from cdk.api.request import ParamTable, RequestBuilder
from cdk.api.write_plan import (
    StreamWritePlan,
    body_with_idempotency_key,
    build_write_plan,
    collect_input_field_names,
    collect_json_fields,
    idempotency_config_problem,
    retry_verdict,
    write_mode_block,
)
from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.resolver import ResolutionContext, Resolver
from cdk.types import RetrySemantics, SchemaSpec, WriteMode

pytestmark = pytest.mark.unit


def _resolver(**parameters: Any) -> Resolver:
    """The request resolver a connected write role hands the plan builder."""
    return Resolver(
        ResolutionContext(
            connection={"parameters": parameters or {}},
            runtime={"connection_id": "test-conn"},
        ),
        functions=DEFAULT_FUNCTIONS,
    )


def _document(
    *,
    mode: str = "insert",
    body: Any = None,
    batching: dict[str, Any] | None = None,
    idempotency: dict[str, Any] | None = None,
    properties: dict[str, Any] | None = None,
    path: str = "/items",
    path_params: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
    headers_remove: list[str] | None = None,
    query: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    content_type: str | None = None,
    response: dict[str, Any] | None = None,
) -> ApiEndpointDoc:
    """Author a write document in wire form and parse it as production does."""
    # The contract requires every write body to address the in-flight
    # record(s), so a document that declares none cannot be authored.
    if body is None:
        body = (
            {"items": {"from_input": "records"}}
            if batching is not None
            else {"item": {"from_input": "record"}}
        )
    request: dict[str, Any] = {"method": "POST", "path": path, "body": body}
    if path_params is not None:
        request["path_params"] = path_params
    if headers is not None:
        request["headers"] = headers
    if headers_remove is not None:
        request["headers_remove"] = headers_remove
    if query is not None:
        request["query"] = query
    if content_type is not None:
        request["content_type"] = content_type
    block: dict[str, Any] = {
        "request": request,
        "input": {
            "schema": {
                "type": "object",
                "properties": properties
                or {
                    "id": {
                        "type": "integer",
                        "native_type": "integer",
                        "arrow_type": "Int64",
                    },
                    "payload": {
                        "type": "string",
                        "native_type": "string",
                        "arrow_type": "Json",
                    },
                },
            }
        },
    }
    if batching is not None:
        block["batching"] = batching
    if idempotency is not None:
        block["idempotency"] = idempotency
    if params is not None:
        block["params"] = params
    if response is not None:
        block["response"] = response
    if mode == "upsert":
        block["conflict_keys"] = ["id"]
    raw = {
        "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
        "endpoint_id": "items",
        "operations": {"write": {mode: block}},
    }
    return ApiEndpointDoc.model_validate(raw)


def _spec(mode: WriteMode = WriteMode.WRITE_MODE_INSERT) -> SchemaSpec:
    return SchemaSpec(
        stream_id="items", version=1, write_mode=mode, ack_timeout_seconds=30
    )


class TestTheAliasedFieldsAreReadFromTheAuthoredDocument:
    """Wire name in, attribute out, on the fields where the two differ.

    The static guard that used to sit here -- no module may name a model
    attribute in a string literal -- argued the opposite rule and is gone
    with it. It is no longer the guard that matters either: reading a
    document key off a parsed model raises ``AttributeError`` on the first
    call, so the silent-None failure it was written against cannot ship.
    """

    def test_the_idempotency_placement_is_read_through_its_alias(self) -> None:
        # The model spells this field ``location``; the document spells it
        # ``in``. Reading the wrong one answers None, the placement check
        # never fires, and the ack still promises exactly-once.
        doc = _document(
            idempotency={"in": "header", "name": "Idempotency-Key"},
        )
        plan = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(plan, StreamWritePlan)
        assert plan.idempotency_in == "header"
        assert plan.idempotency_name == "Idempotency-Key"

    def test_the_input_schema_is_read_through_its_alias(self) -> None:
        # ``schema_`` on the model, ``schema`` in the document.
        doc = _document()
        plan = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(plan, StreamWritePlan)
        assert plan.json_fields == {"payload"}

    def test_the_request_and_batching_reach_the_plan(self) -> None:
        doc = _document(batching={"max_records": 50})
        plan = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(plan, StreamWritePlan)
        assert (plan.method, plan.endpoint, plan.max_records) == ("POST", "/items", 50)


class TestDeclaredResponseRefusals:
    def test_an_unknown_function_in_the_response_block_is_refused_at_configure(
        self,
    ) -> None:
        # Decidable from the document alone: found on the first write
        # instead, the record may already have landed.
        outcome = build_write_plan(
            _document(
                response={
                    "success_when": {
                        "eq": [
                            {"function": "no_such_function", "input": {"literal": 1}},
                            1,
                        ]
                    }
                }
            ),
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str)
        assert "response block" in outcome
        assert "unknown derived function 'no_such_function'" in outcome

    def test_a_reference_outside_the_write_scope_is_refused_at_configure(
        self,
    ) -> None:
        # `response.records` is a read-side spelling; a write response
        # carries body, headers, status and metadata and nothing else, so
        # this resolves to nothing on every batch and each would be
        # reported as a provider rejection.
        outcome = build_write_plan(
            _document(
                response={"success_when": {"not_empty": {"ref": "response.records"}}}
            ),
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str)
        assert "response block" in outcome
        assert "reads 'response.records'" in outcome

    @pytest.mark.parametrize(
        "ref",
        [
            "response.body.ok",
            "response.headers.Location",
            "response.status",
            "response.metadata.batch_id",
        ],
    )
    def test_every_write_scope_key_is_accepted(self, ref: str) -> None:
        plan = build_write_plan(
            _document(
                response={
                    "metadata": {"batch_id": {"ref": "response.body.id"}},
                    "success_when": {"exists": {"ref": ref}},
                }
            ),
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(plan, StreamWritePlan)


class TestModeDispatch:
    def test_the_declared_mode_selects_its_block(self) -> None:
        doc = _document(mode="upsert")
        assert write_mode_block(doc, "upsert") is not None
        assert write_mode_block(doc, "insert") is None

    def test_an_unsupported_write_mode_is_refused_with_a_reason(self) -> None:
        outcome = build_write_plan(
            _document(),
            _spec(WriteMode.WRITE_MODE_TRUNCATE_INSERT),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str)
        assert "does not support write_mode" in outcome

    def test_a_missing_block_names_the_modes_that_are_present(self) -> None:
        outcome = build_write_plan(
            _document(mode="upsert"),
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str)
        assert "operations.write.insert" in outcome and "upsert" in outcome


class TestIdempotencyRefusals:
    """The cross-block rules the per-model contract validation cannot express.

    The header rule is one only this side knows: which headers the
    connection already sends is a session fact, not a document one. The
    rest mirror rules the contract also enforces -- kept as a second line
    of defence, and reached by handing the blocks in directly, precisely
    because a contract-valid document can no longer carry them.
    """

    def test_a_header_the_connection_already_sends_is_refused(self) -> None:
        # Layering the key over it would shadow the connection's own value
        # on every request -- or send the record id as the credential.
        doc = _document(idempotency={"in": "header", "name": "Authorization"})
        outcome = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: {"authorization"},
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str) and "collides" in outcome

    def test_a_name_the_client_cannot_send_is_refused(self) -> None:
        # The key reaches the wire by a different route than the declared
        # header map, and the client judges it the same way.
        doc = _document(idempotency={"in": "header", "name": "Bad Key"})
        outcome = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str) and "not an HTTP token" in outcome

    def test_batching_and_idempotency_cannot_combine(self) -> None:
        # A restart re-batches records, so a per-request key over several
        # of them cannot dedup.
        problem = idempotency_config_problem(
            Idempotency.model_validate({"in": "header", "name": "Idempotency-Key"}),
            Batching.model_validate({"max_records": 10}),
            StreamWritePlan(),
            reserved_headers=set(),
            declared_input_fields=set(),
        )
        assert problem is not None and "batching" in problem

    def test_a_body_field_the_record_already_declares_is_refused(self) -> None:
        # No body template: the record itself is the body, so a declared
        # field with the reserved name collides on every record -- after
        # the ack already promised exactly-once.
        problem = idempotency_config_problem(
            Idempotency.model_validate({"in": "body", "name": "id"}),
            None,
            StreamWritePlan(body_spec=None),
            reserved_headers=set(),
            declared_input_fields={"id"},
        )
        assert problem is not None and "write input schema" in problem

    def test_a_body_template_field_the_key_reserves_is_refused(self) -> None:
        problem = idempotency_config_problem(
            Idempotency.model_validate({"in": "body", "name": "key"}),
            None,
            StreamWritePlan(body_spec={"key": {"literal": "x"}}),
            reserved_headers=set(),
            declared_input_fields=set(),
        )
        assert problem is not None and "request.body already declares" in problem

    def test_a_non_object_body_cannot_carry_a_body_key(self) -> None:
        problem = idempotency_config_problem(
            Idempotency.model_validate({"in": "body", "name": "key"}),
            None,
            StreamWritePlan(body_spec=[{"from_input": "record"}]),
            reserved_headers=set(),
            declared_input_fields=set(),
        )
        assert problem is not None and "JSON-object request body" in problem


class TestTheRequestTheStreamWillActuallySend:
    """The write path binds the same three maps the read path does.

    Before this, the plan carried ``request.path`` verbatim: an endpoint
    keyed on a record id POSTed to the literal ``/Contact/{id}``, and every
    declared header was dropped.
    """

    def test_the_declared_media_type_reaches_the_plan(self) -> None:
        # It selects the body encoding and is sent as the header; one field
        # decides both, so the two cannot disagree.
        doc = _document(content_type="application/x-www-form-urlencoded")
        plan = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(plan, StreamWritePlan)
        assert plan.content_type == "application/x-www-form-urlencoded"

    def test_declaring_none_leaves_the_plan_on_json(self) -> None:
        plan = build_write_plan(
            _document(),
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(plan, StreamWritePlan)
        assert plan.content_type is None

    def test_batching_and_form_encoding_refuses_the_schema(self) -> None:
        """Incompatible by shape, so it is settled before any batch is built.

        A batched body binds `records`, which is a list, and a form carries
        flat name/value pairs -- so every chunk this stream could ever build
        fails encoding. Accepting the handshake advertises a stream that
        cannot write a batch, and the failure then arrives per batch
        instead of once, at configure time.
        """
        outcome = build_write_plan(
            _document(
                batching={"max_records": 50},
                content_type="application/x-www-form-urlencoded",
            ),
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str)
        assert "batching" in outcome and "flat name/value pairs" in outcome

    def test_a_form_write_without_batching_is_accepted(self) -> None:
        """One record per request is the shape a form can carry."""
        plan = build_write_plan(
            _document(content_type="application/x-www-form-urlencoded"),
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(plan, StreamWritePlan)
        assert plan.max_records is None

    def test_a_media_type_the_engine_cannot_encode_refuses_the_schema(self) -> None:
        # Refused at the handshake, not per record inside the send, where it
        # would surface as a failed batch rather than a refused schema.
        outcome = build_write_plan(
            _document(content_type="application/xml"),
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str)
        assert "cannot encode" in outcome

    def test_a_path_placeholder_is_substituted_into_the_plans_endpoint(self) -> None:
        doc = _document(
            path="/Contact/{id}",
            path_params={"id": {"from_param": "id"}},
            params={
                "id": {
                    "in": "path",
                    "type": "string",
                    "required": True,
                    "default": {"ref": "connection.parameters.contact"},
                }
            },
        )
        plan = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(contact="c-9"),
        )
        assert isinstance(plan, StreamWritePlan)
        assert plan.endpoint == "/Contact/c-9"

    def test_a_path_placeholder_with_nothing_to_bind_is_rejected(self) -> None:
        # Write params resolve their default through the connection, secrets
        # and runtime scopes; a param with no default has nothing to give,
        # and a URL that still carries braces is answered 200 by many
        # providers. The contract decides this from the document alone
        # (RULE-ENDP-027/028), so the plan builder is never handed one --
        # the parse is the refusal.
        with pytest.raises(ValidationError):
            _document(
                path="/Contact/{id}",
                path_params={"id": {"from_param": "id"}},
                params={"id": {"in": "path", "type": "string", "required": True}},
            )

    def test_declared_headers_and_query_land_on_the_plan(self) -> None:
        doc = _document(
            headers={"X-Tenant": {"from_param": "tenant"}},
            query={"page[limit]": {"literal": 50}},
            params={
                "tenant": {
                    "in": "header",
                    "type": "string",
                    "required": True,
                    "default": {"ref": "connection.parameters.tenant"},
                }
            },
        )
        plan = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(tenant="acme"),
        )
        assert isinstance(plan, StreamWritePlan)
        assert plan.headers["X-Tenant"] == "acme"
        assert plan.query == {"page[limit]": 50}

    def test_the_write_role_binds_exactly_as_the_read_role_does(self) -> None:
        # Parity is the invariant: the two roles read one document through
        # one binder, so a declaration that reaches the wire on a read has
        # to reach it on a write, under the same key and with the same
        # value. A second implementation here is what let the two roles
        # disagree about the same endpoint.
        doc = _document(
            headers={"X-Tenant": {"from_param": "tenant"}},
            query={"page[limit]": {"from_param": "limit"}},
            params={
                "tenant": {
                    "in": "header",
                    "type": "string",
                    "required": True,
                    "default": {"ref": "connection.parameters.tenant"},
                },
                "limit": {
                    "in": "query",
                    "type": "integer",
                    "required": False,
                    "default": {"literal": 50},
                },
            },
        )
        resolver = _resolver(tenant="acme")
        plan = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=resolver,
        )
        assert isinstance(plan, StreamWritePlan)

        block = write_mode_block(doc, "insert")
        assert block is not None
        request = block.request
        table = ParamTable.for_write(block.params, resolver)
        as_read = RequestBuilder(
            table,
            raw_body=None,
            resolver=resolver,
            endpoint="/items",
            declared_query=request.query,
            declared_headers=request.headers,
        ).for_page(table.values)
        assert (plan.query, plan.headers) == (as_read.query, as_read.headers)

    def test_a_param_bound_under_a_reserved_key_is_rejected(self) -> None:
        # The binding KEY is the wire name, so it is what the reserved-header
        # rule judges. An innocuous param name under 'Authorization' still
        # overwrites the connection's credential.
        doc = _document(
            headers={"Authorization": {"from_param": "token"}},
            params={
                "token": {
                    "in": "header",
                    "type": "string",
                    "required": True,
                    "default": {"literal": "Bearer attacker"},
                }
            },
        )
        outcome = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: {"authorization"},
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str)
        assert "Authorization" in outcome

    def test_a_write_param_default_reading_a_secret_is_refused(self) -> None:
        # The write call site threads its declared params into the
        # never-fillable-scope walk: a write param default reading
        # `secrets.*` would otherwise vanish from every write request with
        # only a log line, run green.
        doc = _document(
            headers={"X-Api-Key": {"from_param": "key"}},
            params={
                "key": {
                    "in": "header",
                    "type": "string",
                    "required": True,
                    "default": {"ref": "secrets.api_key"},
                }
            },
        )
        outcome = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str)
        assert "'secrets.api_key'" in outcome
        assert "request-time resolution never supplies" in outcome

    def test_a_param_bound_under_a_harmless_key_is_permitted(self) -> None:
        # The mirror image: a param CALLED Authorization that lands under
        # X-Legacy-Auth shadows nothing, so refusing it would fail a working
        # endpoint. Only the key reaches the provider.
        doc = _document(
            headers={"X-Legacy-Auth": {"from_param": "Authorization"}},
            params={
                "Authorization": {
                    "in": "header",
                    "type": "string",
                    "required": True,
                    "default": {"literal": "legacy"},
                }
            },
        )
        plan = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: {"authorization"},
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(plan, StreamWritePlan)
        assert plan.headers == {"X-Legacy-Auth": "legacy"}

    def test_a_query_key_named_ref_survives_resolution(self) -> None:
        # "ref" is a real query parameter name. Resolving the map as one node
        # reads the key as an expression marker and the endpoint breaks.
        doc = _document(query={"ref": {"literal": "main"}})
        plan = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(plan, StreamWritePlan)
        assert plan.query == {"ref": "main"}

    def test_a_path_value_is_encoded_as_exactly_one_segment(self) -> None:
        # The write role's half of RULE-ENDP-027: the engine already encodes
        # the segment, which is why a binding that encodes too is a contract
        # error. The encoding is what stays the engine's, so that is what is
        # pinned.
        doc = _document(
            path="/Contact/{id}",
            path_params={"id": {"from_param": "id"}},
            params={
                "id": {
                    "in": "path",
                    "type": "string",
                    "required": True,
                    "default": {"literal": "a/b"},
                }
            },
        )
        plan = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(plan, StreamWritePlan)
        assert plan.endpoint == "/Contact/a%2Fb"

    def test_a_path_placeholder_binding_to_an_empty_value_is_rejected(self) -> None:
        # "/Contact/" addresses the whole collection: this write would PATCH
        # every contact instead of one.
        doc = _document(
            path="/Contact/{id}",
            path_params={"id": {"from_param": "id"}},
            params={
                "id": {
                    "in": "path",
                    "type": "string",
                    "required": True,
                    "default": {"literal": ""},
                }
            },
        )
        outcome = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str)
        assert "{id}" in outcome

    def test_a_request_removing_a_transport_header_is_rejected(self) -> None:
        # The connection's defaults live on the shared session; a
        # per-request header can add or override, never delete.
        doc = _document(headers_remove=["Authorization"])
        outcome = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str)
        assert "headers_remove" in outcome

    def test_a_declared_header_the_connection_owns_is_rejected(self) -> None:
        # The request build never sees the connection's header values, only
        # their names, so re-declaring one can only shadow it.
        doc = _document(headers={"Authorization": {"literal": "Bearer x"}})
        outcome = build_write_plan(
            doc,
            _spec(),
            header_names_for=lambda _ref: {"authorization"},
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(outcome, str)
        assert "Authorization" in outcome

    def test_an_idempotency_key_colliding_with_a_declared_header_is_rejected(
        self,
    ) -> None:
        # One reserved set: the engine-owned key must not be layered over a
        # header this endpoint declares either. Put to the rule directly,
        # because the contract rejects such a document too -- no document
        # build_write_plan will parse can carry this collision to the check.
        plan = StreamWritePlan(headers={"Idempotency-Key": "authored"})
        problem = idempotency_config_problem(
            Idempotency.model_validate({"in": "header", "name": "Idempotency-Key"}),
            None,
            plan,
            # The set the caller folds the endpoint's own declared headers
            # into; nothing connection-owned is needed to make this collide.
            reserved_headers={name.lower() for name in plan.headers},
            declared_input_fields=set(),
        )
        assert problem is not None and "collides" in problem


class TestRetryVerdicts:
    def test_upsert_is_exactly_once_without_a_declared_key(self) -> None:
        verdict = retry_verdict("upsert", StreamWritePlan())
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE

    def test_insert_with_a_declared_key_is_exactly_once(self) -> None:
        plan = StreamWritePlan(idempotency_in="header", idempotency_name="Key")
        verdict = retry_verdict("insert", plan)
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE
        assert "Key" in verdict.reason

    def test_insert_without_one_is_at_least_once(self) -> None:
        verdict = retry_verdict("insert", StreamWritePlan())
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE

    def test_the_plan_carries_its_verdict(self) -> None:
        plan = build_write_plan(
            _document(),
            _spec(),
            header_names_for=lambda _ref: set(),
            transport_problem=lambda _ref: None,
            resolver=_resolver(),
        )
        assert isinstance(plan, StreamWritePlan)
        assert plan.retry_verdict is not None


class TestFieldCollection:
    def test_the_flat_columns_shape_is_walked_too(self) -> None:
        # The write input schema's CONTENTS stay free-form JSON, so the
        # flat-columns spelling is authored inside the block and parsed
        # with it.
        block = WriteOperation.model_validate(
            {
                "request": {
                    "method": "POST",
                    "path": "/items",
                    "body": {"item": {"from_input": "record"}},
                },
                "input": {
                    "schema": {
                        "columns": [
                            {"name": "blob", "arrow_type": "Json"},
                            {"name": "id", "arrow_type": "Int64"},
                        ]
                    }
                },
            }
        )
        assert collect_json_fields(block) == {"blob"}
        assert collect_input_field_names(block) == {"blob", "id"}


class TestBodyKeyInjection:
    def test_the_engine_owned_key_is_added_as_a_top_level_field(self) -> None:
        plan = StreamWritePlan(idempotency_in="body", idempotency_name="key")
        assert body_with_idempotency_key(plan, {"id": 1}, "abc") == {
            "id": 1,
            "key": "abc",
        }

    def test_a_body_that_resolved_away_its_object_shape_is_refused(self) -> None:
        plan = StreamWritePlan(idempotency_in="body", idempotency_name="key")
        with pytest.raises(ValueError, match="JSON-object request body"):
            body_with_idempotency_key(plan, [1, 2], "abc")

    def test_a_collision_at_write_time_is_never_silently_overwritten(self) -> None:
        plan = StreamWritePlan(idempotency_in="body", idempotency_name="key")
        with pytest.raises(ValueError, match="already carries the field"):
            body_with_idempotency_key(plan, {"key": "authored"}, "abc")
