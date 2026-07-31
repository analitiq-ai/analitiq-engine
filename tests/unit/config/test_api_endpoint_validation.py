"""The engine, not a connector, is what refuses a malformed api endpoint.

The connector navigates an already-validated document raw, so nothing on
the connector side can catch a cross-field mistake any more. These pin the
rules the api read tests used to satisfy implicitly through a builder that
assembled contract-valid documents on their behalf: what a document must
declare before a read can bind it, and what the contract refuses outright.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.config.schema_validator import ContractValidationError, validate

pytestmark = pytest.mark.unit

_RESPONSE = {
    "schema": {
        "type": "object",
        "properties": {
            "records": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "integer",
                            "native_type": "integer",
                            "arrow_type": "Int64",
                        }
                    },
                },
            }
        },
    },
    "records": {"ref": "response.body.records"},
}


def _document(read: dict[str, Any]) -> dict[str, Any]:
    return {
        "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
        "endpoint_id": "items",
        "operations": {"read": read},
    }


def _paginated_read(**overrides: Any) -> dict[str, Any]:
    read: dict[str, Any] = {
        "request": {
            "method": "GET",
            "path": "/items",
            "query": {
                "skip": {"from_param": "skip"},
                "limit": {"from_param": "limit"},
            },
        },
        "params": {
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
        },
        "response": _RESPONSE,
        "pagination": {
            "type": "offset",
            "offset": {
                "param": "skip",
                "initial": 0,
                "increment_by": {"ref": "response.record_count"},
            },
            "limit": {"param": "limit", "default": {"literal": 50}},
            "stop_when": {"empty": {"ref": "response.body.records"}},
        },
    }
    read.update(overrides)
    return read


class TestAPaginatedReadIsAccepted:
    def test_the_shape_the_read_path_binds_validates(self) -> None:
        assert validate("api-endpoint", _document(_paginated_read())) is not None


class TestTheDocumentMustDeclareWhatItBinds:
    def test_a_pagination_param_must_be_declared(self) -> None:
        read = _paginated_read()
        del read["params"]["skip"]
        with pytest.raises(ContractValidationError):
            validate("api-endpoint", _document(read))

    def test_a_declared_param_must_be_bound_into_the_request(self) -> None:
        read = _paginated_read()
        del read["request"]["query"]["skip"]
        with pytest.raises(ContractValidationError):
            validate("api-endpoint", _document(read))

    def test_a_param_bound_twice_is_refused(self) -> None:
        read = _paginated_read()
        read["request"]["headers"] = {"X-Skip": {"from_param": "skip"}}
        with pytest.raises(ContractValidationError):
            validate("api-endpoint", _document(read))

    def test_a_replication_mapping_must_name_a_declared_param(self) -> None:
        read = _paginated_read(
            replication={
                "supported_methods": ["full_refresh", "incremental"],
                "cursor_mappings": [
                    {"cursor_field": "updated", "param": "undeclared", "operator": "gte"}
                ],
            }
        )
        with pytest.raises(ContractValidationError):
            validate("api-endpoint", _document(read))

    def test_a_pagination_block_must_carry_its_stop_condition(self) -> None:
        # The engine does no row-count guessing, so an absent stop_when
        # would leave the loop with nothing authoritative to end on.
        read = _paginated_read()
        del read["pagination"]["stop_when"]
        with pytest.raises(ContractValidationError):
            validate("api-endpoint", _document(read))


class TestTheContractBoundsThePageSize:
    @pytest.mark.parametrize(
        "default",
        [
            pytest.param(0, id="zero"),
            pytest.param(-3, id="negative"),
            pytest.param(2.9, id="fractional"),
            pytest.param("not-a-size", id="non-numeric"),
            # The published schema says ``type: integer``, so a numeric
            # string and a bool are refused too.
            pytest.param("50", id="numeric-string"),
            pytest.param(True, id="boolean"),
        ],
    )
    def test_a_bare_default_outside_the_range_never_reaches_a_read(
        self, default: Any
    ) -> None:
        read = _paginated_read()
        read["pagination"]["limit"] = {"param": "limit", "default": default}
        with pytest.raises(ContractValidationError):
            validate("api-endpoint", _document(read))
