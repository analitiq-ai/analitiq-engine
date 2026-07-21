"""Unit tests for :mod:`src.source.connectors.response_expr`.

The module evaluates contract value expressions and stop-condition
predicates against a parsed response body; these pin the grammar's
semantics directly (the pagination tests exercise them end-to-end).
"""

from __future__ import annotations

import pytest
from analitiq.contracts.endpoints import Predicate
from pydantic import TypeAdapter

from src.source.connectors.response_expr import (
    evaluate_predicate,
    resolve_response_expr,
)

_PREDICATE = TypeAdapter(Predicate)


def _pred(raw: dict) -> object:
    """Parse a raw predicate dict through the contract's own union."""
    return _PREDICATE.validate_python(raw)


def _expr(raw: object) -> object:
    """Parse a raw value-expression node the way the contract models do."""
    from analitiq.contracts.endpoints import Cursor

    return Cursor.model_validate({"param": "p", "next_cursor": raw}).next_cursor


class TestResolveResponseExpr:
    def test_body_ref_returns_whole_body(self):
        """``response.body`` addresses the payload itself."""
        assert resolve_response_expr(_expr({"ref": "response.body"}), [1, 2]) == [1, 2]

    def test_dotted_ref_walks_the_body(self):
        """A dotted ref walks nested objects to the terminal value."""
        data = {"meta": {"next": "tok"}}
        assert (
            resolve_response_expr(_expr({"ref": "response.body.meta.next"}), data)
            == "tok"
        )

    def test_missing_path_resolves_to_none(self):
        """An absent path is ``None`` — the loop's stop signal, not an error."""
        assert (
            resolve_response_expr(_expr({"ref": "response.body.next"}), {"a": 1})
            is None
        )

    def test_non_response_ref_raises(self):
        """Only response-scoped refs are resolvable from a page body."""
        with pytest.raises(ValueError, match="not response-scoped"):
            resolve_response_expr(_expr({"ref": "connection.parameters.token"}), {})

    def test_literal_passes_through(self):
        """A literal expression yields its value."""
        assert resolve_response_expr(_expr({"literal": 5}), {}) == 5

    def test_plain_scalar_passes_through(self):
        """Predicate operands may be plain scalars, not expression nodes."""
        assert resolve_response_expr(10, {}) == 10

    def test_template_raises(self):
        """Template expressions have no response-scope semantics yet."""
        with pytest.raises(ValueError, match="template"):
            resolve_response_expr(_expr({"template": "${response.body.x}"}), {})


class TestEvaluatePredicate:
    def test_empty_on_absent_and_empty_list(self):
        """``empty`` holds for an absent path and for a zero-length list."""
        pred = _pred({"empty": {"ref": "response.body.records"}})
        assert evaluate_predicate(pred, {}) is True
        assert evaluate_predicate(pred, {"records": []}) is True
        assert evaluate_predicate(pred, {"records": [1]}) is False

    def test_scalars_are_never_empty(self):
        """``empty`` asks "is there nothing here" — 0 and False are values."""
        pred = _pred({"empty": {"ref": "response.body.count"}})
        assert evaluate_predicate(pred, {"count": 0}) is False
        assert evaluate_predicate(pred, {"count": False}) is False

    def test_exists_and_missing(self):
        """``exists``/``missing`` test presence of the addressed value."""
        data = {"next": "tok"}
        assert evaluate_predicate(
            _pred({"exists": {"ref": "response.body.next"}}), data
        )
        assert not evaluate_predicate(
            _pred({"missing": {"ref": "response.body.next"}}), data
        )
        assert evaluate_predicate(
            _pred({"missing": {"ref": "response.body.gone"}}), data
        )

    def test_comparisons_resolve_both_operands(self):
        """Comparison operands each resolve through the expression grammar."""
        data = {"page": 3, "total_pages": 3}
        assert evaluate_predicate(
            _pred(
                {
                    "gte": [
                        {"ref": "response.body.page"},
                        {"ref": "response.body.total_pages"},
                    ]
                }
            ),
            data,
        )
        assert not evaluate_predicate(
            _pred({"lt": [{"ref": "response.body.page"}, 3]}), data
        )
        assert evaluate_predicate(
            _pred({"eq": [{"ref": "response.body.page"}, 3]}), data
        )
        assert evaluate_predicate(
            _pred({"neq": [{"ref": "response.body.page"}, 4]}), data
        )

    def test_incomparable_operands_raise(self):
        """Ordering ``None`` against a number is unanswerable — fail loud."""
        with pytest.raises(ValueError, match="cannot compare"):
            evaluate_predicate(_pred({"lt": [{"ref": "response.body.absent"}, 10]}), {})

    def test_boolean_composition(self):
        """``and``/``or``/``not`` recurse over their branches."""
        data = {"records": [], "has_more": False}
        pred = _pred(
            {
                "and": [
                    {"empty": {"ref": "response.body.records"}},
                    {"not": {"eq": [{"ref": "response.body.has_more"}, True]}},
                ]
            }
        )
        assert evaluate_predicate(pred, data) is True
        assert evaluate_predicate(pred, {"records": [1], "has_more": False}) is False
        assert evaluate_predicate(
            _pred(
                {
                    "or": [
                        {"not_empty": {"ref": "response.body.records"}},
                        {"exists": {"ref": "response.body.has_more"}},
                    ]
                }
            ),
            data,
        )
