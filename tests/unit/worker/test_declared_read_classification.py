"""Declared error_map consumption in the source worker's read verdict (#401).

``classify_read_error`` resolves declared map -> typed-error ladder; the
declared category's read verdict comes from the engine-owned table, so a
connector fixes a misclassified driver error with a JSON edit (the #245
class), no connector Python.
"""

from __future__ import annotations

from cdk.declarations import parse_declared_error_map
from src.worker.source_service import classify_read_error


class AutoReconnect(Exception):
    """Stands in for a driver's network blip class (the #245 shape)."""


def _map(block):
    parsed = parse_declared_error_map(block)
    assert parsed is not None
    return parsed


class TestDeclaredFirst:
    def test_declared_transient_makes_a_ladder_deterministic_type_retryable(self):
        # ValueError sits in _DETERMINISTIC_READ_ERRORS; the declared map
        # outranks the ladder (resolution order: map -> hook).
        error_map = _map({"exception": {"ValueError": "transient"}})
        deterministic, declared = classify_read_error(ValueError("blip"), error_map)
        assert deterministic is False
        assert declared == "exception:ValueError -> transient"

    def test_declared_config_makes_an_unknown_type_deterministic(self):
        error_map = _map({"exception": {"AutoReconnect": "config"}})
        deterministic, declared = classify_read_error(
            AutoReconnect("bad topology"), error_map
        )
        assert deterministic is True
        assert declared is not None

    def test_the_245_class_is_a_json_edit(self):
        # An undeclared driver network error classifies via the ladder;
        # declaring it transient needs only connector.json.
        undeclared_verdict, _ = classify_read_error(AutoReconnect("net down"), None)
        assert undeclared_verdict is False  # not in the ladder -> retryable
        error_map = _map({"exception": {"AutoReconnect": "transient"}})
        deterministic, declared = classify_read_error(
            AutoReconnect("net down"), error_map
        )
        assert deterministic is False
        assert declared is not None

    def test_declared_sqlstate_on_the_cause_chain(self):
        error_map = _map({"sqlstate": {"28": "auth"}})
        inner = Exception("auth denied")
        inner.sqlstate = "28000"
        outer = RuntimeError("read failed")
        outer.__cause__ = inner
        deterministic, _ = classify_read_error(outer, error_map)
        assert deterministic is True


class TestLadderFallback:
    def test_unclaimed_exception_uses_the_ladder(self):
        error_map = _map({"exception": {"SomethingElse": "transient"}})
        deterministic, declared = classify_read_error(ValueError("boom"), error_map)
        assert deterministic is True
        assert declared is None

    def test_no_map_keeps_the_ladder(self):
        deterministic, declared = classify_read_error(TypeError("boom"), None)
        assert deterministic is True
        assert declared is None
