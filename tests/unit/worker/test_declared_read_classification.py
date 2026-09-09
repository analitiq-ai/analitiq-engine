"""Declared error_map consumption in the source worker's read verdict (#401, #513).

``classify_read_error`` resolves declared map -> classify_error hook ->
typed-error ladder; the declared category's read verdict comes from the
engine-owned table, so a connector fixes a misclassified driver error with a
JSON edit (the #245 class) when it fits the declarative shape, or a
``classify_error`` override when it needs more, no other connector Python.
"""

from __future__ import annotations

from cdk.declarations import CLASS_NAME_SIGNAL, parse_declared_error_map
from src.worker.source_service import classify_read_error


class AutoReconnect(Exception):
    """Stands in for a driver's network blip class (the #245 shape)."""


def _map(block):
    parsed = parse_declared_error_map(block)
    assert parsed is not None
    return parsed


def _by_class(**codes):
    return {"key_attrs": [CLASS_NAME_SIGNAL], "codes": codes}


class TestDeclaredFirst:
    def test_declared_transient_makes_a_ladder_deterministic_type_retryable(self):
        # ValueError sits in _DETERMINISTIC_READ_ERRORS; the declared map
        # outranks the ladder (resolution order: map -> hook -> ladder).
        error_map = _map(_by_class(ValueError="transient"))
        deterministic, declared = classify_read_error(ValueError("blip"), error_map)
        assert deterministic is False
        assert declared == "transient"

    def test_declared_config_makes_an_unknown_type_deterministic(self):
        error_map = _map(_by_class(AutoReconnect="config"))
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
        error_map = _map(_by_class(AutoReconnect="transient"))
        deterministic, declared = classify_read_error(
            AutoReconnect("net down"), error_map
        )
        assert deterministic is False
        assert declared is not None

    def test_declared_sqlstate_on_the_cause_chain(self):
        # Issue #513: exact-code match only, no class-prefix fallback.
        error_map = _map({"key_attrs": ["sqlstate"], "codes": {"28000": "auth"}})
        inner = Exception("auth denied")
        inner.sqlstate = "28000"
        outer = RuntimeError("read failed")
        outer.__cause__ = inner
        deterministic, _ = classify_read_error(outer, error_map)
        assert deterministic is True


class TestClassifyErrorFallback:
    def test_classify_error_runs_when_the_map_claims_nothing(self):
        error_map = _map(_by_class(SomethingElse="transient"))
        deterministic, declared = classify_read_error(
            ValueError("boom"), error_map, lambda exc: "config"
        )
        assert deterministic is True
        assert declared == "config"

    def test_classify_error_runs_with_no_map_at_all(self):
        deterministic, declared = classify_read_error(
            ValueError("boom"), None, lambda exc: "rate_limited"
        )
        assert deterministic is False
        assert declared == "rate_limited"

    def test_map_outranks_classify_error(self):
        error_map = _map(_by_class(ValueError="config"))
        deterministic, declared = classify_read_error(
            ValueError("boom"), error_map, lambda exc: "rate_limited"
        )
        assert deterministic is True
        assert declared == "config"

    def test_off_vocabulary_classify_error_return_maps_to_config(self):
        # The hook ran and answered -- wrongly. The engine cannot guess
        # what it meant, so it's treated as a broken classification
        # mechanism: deterministic (non-retryable), same as a crash.
        deterministic, declared = classify_read_error(
            ValueError("boom"), None, lambda exc: "retry_me"
        )
        assert deterministic is True
        assert declared == "config"

    def test_a_crashing_classify_error_maps_to_config(self):
        def _broken(exc):
            raise RuntimeError("connector bug")

        deterministic, declared = classify_read_error(TypeError("boom"), None, _broken)
        assert deterministic is True
        assert declared == "config"


class TestBirthSiteCategory:
    def test_typed_error_carries_its_birth_site_category(self):
        # A connector's HTTP site stamps the declared category on the
        # typed error; the worker forwards it without re-matching (the
        # error_map here would say nothing about ReadError).
        from cdk.exceptions import TransientReadError

        exc = TransientReadError("status 403", declared_category="rate_limited")
        deterministic, declared = classify_read_error(exc, None)
        assert deterministic is False
        assert declared == "rate_limited"

    def test_birth_site_category_outranks_the_map(self):
        from cdk.exceptions import ReadError

        error_map = _map(_by_class(ReadError="transient"))
        exc = ReadError("status 503", declared_category="auth")
        deterministic, declared = classify_read_error(exc, error_map)
        assert deterministic is True
        assert declared == "auth"

    def test_off_vocabulary_birth_site_category_maps_to_config(self):
        # ReadError/TransientReadError accept any string for
        # declared_category with no vocabulary check at construction, and
        # are public CDK classes untrusted connector code can raise
        # directly. classify_read_error runs inside the except block that
        # is reporting exc itself, so an off-vocabulary value here must not
        # raise and displace it -- and the engine does not guess at what it
        # might have meant, so it maps to "config" (deterministic), the
        # same answer call_declared_hook gives a broken classify_error.
        from cdk.exceptions import ReadError

        exc = ReadError("status 503", declared_category="retry_me")
        deterministic, declared = classify_read_error(exc, None)
        assert deterministic is True
        assert declared == "config"

    def test_off_vocabulary_birth_site_category_logs_a_warning(self, caplog):
        import logging

        from cdk.exceptions import ReadError

        exc = ReadError("status 503", declared_category="retry_me")
        with caplog.at_level(logging.WARNING, logger="src.worker.source_service"):
            classify_read_error(exc, None)
        assert any("declared_category" in r.message for r in caplog.records)


class TestLadderFallback:
    def test_unclaimed_exception_uses_the_ladder(self):
        error_map = _map(_by_class(SomethingElse="transient"))
        deterministic, declared = classify_read_error(ValueError("boom"), error_map)
        assert deterministic is True
        assert declared is None

    def test_no_map_keeps_the_ladder(self):
        deterministic, declared = classify_read_error(TypeError("boom"), None)
        assert deterministic is True
        assert declared is None
