"""Unit tests for src.config.schema_validator.

Validation is offline: each artifact kind is checked against a Pydantic
model from ``analitiq-contract-models``. No network, no schema mirror.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.config import schema_validator
from src.config.schema_validator import (
    ARTIFACT_KINDS,
    BundleValidationError,
    ContractValidationError,
    validate,
    validate_bundle,
    validate_file,
)


def _valid_pipeline() -> dict:
    return {
        "$schema": "https://schemas.analitiq.ai/pipeline/latest.json",
        "display_name": "Test Pipeline",
        "status": "active",
        "connections": {
            "source": "00000000-0000-0000-0000-000000000001",
            "destinations": ["00000000-0000-0000-0000-000000000002"],
        },
        "streams": ["00000000-0000-0000-0000-000000000003"],
        "schedule": {"type": "manual", "timezone": "UTC"},
    }


def _valid_connection() -> dict:
    return {
        "$schema": "https://schemas.analitiq.ai/connection/latest.json",
        "connector_id": "postgresql",
    }


class TestValidate:
    def test_valid_document_passes(self) -> None:
        validate("pipeline", _valid_pipeline())

    def test_invalid_document_raises_contract_validation_error(self) -> None:
        with pytest.raises(ContractValidationError) as ei:
            validate("pipeline", {}, source="/tmp/pipeline.json")
        message = str(ei.value)
        assert "pipeline" in message
        assert "/tmp/pipeline.json" in message
        assert ei.value.kind == "pipeline"
        assert len(ei.value.errors) >= 1

    def test_schema_host_is_ignored(self) -> None:
        """The ``$schema`` value is an informational host pointer; a document
        advertising a non-canonical host still validates on its body alone."""
        doc = _valid_pipeline()
        doc["$schema"] = "https://schemas.analitiq.work/pipeline/latest.json"
        validate("pipeline", doc)

    def test_error_message_truncated_at_ten_errors(self) -> None:
        # A stream document with many malformed fields yields >10 errors.
        bad = {f"unexpected_{i}": i for i in range(15)}
        with pytest.raises(ContractValidationError) as ei:
            validate("stream", bad)
        message = str(ei.value)
        if len(ei.value.errors) > 10:
            assert message.count("\n  - ") == 10
            assert "more" in message

    def test_unknown_kind_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unknown artifact kind"):
            validate("not-a-real-kind", {})


class TestValidateFile:
    def test_valid_file_parses_and_validates(self, tmp_path: Path) -> None:
        doc_path = tmp_path / "conn.json"
        doc_path.write_text(json.dumps(_valid_connection()))
        result = validate_file("connection", doc_path)
        assert result == _valid_connection()

    def test_missing_file_raises_file_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            validate_file("connection", tmp_path / "missing.json")

    def test_malformed_json_raises_value_error(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.json"
        bad.write_text("{not json")
        with pytest.raises(ValueError, match="Invalid JSON"):
            validate_file("connection", bad)

    def test_invalid_document_in_file_raises(self, tmp_path: Path) -> None:
        doc_path = tmp_path / "conn.json"
        doc_path.write_text(json.dumps({"$schema": _valid_connection()["$schema"]}))
        with pytest.raises(ContractValidationError):
            validate_file("connection", doc_path)


class TestArtifactKinds:
    def test_full_kind_coverage(self) -> None:
        """Every artifact kind the engine loads must map to a contract model
        so the endpoint-resolver dispatch never falls through to 'unknown
        kind'."""
        assert set(ARTIFACT_KINDS) == {
            "connector",
            "connection",
            "pipeline",
            "stream",
            "api-endpoint",
            "database-endpoint",
        }


def _finding(severity: str, path: str = "/x", message: str = "boom") -> dict:
    return {
        "validator": "bundle-stream-ref",
        "severity": severity,
        "path": path,
        "message": message,
    }


class TestValidateBundle:
    """``validate_bundle`` wraps the published ``validate_pipeline_bundle``.

    The published referential rules are the validator's own concern; these
    pin the engine wrapper: which severities block, and that non-blocking
    findings are surfaced (logged) rather than silently dropped.
    """

    def _patch(self, monkeypatch, findings: list[dict]) -> None:
        monkeypatch.setattr(
            schema_validator, "validate_pipeline_bundle", lambda bundle: findings
        )

    def test_sound_bundle_passes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._patch(monkeypatch, [])
        validate_bundle({"pipeline": {}}, source="/p")  # no raise

    def test_error_finding_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._patch(monkeypatch, [_finding("error", "/streams/0", "unresolved")])
        with pytest.raises(BundleValidationError, match="unresolved") as ei:
            validate_bundle({}, source="/p")
        assert ei.value.source == "/p"
        assert "/streams/0" in str(ei.value)
        assert len(ei.value.findings) == 1

    def test_warning_finding_does_not_block(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Advisory warnings are logged, not raised.
        self._patch(monkeypatch, [_finding("warning")])
        validate_bundle({}, source="/p")  # no raise

    def test_unknown_severity_blocks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A future higher-than-error severity must not slip through the gate.
        self._patch(monkeypatch, [_finding("fatal")])
        with pytest.raises(BundleValidationError):
            validate_bundle({}, source="/p")

    def test_message_truncated_at_ten(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._patch(
            monkeypatch, [_finding("error", f"/s/{i}", f"e{i}") for i in range(15)]
        )
        with pytest.raises(BundleValidationError) as ei:
            validate_bundle({}, source="/p")
        message = str(ei.value)
        assert message.count("\n  - ") == 10
        assert "and 5 more" in message
