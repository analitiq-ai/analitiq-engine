"""Unit tests for src.config.schema_validator.

The validator fetches schemas via HTTP and caches them per-process with
``functools.lru_cache``. The tests use ``ANALITIQ_SCHEMA_BASE_URL=file://``
plus a temp directory of fixture schemas so no network access is needed.
"""

from __future__ import annotations

import json
import urllib.error
from pathlib import Path
from unittest.mock import patch

import pytest

from src.config import schema_validator
from src.config.schema_validator import (
    ARTIFACT_KINDS,
    ContractValidationError,
    _load_schema,
    validate,
    validate_file,
)


@pytest.fixture(autouse=True)
def clear_schema_cache():
    """LRU cache on _load_schema is process-wide; reset between tests so
    each test sees its own fixture schemas."""
    _load_schema.cache_clear()
    yield
    _load_schema.cache_clear()


def _write_schema(base_dir: Path, kind: str, schema: dict) -> None:
    target = base_dir / kind / "latest.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(schema))


@pytest.fixture
def schema_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the validator at a local ``file://`` schema mirror."""
    monkeypatch.setenv("ANALITIQ_SCHEMA_BASE_URL", tmp_path.as_uri())
    return tmp_path


class TestValidate:
    def test_valid_document_passes(self, schema_root: Path) -> None:
        _write_schema(
            schema_root,
            "pipeline",
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["alias"],
                "properties": {"alias": {"type": "string"}},
            },
        )
        validate("pipeline", {"alias": "p"})

    def test_invalid_document_raises_contract_validation_error(
        self, schema_root: Path
    ) -> None:
        _write_schema(
            schema_root,
            "pipeline",
            {
                "type": "object",
                "required": ["alias"],
                "properties": {"alias": {"type": "string"}},
            },
        )
        with pytest.raises(ContractValidationError) as ei:
            validate("pipeline", {}, source="/tmp/pipeline.json")
        message = str(ei.value)
        assert "pipeline" in message
        assert "/tmp/pipeline.json" in message
        assert ei.value.kind == "pipeline"
        assert len(ei.value.errors) >= 1

    def test_error_message_truncated_at_ten_errors(self, schema_root: Path) -> None:
        _write_schema(
            schema_root,
            "pipeline",
            {
                "type": "object",
                "properties": {f"f{i}": {"type": "string"} for i in range(15)},
            },
        )
        bad = {f"f{i}": i for i in range(15)}  # all wrong type
        with pytest.raises(ContractValidationError) as ei:
            validate("pipeline", bad)
        message = str(ei.value)
        # 10 enumerated + a "... and N more" line
        assert message.count("\n  - ") == 10
        assert "and 5 more" in message

    def test_unknown_kind_rejected(self, schema_root: Path) -> None:
        with pytest.raises(ValueError, match="Unknown artifact kind"):
            validate("not-a-real-kind", {})


class TestSchemaFetching:
    def test_url_error_wrapped_as_runtime_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unreachable schema host must surface a precise error, not
        a silent default-true validation."""
        monkeypatch.setenv("ANALITIQ_SCHEMA_BASE_URL", "http://127.0.0.1:1")
        with patch.object(
            schema_validator.urllib.request,
            "urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        ):
            with pytest.raises(RuntimeError, match="Could not fetch"):
                validate("pipeline", {})

    def test_malformed_schema_json_wrapped_as_runtime_error(
        self, schema_root: Path
    ) -> None:
        target = schema_root / "pipeline" / "latest.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("{not json")
        with pytest.raises(RuntimeError, match="not valid JSON"):
            validate("pipeline", {})

    def test_env_override_changes_base_url(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Setting ``ANALITIQ_SCHEMA_BASE_URL`` must redirect lookups."""
        mirror_a = tmp_path / "a"
        mirror_b = tmp_path / "b"
        _write_schema(
            mirror_a,
            "pipeline",
            {
                "type": "object",
                "required": ["a"],
                "properties": {"a": {"type": "string"}},
            },
        )
        _write_schema(
            mirror_b,
            "pipeline",
            {
                "type": "object",
                "required": ["b"],
                "properties": {"b": {"type": "string"}},
            },
        )

        monkeypatch.setenv("ANALITIQ_SCHEMA_BASE_URL", mirror_a.as_uri())
        with pytest.raises(ContractValidationError):
            validate("pipeline", {})  # missing "a"
        _load_schema.cache_clear()

        monkeypatch.setenv("ANALITIQ_SCHEMA_BASE_URL", mirror_b.as_uri())
        with pytest.raises(ContractValidationError) as ei:
            validate("pipeline", {})  # missing "b"
        assert any("'b' is a required property" in e.message for e in ei.value.errors)


class TestValidateFile:
    def test_valid_file_parses_and_validates(
        self, schema_root: Path, tmp_path: Path
    ) -> None:
        _write_schema(
            schema_root,
            "connection",
            {
                "type": "object",
                "required": ["alias", "connector_alias"],
                "properties": {
                    "alias": {"type": "string"},
                    "connector_alias": {"type": "string"},
                },
            },
        )
        doc_path = tmp_path / "conn.json"
        doc_path.write_text(json.dumps({"alias": "c", "connector_alias": "k"}))
        result = validate_file("connection", doc_path)
        assert result == {"alias": "c", "connector_alias": "k"}

    def test_missing_file_raises_file_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            validate_file("connection", tmp_path / "missing.json")

    def test_malformed_json_raises_value_error(
        self, schema_root: Path, tmp_path: Path
    ) -> None:
        bad = tmp_path / "bad.json"
        bad.write_text("{not json")
        with pytest.raises(ValueError, match="Invalid JSON"):
            validate_file("connection", bad)


class TestArtifactKinds:
    def test_full_kind_coverage(self) -> None:
        """Every documented artifact kind must be in ARTIFACT_KINDS so the
        endpoint-resolver dispatch never falls through to 'unknown kind'."""
        assert set(ARTIFACT_KINDS) >= {
            "connector",
            "connection",
            "pipeline",
            "stream",
            "endpoint",
            "api-endpoint",
            "database-endpoint",
        }
