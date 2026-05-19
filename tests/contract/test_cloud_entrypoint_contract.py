"""Contract tests for ``docker-cloud/cloud_entrypoint.py`` against the
published Analitiq stream schema.

The cloud Lambda hands the engine endpoint references shaped exactly as the
published stream contract: ``{scope, connection_id, endpoint_id}``. The
engine's on-disk stream-node shape must also validate against the same
contract so that ``cloud_entrypoint`` output remains a legal stream document.

The asserted shape comes from the schema snapshot under
``tests/contract/schemas/``. Regenerate the snapshot from
``https://schemas.analitiq.ai/stream/latest.json`` when the published
contract bumps.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = Path(__file__).parent / "schemas"
STREAM_SCHEMA_PATH = SCHEMA_DIR / "stream-latest.json"
CLOUD_ENTRYPOINT_PATH = REPO_ROOT / "docker-cloud" / "cloud_entrypoint.py"

# ``docker-cloud/`` is gitignored and only present in deploy/dev checkouts.
# Skip the whole contract suite cleanly when the file isn't on disk so fresh
# clones (e.g. CI without the cloud bundle) don't fail collection.
pytestmark = pytest.mark.skipif(
    not CLOUD_ENTRYPOINT_PATH.is_file(),
    reason=(
        "docker-cloud/cloud_entrypoint.py not present in this checkout; "
        "contract suite runs only where the cloud bundle is available."
    ),
)


@pytest.fixture(scope="module")
def stream_schema() -> dict[str, Any]:
    return json.loads(STREAM_SCHEMA_PATH.read_text())


@pytest.fixture(scope="module")
def stream_validator(stream_schema) -> Draft202012Validator:
    return Draft202012Validator(stream_schema)


@pytest.fixture(scope="module")
def cloud_entrypoint_module() -> ModuleType:
    """Import cloud_entrypoint without triggering its CLI side effects.

    The module has no import-time work besides class/function defs, so a
    plain import works as long as its parent dir is on sys.path.
    """
    sys.path.insert(0, str(REPO_ROOT / "docker-cloud"))
    spec = importlib.util.spec_from_file_location(
        "cloud_entrypoint", CLOUD_ENTRYPOINT_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _make_entrypoint(module: ModuleType):
    """Construct a ConfigFetcher with safe defaults; no AWS/network calls."""
    cls = module.ConfigFetcher
    inst = cls.__new__(cls)
    inst.region = "us-east-1"
    inst.env = "test"
    inst.pipeline_id = "00000000-0000-4000-8000-000000000099"
    inst.org_id = "test-org"
    inst.stream_id = None
    inst.invocation_id = None
    inst.batch_job_id = None
    inst.batch_job_data_lambda = "noop"
    inst.secrets_bucket = "test-bucket"
    inst._s3 = MagicMock()
    inst._lambda = MagicMock()
    return inst


def _minimal_stream(source_ref: dict, dest_ref: dict) -> dict[str, Any]:
    """Smallest stream document accepted by the published stream contract."""
    return {
        "$schema": "https://schemas.analitiq.ai/stream/latest.json",
        "pipeline_id": "00000000-0000-4000-8000-000000000099",
        "source": {"endpoint_ref": source_ref},
        "destinations": [
            {"endpoint_ref": dest_ref, "write": {"mode": "insert"}}
        ],
    }


class TestEndpointRefTranslation:
    """``_translate_endpoint_ref`` must read ``endpoint_id`` and emit
    ``{scope, connection_id, endpoint_id}`` — never the stale ``endpoint`` key.
    """

    @pytest.fixture
    def conn_id_to_slug(self) -> dict[str, str]:
        return {
            "00000000-0000-4000-8000-000000000001": "wise",
            "00000000-0000-4000-8000-000000000002": "prod-pg",
        }

    def test_connection_scope_passthrough(
        self, cloud_entrypoint_module, conn_id_to_slug
    ):
        ep = _make_entrypoint(cloud_entrypoint_module)
        out = ep._translate_endpoint_ref(
            {
                "scope": "connection",
                "connection_id": "00000000-0000-4000-8000-000000000002",
                "endpoint_id": "public_transfers",
            },
            conn_id_to_slug,
        )
        assert out == {
            "scope": "connection",
            "connection_id": "00000000-0000-4000-8000-000000000002",
            "endpoint_id": "public_transfers",
        }

    def test_connector_scope_rewrites_connection_id_to_slug(
        self, cloud_entrypoint_module, conn_id_to_slug
    ):
        ep = _make_entrypoint(cloud_entrypoint_module)
        out = ep._translate_endpoint_ref(
            {
                "scope": "connector",
                "connection_id": "00000000-0000-4000-8000-000000000001",
                "endpoint_id": "transfers",
            },
            conn_id_to_slug,
        )
        assert out == {
            "scope": "connector",
            "connection_id": "wise",
            "endpoint_id": "transfers",
        }

    def test_stale_endpoint_key_is_rejected(
        self, cloud_entrypoint_module, conn_id_to_slug
    ):
        """The pre-rename ``endpoint`` key must not silently round-trip."""
        ep = _make_entrypoint(cloud_entrypoint_module)
        with pytest.raises(ValueError, match="endpoint_id"):
            ep._translate_endpoint_ref(
                {
                    "scope": "connection",
                    "connection_id": "x",
                    "endpoint": "stale_key",
                },
                conn_id_to_slug,
            )


class TestRewriteStreamNodeContract:
    """``_rewrite_stream_node`` output must validate against the published
    stream schema when wrapped in a minimal stream document.
    """

    @pytest.fixture
    def conn_id_to_slug(self) -> dict[str, str]:
        return {
            "00000000-0000-4000-8000-000000000001": "wise",
            "00000000-0000-4000-8000-000000000002": "prod-pg",
        }

    def test_connection_scope_node_validates(
        self, cloud_entrypoint_module, conn_id_to_slug, stream_validator
    ):
        ep = _make_entrypoint(cloud_entrypoint_module)
        source = {
            "endpoint_ref": {
                "scope": "connection",
                "connection_id": "00000000-0000-4000-8000-000000000002",
                "endpoint_id": "public_transfers",
            }
        }
        ep._rewrite_stream_node(source, conn_id_to_slug)
        dest = {
            "endpoint_ref": {
                "scope": "connection",
                "connection_id": "00000000-0000-4000-8000-000000000002",
                "endpoint_id": "public_transfers",
            },
            "write": {"mode": "insert"},
        }
        ep._rewrite_stream_node(dest, conn_id_to_slug)

        # connection_ref is an engine-side sibling that's not part of the
        # published stream contract; strip it before contract validation.
        source.pop("connection_ref", None)
        dest.pop("connection_ref", None)

        doc = {
            "$schema": "https://schemas.analitiq.ai/stream/latest.json",
            "pipeline_id": "00000000-0000-4000-8000-000000000099",
            "source": source,
            "destinations": [dest],
        }
        errors = sorted(stream_validator.iter_errors(doc), key=lambda e: e.path)
        assert errors == [], [
            f"{'/'.join(map(str, e.absolute_path))}: {e.message}" for e in errors
        ]

    def test_connector_scope_node_validates(
        self, cloud_entrypoint_module, conn_id_to_slug, stream_validator
    ):
        ep = _make_entrypoint(cloud_entrypoint_module)
        source = {
            "endpoint_ref": {
                "scope": "connector",
                "connection_id": "00000000-0000-4000-8000-000000000001",
                "endpoint_id": "transfers",
            }
        }
        ep._rewrite_stream_node(source, conn_id_to_slug)
        source.pop("connection_ref", None)

        doc = _minimal_stream(
            source_ref=source["endpoint_ref"],
            dest_ref={
                "scope": "connection",
                "connection_id": "00000000-0000-4000-8000-000000000002",
                "endpoint_id": "public_transfers",
            },
        )
        errors = sorted(stream_validator.iter_errors(doc), key=lambda e: e.path)
        assert errors == [], [
            f"{'/'.join(map(str, e.absolute_path))}: {e.message}" for e in errors
        ]


class TestWriteStreamsContract:
    """End-to-end: ``_write_streams`` writes files whose endpoint_ref blocks
    match the published contract (after stripping the engine-side
    ``connection_ref`` sibling).
    """

    def test_written_stream_endpoint_refs_validate(
        self, cloud_entrypoint_module, stream_validator, tmp_path, monkeypatch
    ):
        ep = _make_entrypoint(cloud_entrypoint_module)
        monkeypatch.setattr(
            cloud_entrypoint_module, "PIPELINES_DIR", tmp_path / "pipelines"
        )

        conn_id_to_slug = {
            "00000000-0000-4000-8000-000000000001": "wise",
        }
        streams = [
            {
                "stream_id": "11111111-1111-4111-8111-111111111111",
                "source": {
                    "endpoint_ref": {
                        "scope": "connector",
                        "connection_id": "00000000-0000-4000-8000-000000000001",
                        "endpoint_id": "transfers",
                    }
                },
                "destinations": [
                    {
                        "endpoint_ref": {
                            "scope": "connection",
                            "connection_id": "00000000-0000-4000-8000-000000000002",
                            "endpoint_id": "public_transfers",
                        },
                        "write": {"mode": "insert"},
                    }
                ],
            }
        ]
        stream_ids = ep._write_streams(streams, conn_id_to_slug)
        assert stream_ids == ["11111111-1111-4111-8111-111111111111"]

        path = (
            tmp_path
            / "pipelines"
            / ep.pipeline_id
            / "streams"
            / "11111111-1111-4111-8111-111111111111.json"
        )
        written = json.loads(path.read_text())

        # Engine-side sibling, not part of the published contract.
        written["source"].pop("connection_ref", None)
        for dest in written["destinations"]:
            dest.pop("connection_ref", None)

        # Add the pipeline_id + $schema markers the contract requires.
        doc = {
            "$schema": "https://schemas.analitiq.ai/stream/latest.json",
            "pipeline_id": ep.pipeline_id,
            "source": written["source"],
            "destinations": written["destinations"],
        }
        errors = sorted(stream_validator.iter_errors(doc), key=lambda e: e.path)
        assert errors == [], [
            f"{'/'.join(map(str, e.absolute_path))}: {e.message}" for e in errors
        ]

        # And: no stale 'endpoint' key anywhere in the emitted refs.
        for node in [written["source"], *written["destinations"]]:
            ref = node["endpoint_ref"]
            assert "endpoint" not in ref, ref
            assert "endpoint_id" in ref, ref
            assert "connection_id" in ref, ref
