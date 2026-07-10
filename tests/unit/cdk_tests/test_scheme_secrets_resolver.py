"""Tests for the scheme-dispatch secrets resolver.

Each ``secret_refs.<name>`` value names its source by scheme; the resolver
dispatches on that scheme. Covered per scheme: a successful resolve and a
missing-source failure. The optional ``s3://`` path is covered both when
``boto3`` is present (mocked) and when it is absent (import guard).
"""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

from cdk.secrets import (
    SchemeSecretsResolver,
    SecretNotFoundError,
    UnsupportedSecretRefScheme,
)
from cdk.secrets.exceptions import SecretResolutionError


@pytest.fixture
def connection_dir(tmp_path):
    """A connection directory with a populated sidecar credentials file."""
    secrets = tmp_path / ".secrets"
    secrets.mkdir()
    (secrets / "credentials.json").write_text(
        json.dumps({"password": "sidecar-pw", "api/key.v2": "sk-nested"})
    )
    return tmp_path


# ---------------------------------------------------------------------------
# env:
# ---------------------------------------------------------------------------


class TestEnvScheme:
    async def test_resolves_env_var(self, connection_dir, monkeypatch):
        monkeypatch.setenv("PG_PASSWORD", "from-env")
        resolver = SchemeSecretsResolver(connection_dir)
        out = await resolver.resolve("conn", {"password": "env:PG_PASSWORD"})
        assert out == {"password": "from-env"}

    async def test_empty_env_var_is_a_value(self, connection_dir, monkeypatch):
        # A set-but-empty variable is a present value, not a missing source.
        monkeypatch.setenv("EMPTY_VAR", "")
        resolver = SchemeSecretsResolver(connection_dir)
        out = await resolver.resolve("conn", {"x": "env:EMPTY_VAR"})
        assert out == {"x": ""}

    async def test_missing_env_var_fails_loud(self, connection_dir, monkeypatch):
        monkeypatch.delenv("NOT_SET_VAR", raising=False)
        resolver = SchemeSecretsResolver(connection_dir)
        with pytest.raises(SecretNotFoundError, match="NOT_SET_VAR"):
            await resolver.resolve("conn", {"x": "env:NOT_SET_VAR"})


# ---------------------------------------------------------------------------
# file:
# ---------------------------------------------------------------------------


class TestFileScheme:
    async def test_resolves_relative_file(self, connection_dir):
        (connection_dir / "pw.txt").write_text("file-pw")
        resolver = SchemeSecretsResolver(connection_dir)
        out = await resolver.resolve("conn", {"password": "file:./pw.txt"})
        assert out == {"password": "file-pw"}

    async def test_single_trailing_newline_is_stripped(self, connection_dir):
        (connection_dir / "pw.txt").write_text("file-pw\n")
        resolver = SchemeSecretsResolver(connection_dir)
        out = await resolver.resolve("conn", {"password": "file:pw.txt"})
        assert out == {"password": "file-pw"}

    async def test_interior_whitespace_preserved(self, connection_dir):
        (connection_dir / "pw.txt").write_text("  spaced secret  \n")
        resolver = SchemeSecretsResolver(connection_dir)
        out = await resolver.resolve("conn", {"password": "file:pw.txt"})
        assert out == {"password": "  spaced secret  "}

    async def test_missing_file_fails_loud(self, connection_dir):
        resolver = SchemeSecretsResolver(connection_dir)
        with pytest.raises(SecretNotFoundError, match="no file at"):
            await resolver.resolve("conn", {"x": "file:./nope.txt"})

    async def test_path_traversal_is_rejected(self, connection_dir):
        resolver = SchemeSecretsResolver(connection_dir)
        with pytest.raises(SecretResolutionError, match="escapes the connection"):
            await resolver.resolve("conn", {"x": "file:../../etc/hosts"})

    async def test_symlink_escaping_dir_is_rejected(self, connection_dir, tmp_path):
        outside = tmp_path.parent / "outside-secret.txt"
        outside.write_text("leaked")
        link = connection_dir / "link.txt"
        link.symlink_to(outside)
        resolver = SchemeSecretsResolver(connection_dir)
        with pytest.raises(SecretResolutionError, match="escapes the connection"):
            await resolver.resolve("conn", {"x": "file:link.txt"})


# ---------------------------------------------------------------------------
# sidecar:
# ---------------------------------------------------------------------------


class TestSidecarScheme:
    async def test_resolves_sidecar_entry(self, connection_dir):
        resolver = SchemeSecretsResolver(connection_dir)
        out = await resolver.resolve("conn", {"password": "sidecar:password"})
        assert out == {"password": "sidecar-pw"}

    async def test_nested_key_name_is_representable(self, connection_dir):
        # The sidecar key part accepts any characters (/, ., -).
        resolver = SchemeSecretsResolver(connection_dir)
        out = await resolver.resolve("conn", {"k": "sidecar:api/key.v2"})
        assert out == {"k": "sk-nested"}

    async def test_missing_sidecar_key_fails_loud(self, connection_dir):
        resolver = SchemeSecretsResolver(connection_dir)
        with pytest.raises(SecretNotFoundError, match="not found in"):
            await resolver.resolve("conn", {"x": "sidecar:absent"})

    async def test_missing_sidecar_file_fails_loud(self, tmp_path):
        resolver = SchemeSecretsResolver(tmp_path)  # no .secrets/credentials.json
        with pytest.raises(SecretNotFoundError, match="sidecar credentials file"):
            await resolver.resolve("conn", {"x": "sidecar:password"})

    async def test_invalid_sidecar_json_fails_loud(self, tmp_path):
        (tmp_path / ".secrets").mkdir()
        (tmp_path / ".secrets" / "credentials.json").write_text("{not json")
        resolver = SchemeSecretsResolver(tmp_path)
        with pytest.raises(SecretResolutionError, match="invalid JSON"):
            await resolver.resolve("conn", {"x": "sidecar:password"})


# ---------------------------------------------------------------------------
# s3:// (optional [s3] extra)
# ---------------------------------------------------------------------------


class TestS3Scheme:
    def _mock_boto3(self, body: bytes):
        client = MagicMock()
        client.get_object.return_value = {"Body": MagicMock(read=lambda: body)}
        module = MagicMock()
        module.client.return_value = client
        return module, client

    async def test_resolves_s3_object(self, connection_dir):
        module, client = self._mock_boto3(b"s3-secret\n")
        resolver = SchemeSecretsResolver(connection_dir)
        with patch.dict(sys.modules, {"boto3": module}):
            out = await resolver.resolve("conn", {"tok": "s3://bkt/path/to/key"})
        assert out == {"tok": "s3-secret"}  # trailing newline stripped
        client.get_object.assert_called_once_with(Bucket="bkt", Key="path/to/key")

    async def test_custom_endpoint_and_region_are_passed(self, connection_dir):
        module, _ = self._mock_boto3(b"v")
        resolver = SchemeSecretsResolver(
            connection_dir,
            s3_endpoint_url="http://minio:9000",
            s3_region="us-east-1",
        )
        with patch.dict(sys.modules, {"boto3": module}):
            await resolver.resolve("conn", {"tok": "s3://bkt/key"})
        module.client.assert_called_once_with(
            "s3", endpoint_url="http://minio:9000", region_name="us-east-1"
        )

    async def test_missing_extra_fails_loud(self, connection_dir):
        resolver = SchemeSecretsResolver(connection_dir)
        # Simulate boto3 not installed: import boto3 -> ImportError.
        with patch.dict(sys.modules, {"boto3": None}):
            with pytest.raises(SecretResolutionError, match=r"\[s3\] extra"):
                await resolver.resolve("conn", {"tok": "s3://bkt/key"})

    async def test_missing_object_fails_loud(self, connection_dir):
        from botocore.exceptions import ClientError

        module = MagicMock()
        client = MagicMock()
        client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "not there"}}, "GetObject"
        )
        module.client.return_value = client
        resolver = SchemeSecretsResolver(connection_dir)
        with patch.dict(sys.modules, {"boto3": module}):
            with pytest.raises(SecretResolutionError, match="NoSuchKey"):
                await resolver.resolve("conn", {"tok": "s3://bkt/key"})

    async def test_malformed_s3_uri_fails_loud(self, connection_dir):
        module, _ = self._mock_boto3(b"v")
        resolver = SchemeSecretsResolver(connection_dir)
        with patch.dict(sys.modules, {"boto3": module}):
            with pytest.raises(SecretResolutionError, match="malformed s3 URI"):
                await resolver.resolve("conn", {"tok": "s3://bucket-only"})


# ---------------------------------------------------------------------------
# Scheme rejection: unsupported / bare
# ---------------------------------------------------------------------------


class TestSchemeRejection:
    @pytest.mark.parametrize(
        "locator",
        [
            "arn:aws:secretsmanager:us-east-1:123456789012:secret:db",
            "ssm:/analitiq/db/password",
        ],
    )
    async def test_cloud_vault_schemes_are_unsupported(self, connection_dir, locator):
        resolver = SchemeSecretsResolver(connection_dir)
        with pytest.raises(UnsupportedSecretRefScheme):
            await resolver.resolve("conn", {"x": locator})

    async def test_bare_value_is_rejected(self, connection_dir):
        resolver = SchemeSecretsResolver(connection_dir)
        with pytest.raises(UnsupportedSecretRefScheme, match="no scheme"):
            await resolver.resolve("conn", {"x": "just-a-raw-token"})

    async def test_non_string_locator_is_rejected(self, connection_dir):
        resolver = SchemeSecretsResolver(connection_dir)
        with pytest.raises(SecretResolutionError, match="must be a string"):
            await resolver.resolve("conn", {"x": 1234})  # type: ignore[dict-item]


# ---------------------------------------------------------------------------
# Multi-ref + lifecycle
# ---------------------------------------------------------------------------


class TestResolveAll:
    async def test_resolves_mixed_schemes(self, connection_dir, monkeypatch):
        monkeypatch.setenv("API_KEY", "env-key")
        (connection_dir / "tok.txt").write_text("file-tok\n")
        resolver = SchemeSecretsResolver(connection_dir)
        out = await resolver.resolve(
            "conn",
            {
                "api_key": "env:API_KEY",
                "token": "file:tok.txt",
                "password": "sidecar:password",
            },
        )
        assert out == {
            "api_key": "env-key",
            "token": "file-tok",
            "password": "sidecar-pw",
        }

    async def test_empty_refs_returns_empty(self, connection_dir):
        resolver = SchemeSecretsResolver(connection_dir)
        assert await resolver.resolve("conn", {}) == {}

    async def test_close_clears_sidecar_cache(self, connection_dir):
        resolver = SchemeSecretsResolver(connection_dir)
        await resolver.resolve("conn", {"password": "sidecar:password"})
        assert resolver._sidecar_cache is not None
        await resolver.close()
        assert resolver._sidecar_cache is None
