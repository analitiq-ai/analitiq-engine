"""Tests for the scheme-dispatch secrets resolver.

Each ``secret_refs.<name>`` value names its source by scheme; the resolver
dispatches on that scheme. Covered per scheme: a successful resolve and a
missing-source failure. The optional ``s3://`` path is exercised with a fully
faked ``boto3`` + ``botocore`` so it runs hermetically whether or not the
``[s3]`` extra is installed, plus the import-guard when ``boto3`` is absent.
"""

import json
import sys
import types
from contextlib import contextmanager
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
# Fake boto3 / botocore so the s3:// path runs without the [s3] extra
# ---------------------------------------------------------------------------


def _fake_botocore():
    """Return stand-in ``botocore`` / ``botocore.exceptions`` modules.

    ``_resolve_s3`` catches ``botocore.exceptions.{BotoCoreError, ClientError}``;
    faking them keeps the s3 tests independent of the optional dependency.
    """
    exceptions = types.ModuleType("botocore.exceptions")

    class BotoCoreError(Exception):
        pass

    class ClientError(Exception):
        pass

    exceptions.BotoCoreError = BotoCoreError
    exceptions.ClientError = ClientError
    botocore = types.ModuleType("botocore")
    botocore.exceptions = exceptions
    return botocore, exceptions


@contextmanager
def _patched_s3(*, body=None, error=None):
    """Patch ``boto3`` + ``botocore`` in ``sys.modules`` and yield the client mock.

    ``body`` is what ``get_object`` returns; ``error`` (a class) is raised from
    ``get_object`` instead.
    """
    botocore, exceptions = _fake_botocore()
    client = MagicMock()
    if error is not None:
        client.get_object.side_effect = error
    else:
        client.get_object.return_value = {"Body": MagicMock(read=lambda: body)}
    boto3 = MagicMock()
    boto3.client.return_value = client
    with patch.dict(
        sys.modules,
        {"boto3": boto3, "botocore": botocore, "botocore.exceptions": exceptions},
    ):
        yield boto3, client, exceptions


# ---------------------------------------------------------------------------
# env:
# ---------------------------------------------------------------------------


async def test_env_resolves_var(connection_dir, monkeypatch):
    monkeypatch.setenv("PG_PASSWORD", "from-env")
    resolver = SchemeSecretsResolver(connection_dir)
    assert await resolver.resolve("conn", {"password": "env:PG_PASSWORD"}) == {
        "password": "from-env"
    }


async def test_env_empty_var_is_a_value(connection_dir, monkeypatch):
    # A set-but-empty variable is a present value, not a missing source.
    monkeypatch.setenv("EMPTY_VAR", "")
    resolver = SchemeSecretsResolver(connection_dir)
    assert await resolver.resolve("conn", {"x": "env:EMPTY_VAR"}) == {"x": ""}


async def test_env_missing_var_fails_loud(connection_dir, monkeypatch):
    monkeypatch.delenv("NOT_SET_VAR", raising=False)
    resolver = SchemeSecretsResolver(connection_dir)
    with pytest.raises(SecretNotFoundError, match="NOT_SET_VAR"):
        await resolver.resolve("conn", {"x": "env:NOT_SET_VAR"})


# ---------------------------------------------------------------------------
# file:
# ---------------------------------------------------------------------------


async def test_file_resolves_relative(connection_dir):
    (connection_dir / "pw.txt").write_text("file-pw")
    resolver = SchemeSecretsResolver(connection_dir)
    assert await resolver.resolve("conn", {"password": "file:./pw.txt"}) == {
        "password": "file-pw"
    }


async def test_file_single_trailing_newline_is_stripped(connection_dir):
    (connection_dir / "pw.txt").write_text("file-pw\n")
    resolver = SchemeSecretsResolver(connection_dir)
    assert await resolver.resolve("conn", {"password": "file:pw.txt"}) == {
        "password": "file-pw"
    }


async def test_file_interior_whitespace_preserved(connection_dir):
    (connection_dir / "pw.txt").write_text("  spaced secret  \n")
    resolver = SchemeSecretsResolver(connection_dir)
    assert await resolver.resolve("conn", {"password": "file:pw.txt"}) == {
        "password": "  spaced secret  "
    }


async def test_file_missing_fails_loud(connection_dir):
    resolver = SchemeSecretsResolver(connection_dir)
    with pytest.raises(SecretNotFoundError, match="no file at"):
        await resolver.resolve("conn", {"x": "file:./nope.txt"})


async def test_file_path_traversal_is_rejected(connection_dir):
    resolver = SchemeSecretsResolver(connection_dir)
    with pytest.raises(SecretResolutionError, match="escapes the connection"):
        await resolver.resolve("conn", {"x": "file:../../etc/hosts"})


async def test_file_symlink_escaping_dir_is_rejected(connection_dir, tmp_path):
    outside = tmp_path.parent / "outside-secret.txt"
    outside.write_text("leaked")
    (connection_dir / "link.txt").symlink_to(outside)
    resolver = SchemeSecretsResolver(connection_dir)
    with pytest.raises(SecretResolutionError, match="escapes the connection"):
        await resolver.resolve("conn", {"x": "file:link.txt"})


# ---------------------------------------------------------------------------
# sidecar:
# ---------------------------------------------------------------------------


async def test_sidecar_resolves_entry(connection_dir):
    resolver = SchemeSecretsResolver(connection_dir)
    assert await resolver.resolve("conn", {"password": "sidecar:password"}) == {
        "password": "sidecar-pw"
    }


async def test_sidecar_nested_key_name_is_representable(connection_dir):
    # The sidecar key part accepts any characters (/, ., -).
    resolver = SchemeSecretsResolver(connection_dir)
    assert await resolver.resolve("conn", {"k": "sidecar:api/key.v2"}) == {
        "k": "sk-nested"
    }


async def test_sidecar_missing_key_fails_loud(connection_dir):
    resolver = SchemeSecretsResolver(connection_dir)
    with pytest.raises(SecretNotFoundError, match="not found in"):
        await resolver.resolve("conn", {"x": "sidecar:absent"})


async def test_sidecar_missing_file_fails_loud(tmp_path):
    resolver = SchemeSecretsResolver(tmp_path)  # no .secrets/credentials.json
    with pytest.raises(SecretNotFoundError, match="sidecar credentials file"):
        await resolver.resolve("conn", {"x": "sidecar:password"})


async def test_sidecar_invalid_json_fails_loud(tmp_path):
    (tmp_path / ".secrets").mkdir()
    (tmp_path / ".secrets" / "credentials.json").write_text("{not json")
    resolver = SchemeSecretsResolver(tmp_path)
    with pytest.raises(SecretResolutionError, match="invalid JSON"):
        await resolver.resolve("conn", {"x": "sidecar:password"})


# ---------------------------------------------------------------------------
# s3:// (optional [s3] extra)
# ---------------------------------------------------------------------------


async def test_s3_resolves_object(connection_dir):
    resolver = SchemeSecretsResolver(connection_dir)
    with _patched_s3(body=b"s3-secret\n") as (_boto3, client, _exc):
        out = await resolver.resolve("conn", {"tok": "s3://bkt/path/to/key"})
    assert out == {"tok": "s3-secret"}  # trailing newline stripped
    client.get_object.assert_called_once_with(Bucket="bkt", Key="path/to/key")


async def test_s3_custom_endpoint_and_region_are_passed(connection_dir):
    resolver = SchemeSecretsResolver(
        connection_dir, s3_endpoint_url="http://minio:9000", s3_region="us-east-1"
    )
    with _patched_s3(body=b"v") as (boto3, _client, _exc):
        await resolver.resolve("conn", {"tok": "s3://bkt/key"})
    boto3.client.assert_called_once_with(
        "s3", endpoint_url="http://minio:9000", region_name="us-east-1"
    )


async def test_s3_missing_extra_fails_loud(connection_dir):
    resolver = SchemeSecretsResolver(connection_dir)
    # Simulate boto3 not installed: import boto3 -> ImportError.
    with patch.dict(sys.modules, {"boto3": None}), pytest.raises(
        SecretResolutionError, match=r"'s3' extra"
    ):
        await resolver.resolve("conn", {"tok": "s3://bkt/key"})


async def test_s3_missing_object_fails_loud(connection_dir):
    resolver = SchemeSecretsResolver(connection_dir)
    with _patched_s3() as (_boto3, client, exc):
        client.get_object.side_effect = exc.ClientError("NoSuchKey: not there")
        with pytest.raises(SecretResolutionError, match="NoSuchKey"):
            await resolver.resolve("conn", {"tok": "s3://bkt/key"})


async def test_s3_malformed_uri_fails_loud(connection_dir):
    resolver = SchemeSecretsResolver(connection_dir)
    with _patched_s3(body=b"v"), pytest.raises(
        SecretResolutionError, match="malformed s3 URI"
    ):
        await resolver.resolve("conn", {"tok": "s3://bucket-only"})


# ---------------------------------------------------------------------------
# Scheme rejection: unsupported / bare
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "scheme,locator",
    [
        ("arn", "arn:aws:secretsmanager:us-east-1:123456789012:secret:db"),
        ("ssm", "ssm:/analitiq/db/password"),
    ],
)
async def test_cloud_vault_schemes_are_unsupported(connection_dir, scheme, locator):
    # arn:/ssm: are valid contract schemes the engine cannot resolve; the scheme
    # keyword is named, but the locator (which may carry account ids) is not.
    resolver = SchemeSecretsResolver(connection_dir)
    with pytest.raises(UnsupportedSecretRefScheme, match=scheme) as exc_info:
        await resolver.resolve("conn", {"x": locator})
    assert locator not in str(exc_info.value)


async def test_bare_value_is_rejected_without_echoing_it(connection_dir):
    # A bare value is likely a pasted raw secret; the error (which reaches logs)
    # must name the ref but never echo the value.
    resolver = SchemeSecretsResolver(connection_dir)
    with pytest.raises(UnsupportedSecretRefScheme, match="no scheme") as exc_info:
        await resolver.resolve("conn", {"x": "s3cr3t-raw-token"})
    assert "s3cr3t-raw-token" not in str(exc_info.value)


async def test_colon_containing_raw_secret_is_not_echoed(connection_dir):
    # A pasted secret that happens to contain a colon parses as an unknown
    # scheme; neither the value nor its leading segment may reach the message.
    resolver = SchemeSecretsResolver(connection_dir)
    raw = "abc123:def456:ghi789"
    with pytest.raises(UnsupportedSecretRefScheme, match="unrecognized") as exc_info:
        await resolver.resolve("conn", {"x": raw})
    msg = str(exc_info.value)
    assert raw not in msg and "abc123" not in msg


async def test_non_string_locator_is_rejected(connection_dir):
    resolver = SchemeSecretsResolver(connection_dir)
    with pytest.raises(SecretResolutionError, match="must be a string"):
        await resolver.resolve("conn", {"x": 1234})  # type: ignore[dict-item]


# ---------------------------------------------------------------------------
# Multi-ref + cache lifecycle
# ---------------------------------------------------------------------------


async def test_resolves_mixed_schemes(connection_dir, monkeypatch):
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
    assert out == {"api_key": "env-key", "token": "file-tok", "password": "sidecar-pw"}


async def test_empty_refs_returns_empty(connection_dir):
    resolver = SchemeSecretsResolver(connection_dir)
    assert await resolver.resolve("conn", {}) == {}


async def test_sidecar_cache_dropped_after_resolve(connection_dir):
    # The resolver retains no sidecar material once resolve() returns, so a
    # rotated credentials file is re-read on the next call.
    resolver = SchemeSecretsResolver(connection_dir)
    await resolver.resolve("conn", {"password": "sidecar:password"})
    assert resolver._sidecar_cache is None

    creds = connection_dir / ".secrets" / "credentials.json"
    creds.write_text(json.dumps({"password": "rotated-pw"}))
    out = await resolver.resolve("conn", {"password": "sidecar:password"})
    assert out == {"password": "rotated-pw"}


async def test_cache_is_dropped_even_on_failure(connection_dir):
    resolver = SchemeSecretsResolver(connection_dir)
    with pytest.raises(SecretNotFoundError):
        await resolver.resolve(
            "conn", {"ok": "sidecar:password", "bad": "sidecar:absent"}
        )
    assert resolver._sidecar_cache is None
