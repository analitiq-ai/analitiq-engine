"""Scheme-dispatch secrets resolver.

Each ``secret_refs.<name>`` value carries an explicit scheme that names
*where* the secret comes from, so an authored connection is self-describing
about its secret sources and the engine stays runnable with zero external
services for the common cases:

    env:VAR         value from environment variable ``VAR``
    file:./path     contents of a local file, relative to the connection dir
    sidecar:<name>  entry ``<name>`` in the connection's local credentials file
    s3://bucket/key object from S3 / an S3-compatible store (``[s3]`` extra)

``env:``, ``file:`` and ``sidecar:`` are built-in and add no dependencies.
``s3://`` lazily imports ``boto3`` (installed with the ``[s3]`` extra), so the
core engine stays dependency-light and cloud-free unless a connection actually
uses it.

The public contract also permits the operator's own cloud-vault pointers
(``arn:aws:*``, ``ssm:/*``); those are not resolvable by the open-source engine
and fail loud here (:class:`UnsupportedSecretRefScheme`). Every value carries an
explicit scheme -- a bare token has none and is likewise rejected, since raw
secret material never belongs in ``secret_refs``.
"""

import asyncio
import json
import logging
import os
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlparse

from cdk.secrets.exceptions import (
    SecretNotFoundError,
    SecretResolutionError,
    UnsupportedSecretRefScheme,
)
from cdk.secrets.protocol import SecretsResolver

logger = logging.getLogger(__name__)

# The sidecar credentials file lives beside the connection config, under
# ``.secrets/``. It is the one file the ``sidecar:`` scheme reads from.
_SIDECAR_DIR = ".secrets"
_SIDECAR_FILENAME = "credentials.json"

# Schemes the public contract permits but the open-source engine cannot resolve
# (they name the operator's own cloud vault). Their keyword is safe to name in an
# error; any other unknown scheme is treated as possibly-raw and never echoed.
_CONTRACT_ONLY_SCHEMES = frozenset({"arn", "ssm"})


def _strip_trailing_newline(text: str) -> str:
    r"""Drop a single trailing newline from a file/object payload.

    Secret files are commonly written with a trailing newline that is not part
    of the secret (``echo secret > file``). Exactly one trailing ``\n`` (or
    ``\r\n``) is removed; the value is otherwise verbatim, so intentional
    interior or leading whitespace is preserved.
    """
    if text.endswith("\r\n"):
        return text[:-2]
    if text.endswith("\n"):
        return text[:-1]
    return text


class SchemeSecretsResolver(SecretsResolver):
    """Resolve each declared ``secret_refs.<name>`` by the scheme its value carries.

    Constructed per connection with the connection directory: ``file:`` paths
    resolve relative to it (and are scope-checked so ``..`` cannot escape), and
    the ``sidecar:`` scheme reads ``<connection_dir>/.secrets/credentials.json``.
    ``s3_endpoint_url`` / ``s3_region`` are passed through to the S3 client so
    S3-compatible stores (e.g. MinIO) work; ``None`` lets ``boto3`` use its own
    environment/config defaults.
    """

    def __init__(
        self,
        connection_dir: str | Path,
        *,
        s3_endpoint_url: str | None = None,
        s3_region: str | None = None,
    ) -> None:
        self._connection_dir = Path(connection_dir)
        self._s3_endpoint_url = s3_endpoint_url
        self._s3_region = s3_region
        # The sidecar file is read at most once per resolver, on first use.
        self._sidecar_cache: dict[str, object] | None = None

    async def resolve(
        self,
        connection_id: str,
        secret_refs: Mapping[str, str],
    ) -> dict[str, str]:
        """Resolve every declared ref to its value, keyed by ref name.

        Fails loud on the first unresolvable ref (missing env var, missing
        file/object, missing sidecar entry, unsupported scheme) -- never falls
        back to an empty or silent secret.

        The sidecar file is cached only for the duration of this call (read once
        across multiple ``sidecar:`` refs) and dropped before returning, so the
        resolver holds no secret material between resolutions and a rotated
        credentials file is picked up on the next call.
        """
        resolved: dict[str, str] = {}
        try:
            for name, locator in secret_refs.items():
                if not isinstance(locator, str):
                    raise SecretResolutionError(
                        f"secret_ref {name!r} value must be a string, "
                        f"got {type(locator).__name__}",
                        connection_id,
                    )
                resolved[name] = await self._resolve_one(connection_id, name, locator)
        finally:
            self._sidecar_cache = None
        logger.debug("Resolved %d secret_ref(s) for %s", len(resolved), connection_id)
        return resolved

    async def _resolve_one(self, connection_id: str, name: str, locator: str) -> str:
        scheme, sep, _ = locator.partition(":")
        if not sep:
            # A bare value in this branch is likely a pasted raw secret, so the
            # locator is NEVER echoed into the error (which reaches logs) -- only
            # the ref name is named.
            raise UnsupportedSecretRefScheme(
                "<none>",
                connection_id,
                detail=(
                    f"secret_ref {name!r} has no scheme; a bare value is never "
                    "accepted -- use env:/file:/sidecar:/s3://"
                ),
            )
        if scheme == "env":
            return self._resolve_env(connection_id, name, locator)
        if scheme == "file":
            return self._resolve_file(connection_id, name, locator)
        if scheme == "sidecar":
            return self._resolve_sidecar(connection_id, name, locator)
        if scheme == "s3":
            return await asyncio.to_thread(
                self._resolve_s3, connection_id, name, locator
            )
        if scheme in _CONTRACT_ONLY_SCHEMES:
            # Valid public-contract schemes the open-source engine does not
            # resolve. The scheme keyword is a known token (safe to name); the
            # locator is not echoed to keep every rejection message leak-proof.
            raise UnsupportedSecretRefScheme(
                scheme,
                connection_id,
                detail=(
                    f"secret_ref {name!r} uses the {scheme}: scheme, which this "
                    "engine does not resolve; supported schemes are env:, file:, "
                    "sidecar: (built-in) and s3:// (with the s3 extra)"
                ),
            )
        # An unrecognized scheme may be a raw secret that happens to contain a
        # colon, so neither the scheme token nor the locator is echoed.
        raise UnsupportedSecretRefScheme(
            "<unrecognized>",
            connection_id,
            detail=(
                f"secret_ref {name!r} has an unrecognized or malformed scheme; a "
                "raw secret is never accepted -- use env:/file:/sidecar:/s3://"
            ),
        )

    # -- schemes ---------------------------------------------------------

    @staticmethod
    def _resolve_env(connection_id: str, name: str, locator: str) -> str:
        # No instance state: the environment is process-global (unlike file:,
        # sidecar:, s3: which read from the connection dir / resolver config).
        var = locator[len("env:") :]
        try:
            return os.environ[var]
        except KeyError:
            raise SecretNotFoundError(
                connection_id=connection_id,
                detail=(
                    f"secret_ref {name!r} -> {locator}: environment variable "
                    f"{var!r} is not set"
                ),
            ) from None

    def _resolve_file(self, connection_id: str, name: str, locator: str) -> str:
        rel = locator[len("file:") :]
        base = self._connection_dir.resolve()
        target = (base / rel).resolve()
        # The contract's regex is structural, not a traversal guard: canonicalize
        # and confirm the target stays within the connection directory so a
        # ``..`` sequence cannot read an arbitrary file.
        if target != base and base not in target.parents:
            raise SecretResolutionError(
                f"secret_ref {name!r} -> {locator}: path escapes the connection "
                f"directory {base}",
                connection_id,
            )
        try:
            content = target.read_text(encoding="utf-8")
        except FileNotFoundError:
            raise SecretNotFoundError(
                connection_id=connection_id,
                detail=f"secret_ref {name!r} -> {locator}: no file at {target}",
            ) from None
        except OSError as e:
            raise SecretResolutionError(
                f"secret_ref {name!r} -> {locator}: {e}", connection_id
            ) from e
        return _strip_trailing_newline(content)

    def _resolve_sidecar(self, connection_id: str, name: str, locator: str) -> str:
        key = locator[len("sidecar:") :]
        sidecar = self._load_sidecar(connection_id)
        try:
            value = sidecar[key]
        except KeyError:
            raise SecretNotFoundError(
                connection_id=connection_id,
                detail=(
                    f"secret_ref {name!r} -> {locator}: key {key!r} not found in "
                    f"{_SIDECAR_DIR}/{_SIDECAR_FILENAME}"
                ),
            ) from None
        return str(value)

    def _resolve_s3(self, connection_id: str, name: str, locator: str) -> str:
        # Lazy import keeps the core engine cloud-free and dependency-light: boto3
        # loads only when a connection actually uses an s3:// ref (the [s3] extra).
        try:
            import boto3  # skipcq: PYL-C0415
            import botocore.exceptions as boto  # skipcq: PYL-C0415
        except ImportError as e:
            raise SecretResolutionError(
                f"secret_ref {name!r} -> {locator}: the s3:// scheme requires "
                "boto3; install the optional 's3' extra of the package that "
                "supplies this resolver (e.g. analitiq-cdk[s3] or "
                "analitiq-core[s3])",
                connection_id,
            ) from e

        parsed = urlparse(locator)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise SecretResolutionError(
                f"secret_ref {name!r} -> {locator}: malformed s3 URI "
                "(expected s3://bucket/key)",
                connection_id,
            )
        # Client construction is inside the wrapped block: a bad profile/region/
        # endpoint raises at client() time, and it must surface as a
        # SecretResolutionError like every other failure, not a raw boto error.
        try:
            client = boto3.client(
                "s3",
                endpoint_url=self._s3_endpoint_url,
                region_name=self._s3_region,
            )
            obj = client.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read()
        except (boto.BotoCoreError, boto.ClientError) as e:
            raise SecretResolutionError(
                f"secret_ref {name!r} -> {locator}: {e}", connection_id
            ) from e
        return _strip_trailing_newline(body.decode("utf-8"))

    # -- sidecar file ----------------------------------------------------

    def _load_sidecar(self, connection_id: str) -> Mapping[str, object]:
        if self._sidecar_cache is not None:
            return self._sidecar_cache
        path = self._connection_dir / _SIDECAR_DIR / _SIDECAR_FILENAME
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            raise SecretNotFoundError(
                connection_id=connection_id,
                detail=f"sidecar credentials file not found at {path}",
            ) from None
        except OSError as e:
            raise SecretResolutionError(
                f"error reading sidecar credentials file {path}: {e}",
                connection_id,
            ) from e
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise SecretResolutionError(
                f"invalid JSON in sidecar credentials file {path}: {e}",
                connection_id,
            ) from e
        if not isinstance(data, dict):
            raise SecretResolutionError(
                f"sidecar credentials file {path} must contain a JSON object",
                connection_id,
            )
        self._sidecar_cache = data
        return data

    async def close(self) -> None:
        """Drop the cached sidecar contents."""
        self._sidecar_cache = None

    def __repr__(self) -> str:
        """Return the string representation."""
        return f"SchemeSecretsResolver({self._connection_dir})"
