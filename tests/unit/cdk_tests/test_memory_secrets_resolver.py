"""Tests for the in-memory secrets resolver (test/dev double).

It resolves declared refs from a per-connection store, mirroring the real
resolver's semantics: no declared refs -> empty result; a declared ref with no
stored value -> fail loud.
"""

import pytest

from cdk.secrets import InMemorySecretsResolver, SecretNotFoundError


async def test_empty_refs_returns_empty_even_for_unknown_connection():
    # No declared refs -> nothing to resolve, so an empty store is not an error
    # (matches SchemeSecretsResolver and the _load_secrets short-circuit).
    resolver = InMemorySecretsResolver({})
    assert await resolver.resolve("unknown-conn", {}) == {}


async def test_resolves_declared_refs_from_store():
    resolver = InMemorySecretsResolver({"c1": {"api_key": "shh", "extra": "x"}})
    out = await resolver.resolve("c1", {"api_key": "sidecar:api_key"})
    assert out == {"api_key": "shh"}


async def test_missing_declared_ref_fails_loud():
    resolver = InMemorySecretsResolver({"c1": {"api_key": "shh"}})
    with pytest.raises(SecretNotFoundError, match="password"):
        await resolver.resolve("c1", {"password": "sidecar:password"})


async def test_unknown_connection_with_declared_refs_fails_loud():
    resolver = InMemorySecretsResolver({})
    with pytest.raises(SecretNotFoundError):
        await resolver.resolve("nope", {"api_key": "sidecar:api_key"})
