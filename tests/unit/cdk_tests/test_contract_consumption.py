"""The committed ``contract-consumption`` manifest matches the contract it names.

The census itself (``tools/contract_consumption.py``) is a full mypy build
and runs as a CI step, not here. What this suite pins is cheap and does not
need mypy: that the committed document is the shape the contract repo
vendors, that it names this CDK's version and the installed contract-models,
and that every claim lands on a field the installed contract actually
declares on a model reachable from a declared root. A contract-models bump
that renames or drops a field the engine reads fails here before the engine
does at runtime.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import re

import pytest
from pydantic import BaseModel

import cdk
from cdk.contract_consumption import (
    CONTRACT_CONSUMPTION_PATH,
    contract_models,
    load_published_consumption,
    model_name,
    reachable_models,
)

pytestmark = pytest.mark.unit

_SITE = re.compile(r"^[a-z_][a-z0-9_.]*:([1-9][0-9]*|[A-Z_][A-Z0-9_]*)$")


def _model(name: str) -> type[BaseModel]:
    module, _, attribute = name.rpartition(".")
    found = getattr(importlib.import_module(module), attribute)
    assert isinstance(found, type) and issubclass(found, BaseModel), name
    return found


@pytest.fixture(scope="module")
def manifest() -> dict:
    return load_published_consumption()


def test_manifest_is_committed_beside_the_module() -> None:
    assert CONTRACT_CONSUMPTION_PATH.is_file()


def test_manifest_names_this_release_and_the_installed_contract(manifest: dict) -> None:
    # The publisher and the wheel check read these; the version is the cdk-v*
    # release coordinate, not a hand-bumped number.
    assert manifest["version"] == cdk.__version__
    # The S3 key is v{version} and the publisher orders versions as plain
    # semver; a pre-release CDK version would pass the tag check, ship to
    # PyPI, and only then be refused by the sync. Refused here instead, on
    # the PR that bumps the version.
    assert re.fullmatch(
        r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)", manifest["version"]
    )
    assert manifest["contract_models_version"] == importlib.metadata.version(
        "analitiq-contract-models"
    )
    assert manifest["scope"] == {"runtime": ["cdk", "src"], "kit": ["cdk.conformance"]}


def test_every_root_is_an_installed_contract_model(manifest: dict) -> None:
    for name in manifest["roots"]:
        assert name.startswith("analitiq.contracts."), name
        _model(name)


def test_every_claim_lands_on_a_declared_field_of_a_reachable_model(
    manifest: dict,
) -> None:
    reachable = reachable_models(_model(name) for name in manifest["roots"])
    for section in ("claims", "kit_reads"):
        for name, fields in manifest[section].items():
            assert (
                name in reachable
            ), f"{section}: {name} is not reachable from the roots"
            declared = reachable[name].model_fields
            for field, sites in fields.items():
                assert (
                    field in declared
                ), f"{section}: {name}.{field} is not a contract field"
                assert sites and len(sites) == len(
                    set(sites)
                ), f"{name}.{field}: {sites}"
                for site in sites:
                    assert _SITE.match(site), site


def test_claims_come_from_runtime_modules_and_kit_reads_from_the_kit(
    manifest: dict,
) -> None:
    def modules(section: str) -> set[str]:
        return {
            site.split(":")[0]
            for fields in manifest[section].values()
            for sites in fields.values()
            for site in sites
        }

    kit = "cdk.conformance"
    assert all(not m.startswith(kit) for m in modules("claims"))
    assert all(m.startswith(kit) for m in modules("kit_reads"))
    assert all(
        m.startswith("src.") for m in {s.split(":")[0] for s in manifest["transport"]}
    )


def test_opaque_registrations_are_reachable_and_prove_a_live_consumer(
    manifest: dict,
) -> None:
    reachable = reachable_models(_model(name) for name in manifest["roots"])
    for name, entry in manifest["opaque"].items():
        assert name in reachable, name
        importlib.import_module(entry["consumer"])
        # A registration nothing dumps and no consumer reads is dead, and
        # would mask that model's unread fields forever.
        assert entry["dumps"] or entry["entries"], name
        for site in entry["dumps"] + entry["entries"]:
            assert _SITE.match(site), site
        assert all(s.startswith(entry["consumer"] + ":") for s in entry["entries"])


def test_the_known_reads_and_non_reads_hold(manifest: dict) -> None:
    # The state issue #460 was opened on: the reads it verified, and the
    # unread field the typing refactor found. A census that stops seeing
    # these has lost its receiver types, not the engine its reads.
    param = manifest["claims"]["analitiq.contracts.endpoints.Param"]
    assert "style" in param and "explode" in param
    assert "operators" not in param
    request = manifest["claims"]["analitiq.contracts.endpoints.GetReadRequest"]
    assert "transport_ref" in request


def test_contract_models_unwraps_unions_annotated_and_containers() -> None:
    from typing import Annotated, Optional

    class Leaf(BaseModel):
        x: int

    class Other(BaseModel):
        y: int

    assert contract_models(Leaf) == [Leaf]
    assert contract_models(Optional[Leaf]) == [Leaf]
    assert contract_models(Annotated[Leaf | Other, "tag"]) == [Other, Leaf]
    assert contract_models(list[Leaf]) == [Leaf]
    assert contract_models(dict[str, Leaf]) == [Leaf]
    assert contract_models(int) == []


def test_reachable_models_follows_field_annotations() -> None:
    class Leaf(BaseModel):
        x: int

    class Branch(BaseModel):
        leaves: list[Leaf]

    class Root(BaseModel):
        branch: Branch | None = None

    assert set(reachable_models([Root])) == {
        model_name(m) for m in (Root, Branch, Leaf)
    }
    assert set(reachable_models([Branch])) == {model_name(m) for m in (Branch, Leaf)}
