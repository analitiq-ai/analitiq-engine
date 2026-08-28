"""The census classifier and the ``--check`` verdict, driven without mypy.

``tools/contract_consumption.py`` computes reads from a full mypy build,
which runs as a CI job. Its classification rules and its stale/current
verdict are pure over ``Access`` records and rendered text, so they are
pinned here with synthetic inputs: the failure modes that matter are a read
vanishing from the manifest without a word, and a stale manifest passing.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest
from pydantic import BaseModel

from cdk.contract_consumption import model_name

pytestmark = pytest.mark.unit

_SCRIPT = Path(__file__).resolve().parents[3] / "tools" / "contract_consumption.py"


@pytest.fixture(scope="module")
def census() -> ModuleType:
    spec = importlib.util.spec_from_file_location("contract_consumption_tool", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registered before execution: the script's dataclasses resolve their
    # string annotations through sys.modules[__module__].
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class Leaf(BaseModel):
    x: int = 0
    y: int = 0


class Other(BaseModel):
    z: int = 0


_MODELS = {model_name(Leaf): Leaf, model_name(Other): Other}


def _access(
    census: ModuleType,
    name: str | None,
    *,
    lenient: bool = False,
    module: str = "src.x",
    models=(Leaf,),
):
    return census.Access(
        census.Site(module, 7),
        tuple(model_name(m) for m in models),
        name,
        lenient=lenient,
    )


def _classify(census: ModuleType, *accesses):
    manifest = census.classify(accesses, _MODELS)
    return manifest, {m: dict(f) for m, f in manifest.claims.items()}


def test_a_field_read_is_a_claim_at_its_site(census: ModuleType) -> None:
    manifest, claims = _classify(census, _access(census, "x"))
    assert manifest.problems == []
    assert claims[model_name(Leaf)] == {"x": {census.Site("src.x", 7)}}


def test_kit_reads_never_become_claims(census: ModuleType) -> None:
    manifest, claims = _classify(
        census, _access(census, "x", module="cdk.conformance.probe")
    )
    assert claims == {}
    assert dict(manifest.kit_reads[model_name(Leaf)]) == {
        "x": {census.Site("cdk.conformance.probe", 7)}
    }


def test_a_lenient_getattr_skips_the_members_that_lack_the_field(
    census: ModuleType,
) -> None:
    manifest, claims = _classify(
        census, _access(census, "x", lenient=True, models=(Leaf, Other))
    )
    assert manifest.problems == []
    assert claims == {model_name(Leaf): {"x": {census.Site("src.x", 7)}}}


def test_a_lenient_getattr_no_member_declares_is_a_problem(census: ModuleType) -> None:
    # A typo, or a field the contract renamed: the read returns the default
    # on every member and would otherwise vanish from the manifest silently.
    manifest, claims = _classify(
        census, _access(census, "bdoy", lenient=True, models=(Leaf, Other))
    )
    assert claims == {}
    assert len(manifest.problems) == 1 and "'bdoy'" in manifest.problems[0]


def test_an_unclassified_attribute_is_a_problem(census: ModuleType) -> None:
    manifest, _ = _classify(census, _access(census, "some_property"))
    assert len(manifest.problems) == 1 and ".some_property" in manifest.problems[0]


def test_introspection_is_not_a_read(census: ModuleType) -> None:
    manifest, claims = _classify(census, _access(census, "model_fields"))
    assert manifest.problems == [] and claims == {}


def test_a_dump_needs_an_opaque_or_transport_registration(census: ModuleType) -> None:
    manifest, _ = _classify(census, _access(census, "model_dump"))
    assert len(manifest.problems) == 1 and "OPAQUE" in manifest.problems[0]
    transport, _ = _classify(
        census, _access(census, "model_dump", module="src.models.resolved")
    )
    assert transport.problems == [] and transport.transport == {
        census.Site("src.models.resolved", 7)
    }


def test_a_read_on_a_model_outside_the_roots_is_a_problem(census: ModuleType) -> None:
    class Stray(BaseModel):
        q: int = 0

    manifest, _ = _classify(census, _access(census, "q", models=(Stray,)))
    assert (
        len(manifest.problems) == 1
        and "no declared root reaches" in manifest.problems[0]
    )


def test_a_dynamic_getattr_needs_a_registered_table(census: ModuleType) -> None:
    manifest, _ = _classify(census, _access(census, None))
    assert (
        len(manifest.problems) == 1
        and "DYNAMIC_ATTRIBUTE_TABLES" in manifest.problems[0]
    )


def test_a_registered_module_allows_one_dynamic_site(
    census: ModuleType, monkeypatch
) -> None:
    monkeypatch.setattr(
        census,
        "DYNAMIC_ATTRIBUTE_TABLES",
        {"src.x": ("cdk.api.request", "_REQUEST_SLOTS")},
    )
    monkeypatch.setattr(census, "_table_entries", lambda module, attr: ("x", "y", "z"))
    one, claims = _classify(census, _access(census, None, models=(Leaf, Other)))
    assert one.problems == []
    assert claims == {
        model_name(Leaf): {
            "x": {census.Site("src.x", 7)},
            "y": {census.Site("src.x", 7)},
        },
        model_name(Other): {"z": {census.Site("src.x", 7)}},
    }
    # A table name no member declares is a stale entry, not a silent miss.
    monkeypatch.setattr(census, "_table_entries", lambda module, attr: ("x", "gone"))
    stale, _ = _classify(census, _access(census, None, models=(Leaf, Other)))
    assert len(stale.problems) == 1 and "'gone'" in stale.problems[0]
    monkeypatch.setattr(census, "_table_entries", lambda module, attr: ("x",))
    two, _ = _classify(
        census,
        _access(census, None),
        census.Access(census.Site("src.x", 9), (model_name(Leaf),), None),
    )
    assert len(two.problems) == 1 and "2 non-literal getattr sites" in two.problems[0]


def test_check_reports_stale_with_the_claims_that_moved(
    census: ModuleType, monkeypatch, capsys
) -> None:
    committed = json.loads(census.CONTRACT_CONSUMPTION_PATH.read_text())
    moved = json.loads(json.dumps(committed))
    param = "analitiq.contracts.endpoints.Param"
    del moved["claims"][param]["style"]
    moved["claims"][param]["operators"] = ["cdk.api.query_style:1"]
    monkeypatch.setattr(
        census,
        "render_contract_consumption",
        lambda: json.dumps(moved, indent=2, sort_keys=True) + "\n",
    )
    assert census.main(["--check"]) == 1
    out = capsys.readouterr()
    assert f"+ {param}.operators" in out.out and f"- {param}.style" in out.out
    assert "stale" in out.err


def test_check_passes_on_the_committed_bytes(
    census: ModuleType, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        census,
        "render_contract_consumption",
        census.CONTRACT_CONSUMPTION_PATH.read_text,
    )
    assert census.main(["--check"]) == 0
    assert "current" in capsys.readouterr().out


def test_a_path_table_claims_each_step_through_every_carrier(
    census: ModuleType, monkeypatch
) -> None:
    class Inner(BaseModel):
        deep: int = 0

    class Carrier(BaseModel):
        x: Inner | None = None

    models = {**_MODELS, model_name(Inner): Inner, model_name(Carrier): Carrier}
    monkeypatch.setattr(census, "PATH_TABLES", ((Leaf | Carrier, "m", "T"),))
    monkeypatch.setattr(
        census,
        "_table_entries",
        lambda module, attr: (("x", "deep"), ("q",), ("x", "gone")),
    )
    manifest = census.Manifest()
    census.claim_path_tables(manifest, models)
    site = census.Site("m", 0, "T")
    assert {m: dict(f) for m, f in manifest.claims.items()} == {
        model_name(Leaf): {"x": {site}},
        model_name(Carrier): {"x": {site}},
        model_name(Inner): {"deep": {site}},
    }
    # A path no member starts, or one no member resolves to its last step,
    # is a stale table entry, not a silent no-op. ('x', 'deep') stopping
    # short in Leaf is fine: Carrier resolves it.
    assert len(manifest.problems) == 2
    assert "('q',)" in manifest.problems[0]
    assert (
        "('x', 'gone') resolves to its last step through no member"
        in manifest.problems[1]
    )


def test_a_dump_of_an_opaque_model_is_recorded_as_its_site(
    census: ModuleType, monkeypatch
) -> None:
    monkeypatch.setattr(census, "_OPAQUE_MODELS", frozenset({model_name(Leaf)}))
    manifest, claims = _classify(
        census,
        _access(census, "model_dump"),
        census.Access(census.Site("src.y", 3), (model_name(Leaf),), "authored_json"),
    )
    assert manifest.problems == [] and claims == {}
    assert manifest.opaque_dumps[model_name(Leaf)] == {
        census.Site("src.x", 7),
        census.Site("src.y", 3),
    }


def test_an_opaque_registration_needs_a_dump_or_a_reading_consumer(
    census: ModuleType,
) -> None:
    manifest = census.Manifest()
    dead = census._opaque_entry("m.Leaf", "cdk.nowhere", manifest, {})
    assert dead == {"consumer": "cdk.nowhere", "dumps": [], "entries": []}
    assert (
        len(manifest.problems) == 1
        and "delete the registration" in manifest.problems[0]
    )
    live = census._opaque_entry(
        "m.Leaf",
        "cdk.reader",
        census.Manifest(),
        {"cdk.reader": {census.Site("cdk.reader", 9)}},
    )
    assert live["entries"] == ["cdk.reader:9"]
