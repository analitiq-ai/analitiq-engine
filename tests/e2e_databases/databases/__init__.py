"""Per-database knowledge: spin-up, seed, read-back.

Each module under this package exports a single ``SPEC`` symbol whose
``slug`` registers the database with the matrix. Adding a new DB is a
single new module — no central list to edit.
"""
from __future__ import annotations

import importlib
import pkgutil
from typing import Dict

from tests.e2e_databases.databases._base import DatabaseSpec

_REGISTRY: Dict[str, DatabaseSpec] = {}


def _discover() -> None:
    if _REGISTRY:
        return
    package_path = __path__  # type: ignore[name-defined]
    for module_info in pkgutil.iter_modules(package_path):
        if module_info.name.startswith("_"):
            continue
        module = importlib.import_module(f"{__name__}.{module_info.name}")
        spec = getattr(module, "SPEC", None)
        if isinstance(spec, DatabaseSpec):
            _REGISTRY[spec.slug] = spec


def all_specs() -> Dict[str, DatabaseSpec]:
    _discover()
    return dict(_REGISTRY)


def spec_for(slug: str) -> DatabaseSpec:
    _discover()
    if slug not in _REGISTRY:
        raise KeyError(
            f"No DatabaseSpec registered for {slug!r}. Known: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[slug]
