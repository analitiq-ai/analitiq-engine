r"""Connector conformance kit (issue #391; ADR sql-write-path-v2 section 10).

The CDK's acceptance suite for connector packages: every connector repo
installs ``analitiq-cdk[conformance]``, points the suite at its own
connector, and runs it in CI against the pinned CDK version — so a CDK
change that breaks a connector turns that connector's CI red before
release, not in a customer pipeline.

Two tiers:

* **Tier 1 — contract tests** (``cdk.conformance.tier1``): no live
  database. Certifies that rendering matches the declared
  ``sql_capabilities``, that refusals fire (never a guessed default),
  that the overridden surface is the sanctioned one, and that the
  connector's type maps are stable under a write/read round trip.
* **Tier 2 — live tests** (``cdk.conformance.tier2``): the full
  write/read/replay cycle against a real database the connector repo
  provides as a CI service container. Skips itself, loudly, when no
  live connection is configured.

Run from a connector repo (the package installed, the repo root holding
``definition/connector.json``)::

    pytest --pyargs cdk.conformance.tier1 --connector-dir .
    pytest --pyargs cdk.conformance.tier2 --connector-dir . \
        --live-connection ci/live-connection.json

Every check is also importable directly (:func:`check_override_surface`,
:func:`check_declaration_consistency`, :func:`check_type_map_round_trip`)
so a repo can wire them into its own harness; the pytest modules are thin
wrappers over these functions.

Exports resolve lazily (PEP 562): the ``pytest11`` entry point makes
pytest import :mod:`cdk.conformance.plugin` — and therefore this
package — at startup in every environment with the core CDK installed,
including ones without the ``conformance``/``arrow`` extras. The check
modules reach the Arrow-dependent SQL surface, so importing them here
eagerly would break pytest startup for a core-only consumer; they load
on first attribute access instead.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .declaration import check_declaration_consistency
    from .roundtrip import check_type_map_round_trip
    from .surface import check_override_surface, sanctioned_dialect_surface
    from .target import ConformanceSetupError, ConformanceTarget, load_target
    from .violations import Violation, violation_report

#: Export name -> defining submodule, resolved on first access.
_EXPORTS = {
    "ConformanceSetupError": "target",
    "ConformanceTarget": "target",
    "load_target": "target",
    "Violation": "violations",
    "violation_report": "violations",
    "check_declaration_consistency": "declaration",
    "check_override_surface": "surface",
    "sanctioned_dialect_surface": "surface",
    "check_type_map_round_trip": "roundtrip",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    """Resolve a public export from its submodule on first access."""
    submodule = _EXPORTS.get(name)
    if submodule is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(import_module(f".{submodule}", __name__), name)
