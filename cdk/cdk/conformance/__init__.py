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
"""

from .declaration import check_declaration_consistency
from .roundtrip import check_type_map_round_trip
from .surface import check_override_surface, sanctioned_dialect_surface
from .target import ConformanceSetupError, ConformanceTarget, load_target
from .violations import Violation, violation_report

__all__ = [
    "ConformanceSetupError",
    "ConformanceTarget",
    "Violation",
    "check_declaration_consistency",
    "check_override_surface",
    "check_type_map_round_trip",
    "load_target",
    "sanctioned_dialect_surface",
    "violation_report",
]
