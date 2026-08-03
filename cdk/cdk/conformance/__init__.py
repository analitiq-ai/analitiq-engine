r"""Connector conformance kit (issue #391; spec sql-write-path section 10).

The CDK's acceptance suite for connector packages: every connector repo
installs ``analitiq-cdk[conformance]``, points the suite at its own
connector, and runs it in CI against the pinned CDK version — so a CDK
change that breaks a connector turns that connector's CI red before
release, not in a customer pipeline.

Two tiers:

* **Tier 1 — contract tests** (``cdk.conformance.tier1``): no live
  system. For a database, that the rendering matches the declared
  ``sql_capabilities``, that refusals fire (never a guessed default),
  that the overridden surface is the sanctioned one, and that the
  connector's type maps are stable under a write/read round trip. For an
  api, that each endpoint document's read compiles into a request,
  advances past a page, and stops when the author said it should.
* **Tier 2 — live tests** (``cdk.conformance.tier2``): the full
  write/read/replay cycle against a real database the connector repo
  provides as a CI service container. Skips itself, loudly, when no
  live connection is configured, and registers as inapplicable for a
  kind it has no round trip for (:data:`cdk.conformance.skips.NO_LIVE_TIER`).

Either tier *fails* when it carries no check for the connector's kind
(:mod:`cdk.conformance.applicability`): a suite that structurally cannot
assess a connector must not report success for it, or its green check
would mean "not assessed" while reading as "passed".

Run from a connector repo (the package installed, the repo root holding
``definition/connector.json``) — the options plugin is loaded
explicitly, or skipped entirely by configuring through the environment
variables the fixtures read (``ANALITIQ_CONNECTOR_DIR``,
``ANALITIQ_CONNECTOR_CLASS``, ``ANALITIQ_LIVE_CONNECTION``)::

    pytest -p cdk.conformance.plugin --pyargs cdk.conformance.tier1 \
        --connector-dir .
    pytest -p cdk.conformance.plugin --pyargs cdk.conformance.tier2 \
        --connector-dir . --live-connection ci/live-connection.json

Every check is also importable directly (:func:`check_override_surface`,
:func:`check_declaration_consistency`, :func:`check_type_map_round_trip`)
so a repo can wire them into its own harness; the pytest modules are thin
wrappers over these functions.
"""

from .api_read_path import (
    check_api_page_references,
    check_api_read_advances,
    check_api_read_compiles,
    check_api_read_stop_condition,
    check_api_record_schema,
)
from .api_surface import check_api_has_reads, check_read_transport_selection
from .applicability import check_kind_applicability
from .declaration import check_declaration_consistency
from .roundtrip import check_type_map_grammar, check_type_map_round_trip
from .surface import check_override_surface, sanctioned_dialect_surface
from .target import ConformanceSetupError, ConformanceTarget, load_target
from .violations import Violation, violation_report

__all__ = [
    "ConformanceSetupError",
    "ConformanceTarget",
    "Violation",
    "check_api_has_reads",
    "check_api_page_references",
    "check_api_read_advances",
    "check_api_read_compiles",
    "check_api_read_stop_condition",
    "check_api_record_schema",
    "check_declaration_consistency",
    "check_kind_applicability",
    "check_override_surface",
    "check_read_transport_selection",
    "check_type_map_grammar",
    "check_type_map_round_trip",
    "load_target",
    "sanctioned_dialect_surface",
    "violation_report",
]
