r"""Connector conformance kit (issue #391; spec sql-write-path section 10).

The CDK's acceptance suite for connector packages: every connector repo
installs ``analitiq-cdk[conformance]``, points the suite at its own
connector, and runs it in CI against the pinned CDK version — so a CDK
change that breaks a connector turns that connector's CI red before
release, not in a customer pipeline.

Two tiers:

* **Tier 1 — contract tests** (``cdk.conformance.tier1``): no live
  database. For a ``kind: database`` connector, certifies that rendering
  matches the declared ``sql_capabilities``, that refusals fire (never a
  guessed default), that the overridden surface is the sanctioned one,
  and that the connector's type maps are stable under a write/read round
  trip. For a ``kind: api`` connector — which ships no class, its
  definition being executed by the CDK's own generic API path —
  certifies that the declared transport materializes against the
  connection its ``connection_contract`` promises, that the declared auth
  type matches the credential the transport carries, and that every
  endpoint expression addresses a scope the phase using it populates.
* **Tier 2 — live tests** (``cdk.conformance.tier2``): the full
  write/read/replay cycle against a real database the connector repo
  provides as a CI service container. Skips itself, loudly, when no
  live connection is configured.

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

from .api_endpoints import (
    check_api_pagination,
    check_api_query_bindings,
    check_api_request_expressions,
    check_api_response_records,
)
from .api_transport import check_api_auth, check_api_transport
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
    "check_api_auth",
    "check_api_pagination",
    "check_api_query_bindings",
    "check_api_request_expressions",
    "check_api_response_records",
    "check_api_transport",
    "check_declaration_consistency",
    "check_kind_applicability",
    "check_override_surface",
    "check_type_map_grammar",
    "check_type_map_round_trip",
    "load_target",
    "sanctioned_dialect_surface",
    "violation_report",
]
