r"""Tier 2 — live tests against a real database (spec sql-write-path s.10).

Runs only where the connector repo provides its system as a CI service
container and points the suite at it::

    pytest -p cdk.conformance.plugin --pyargs cdk.conformance.tier2 \
        --connector-dir . --live-connection ci/live-connection.json

Without a live connection every test skips with that reason. Cloud
warehouses with no containerizable server stay contract-tier-only — the
accepted residual risk recorded in issue #391.
"""
