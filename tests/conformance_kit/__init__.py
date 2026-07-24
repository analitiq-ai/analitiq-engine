"""Acceptance tests for the CDK conformance kit (issue #391).

The kit is certified here the way it certifies connectors: the
postgres-shaped reference connector under ``reference/`` passes both
tiers, and deliberately broken connectors fail tier 1 with actionable
messages.
"""
