"""Analitiq Connector Development Kit (CDK).

A vendor-neutral, transport-neutral toolbox of reusable building blocks a
connector uses — consumed by both the OSS engine and the cloud control-plane.

The public surface is the submodules (``cdk.connection_runtime``, ``cdk.types``,
``cdk.contract``, ``cdk.type_map``, ``cdk.secrets``, ``cdk.transport_factory``,
``cdk.query_builder``, ``cdk.base_handler``, ...). This package ``__init__`` is
kept import-light on purpose: importing ``cdk`` must not eagerly drag in
sqlalchemy / aiohttp / pyarrow — callers import the specific submodule they need.

The one rule: the dependency points engine -> CDK, never back. No module here
imports anything engine-side.

``MissingExtraError`` is re-exported here (it lives in the pure, dependency-free
``cdk._extras``) so a control-plane consumer can ``except cdk.MissingExtraError``
without reaching into a private module. This keeps ``__init__`` import-light:
the re-export pulls only ``typing``.
"""

from ._extras import MissingExtraError

__version__ = "0.2.0"

__all__ = ["MissingExtraError"]
