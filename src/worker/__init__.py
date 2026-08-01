"""Connector worker: the isolated process that runs connector code.

The shell (engine source side or destination service) spawns this package
as a subprocess (``python -m src.worker``), feeds it a one-shot JSON
bootstrap on stdin, and talks to it over a Unix domain socket. The worker
owns the connector class, the driver, and every external connection; it
never sees the secret store, the config volume, or the engine's state —
everything it may use arrived resolved in the bootstrap.

The engine binds no connector class. Every kind default is the CDK's own
generic connector, registered from ``cdk.registry.KIND_DEFAULTS``; the
worker resolves through that registry and imports no connector itself.
"""
