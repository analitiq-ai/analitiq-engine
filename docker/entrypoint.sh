#!/bin/sh
# Attach connectors, then start the engine.
#
# The engine image ships NO database drivers: each connector package brings
# its own (the "driver belongs to the connector" principle). At container
# start this script walks the mounted connectors directory and installs
# every attached connector into the user site:
#
#   - a directory with pyproject.toml is an installable connector package
#     (connector class + dialect + driver dependencies) -> pip install it;
#     its entry points register the class with the engine's registry.
#   - a directory with only requirements.txt is a driver-only (thin)
#     connector -> install just the driver; the generic class for its kind
#     serves it.
#   - a definition-only directory (no code, no driver) needs nothing.
#
# A failing install is logged and skipped, mirroring the registry's
# best-effort entry-point discovery: one broken connector must not take
# the engine down. The pipeline that actually needs the broken connector
# fails loudly at connect time with the missing-driver error.
set -u

CONNECTORS_DIR="${CONNECTORS_DIR:-/app/connectors}"

if [ -d "$CONNECTORS_DIR" ]; then
    for dir in "$CONNECTORS_DIR"/*/; do
        [ -d "$dir" ] || continue
        name=$(basename "$dir")
        if [ -f "$dir/pyproject.toml" ]; then
            echo "[entrypoint] installing connector package: $name"
            pip install --user -q "$dir" \
                || echo "[entrypoint] WARNING: connector package install failed: $name (continuing)"
        elif [ -f "$dir/requirements.txt" ]; then
            echo "[entrypoint] installing connector driver requirements: $name"
            pip install --user -q -r "$dir/requirements.txt" \
                || echo "[entrypoint] WARNING: connector requirements install failed: $name (continuing)"
        fi
    done
fi

exec python -m src.main "$@"
