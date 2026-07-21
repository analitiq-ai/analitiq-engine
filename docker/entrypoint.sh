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

# Bundle mode: when CONFIG_BUNDLE points at a config archive (a local path or
# an http(s):// URL such as a presigned object-store URL), hydrate it into the
# working directory before installing connectors or starting the engine.
# The archive carries the same connectors/connections/pipelines layout the
# engine reads locally; in the cloud AWS Batch attaches it per run instead of
# bind-mounting host dirs. Local runs leave CONFIG_BUNDLE unset and bind-mount
# the dirs as before, so this is a no-op for them.
if [ -n "${CONFIG_BUNDLE:-}" ]; then
    # Strip any query string before logging: a presigned object-store URL
    # carries its signature there, and container logs outlive the URL.
    echo "[entrypoint] hydrating config bundle: ${CONFIG_BUNDLE%%\?*}"
    python -m src.runtime_archive hydrate "$CONFIG_BUNDLE" \
        || { echo "[entrypoint] FATAL: config bundle hydration failed"; exit 1; }
fi

CONNECTORS_DIR="${CONNECTORS_DIR:-/app/connectors}"

if [ -d "$CONNECTORS_DIR" ]; then
    for dir in "$CONNECTORS_DIR"/*/; do
        [ -d "$dir" ] || continue
        name=$(basename "$dir")
        if [ -f "$dir/pyproject.toml" ]; then
            echo "[entrypoint] installing connector package: $name"
            # Connector sources may be bind-mounted read-only (a symlink into a
            # DIP registry checkout mounted :ro). pip builds the wheel in-tree,
            # which fails on a read-only source, so stage a writable copy first.
            # -L dereferences the registry symlink; .git is dropped so the
            # connector's own repo history is not dragged into the build.
            stage=$(mktemp -d)
            if cp -RL "$dir." "$stage/" 2>/dev/null; then
                rm -rf "$stage/.git"
                pip install --user -q --no-build-isolation "$stage" \
                    || echo "[entrypoint] WARNING: connector package install failed: $name (continuing)"
            else
                echo "[entrypoint] WARNING: could not stage connector for install: $name (continuing)"
            fi
            rm -rf "$stage"
        elif [ -f "$dir/requirements.txt" ]; then
            echo "[entrypoint] installing connector driver requirements: $name"
            pip install --user -q -r "$dir/requirements.txt" \
                || echo "[entrypoint] WARNING: connector requirements install failed: $name (continuing)"
        fi
    done
fi

exec python -m src.main "$@"
