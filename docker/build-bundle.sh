#!/usr/bin/env bash
# Build a per-pipeline config bundle from the local connectors/connections/
# pipelines dirs. This is the local stand-in for the per-run bundle that AWS
# Batch will receive in the cloud: it resolves exactly the connections and
# connectors the given pipeline references (not the whole local registry) and
# tars them together with pipelines/manifest.json.
set -euo pipefail

PIPELINE_ID="${1:?usage: build-bundle.sh <pipeline_id>}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$REPO_ROOT/docker/bundle"
OUT="$OUT_DIR/config.tgz"
LIST="$(mktemp)"
trap 'rm -f "$LIST"' EXIT
mkdir -p "$OUT_DIR"

# Resolve the pipeline dir plus the connection/connector ids it references.
PIPELINE_ID="$PIPELINE_ID" REPO_ROOT="$REPO_ROOT" python3 - > "$LIST" <<'PY'
import json, os, sys

root = os.environ["REPO_ROOT"]
pid = os.environ["PIPELINE_ID"]

manifest = json.load(open(os.path.join(root, "pipelines", "manifest.json")))
entry = next((e for e in manifest["pipelines"] if e.get("pipeline_id") == pid), None)
if entry is None:
    sys.exit(f"pipeline_id {pid} not found in pipelines/manifest.json")

pipe_dir = os.path.dirname(entry["path"])  # e.g. "e2e-local-postgres-to-postgres"
pipeline = json.load(open(os.path.join(root, "pipelines", pipe_dir, "pipeline.json")))

conns = pipeline.get("connections", {})
conn_ids = set(filter(None, [conns.get("source"), *(conns.get("destinations") or [])]))

connector_ids = set()
for cid in conn_ids:
    cj = json.load(open(os.path.join(root, "connections", cid, "connection.json")))
    if cj.get("connector_id"):
        connector_ids.add(cj["connector_id"])

paths = ["pipelines/manifest.json", f"pipelines/{pipe_dir}"]
paths += [f"connections/{c}" for c in sorted(conn_ids)]

# The engine loaders accept both "connectors/{id}" and the prefixed
# "connectors/connector-{id}" layout; resolve whichever exists on disk so a
# pipeline using the prefixed form still bundles.
for cid in sorted(connector_ids):
    for candidate in (f"connectors/{cid}", f"connectors/connector-{cid}"):
        if os.path.isdir(os.path.join(root, candidate)):
            paths.append(candidate)
            break
    else:
        sys.exit(
            f"connector dir not found for {cid}: tried "
            f"connectors/{cid} and connectors/connector-{cid}"
        )
print("\n".join(paths))
PY

echo "[build-bundle] pipeline $PIPELINE_ID -> bundling:"
sed 's/^/  /' "$LIST"

rm -f "$OUT"
# COPYFILE_DISABLE=1 stops macOS tar embedding AppleDouble ("._*") resource-fork
# entries, which stay invisible on macOS but extract as junk "._*" files on Linux
# and break the engine's "streams/*.json" glob. Harmless no-op on Linux builders.
COPYFILE_DISABLE=1 tar -czf "$OUT" -C "$REPO_ROOT" \
    --exclude='__pycache__' --exclude='*.pyc' --exclude='*.egg-info' \
    -T "$LIST"

echo "[build-bundle] wrote $OUT ($(du -h "$OUT" | cut -f1))"
