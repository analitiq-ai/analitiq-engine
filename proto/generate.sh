#!/usr/bin/env bash
# Regenerate the committed gRPC modules from the .proto definitions beside this
# script.
#
# Every definition under analitiq/v1 is compiled, not a hand-listed subset: the
# generated modules are committed and imported by the engine, so one left off a
# list would keep working while silently no longer tracking its source.
#
# Forgetting to run this after editing a .proto is caught by
# tests/unit/grpc_tests/test_generated_protos.py, which compares each committed
# module's descriptor against its definition.
#
# Run after: poetry install

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="$PROJECT_ROOT/src/grpc/generated"

mkdir -p "$OUTPUT_DIR/analitiq/v1"

poetry run python -m grpc_tools.protoc \
    --proto_path="$SCRIPT_DIR" \
    --python_out="$OUTPUT_DIR" \
    --grpc_python_out="$OUTPUT_DIR" \
    "$SCRIPT_DIR"/analitiq/v1/*.proto

# Package markers. The v1 one carries hand-written re-exports in the committed
# tree, so this only creates it where absent; it never overwrites.
for marker in "" "/analitiq" "/analitiq/v1"; do
    [ -f "$OUTPUT_DIR$marker/__init__.py" ] || touch "$OUTPUT_DIR$marker/__init__.py"
done

# protoc emits absolute imports (from analitiq.v1 import stream_pb2); the
# generated tree is a package under src/grpc, so they must be relative. The
# .bak suffix is the one -i spelling both BSD and GNU sed accept.
sed -i.bak 's/from analitiq\.v1 import/from . import/g' "$OUTPUT_DIR/analitiq/v1/"*.py
rm -f "$OUTPUT_DIR/analitiq/v1/"*.py.bak

echo "Generated into $OUTPUT_DIR/analitiq/v1:"
ls "$OUTPUT_DIR/analitiq/v1"
