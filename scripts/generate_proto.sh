#!/bin/bash
# Generate Python code from protobuf definitions
# Run this after: poetry install

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$PROJECT_ROOT/proto"
OUTPUT_DIR="$PROJECT_ROOT/src/grpc/generated"

echo "Generating Python code from protobuf definitions..."
echo "Proto dir: $PROTO_DIR"
echo "Output dir: $OUTPUT_DIR"

# Create output directory
mkdir -p "$OUTPUT_DIR/analitiq/v1"

# Generate Python code
poetry run python -m grpc_tools.protoc \
    --proto_path="$PROTO_DIR" \
    --python_out="$OUTPUT_DIR" \
    --grpc_python_out="$OUTPUT_DIR" \
    "$PROTO_DIR/analitiq/v1/stream.proto" \
    "$PROTO_DIR/analitiq/v1/destination_service.proto"

# Create __init__.py files
touch "$OUTPUT_DIR/__init__.py"
touch "$OUTPUT_DIR/analitiq/__init__.py"
touch "$OUTPUT_DIR/analitiq/v1/__init__.py"

# Fix imports in generated files (protoc generates absolute imports)
# Change: from analitiq.v1 import stream_pb2
# To: from . import stream_pb2
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' 's/from analitiq\.v1 import/from . import/g' "$OUTPUT_DIR/analitiq/v1/"*.py
else
    # Linux
    sed -i 's/from analitiq\.v1 import/from . import/g' "$OUTPUT_DIR/analitiq/v1/"*.py
fi

echo "Done! Generated files:"
ls -la "$OUTPUT_DIR/analitiq/v1/"
