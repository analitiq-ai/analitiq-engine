"""Drift guard for the committed gRPC modules.

``src/grpc/generated`` is generated code that is committed and imported all
over the engine, so nothing forces it to follow ``proto/*.proto``: an edited
definition whose modules were never regenerated keeps every import working and
every test passing while the wire shape silently disagrees with its source.

The comparison is on the protobuf *descriptor* each committed module carries,
against one compiled from the ``.proto`` right here -- not on the generated
Python text. The descriptor is a function of the definition alone, while the
surrounding boilerplate is a function of the protoc version, and this repo
ships no dependency lockfile: a byte comparison would fail on a routine
``grpcio-tools`` bump that changed nothing about the contract.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path

import pytest
from google.protobuf import descriptor_pb2

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PROTO_DIR = _REPO_ROOT / "proto"
_GENERATED = _REPO_ROOT / "src" / "grpc" / "generated" / "analitiq" / "v1"
_PROTO_STEMS = sorted(
    path.stem for path in (_PROTO_DIR / "analitiq" / "v1").glob("*.proto")
)


def _drop_json_names(
    file: descriptor_pb2.FileDescriptorProto,
) -> descriptor_pb2.FileDescriptorProto:
    """Return *file* without the ``json_name`` protoc derives from field names.

    protoc fills it in a ``--descriptor_set_out`` dump but omits it from the
    descriptor it embeds in generated code. Both spellings describe the same
    definition, so dropping it compares the contract rather than the dump
    format.
    """
    normalized = descriptor_pb2.FileDescriptorProto()
    normalized.CopyFrom(file)

    def strip(messages: Iterable[descriptor_pb2.DescriptorProto]) -> None:
        for message in messages:
            for field in list(message.field) + list(message.extension):
                field.ClearField("json_name")
            strip(message.nested_type)

    strip(normalized.message_type)
    for field in normalized.extension:
        field.ClearField("json_name")
    return normalized


def _committed_descriptor(stem: str) -> descriptor_pb2.FileDescriptorProto:
    """The descriptor embedded in the committed ``{stem}_pb2`` module."""
    module = __import__(
        f"src.grpc.generated.analitiq.v1.{stem}_pb2", fromlist=["DESCRIPTOR"]
    )
    parsed = descriptor_pb2.FileDescriptorProto()
    parsed.ParseFromString(module.DESCRIPTOR.serialized_pb)
    return parsed


@pytest.fixture(scope="module")
def compiled() -> dict[str, descriptor_pb2.FileDescriptorProto]:
    """Descriptors compiled from the .proto sources, keyed by file stem."""
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "descriptors.pb"
        subprocess.run(  # noqa: S603 - fixed argv, repo-local sources
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"--proto_path={_PROTO_DIR}",
                f"--descriptor_set_out={out}",
                *(
                    str(_PROTO_DIR / "analitiq" / "v1" / f"{stem}.proto")
                    for stem in _PROTO_STEMS
                ),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        descriptor_set = descriptor_pb2.FileDescriptorSet()
        descriptor_set.ParseFromString(out.read_bytes())
    return {Path(file.name).stem: file for file in descriptor_set.file}


def test_every_proto_has_committed_modules() -> None:
    """A new .proto must arrive with its generated modules, not without them."""
    expected = {f"{stem}_pb2.py" for stem in _PROTO_STEMS} | {
        f"{stem}_pb2_grpc.py" for stem in _PROTO_STEMS
    }
    assert {path.name for path in _GENERATED.glob("*_pb2*.py")} == expected


@pytest.mark.parametrize("stem", _PROTO_STEMS)
def test_committed_module_describes_its_proto(
    compiled: dict[str, descriptor_pb2.FileDescriptorProto], stem: str
) -> None:
    assert _drop_json_names(_committed_descriptor(stem)) == _drop_json_names(
        compiled[stem]
    ), (
        f"{stem}.proto changed without regenerating its modules; "
        "run proto/generate.sh and commit the result"
    )
