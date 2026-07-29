"""Drift guard for the committed gRPC modules.

``src/grpc/generated`` is generated code that is committed and imported all
over the engine, so nothing forces it to follow ``proto/analitiq/v1/*.proto``:
an edited definition whose modules were never regenerated keeps every import
working and every test passing while the wire shape silently disagrees with
its source.

Three things are pinned, because the tree has three kinds of file:

- ``*_pb2.py`` carry a protobuf descriptor, compared against one compiled from
  the ``.proto`` here. The comparison is on the descriptor rather than the
  generated Python text: the descriptor is a function of the definition alone,
  while the boilerplate around it is a function of the protoc version, and the
  Python dependency set is not locked (``grpcio-tools`` floats on a caret
  range), so a byte comparison would fail on a routine bump.
- ``*_pb2_grpc.py`` carry no descriptor, only the RPC path strings the client
  and server dispatch on, so those are compared instead.
- ``__init__.py`` is hand-written, not generated, and is the surface the engine
  imports these names through. It must name every message and enum the
  definitions declare, or a regenerated tree ships a symbol nothing can import.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path

import pytest
from google.protobuf import descriptor_pb2

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PROTO_DIR = _REPO_ROOT / "proto" / "analitiq" / "v1"
_GENERATED = _REPO_ROOT / "src" / "grpc" / "generated" / "analitiq" / "v1"
_PROTO_STEMS = sorted(path.stem for path in _PROTO_DIR.glob("*.proto"))

# An empty stem list would collapse every check below into a vacuous pass: the
# parametrized tests would collect nothing and the set comparisons would hold
# between two empty sets. Path discovery is the one thing this file cannot
# afford to get silently wrong.
assert _PROTO_STEMS, f"no .proto definitions found under {_PROTO_DIR}"

# The RPC paths a generated stub dispatches on, e.g.
# '/analitiq.v1.SourceService/ReadStream'.
_RPC_PATH_RE = re.compile(r"'(/[A-Za-z0-9_.]+/[A-Za-z0-9_]+)'")


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


def _declared_rpc_paths(file: descriptor_pb2.FileDescriptorProto) -> set[str]:
    return {
        f"/{file.package}.{service.name}/{method.name}"
        for service in file.service
        for method in service.method
    }


@pytest.fixture(scope="module")
def compiled() -> dict[str, descriptor_pb2.FileDescriptorProto]:
    """Descriptors compiled from the .proto sources, keyed by file stem."""
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "descriptors.pb"
        result = subprocess.run(  # noqa: S603 - fixed argv, repo-local sources
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"--proto_path={_REPO_ROOT / 'proto'}",
                f"--descriptor_set_out={out}",
                *(str(_PROTO_DIR / f"{stem}.proto") for stem in _PROTO_STEMS),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            # check=True would raise a CalledProcessError whose str() omits
            # stderr, hiding the file and line protoc is complaining about.
            pytest.fail(f"protoc failed:\n{result.stderr}")
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
    committed = _drop_json_names(_committed_descriptor(stem))
    current = _drop_json_names(compiled[stem])
    if committed != current:
        pytest.fail(
            f"{stem}.proto changed without regenerating its modules; run "
            f"proto/generate.sh and commit the result.\n"
            f"committed:\n{committed}\ncompiled from the .proto:\n{current}"
        )


@pytest.mark.parametrize("stem", _PROTO_STEMS)
def test_committed_stub_dispatches_its_declared_rpcs(
    compiled: dict[str, descriptor_pb2.FileDescriptorProto], stem: str
) -> None:
    """The stubs carry no descriptor, so pin the paths they dispatch on."""
    stub = (_GENERATED / f"{stem}_pb2_grpc.py").read_text()
    assert set(_RPC_PATH_RE.findall(stub)) == _declared_rpc_paths(compiled[stem]), (
        f"{stem}_pb2_grpc.py does not dispatch exactly the RPCs "
        f"{stem}.proto declares; run proto/generate.sh and commit the result"
    )


def test_package_exports_every_declared_message(
    compiled: dict[str, descriptor_pb2.FileDescriptorProto],
) -> None:
    """The hand-written ``__init__`` must keep up with the definitions.

    ``proto/generate.sh`` deliberately never overwrites it, so a message added
    to a .proto regenerates the modules and leaves this list stale — the engine
    imports these names from the package, not from ``*_pb2`` directly.
    """
    import src.grpc.generated.analitiq.v1 as package

    declared = {
        entity.name
        for file in compiled.values()
        for entity in list(file.message_type) + list(file.enum_type)
    }
    missing = declared - set(package.__all__)
    assert not missing, (
        f"src/grpc/generated/analitiq/v1/__init__.py does not re-export "
        f"{sorted(missing)}; add them to its imports and __all__"
    )
