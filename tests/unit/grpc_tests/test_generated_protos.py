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
- ``*_pb2_grpc.py`` carry no descriptor. They carry the RPC paths the client
  and server dispatch on and the classes each RPC serializes through, and both
  are pinned -- the second by driving the committed stub and servicer and
  reading back what they bound, so a method that keeps its path while swapping
  to another live message is caught here rather than at the first call.
- ``__init__.py`` is hand-written, not generated, and is the surface the engine
  imports these names through. It must name every message, enum and service
  the definitions declare, or a regenerated tree ships a symbol nothing can
  import.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

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


class _RecordingChannel:
    """Captures what a generated Stub binds each RPC path to."""

    def __init__(self) -> None:
        self.bound: dict[str, Any] = {}

    def _record(self, path: str, response_deserializer: Any = None, **_: Any) -> None:
        # The request side is not recoverable here: protoc binds
        # `Request.SerializeToString`, an unbound descriptor that does not name
        # its class. The servicer below carries it as a bound classmethod.
        self.bound[path] = response_deserializer
        return None

    unary_unary = unary_stream = stream_unary = stream_stream = _record


class _RecordingServer:
    """Captures the generic handler a generated ``add_*_to_server`` installs."""

    def __init__(self) -> None:
        self.handlers: list[Any] = []

    def add_generic_rpc_handlers(self, handlers: Any) -> None:
        self.handlers.extend(handlers)


class _CallDetails:
    def __init__(self, method: str) -> None:
        self.method = method


def _bound_rpc_types(
    module: Any,
    file: descriptor_pb2.FileDescriptorProto,
    service: descriptor_pb2.ServiceDescriptorProto,
) -> dict[str, tuple[str, str, bool, bool]]:
    """What the committed stub and servicer actually bind, per RPC name."""
    channel = _RecordingChannel()
    getattr(module, f"{service.name}Stub")(channel)
    server = _RecordingServer()
    getattr(module, f"add_{service.name}Servicer_to_server")(MagicMock(), server)
    generic = server.handlers[0]

    bound = {}
    for method in service.method:
        path = f"/{file.package}.{service.name}/{method.name}"
        handler = generic.service(_CallDetails(path))
        bound[method.name] = (
            handler.request_deserializer.__self__.DESCRIPTOR.full_name,
            channel.bound[path].__self__.DESCRIPTOR.full_name,
            handler.request_streaming,
            handler.response_streaming,
        )
    return bound


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


@pytest.mark.parametrize("stem", _PROTO_STEMS)
def test_committed_stub_binds_each_rpc_to_its_declared_types(
    compiled: dict[str, descriptor_pb2.FileDescriptorProto], stem: str
) -> None:
    """Each RPC must serialize through the exact types its .proto declares.

    Checking that referenced messages merely exist is not enough: a method
    that keeps its path but swaps its request type for another live message
    passes that, and fails at the first call instead. So the committed stub
    and servicer are driven for real -- a recording channel and a recording
    server hand back the classes they bound -- and compared per RPC against
    the descriptor, streaming arity included.
    """
    services = compiled[stem].service
    if not services:
        pytest.skip(f"{stem}.proto declares no services")
    module = __import__(
        f"src.grpc.generated.analitiq.v1.{stem}_pb2_grpc", fromlist=["__name__"]
    )
    for service in services:
        bound = _bound_rpc_types(module, compiled[stem], service)
        declared = {
            method.name: (
                # Descriptor type names are fully qualified with a leading dot.
                method.input_type.lstrip("."),
                method.output_type.lstrip("."),
                method.client_streaming,
                method.server_streaming,
            )
            for method in service.method
        }
        assert bound == declared, (
            f"{stem}_pb2_grpc.py binds {service.name} to types "
            f"{stem}.proto does not declare; run proto/generate.sh and commit "
            f"the result"
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
    # Services are re-exported under protoc's three generated spellings, and
    # the engine imports them from the package (src/worker/readable.py takes
    # SourceServiceStub this way), so a new service leaves __init__ stale in
    # exactly the same way a new message does.
    declared |= {
        name
        for file in compiled.values()
        for service in file.service
        for name in (
            f"{service.name}Stub",
            f"{service.name}Servicer",
            f"add_{service.name}Servicer_to_server",
        )
    }
    # Both halves: an attribute that is missing breaks `from ... import Name`,
    # and a name absent from __all__ breaks a star-import while still passing
    # an attribute check. Testing __all__ alone would pass a name listed there
    # but never imported -- the failure this exists to catch.
    unimportable = {name for name in declared if not hasattr(package, name)}
    unexported = declared - set(package.__all__)
    assert not (unimportable | unexported), (
        f"src/grpc/generated/analitiq/v1/__init__.py does not re-export "
        f"{sorted(unimportable | unexported)}; add them to its imports and __all__"
    )
