"""A contract document that remembers which of its fields were looked up.

The engine navigates an endpoint document as plain mappings -- it is
handed a validated document across the process boundary and reads it by
key. So the record of what it consumed is the record of which keys it
asked for, and the honest way to take that record is to hand it the real
document and watch.

Each node that corresponds to a contract model is wrapped in a ``dict``
subclass tagged with that model's name; every lookup on it is credited to
the model. A ``dict`` subclass rather than a ``Mapping`` because the read
path asks ``isinstance(decl, dict)`` in places, and a node that answered
no there would change what the engine does -- the recording must observe
the run, never alter it.

Free-form subtrees (a response JSON Schema, a header map) are left as
plain dicts: their keys are the author's, not the contract's, and
crediting them to a model would invent fields the census has never heard
of.

A lookup is credited only when the frame asking for it belongs to the
engine. The kit that certifies a connector reads the same document to
build its probes, and a field only IT reads is precisely what this
manifest must not claim.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from copy import deepcopy
from typing import Any

from pydantic import BaseModel

from src.config.schema_validator import EndpointDocument
from src.models.resolved import dump_endpoint_document

from .census import wire_name

__all__ = ["ReadLedger", "recording_document"]

#: Module prefixes whose lookups count as the engine consuming a field.
#: ``src`` is the engine process, ``cdk`` the connector path it drives.
_ENGINE_PREFIXES = ("cdk.", "src.")

#: The conformance kit ships inside the CDK but is a checker, not the
#: path a pipeline runs. It reads endpoint documents to build its probes,
#: and a field only the checker reads is not a field the engine consumes.
_NOT_ENGINE_PREFIXES = ("cdk.conformance",)


class ReadLedger:
    """The (model, field) pairs the engine looked up, across every drive."""

    def __init__(self) -> None:
        self.reads: set[tuple[str, str]] = set()

    def credit(self, model: str, key: Any) -> None:
        """Record a lookup of *key* on a node of *model*, if the engine made it."""
        if isinstance(key, str) and _engine_is_reading():
            self.reads.add((model, key))

    def claims(self) -> dict[str, tuple[str, ...]]:
        """Return the ledger as model -> sorted field names."""
        claimed: dict[str, set[str]] = {}
        for model, field in self.reads:
            claimed.setdefault(model, set()).add(field)
        return {model: tuple(sorted(fields)) for model, fields in claimed.items()}


def _engine_is_reading() -> bool:
    """Whether the frame that made this lookup is engine code.

    The walk skips this module's own frames -- the lookup arrives through
    :class:`_RecordingNode`, so the caller is never the immediate frame --
    and stops at the first frame outside it, which is whoever asked.
    """
    frame: Any = sys._getframe(1)
    while frame is not None:
        module = frame.f_globals.get("__name__", "")
        if module != __name__:
            return module.startswith(_ENGINE_PREFIXES) and not module.startswith(
                _NOT_ENGINE_PREFIXES
            )
        frame = frame.f_back
    return False


class _RecordingNode(dict):
    """One contract-model node of a document, watching its own keys."""

    def __init__(self, model: str, ledger: ReadLedger, data: dict[str, Any]) -> None:
        super().__init__(data)
        self._model = model
        self._ledger = ledger

    def __getitem__(self, key: Any) -> Any:
        self._ledger.credit(self._model, key)
        return super().__getitem__(key)

    def get(self, key: Any, default: Any = None) -> Any:
        """Look *key* up, recording the interest whether or not it is present."""
        self._ledger.credit(self._model, key)
        return super().get(key, default)

    def __contains__(self, key: Any) -> bool:
        self._ledger.credit(self._model, key)
        return super().__contains__(key)

    def __iter__(self) -> Iterator[Any]:
        self._credit_all()
        return super().__iter__()

    def keys(self) -> Any:
        """Every key, each credited: a caller walking them reads them all."""
        self._credit_all()
        return super().keys()

    def items(self) -> Any:
        """Every pair, each key credited: a caller walking them reads them all."""
        self._credit_all()
        return super().items()

    def _credit_all(self) -> None:
        for key in super().keys():
            self._ledger.credit(self._model, key)

    def __deepcopy__(self, memo: dict[int, Any]) -> "_RecordingNode":
        """Copy as a recording node, so a copied subtree still reports.

        Without this the default dict-subclass reduction rebuilds the node
        without its model or ledger, and the first lookup on the copy dies
        with an ``AttributeError`` instead of being counted.
        """
        copied = _RecordingNode(
            self._model,
            self._ledger,
            {key: deepcopy(value, memo) for key, value in dict.items(self)},
        )
        memo[id(self)] = copied
        return copied


def recording_document(
    document: EndpointDocument, ledger: ReadLedger
) -> dict[str, Any]:
    """Serialise *document* the way the engine does, watching every node.

    The dump is the engine's own -- the one function that turns a typed
    contract document into the payload a connector is handed -- so the
    drive reads the bytes a run reads, down to which omitted fields are
    absent rather than defaulted. A private dump here with its own
    settings would quietly census a document no connector ever sees.
    """
    return _wrap_model(document, dump_endpoint_document(document), ledger)


def _wrap_model(
    instance: BaseModel, dumped: dict[str, Any], ledger: ReadLedger
) -> dict[str, Any]:
    """Wrap one model node and, beneath it, every model node it contains.

    Walking the model instance alongside its dump is what tells a contract
    node (wrapped, credited to its model) from the free-form JSON below one
    (a response schema, a header map -- left alone, because their keys are
    the author's rather than the contract's).
    """
    node: dict[str, Any] = dict(dumped)
    for name, field in type(instance).model_fields.items():
        key = wire_name(field) or name
        if key in node:
            node[key] = _wrap_value(getattr(instance, name), node[key], ledger)
    return _RecordingNode(type(instance).__name__, ledger, node)


def _wrap_value(value: Any, dumped: Any, ledger: ReadLedger) -> Any:
    """Wrap the contract nodes inside one dumped field value."""
    if isinstance(value, BaseModel) and isinstance(dumped, dict):
        return _wrap_model(value, dumped, ledger)
    if isinstance(value, (list, tuple)) and isinstance(dumped, list):
        # strict: a length mismatch between a model list and its dump
        # cannot happen, and if it ever did it would silently leave nodes
        # unwrapped -- which reads as "the engine ignored them".
        return [
            _wrap_value(item, item_dump, ledger)
            for item, item_dump in zip(value, dumped, strict=True)
        ]
    if isinstance(value, dict) and isinstance(dumped, dict):
        # Driven from the DUMPED keys, matching back into the model's own
        # mapping. A dict keyed by anything the dump renders differently --
        # an enum, an int -- would otherwise leave its nodes unwrapped, and
        # an unwrapped node claims nothing however much the engine reads it.
        return {
            key: _wrap_value(_member(value, key), item, ledger)
            for key, item in dumped.items()
        }
    return dumped


def _member(mapping: dict[Any, Any], dumped_key: str) -> Any:
    """Return the model-side value behind one dumped key, or ``None``."""
    if dumped_key in mapping:
        return mapping[dumped_key]
    for key, value in mapping.items():
        if str(key) == dumped_key:
            return value
    return None
