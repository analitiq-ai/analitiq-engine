"""Every field the contract declares, as (model, wire name) pairs.

The census is the vocabulary both halves of the reachability gate speak.
It is deliberately NOT a set of dotted document paths: a ``stop_when``
predicate nests into itself, so ``operations.read.pagination.stop_when``
alone spells infinitely many paths and no finite list of them could ever
be complete. A field belongs to a model, and the models are finite, so
the pair is the only spelling that closes.

The wire name, not the Python attribute name, because that is what the
published JSON Schema renders and what an author writes: the document's
own ``$schema`` field is ``schema_url`` in the model and nothing outside
the model ever calls it that.
"""

from __future__ import annotations

import typing
from typing import Any

from pydantic import BaseModel

__all__ = ["declared_fields", "field_census", "wire_name"]


def wire_name(field: Any) -> str | None:
    """Return the name this field carries in a document, or ``None``.

    ``serialization_alias`` first, then ``alias`` -- the order pydantic's
    own ``by_alias`` dump resolves them in. Reading only ``alias`` would
    name a field one thing here and another in the document, and the
    field would then go unclaimed however much the engine reads it.
    """
    for alias in (field.serialization_alias, field.alias):
        if isinstance(alias, str):
            return alias
    return None


def _models_in(annotation: Any) -> list[type[BaseModel]]:
    """Every contract model reachable through one annotation.

    Containers and unions contribute their members without contributing a
    name of their own: ``dict[WriteMode, WriteOperation]`` is still the
    ``write`` field, and the mode key is a value in the document rather
    than a field of the contract.
    """
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return [annotation]
    found: list[type[BaseModel]] = []
    for arg in typing.get_args(annotation):
        found.extend(_models_in(arg))
    return found


def field_census(root: type[BaseModel]) -> dict[str, tuple[str, ...]]:
    """Map every model reachable from *root* to its fields' wire names.

    Declaration order is kept inside a model so a diff of the census reads
    like the model it came from.
    """
    census: dict[str, tuple[str, ...]] = {}
    pending: list[type[BaseModel]] = [root]
    seen: set[type[BaseModel]] = set()
    while pending:
        model = pending.pop()
        if model in seen:
            continue
        seen.add(model)
        names: list[str] = []
        for name, field in model.model_fields.items():
            names.append(wire_name(field) or name)
            pending.extend(_models_in(field.annotation))
        census[model.__name__] = tuple(names)
    return census


def declared_fields(instance: BaseModel) -> set[tuple[str, str]]:
    """Return the census pairs one DOCUMENT declares, model node by node.

    The counterpart to :func:`field_census`: that answers what the
    contract has, this answers what a given document says. The gap
    between them is the only reason a field can be unclaimed without the
    engine having ignored it.
    """
    declared: set[tuple[str, str]] = set()
    dumped = instance.model_dump(by_alias=True, exclude_unset=True, mode="json")
    for name, field in type(instance).model_fields.items():
        key = wire_name(field) or name
        if key not in dumped:
            continue
        declared.add((type(instance).__name__, key))
        for child in _model_values(getattr(instance, name)):
            declared |= declared_fields(child)
    return declared


def _model_values(value: Any) -> list[BaseModel]:
    """Return the model instances one field value carries, at any depth."""
    if isinstance(value, BaseModel):
        return [value]
    if isinstance(value, (list, tuple)):
        return [child for item in value for child in _model_values(item)]
    if isinstance(value, dict):
        return [child for item in value.values() for child in _model_values(item)]
    return []
