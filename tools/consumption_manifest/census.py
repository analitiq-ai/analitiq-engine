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

__all__ = ["field_census", "wire_name"]


def wire_name(field: Any) -> str | None:
    """Return the name this field carries in a document, or ``None``."""
    alias = field.alias
    return alias if isinstance(alias, str) else None


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
