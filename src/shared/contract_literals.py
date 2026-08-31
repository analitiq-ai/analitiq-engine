"""Reading a contract model's own vocabulary instead of restating it.

Restating a contract enum in engine code is how a contract-valid document
starts being rejected: the contract gains a value, the copy does not, and
nothing fails until an author hits it. Every engine-side check against a
contract enum reads it from the model through here.

Its own module, and import-light on purpose, because two readers need it from
opposite ends of the import graph: :mod:`src.models.resolved`, which builds
the resolved config, and :mod:`src.shared.logging_level`, which the resolved
config imports in turn.
"""

from __future__ import annotations

from typing import get_args

from pydantic import BaseModel

__all__ = ["contract_literals"]


def contract_literals(model: type[BaseModel], field_name: str) -> frozenset[str]:
    """Read one contract model field's ``Literal`` vocabulary.

    Restating a contract enum in engine code is how a contract-valid document
    starts being rejected: the contract gains a value, the copy does not, and
    nothing fails until an author hits it. Reading the annotation keeps one
    source.

    Every shape this cannot read raises here, naming the model and the field.
    These run at import, so the alternative to a loud failure is an engine that
    starts up and rejects documents the contract permits.
    """
    fields = getattr(model, "model_fields", None)
    if fields is None or field_name not in fields:
        raise RuntimeError(
            f"{model!r} does not declare a {field_name!r} field; the contract "
            "changed shape and this reader must follow it"
        )
    values = get_args(fields[field_name].annotation)
    if not values or not all(isinstance(value, str) for value in values):
        raise RuntimeError(
            f"{model.__name__}.{field_name} is not a Literal of strings; the "
            "contract changed shape and this reader must follow it"
        )
    return frozenset(values)
