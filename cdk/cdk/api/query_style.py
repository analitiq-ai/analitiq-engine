"""How a collection param lands on the query string.

A query param typed ``array`` or ``object`` has no one spelling on the
wire -- ``tags=a,b``, ``tags=a&tags=b`` and ``tags[0]=a&tags[1]=b`` are
three different requests -- which is why the published schema REQUIRES
``style`` and ``explode`` on one, and why the request build refuses a
container that declares neither. This module is what those two fields
mean.

The vocabulary is OpenAPI's, and it is closed HERE rather than in the
contract: the schema types ``style`` as a plain string, so a document
naming one the engine has no serialization for is contract-valid and
unsendable. Closing it engine-side is the same shape as the derived
function registry -- a name outside the set resolves on no run, so the
author hears about it at plan time instead of on the connector's first
request.

Only the combinations OpenAPI defines are here. Its own table leaves
``deepObject`` on an array and the delimited styles on an object with no
meaning at all, and inventing one would put the engine's guess on the
provider's wire -- so those are refused by name, with what the style does
serialize named beside them.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .exceptions import RequestSpecError

__all__ = [
    "QueryStyle",
    "declared_query_styles",
    "serialize_query_value",
    "unserializable_style_problem",
]


@dataclass(frozen=True)
class QueryStyle:
    """One param's declared wire serialization, and what it serializes.

    ``type`` rides along because the three are one declaration: OpenAPI
    defines a style for an array, for an object, or for both, so
    ``spaceDelimited`` says nothing about an object and ``deepObject``
    says nothing about an array. Judging the style and the explode alone
    accepts a document at plan time that no value of the param can
    satisfy.
    """

    type: str
    style: str
    explode: bool


#: The delimiter each non-exploded style joins an array with. ``form`` is
#: the OpenAPI default and the one style that also serializes an object.
_DELIMITERS = {"form": ",", "spaceDelimited": " ", "pipeDelimited": "|"}

#: The declared ``type`` each Python kind answers to -- the contract's two
#: collection types, named once so the plan-time check and the value-time
#: one judge the same thing.
_KINDS: dict[str, type] = {"array": list, "object": Mapping}

#: What each style serializes, as ``(style, explode) -> kinds``. Absence is
#: the whole refusal predicate: a pair that is not a key here has no
#: defined spelling, and a pair whose kinds exclude the declared type has
#: none for THAT type, so nothing can send it either way.
_DEFINED: dict[tuple[str, bool], tuple[type, ...]] = {
    ("form", True): (list, Mapping),
    ("form", False): (list, Mapping),
    ("spaceDelimited", False): (list,),
    ("pipeDelimited", False): (list,),
    ("deepObject", True): (Mapping,),
}

#: The styles this engine serializes at all, for a message that can say
#: what the author may write instead.
STYLES = tuple(sorted({style for style, _ in _DEFINED}))


def declared_query_styles(
    declared_query: Any, declared_params: Mapping[str, Any]
) -> dict[str, QueryStyle]:
    """Map each query key to the serialization its bound param declares.

    ``style`` is a fact about a PARAM and the query map is keyed by WIRE
    NAME, so the two are joined here, once, by the binding between them:
    a key whose value is exactly ``{"from_param": X}`` sends X, and X's
    declaration says how a collection it resolves to is spelled.

    Only that shape. A key whose value is a template, a function or a
    deeper structure produces a value no single param owns -- the
    contract requires the style on the param, so there is nothing to read
    -- and a container arriving under one keeps the request build's
    refusal. Guessing which of a function's inputs lent its style would
    put the engine's reading of an expression on the wire.
    """
    if not isinstance(declared_query, Mapping):
        return {}
    styles: dict[str, QueryStyle] = {}
    for key, node in declared_query.items():
        if not isinstance(node, Mapping) or set(node) != {"from_param"}:
            continue
        declaration = declared_params.get(node["from_param"])
        if not isinstance(declaration, Mapping):
            continue
        declared_type = declaration.get("type")
        style, explode = declaration.get("style"), declaration.get("explode")
        if (
            isinstance(declared_type, str)
            and isinstance(style, str)
            and isinstance(explode, bool)
        ):
            styles[str(key)] = QueryStyle(declared_type, style, explode)
    return styles


def unserializable_style_problem(
    key: str, style: QueryStyle, *, endpoint: str
) -> str | None:
    """Why this declared serialization cannot be sent, or ``None``.

    A static fact about the document -- the declaration serializes the
    declared type or it does not, on every connection and for every value
    -- so it is judged with the rest of the request block, before a page
    is fetched, rather than on the first value that happens to be a
    collection. A param a loop fills would otherwise carry an unsendable
    document as far as page two of a read that had already committed
    rows.
    """
    if style.style not in STYLES:
        return (
            f"request.query[{key!r}] for endpoint {endpoint!r} binds a param "
            f"declaring style {style.style!r}, which has no serialization "
            f"here; the engine sends {list(STYLES)}"
        )
    kinds = _DEFINED.get((style.style, style.explode))
    if kinds is None:
        return (
            f"request.query[{key!r}] for endpoint {endpoint!r} binds a param "
            f"declaring style {style.style!r} with explode={style.explode}, a "
            f"combination OpenAPI leaves undefined -- there is no spelling to "
            f"send. Declare "
            f"{sorted({f'{s} explode={e}' for s, e in _DEFINED if s == style.style})}"
        )
    # A style can be defined and still say nothing about THIS type:
    # `spaceDelimited` spells an array and no object, `deepObject` an
    # object and no array. Judging the pair alone certifies a document
    # every value of which fails.
    declared_kind = _KINDS.get(style.type)
    if declared_kind is not None and declared_kind not in kinds:
        serializes = " and ".join(
            name for name, kind in _KINDS.items() if kind in kinds
        )
        return (
            f"request.query[{key!r}] for endpoint {endpoint!r} binds a param "
            f"typed {style.type!r} declaring style {style.style!r} with "
            f"explode={style.explode}, which serializes {serializes} -- "
            f"OpenAPI defines no spelling of a {style.type} that way, so no "
            f"value of this param could be sent"
        )
    return None


def serialize_query_value(
    key: str,
    value: Any,
    style: QueryStyle,
    *,
    endpoint: str,
    sendable: Callable[[str, Any], Any],
) -> dict[str, Any]:
    """Spell one collection value as the query keys it sends.

    Returns the keys and values that go on the wire: one key holding a
    list where the style repeats the name, several keys where it writes
    them out, and one key holding a joined string otherwise. Expanding
    into pairs is :mod:`cdk.api.http`'s job, and putting them in the
    request is the caller's -- this module decides only WHICH names carry
    WHICH values.

    ``sendable`` is the caller's rule for what one value may be on the
    wire, and every value returned here has been through it. It is passed
    in rather than restated because there is exactly one such rule
    (:func:`~cdk.api.request._sendable_value`) and a second one here would
    send ``True,False`` where the same authored ``true`` goes out alone as
    ``true`` -- one value landing two ways depending on whether it sat in
    a collection. A null entry has no spelling at all, and the caller's
    rule is what refuses it.

    A scalar under a collection param comes back untouched: a param typed
    ``array`` whose value resolved to one element is still one value, and
    a style describes how MANY are spelled.
    """
    kinds = _DEFINED.get((style.style, style.explode))
    if kinds is None:
        # Refused with the rest of the request block, so reaching here is
        # a wiring defect rather than an authoring one.
        raise RequestSpecError(
            unserializable_style_problem(key, style, endpoint=endpoint) or ""
        )
    if not isinstance(value, (list, Mapping)):
        return {key: sendable(key, value)}
    if not isinstance(value, kinds):
        return _refuse_kind(key, value, style, endpoint=endpoint)
    items = _flat_items(key, value, endpoint=endpoint)
    if style.style == "deepObject":
        return {
            f"{key}[{name}]": sendable(f"{key}[{name}]", item) for name, item in items
        }
    if style.explode:
        # An exploded object writes its own property names; an exploded
        # array repeats the key it was declared under, which is the one
        # shape that returns a list -- a mapping cannot hold a name twice.
        if isinstance(value, Mapping):
            return {name: sendable(name, item) for name, item in items}
        return {key: [sendable(key, item) for _, item in items]}
    # A delimited style renders the entries itself, so it asks what each
    # may be on the wire before joining them. A non-exploded object
    # flattens to name,value,name,value -- OpenAPI's own spelling, and the
    # reason `form` is the only style that takes one.
    parts = (
        [part for name, item in items for part in (name, str(sendable(key, item)))]
        if isinstance(value, Mapping)
        else [str(sendable(key, item)) for _, item in items]
    )
    return {key: _DELIMITERS[style.style].join(parts)}


def _flat_items(key: str, value: Any, *, endpoint: str) -> list[tuple[str, Any]]:
    """Return the ``(name, item)`` pairs a collection contributes, refusing nesting.

    An array lends the index as a name (which only ``deepObject`` writes)
    and an object its property name. A nested container has no defined
    spelling at any style -- OpenAPI stops at one level -- so it is
    refused rather than rendered as whatever ``str()`` makes of it.
    """
    pairs = (
        [(str(name), item) for name, item in value.items()]
        if isinstance(value, Mapping)
        else [(str(index), item) for index, item in enumerate(value)]
    )
    for name, item in pairs:
        if isinstance(item, (list, Mapping)):
            raise RequestSpecError(
                f"request.query[{key!r}] for endpoint {endpoint!r} resolves to "
                f"a collection whose entry {name!r} is a "
                f"{type(item).__name__}; a declared style spells one level, "
                f"so nothing can send a nested one -- flatten it, or declare "
                f"the inner value as its own param"
            )
    return pairs


def _refuse_kind(
    key: str, value: Any, style: QueryStyle, *, endpoint: str
) -> dict[str, Any]:
    """Refuse a value of a kind this style does not serialize."""
    kinds = _DEFINED[(style.style, style.explode)]
    serializes = " and ".join(
        "an object" if kind is Mapping else "an array" for kind in kinds
    )
    raise RequestSpecError(
        f"request.query[{key!r}] for endpoint {endpoint!r} resolves to "
        f"{'an object' if isinstance(value, Mapping) else 'an array'}, and "
        f"the param declares style {style.style!r} with "
        f"explode={style.explode}, which serializes {serializes}. Declare "
        f"the style that matches the value's shape"
    )
