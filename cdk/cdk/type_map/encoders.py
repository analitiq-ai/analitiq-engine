"""The write-side encoder vocabulary and the functions each name resolves to.

The mirror of :mod:`cdk.type_map.decoders`, but pyarrow-free on both the
vocabulary *and* the function side: an encoder takes one already-Arrow-typed
Python scalar (a ``datetime``, a ``Decimal``, ``bytes`` -- whatever
``pyarrow.RecordBatch.to_pylist()`` produced) and returns a JSON-native
Python value (``str`` / ``int`` / ``float`` / ``bool`` / ``None``). No
``pa.DataType`` is needed to run one -- only to decide, at the call site,
whether a field's ``arrow_type`` *requires* one (:data:`REQUIRES_ENCODING_KINDS`).

These functions replace ``cdk.api.http._orjson_default``'s ``Decimal``/
``bytes`` special cases and orjson's native ``datetime``/``date`` rendering,
which are retired as *implicit* behavior: a field of a kind this module
gates now renders through a declared ``encoding_write`` catalog entry, never
through an engine default nobody wrote down.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable, Mapping
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

from ._param_validation import require_enum_param, require_list_param, require_str_param
from .decoders import EPOCH_UNITS as _READ_EPOCH_UNITS
from .exceptions import InvalidTypeMapError
from .grammar import ConversionKind

#: The sentinel name routing to a ``connector.py`` override of
#: ``ApiDialect.encode_field`` instead of a catalog function. Same role as
#: :data:`cdk.type_map.decoders.CODE_ENCODING_NAME` on the read side.
CODE_ENCODING_NAME: Final[str] = "code"

#: The write-side unit vocabulary: the same base units
#: :data:`cdk.type_map.decoders.EPOCH_UNITS` declares, minus ``"DAY"`` --
#: ``_encode_epoch`` only ever receives a ``datetime`` (never a bare
#: ``date``), so a day-count unit has no value to render here. Derived from
#: the read side's list rather than re-declared, so the two vocabularies
#: cannot drift on the four units they do share.
_EPOCH_UNITS: Final[tuple[str, ...]] = tuple(u for u in _READ_EPOCH_UNITS if u != "DAY")

#: Microseconds per unit -- the resolution a plain ``datetime`` actually
#: has (it holds no finer), so ``NANOSECOND`` is the one unit with no
#: entry: going *to* it from microseconds is an exact multiply, never a
#: division that could leave a remainder.
_MICROSECONDS_PER_UNIT: Final[dict[str, int]] = {
    "SECOND": 1_000_000,
    "MILLISECOND": 1_000,
    "MICROSECOND": 1,
}

_UNIX_EPOCH: Final[datetime] = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _encode_iso8601(_config: Mapping[str, Any]) -> Callable[[Any], Any]:
    """``datetime``/``date``/``time`` -> the same ISO string orjson renders.

    orjson's native (now-retired) rendering of these three types is
    ``value.isoformat()`` with no injected timezone -- a naive value stays
    naive. Matched exactly so a field's wire output is unchanged by
    switching from the implicit default to this declared entry (issue
    Acceptance: byte-identical output).
    """

    def encode(value: Any) -> str:
        if not isinstance(value, (datetime, date, time)):
            raise TypeError(
                f"encoding_write 'iso8601' expects a datetime/date/time value, "
                f"got {type(value).__name__}"
            )
        return value.isoformat()

    return encode


def _encode_strftime(config: Mapping[str, Any]) -> Callable[[Any], Any]:
    pattern = require_str_param(config, "pattern", "encoding_write 'strftime'")

    def encode(value: Any) -> str:
        if not isinstance(value, (datetime, date, time)):
            raise TypeError(
                f"encoding_write 'strftime' expects a datetime/date/time value, "
                f"got {type(value).__name__}"
            )
        return value.strftime(pattern)

    return encode


def _encode_epoch(config: Mapping[str, Any]) -> Callable[[Any], Any]:
    unit = require_enum_param(config, "unit", _EPOCH_UNITS, "encoding_write 'epoch'")
    micros_per_unit = _MICROSECONDS_PER_UNIT.get(unit)

    def encode(value: Any) -> int:
        if not isinstance(value, datetime):
            raise TypeError(
                f"encoding_write 'epoch' expects a datetime value, "
                f"got {type(value).__name__}"
            )
        delta = _as_utc(value) - _UNIX_EPOCH
        # timedelta // timedelta is Python's own exact integer division
        # between two timedeltas (CPython computes it from their exact
        # microsecond-resolution internal representation) -- not hand
        # arithmetic on delta.days/.seconds/.microseconds: this same
        # function shipped a NANOSECOND-ticks-1000x-too-small bug from
        # exactly that hand arithmetic once already (caught by an
        # anti-pattern audit, not a test -- see TestEpochEncoderUnitArithmetic).
        total_micros = delta // timedelta(microseconds=1)
        if micros_per_unit is None:  # NANOSECOND
            # A plain datetime holds no finer than microsecond precision
            # (LandingBatch.records materialises via to_pylist(), which
            # refuses a genuinely sub-microsecond value outright unless
            # pandas is installed -- not a CDK dependency), so going *to*
            # nanoseconds from it is an exact multiply, never lossy.
            return total_micros * 1000
        ticks, remainder = divmod(total_micros, micros_per_unit)
        if remainder:
            # divmod's own remainder, not // alone: // would otherwise
            # floor silently, changing which instant is sent, not just
            # its precision, and asymmetrically for a pre-epoch value
            # (floor rounds toward -inf, not toward the represented instant).
            raise ValueError(
                f"encoding_write 'epoch': {value!r} is not exactly "
                f"representable in unit {unit!r}; it has a nonzero "
                f"remainder that would otherwise be silently discarded"
            )
        return ticks

    return encode


def _encode_decimal(_config: Mapping[str, Any]) -> Callable[[Any], Any]:
    """``Decimal`` -> its exact decimal string, matching ``_orjson_default``."""

    def encode(value: Any) -> str:
        if not isinstance(value, Decimal):
            raise TypeError(
                f"encoding_write 'decimal' expects a Decimal value, "
                f"got {type(value).__name__}"
            )
        return str(value)

    return encode


def _encode_bool_map(config: Mapping[str, Any]) -> Callable[[Any], Any]:
    true_values = require_list_param(config, "true_values", "encoding_write 'bool_map'")
    false_values = require_list_param(
        config, "false_values", "encoding_write 'bool_map'"
    )
    if not true_values or not false_values:
        raise InvalidTypeMapError(
            "encoding_write 'bool_map' requires a non-empty 'true_values' and "
            "'false_values'; the first of each is the rendered token"
        )
    if true_values[0] == false_values[0]:
        raise InvalidTypeMapError(
            f"encoding_write 'bool_map': true_values[0] and false_values[0] "
            f"are both {true_values[0]!r} -- True and False would render "
            f"the identical wire token"
        )

    def encode(value: Any) -> str:
        if not isinstance(value, bool):
            raise TypeError(
                f"encoding_write 'bool_map' expects a bool value, "
                f"got {type(value).__name__}"
            )
        return true_values[0] if value else false_values[0]

    return encode


def _encode_base64(_config: Mapping[str, Any]) -> Callable[[Any], Any]:
    """``bytes`` -> base64 text, matching ``_orjson_default``."""

    def encode(value: Any) -> str:
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"encoding_write 'base64' expects a bytes-like value, "
                f"got {type(value).__name__}"
            )
        return base64.b64encode(bytes(value)).decode("ascii")

    return encode


#: Every encoder name this engine ships, mapped to the factory that builds
#: its runtime function from the field's ``encoding_write`` config (minus
#: ``name``). :data:`CODE_ENCODING_NAME` is deliberately absent -- it has no
#: factory here; a caller routes it to ``ApiDialect.encode_field`` before
#: ever reaching this table.
ENCODER_FACTORIES: Final[
    dict[str, Callable[[Mapping[str, Any]], Callable[[Any], Any]]]
] = {
    "iso8601": _encode_iso8601,
    "strftime": _encode_strftime,
    "epoch": _encode_epoch,
    "decimal": _encode_decimal,
    "bool_map": _encode_bool_map,
    "base64": _encode_base64,
}


def resolve_encoder(
    encoding_write: Mapping[str, Any] | None
) -> Callable[[Any], Any] | None:
    """Build the encode function a field's declared ``encoding_write`` names.

    Returns ``None`` for an undeclared field (the caller decides whether
    that is an error via :func:`requires_write_encoding`) and for
    :data:`CODE_ENCODING_NAME`, which the caller must special-case before
    calling this -- there is no catalog function backing it.
    """
    if encoding_write is None:
        return None
    name = encoding_write.get("name")
    if name == CODE_ENCODING_NAME:
        return None
    if not isinstance(name, str) or name not in ENCODER_FACTORIES:
        raise InvalidTypeMapError(
            f"unknown encoding_write name {name!r}; expected one of "
            f"{', '.join([*ENCODER_FACTORIES, CODE_ENCODING_NAME])}"
        )
    config = {k: v for k, v in encoding_write.items() if k != "name"}
    allowed = {p["name"] for p in _PARAM_SHAPES[name]}
    unknown = set(config) - allowed
    if unknown:
        raise InvalidTypeMapError(
            f"encoding_write {name!r}: unknown parameter(s) {sorted(unknown)!r}; "
            f"a factory silently ignores a key it does not read, so a "
            f"misspelled or unpublished parameter would otherwise render a "
            f"different wire format than the one declared"
        )
    return ENCODER_FACTORIES[name](config)


#: The conversion-matrix kinds a *write* requires an explicit
#: ``encoding_write`` for. Wider than the read-side set
#: (:data:`cdk.type_map.decoders.REQUIRES_ENCODING_KINDS`): JSON has no
#: native wire shape for any of these six kinds, so orjson either rendered
#: them through an implicit special case (``datetime``/``date`` natively,
#: ``Decimal``/bytes via ``_orjson_default``) or never handled them at all
#: (``time``/``duration``). All six now require a declared entry rather
#: than an engine default.
REQUIRES_ENCODING_KINDS: Final[frozenset[ConversionKind]] = frozenset(
    {"timestamp", "date", "time", "duration", "decimal", "binary"}
)


def requires_write_encoding(kind: ConversionKind) -> bool:
    """Whether a field of this conversion kind must declare ``encoding_write``."""
    return kind in REQUIRES_ENCODING_KINDS


#: Which conversion kinds each catalog encoder is meant to render. A
#: ``duration`` field has no catalog entry at all -- only ``code`` covers
#: it -- so it is absent from every set here. Checked eagerly by
#: :meth:`~cdk.schema_contract.SchemaContract.check_required_write_encoding`
#: against the field's actual arrow_type, so a mismatched declaration (a
#: Timestamp field naming ``decimal``) is refused at configure time rather
#: than on the first non-null value the encoder closure happens to reach at
#: ``land()``.
ENCODER_KIND_COMPATIBILITY: Final[dict[str, frozenset[ConversionKind]]] = {
    "iso8601": frozenset({"timestamp", "date", "time"}),
    "strftime": frozenset({"timestamp", "date", "time"}),
    "epoch": frozenset({"timestamp"}),
    "decimal": frozenset({"decimal"}),
    "bool_map": frozenset({"bool"}),
    "base64": frozenset({"binary"}),
}


def encoding_write_matches_kind(name: str, kind: ConversionKind) -> bool:
    """Whether encoder *name* is meant to render a field of *kind*."""
    return kind in ENCODER_KIND_COMPATIBILITY.get(name, frozenset())


#: The JSON Schema ``type`` each catalog encoder actually renders, per its
#: own closure above: ``epoch`` returns a bare ``int``; every other encoder
#: returns ``str`` (``.isoformat()``, ``.strftime()``, ``str(Decimal)``, a
#: declared ``bool_map`` token, base64 text). Checked eagerly by
#: :meth:`~cdk.schema_contract.SchemaContract.check_required_write_encoding`
#: against the field's own declared ``type`` -- a ``"boolean"``-typed field
#: naming ``bool_map`` (which renders a string token) or a ``"string"``-typed
#: field naming ``epoch`` (which renders an int) would otherwise resolve
#: fine and only violate the endpoint's declared input schema once the
#: request is actually sent.
ENCODER_JSON_TYPE: Final[dict[str, str]] = {
    "iso8601": "string",
    "strftime": "string",
    "epoch": "integer",
    "decimal": "string",
    "bool_map": "string",
    "base64": "string",
}


def encoding_write_matches_json_type(name: str, json_type: str) -> bool:
    """Whether encoder *name* renders a value of declared JSON *json_type*.

    ``"integer"`` output also satisfies a declared ``"number"``: JSON
    Schema defines every integer as a valid number, so ``epoch`` (the only
    encoder rendering a bare ``int``) must not be refused on a
    ``"number"``-typed field merely because the two spellings differ.
    """
    output_type = ENCODER_JSON_TYPE.get(name)
    if output_type == json_type:
        return True
    return output_type == "integer" and json_type == "number"


#: The published catalog's own version.
ENCODERS_CATALOG_VERSION: Final[str] = "1.0.0"

_PARAM_SHAPES: Final[dict[str, list[dict[str, Any]]]] = {
    "iso8601": [],
    "strftime": [{"name": "pattern", "kind": "string", "required": True}],
    "epoch": [
        {
            "name": "unit",
            "kind": "enum",
            "required": True,
            "allowed": list(_EPOCH_UNITS),
        }
    ],
    "decimal": [],
    "bool_map": [
        {"name": "true_values", "kind": "list[string]", "required": True},
        {"name": "false_values", "kind": "list[string]", "required": True},
    ],
    "base64": [],
}


def build_encoders_catalog() -> dict[str, Any]:
    """Materialise the published document: every encoder name and its params."""
    encoders: dict[str, Any] = {
        name: {"params": params} for name, params in _PARAM_SHAPES.items()
    }
    encoders[CODE_ENCODING_NAME] = {"params": [], "requires_connector_code": True}
    return {"version": ENCODERS_CATALOG_VERSION, "encoders": encoders}


ENCODERS_CATALOG_PATH: Final[Path] = Path(__file__).with_name("encoders_catalog.json")


def render_encoders_catalog() -> str:
    """Canonical serialisation, matching the committed artifact."""
    return json.dumps(build_encoders_catalog(), indent=2, sort_keys=True) + "\n"


def load_published_encoders_catalog() -> dict[str, Any]:
    """Return the committed, published encoders catalog document."""
    document: dict[str, Any] = json.loads(ENCODERS_CATALOG_PATH.read_text())
    return document
