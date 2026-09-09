"""The read-side decoder vocabulary: which wire shapes need a name, and why.

A ``native_type -> arrow_type`` pair says only the target type; it says
nothing about how the wire value gets there. Two shapes were historically
free (bare ISO-8601 text, bare integer epoch) -- implicit defaults baked into
``SchemaContract``. Every other shape (Xero's ``/Date(ms+offset)/`` wrapper,
a ``strptime`` pattern, a base64 blob) had nowhere to be declared and either
crashed at runtime or was silently wrong.

This module is the published vocabulary of *names* a field's ``encoding`` may
declare -- mirroring :mod:`cdk.type_map.grammar`'s relationship to
:mod:`cdk.type_map.arrow`: this module stays importable without pyarrow, and
the pyarrow-backed decode functions each name resolves to live in
:mod:`cdk.type_map.arrow` (:func:`~cdk.type_map.arrow.resolve_decoder`),
the only module in this package that imports pyarrow.

``REQUIRES_ENCODING_KINDS`` is the other half of the contract: the
conversion-matrix kinds (:data:`cdk.type_map.grammar.ConversionKind`) whose
wire shape is never assumed. A field of one of these kinds with no declared
``encoding`` is a config defect, not a field ``iso8601``/``epoch`` quietly
covers -- neither of those two is applied unless a field names it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from .exceptions import InvalidTypeMapError
from .grammar import ConversionKind

#: The sentinel name routing to a ``connector.py`` override of
#: ``ApiDialect.decode_field`` instead of a catalog function. Published in
#: the vocabulary (an authoring UI must be able to offer it) but never
#: carries an entry in :data:`DECODER_PARAMS` -- there is no static param
#: shape for code the engine does not run.
CODE_ENCODING_NAME: Final[str] = "code"


@dataclass(frozen=True, slots=True)
class DecoderParam:
    """One parameter a decoder's ``encoding`` config may/must carry."""

    name: str
    kind: str  # "string" | "enum" | "list[string]"
    required: bool = True
    allowed: tuple[str, ...] = ()


#: The unit vocabulary ``epoch`` and ``regex_epoch`` share. The first four
#: spellings are the same ones :data:`cdk.type_map.grammar.UNIT_LONG_TO_SHORT`
#: accepts for a Timestamp/Duration/Time32/Time64 ``arrow_type``; ``DAY`` is
#: not part of that grammar (Date32/Date64 take no unit parameter at all) --
#: it exists here because Date's own physical storage already is a day
#: count, so a bare day-count wire value still needs a unit to name. The
#: single source of truth for both the published vocabulary here and the
#: runtime decode functions in :mod:`cdk.type_map.arrow`, which import it
#: rather than re-declaring it.
EPOCH_UNITS: Final[tuple[str, ...]] = (
    "SECOND",
    "MILLISECOND",
    "MICROSECOND",
    "NANOSECOND",
    "DAY",
)

#: Every decoder name this engine ships, with its declared parameters. A name
#: absent here (other than :data:`CODE_ENCODING_NAME`) is not part of the
#: published vocabulary and :func:`~cdk.type_map.arrow.resolve_decoder`
#: refuses it.
DECODER_PARAMS: Final[dict[str, tuple[DecoderParam, ...]]] = {
    "iso8601": (),
    "epoch": (DecoderParam("unit", "enum", allowed=EPOCH_UNITS),),
    "strptime": (DecoderParam("pattern", "string"),),
    "regex_epoch": (
        DecoderParam("pattern", "string"),
        DecoderParam("unit", "enum", allowed=EPOCH_UNITS),
    ),
    "decimal": (),
    "bool_map": (
        DecoderParam("true_values", "list[string]"),
        DecoderParam("false_values", "list[string]"),
    ),
    "base64": (),
    "iso_duration": (),
}

#: The conversion-matrix kinds a *read* requires an explicit ``encoding``
#: for. Exactly the two implicit paths this issue removes -- ISO-8601 text
#: and bare integer epoch -- both of which land only on these kinds
#: (:func:`cdk.schema_contract._reads_unit_offsets` grouped them identically
#: for the same reason). ``decimal``/``binary`` are deliberately absent: a
#: JSON string parses into either via the conversion matrix's own
#: cross-kind allowlist (``string -> decimal`` is ``auto``) or is rejected
#: as a plain type mismatch today, so nothing here goes from "validates,
#: fails at runtime" to "fails loud at authoring time" for them on read --
#: the catalog's ``decimal``/``base64`` decoders are additive capability,
#: never a mandatory declaration.
REQUIRES_ENCODING_KINDS: Final[frozenset[ConversionKind]] = frozenset(
    {"timestamp", "date", "time", "duration"}
)


def requires_read_encoding(kind: ConversionKind) -> bool:
    """Whether a field of this conversion kind must declare ``encoding``."""
    return kind in REQUIRES_ENCODING_KINDS


#: Which conversion kinds each catalog decoder is meant to build. Every
#: entry here is checked eagerly by
#: :meth:`~cdk.schema_contract.SchemaContract.check_required_read_encoding`
#: against the field's actual arrow_type, so a mismatched declaration (a
#: Timestamp field naming ``decimal``) is refused at plan time rather than
#: on the first non-null response the decoder closure happens to reach.
#: ``bool_map``/``base64`` are neither a mandatory declaration (see
#: :data:`REQUIRES_ENCODING_KINDS`) nor, despite that, unconstrained in
#: which kind they may target -- mirrors
#: :data:`cdk.type_map.encoders.ENCODER_KIND_COMPATIBILITY`'s write-side
#: pair exactly, since a decoder and its mirror encoder always agree on
#: the kind they bridge.
DECODER_KIND_COMPATIBILITY: Final[dict[str, frozenset[ConversionKind]]] = {
    "iso8601": frozenset({"timestamp", "date", "time"}),
    "epoch": frozenset({"timestamp", "date", "time", "duration"}),
    "strptime": frozenset({"timestamp", "date", "time"}),
    "regex_epoch": frozenset({"timestamp", "date", "time", "duration"}),
    "decimal": frozenset({"decimal"}),
    "iso_duration": frozenset({"duration"}),
    "bool_map": frozenset({"bool"}),
    "base64": frozenset({"binary"}),
}


def decoder_matches_kind(name: str, kind: ConversionKind) -> bool:
    """Whether decoder *name* is meant to build a field of *kind*."""
    return kind in DECODER_KIND_COMPATIBILITY.get(name, frozenset())


#: The declared JSON Schema ``type`` each catalog decoder actually reads off
#: the wire, per its own closure in :mod:`cdk.type_map.arrow` -- read there,
#: not guessed: ``iso8601``/``strptime``/``regex_epoch``/``base64``/
#: ``iso_duration`` all reject a non-``str`` wire value outright;
#: ``bool_map`` compares the wire value against ``true_values``/
#: ``false_values``, both declared as lists of strings
#: (:func:`cdk.type_map._param_validation.require_list_param`), so only a
#: JSON string can ever match; ``epoch`` accepts ``numbers.Integral`` or a
#: numeric string -- covering a ``"number"``-declared field too, since JSON
#: Schema defines every integer as a valid number and most epoch fields on
#: real APIs carry whole values; a genuinely fractional wire value still
#: fails inside the decoder's own closure at data time, this table being
#: only the declared-type gate, not a value-level check. ``decimal``
#: accepts anything ``Decimal(str(v))`` parses, which covers a JSON
#: boolean as readily as a string. Checked eagerly by
#: :meth:`~cdk.schema_contract.SchemaContract.check_required_read_encoding`
#: against the field's declared ``type``, the same "fails at plan time, not
#: on the first non-null response" contract :data:`DECODER_KIND_COMPATIBILITY`
#: already applies to the target ``arrow_type``.
DECODER_JSON_TYPE_COMPATIBILITY: Final[dict[str, frozenset[str]]] = {
    "iso8601": frozenset({"string"}),
    "epoch": frozenset({"integer", "number", "string"}),
    "strptime": frozenset({"string"}),
    "regex_epoch": frozenset({"string"}),
    "decimal": frozenset({"string", "number", "integer"}),
    "iso_duration": frozenset({"string"}),
    "bool_map": frozenset({"string"}),
    "base64": frozenset({"string"}),
}


def decoder_matches_json_type(name: str, json_type: str) -> bool:
    """Whether decoder *name* can read a wire value of declared JSON *json_type*."""
    return json_type in DECODER_JSON_TYPE_COMPATIBILITY.get(name, frozenset())


#: The published catalog's own version. Bump alongside any change to the
#: name/parameter vocabulary, same discipline as
#: :data:`cdk.type_map.grammar.GRAMMAR_VERSION`.
DECODERS_CATALOG_VERSION: Final[str] = "1.0.0"


def _param_to_json(param: DecoderParam) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "name": param.name,
        "kind": param.kind,
        "required": param.required,
    }
    if param.allowed:
        doc["allowed"] = list(param.allowed)
    return doc


def build_decoders_catalog() -> dict[str, Any]:
    """Materialise the published document: every decoder name and its params.

    Metadata only -- like ``arrow_type_grammar.json``, not a pair-grid like
    ``conversion_matrix.json``: a decoder name is not a function of two
    types, so there is no grid to flatten, only a vocabulary to publish.
    """
    decoders: dict[str, Any] = {
        name: {"params": [_param_to_json(p) for p in params]}
        for name, params in DECODER_PARAMS.items()
    }
    decoders[CODE_ENCODING_NAME] = {"params": [], "requires_connector_code": True}
    return {"version": DECODERS_CATALOG_VERSION, "decoders": decoders}


DECODERS_CATALOG_PATH: Final[Path] = Path(__file__).with_name("decoders_catalog.json")


def render_decoders_catalog() -> str:
    """Canonical serialisation, matching the committed artifact."""
    return json.dumps(build_decoders_catalog(), indent=2, sort_keys=True) + "\n"


def load_published_decoders_catalog() -> dict[str, Any]:
    """Return the committed, published decoders catalog document."""
    document: dict[str, Any] = json.loads(DECODERS_CATALOG_PATH.read_text())
    return document


def decoder_params(name: str) -> tuple[DecoderParam, ...]:
    """Return the declared parameters for *name*, or raise if unknown.

    Raises for :data:`CODE_ENCODING_NAME` too -- it has no static param
    shape to validate against; a caller resolving ``code`` must branch on
    the name before reaching here.
    """
    params = DECODER_PARAMS.get(name)
    if params is None:
        raise InvalidTypeMapError(
            f"unknown decoder {name!r}; expected one of "
            f"{', '.join([*DECODER_PARAMS, CODE_ENCODING_NAME])}"
        )
    return params
