"""What a declared param may carry, judged before the request goes out.

``operations.<role>.params.<name>`` declares more than a name and a type:
``required``, and nine JSON-Schema value keywords (``enum``, ``format``,
``pattern``, the two numeric bounds, and the two length and two item
bounds). They are published as JSON Schema at ``schemas.analitiq.ai`` and
the plugins teach authors to write them as JSON Schema keywords, so they
are enforced by the reference implementation rather than by rules of our
own: anything else would make a document that satisfies the published
schema fail here, or the reverse.

Two things the library does NOT do on its own are done here.

**Formats come from an explicit named list.** ``jsonschema``'s default
registry holds whichever checkers the surrounding install happens to
supply, so a connector that pulls ``rfc3339-validator`` in for its own
reasons would silently switch ``date-time`` enforcement on inside its
worker and nowhere else -- the same document passing in one consumer and
failing in another. A ``format`` outside the list is an annotation, which
is what JSON Schema says it is by default.

**Numbers are compared in one model.** A declared bound is a
``StrictFloat`` in the contract, so the author's ``0.1`` arrives as the
binary float nearest it; a value read back off the wire arrives as an
exact ``Decimal`` (:func:`cdk.api.http.loads_preserving_decimals`), which
is how a keyset cursor reaches the next page's params. Compared as they
arrive, a cursor sitting exactly on a declared fractional bound is refused
on page two of a read whose page one already committed rows.
:func:`normalize_numbers` puts both sides into the decimal model they were
each written in, and it runs over the declared keywords and the value
alike -- one function, so the two can never drift apart.

Nothing here renders the value it refused. Params carry bearer tokens, API
keys and opaque continuation tokens, and a refusal becomes a
:class:`~cdk.api.exceptions.RequestSpecError` that is logged; the worker's
stderr redactor masks DSN-shaped credentials only. The library puts the
instance verbatim in ``ValidationError.message``, so that attribute and
``.instance`` are never read -- every message here is composed from
``.validator``, the authored declaration, and the type of what arrived.
"""

from __future__ import annotations

import logging
import math
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final

from analitiq.contracts.endpoints import Param
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError

from .exceptions import RequestSpecError

logger = logging.getLogger(__name__)

__all__ = ["ParamRules", "normalize_numbers"]


#: The ``Param`` attributes that carry an enforced JSON-Schema keyword.
#: Read at exactly one site (:meth:`ParamRules.compile`), which is what lets
#: the consumption census claim all nine from the table rather than from
#: nine literal attribute reads -- see ``DYNAMIC_ATTRIBUTE_TABLES`` in
#: ``tools/contract_consumption.py``. That census is why these are spelled
#: out rather than derived: it reads the names statically, and a table it
#: has to execute to see claims nothing.
_CONSTRAINT_KEYWORDS: Final[tuple[str, ...]] = (
    "enum",
    "format",
    "pattern",
    "minimum",
    "maximum",
    "min_length",
    "max_length",
    "min_items",
    "max_items",
)


def _keyword_of(attribute: str) -> str:
    """Name the JSON-Schema keyword a ``Param`` attribute carries.

    The contract already answers this: ``Param.min_length`` declares
    ``alias="minLength"``, which IS the wire spelling, and an attribute with
    no alias is spelled the same both ways. Asked of the model rather than
    written down again, because a second copy of the mapping enforces the
    old keyword name for one release after the contract moves -- and the
    engine would then be checking a keyword the published schema no longer
    names, which is the exact drift delegating to the reference
    implementation exists to prevent.
    """
    return Param.model_fields[attribute].alias or attribute


#: The keywords whose refusal is unactionable without a measurement: "too
#: long" says nothing when the value itself may not be printed. A count is
#: not the content.
_MEASURED: Final = frozenset({"minLength", "maxLength", "minItems", "maxItems"})

#: Every ``format`` this engine ENFORCES, named rather than discovered.
#: Exactly the set ``jsonschema[format-nongpl]`` supplies, which is what
#: ``cdk/pyproject.toml``'s ``api`` extra declares -- so the answer is the
#: same in every install of that extra, and a checker some other package
#: happens to register cannot join it.
_ENFORCED_FORMATS: Final = frozenset(
    {
        "date",
        "date-time",
        "duration",
        "email",
        "hostname",
        "idn-email",
        "idn-hostname",
        "ipv4",
        "ipv6",
        "iri",
        "iri-reference",
        "json-pointer",
        "regex",
        "relative-json-pointer",
        "time",
        "uri",
        "uri-reference",
        "uri-template",
        "uuid",
    }
)


def _named_format_checker() -> FormatChecker:
    """Build the checker from :data:`_ENFORCED_FORMATS`, never the registry."""
    missing = sorted(_ENFORCED_FORMATS - set(FormatChecker.checkers))
    if missing:
        raise ImportError(
            f"the format checkers for {missing} are not installed, so a param "
            f"declaring one would be judged here differently than in another "
            f"install; install analitiq-cdk[api], which declares "
            f"jsonschema[format-nongpl]"
        )
    return FormatChecker(sorted(_ENFORCED_FORMATS))


_FORMAT_CHECKER: Final = _named_format_checker()


def normalize_numbers(value: Any) -> Any:
    """Put every number in *value* into the model the others are compared in.

    A ``float`` re-enters through its own decimal spelling, which is the
    text the author (or the provider) wrote: ``0.1`` becomes
    ``Decimal("0.1")`` rather than the binary float nearest it, so a bound
    and a value that were both written ``0.1`` compare equal.

    An exactly-integral number then becomes an ``int``, because JSON
    Schema's ``integer`` is an isinstance test and rejects ``Decimal(9901)``
    outright -- a keyset cursor on an integer id would fail its own
    declaration. This is why the float branch falls THROUGH rather than
    returning: JSON Schema calls ``1.0`` an integer, and the library agrees
    for a ``float`` but not for a ``Decimal``, so a float that stopped at
    ``Decimal("1.0")`` would be refused by a declaration that admits the
    same number written any other way. Everything non-integral stays a
    ``Decimal``: narrowing to ``float`` would round the judgement it is
    about to be used for.

    That narrowing stops at the interpreter's decimal-rendering limit.
    ``Decimal("1E+5000")`` is finite and integral, so it converts to a
    5001-digit ``int`` that raises a bare ``ValueError`` the moment anything
    renders it -- a builtin escaping a module whose entire error vocabulary
    is :class:`RequestSpecError`, on a number a provider can put in a JSON
    body. Left a ``Decimal`` it compares cleanly, and a param declaring
    ``type: integer`` refuses it in words instead of crashing.

    ``bool`` is left alone even though it is an ``int`` subclass: the
    library already tells the two apart (``{"enum": [1]}`` refuses ``True``),
    so touching it here would only break that.

    Recursive, because a number can arrive at any depth -- an ``enum`` of
    arrays matches element by element, and normalising only the outermost
    number looks right in a test and fails on real data.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        # Re-entered, not returned: a float and a Decimal that spell the
        # same number have to leave here as the same object, or ``1.0``
        # would be refused by a ``type: integer`` param that admits
        # ``Decimal("1.0")`` -- and JSON Schema calls both an integer.
        value = Decimal(repr(value))
    if isinstance(value, Decimal):
        if (
            value.is_finite()
            and value == value.to_integral_value()
            and _renderable_as_int(value)
        ):
            return int(value)
        return value
    if isinstance(value, Mapping):
        return {key: normalize_numbers(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_numbers(item) for item in value]
    return value


def _renderable_as_int(value: Decimal) -> bool:
    """Whether *value* has few enough digits for CPython to render as an int.

    ``sys.get_int_max_str_digits`` is read per call because it is settable at
    runtime; ``0`` means the interpreter imposes no limit.
    """
    limit = sys.get_int_max_str_digits()
    return limit == 0 or value.adjusted() < limit


def _non_finite(value: Any) -> bool:
    """Whether *value* holds a number no comparison can order."""
    if isinstance(value, bool):
        return False
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Decimal):
        return not value.is_finite()
    if isinstance(value, Mapping):
        return any(_non_finite(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_non_finite(item) for item in value)
    return False


def _type_name(value: Any) -> str:
    """Name the type of a value without rendering the value."""
    return type(value).__name__


@dataclass(frozen=True)
class _Rule:
    """One param's compiled declaration, and the words to refuse it in."""

    validator: Draft202012Validator
    #: Keyword -> the declaration AS THE AUTHOR WROTE IT. A refusal quotes
    #: this rather than the normalised form it compared against: an author
    #: asked to fix ``maximum`` has to find ``100.0``, not ``Decimal('100')``.
    authored: Mapping[str, Any]
    required: bool
    #: Whether a pagination or replication loop owns this param, which is
    #: the whole of the ``required`` exemption -- ``for_read`` leaves such a
    #: param out of the defaults and the cursor strategy sends no token on
    #: the first request, so it is CORRECTLY absent before page one.
    controlled: bool


@dataclass(frozen=True)
class ParamRules:
    """Every declared param's value rules for one operation, compiled once.

    Compiled rather than re-read per request: a schema is built and its
    patterns are checked once for the whole read, so page one hundred pays
    nothing page one did not.

    It travels on the :class:`~cdk.api.request.ParamTable` and is required
    there with no default. The table is what every request materialises
    from, so no request can be built without the declarations that judge
    it, and a default would be a rule set that refuses nothing.
    """

    #: Named in every refusal. By page one hundred the caller can no longer
    #: say which document it was reading, so it is carried rather than
    #: passed in per call.
    endpoint: str
    rules: Mapping[str, _Rule]

    @classmethod
    def compile(cls, declared: Mapping[str, Param], *, endpoint: str) -> ParamRules:
        """Compile one operation's declared params into judgeable rules.

        Every defect this raises is a fact about the DOCUMENT, true before
        any request is built and actionable only by its author, so it is
        raised once here rather than re-discovered on every page: a bound
        no comparison can order, and a ``pattern`` the regex engine cannot
        compile (which would otherwise surface as a bare ``re.error``
        escaping a module whose whole error vocabulary is
        :class:`RequestSpecError`).
        """
        rules: dict[str, _Rule] = {}
        for name, decl in declared.items():
            schema: dict[str, Any] = {"type": decl.type}
            authored: dict[str, Any] = {"type": decl.type}
            for attribute in _CONSTRAINT_KEYWORDS:
                keyword = _keyword_of(attribute)
                value = getattr(decl, attribute)
                if value is None:
                    continue
                if keyword == "format" and value not in _ENFORCED_FORMATS:
                    # An unenforced format is an annotation, which is what
                    # JSON Schema says a format is by default. Left out of
                    # the schema entirely rather than left to the ambient
                    # registry to answer -- but said out loud, because
                    # ``Param.format`` is an open string and a typo
                    # (``datetime`` for ``date-time``) is otherwise
                    # indistinguishable from a constraint that held.
                    logger.warning(
                        "param %r for endpoint %r declares format %r, which "
                        "this engine does not enforce; it is carried as an "
                        "annotation and nothing is checked against it. "
                        "Enforced formats: %s",
                        name,
                        endpoint,
                        value,
                        sorted(_ENFORCED_FORMATS),
                    )
                    continue
                if _non_finite(value):
                    raise RequestSpecError(
                        f"param {name!r} for endpoint {endpoint!r} declares "
                        f"{keyword} {value!r}, which no comparison can order; "
                        f"declare a finite bound or remove the keyword"
                    )
                if keyword == "pattern":
                    try:
                        re.compile(value)
                    except re.error as err:
                        raise RequestSpecError(
                            f"param {name!r} for endpoint {endpoint!r} declares "
                            f"pattern {value!r}, which is not a valid regular "
                            f"expression: {err}"
                        ) from err
                schema[keyword] = normalize_numbers(value)
                authored[keyword] = value
            _refuse(_inverted_intervals(name, endpoint, authored))
            try:
                Draft202012Validator.check_schema(schema)
            except SchemaError as err:
                raise RequestSpecError(
                    f"param {name!r} for endpoint {endpoint!r} declares "
                    f"constraints that are not a usable schema: {err.message}"
                ) from err
            rules[name] = _Rule(
                validator=Draft202012Validator(schema, format_checker=_FORMAT_CHECKER),
                authored=authored,
                required=decl.required,
                controlled=decl.controlled_by is not None,
            )
        return cls(endpoint=endpoint, rules=rules)

    def check_admissible(self, values: Mapping[str, Any]) -> None:
        """Refuse any PRESENT value its own declaration does not admit.

        Says nothing about what is absent. Whether a value is one its
        declaration admits is the same question on every page, in the
        conformance kit and in a live run; whether it is there at all is
        not -- the kit compiles a definition with no connection and no
        secrets, and page one carries no cursor.

        ``None`` alone is absence. ``""``, ``[]`` and ``{}`` are values --
        the binding emits them, so the param is in the request, and whether
        an empty one is admissible is exactly what ``minLength`` and
        ``minItems`` are for.
        """
        problems = [
            problem
            for name, value in values.items()
            if value is not None and name in self.rules
            for problem in self._problems(name, value)
        ]
        _refuse(problems)

    def check_required(self, values: Mapping[str, Any]) -> None:
        """Refuse a required param that resolved to nothing.

        A param a loop owns is exempt: its loop sets it, and the cursor
        strategy deliberately sends no token on the first request, so it is
        correctly absent here. The exemption stops at presence -- once a
        loop has set a value, the author's declaration is the only
        statement about what that param may carry, which is what catches a
        corrupted resume marker.
        """
        _refuse(
            [
                f"param {name!r} for endpoint {self.endpoint!r} is declared "
                f"required and resolved to nothing; every request built from "
                f"this table would go out without it"
                for name, rule in self.rules.items()
                if rule.required and not rule.controlled and values.get(name) is None
            ]
        )

    def _problems(self, name: str, value: Any) -> list[str]:
        """Say, without quoting it, every way one value fails its declaration."""
        rule = self.rules[name]
        if _non_finite(value):
            # Reported INSTEAD of validating, not as well: ``float('nan')``
            # satisfies ``{"minimum": 1, "maximum": 100}`` with no errors, so
            # a keyset loop would advance on a garbage cursor having passed
            # every constraint the author wrote -- and ``Decimal('NaN')``
            # raises ``decimal.InvalidOperation`` out of the library instead.
            return [
                f"param {name!r} for endpoint {self.endpoint!r} received a "
                f"non-finite number, which no declared bound can order"
            ]
        normalized = normalize_numbers(value)
        arrived = _type_name(value)
        problems = []
        for error in rule.validator.iter_errors(normalized):
            keyword = str(error.validator)
            declared = rule.authored[keyword]
            measured = ""
            if keyword in _MEASURED and isinstance(value, (str, list, tuple)):
                measured = f" (measured {len(value)})"
            problems.append(
                f"param {name!r} for endpoint {self.endpoint!r} declares "
                f"{keyword} {declared!r}, which the {arrived} value it "
                f"received does not satisfy{measured}"
            )
        return problems


#: The keyword pairs that bound one quantity from both ends, low then high.
#: ``check_schema`` judges a keyword at a time, so it accepts a pair no
#: value can satisfy.
_INTERVALS: Final = (
    ("minimum", "maximum"),
    ("minLength", "maxLength"),
    ("minItems", "maxItems"),
)


def _inverted_intervals(
    name: str, endpoint: str, authored: Mapping[str, Any]
) -> list[str]:
    """Say which declared intervals admit nothing at all.

    A fact about the document, true before any request is built and
    actionable only by its author, so it is answered here rather than
    rediscovered per value -- and it has to be answered, because an empty
    interval on a loop-owned param first refuses on page two, after page
    one committed rows.
    """
    return [
        f"param {name!r} for endpoint {endpoint!r} declares {low} "
        f"{authored[low]!r} above {high} {authored[high]!r}, which no value "
        f"can satisfy"
        for low, high in _INTERVALS
        if low in authored and high in authored and authored[low] > authored[high]
    ]


def _refuse(problems: Sequence[str]) -> None:
    """Raise every problem as one error, or return if there are none.

    Joined rather than raised as an ``ExceptionGroup``: every member is the
    same defect class with the same handling, so a group gives a caller
    nothing to branch on and costs every log reader a traceback tree.
    """
    if problems:
        raise RequestSpecError("; ".join(problems))
