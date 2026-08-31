"""Holding a resolved request to the constraints its params declare.

``Param`` carries ten fields describing what a value may be -- ``required``,
``enum``, ``format``, ``pattern``, ``minimum``, ``maximum``, ``minLength``,
``maxLength``, ``minItems``, ``maxItems`` -- and until this landed the request
build read none of them. An endpoint document could declare
``required: true, minimum: 1, maximum: 100`` and the engine would send
whatever the expression resolved to, or send nothing at all.

Nothing at all is the expensive case, and it is why ``required`` is checked
separately from the rest. A param that does not resolve is simply left out of
the request, so a declared narrowing -- a date floor, a tenant id, a status
filter -- disappears silently and the provider answers the UNNARROWED
collection. That answer is a 200 with records in it, so the page loop accepts
it, the rows land, and the run is reported as a success. There is no later
point at which the omission becomes visible; the only place it is still
knowable is here, before the request goes out.

The remaining nine fields are literally JSON Schema keywords, so they are
handed to a JSON Schema implementation rather than re-checked here. So is
``type``, which the contract already required and which the table below
carries as a tenth: it was read before this module (``cdk.api.query_style``
settles a collection's wire spelling from it), never enforced against a
value. A hand
written ``pattern`` test is a second regex dialect, a hand written
``minimum`` test is a second number model, and both would drift from the
meaning the published contract took from JSON Schema in the first place.
What this module owns is the mapping from a ``Param`` to its fragment, and
the message that comes back.

The fragments and their validators are built ONCE, when the operation's
params are read; :meth:`ParamChecker.check` runs per page, and compiling a
schema per page would put the cost of the whole check on the read loop.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import jsonschema
from analitiq.contracts.endpoints import Param

from .exceptions import RequestSpecError

__all__ = ["ParamChecker"]

#: ``Param`` attribute -> the JSON Schema keyword it IS. The pairs differ only
#: where the contract spells a keyword in snake_case under an alias, so this is
#: a rename table and never a translation: nothing here decides what a keyword
#: means. Order is fixed because it is the order the validator reports failures
#: in, and a message that reorders itself between runs is a message no test can
#: pin.
#:
#: A mapping rather than a tuple of pairs because its KEYS are what the
#: consumption census reads: the ``getattr`` below takes its name from here,
#: so this table is what tells the census which ``Param`` fields the engine
#: claims. It is registered in ``tools/contract_consumption.py``'s
#: ``DYNAMIC_ATTRIBUTE_TABLES``, which iterates the table for those names.
_KEYWORDS: dict[str, str] = {
    "type": "type",
    "enum": "enum",
    "format": "format",
    "pattern": "pattern",
    "min_length": "minLength",
    "max_length": "maxLength",
    "min_items": "minItems",
    "max_items": "maxItems",
    "minimum": "minimum",
    "maximum": "maximum",
}

#: The two that ORDER a value rather than describe its shape, and so are the
#: two that cannot be judged in the same number model as the rest. See
#: :func:`_bounds_fragment`.
_ORDERING_KEYWORDS: frozenset[str] = frozenset({"minimum", "maximum"})

#: Equality, like ordering, compares rather than describes -- and compares
#: numbers, so it joins the exact-decimal fragment for the same reason
#: :func:`_bounds_fragment` exists.
_COMPARING_KEYWORDS: frozenset[str] = _ORDERING_KEYWORDS | {"enum"}

#: The keywords whose failure is about a size. The size is reported with the
#: refusal because a count is not the content -- see :func:`_unmet`.
_SIZE_KEYWORDS: frozenset[str] = frozenset(
    {"minLength", "maxLength", "minItems", "maxItems"}
)

#: Two separate things, kept apart because only one of them is a decision.
#:
#: The decision: ``Param.format`` is a free-form ``str`` in the contract and
#: providers name their own (``int64``, ``money``, ``xero-date``), so a name
#: outside JSON Schema's registry is the author saying something to a reader
#: and refusing it would fail documents for being descriptive. Attaching a
#: checker at all is what makes a format enforced where the library knows it
#: and annotation everywhere else.
#:
#: The environment fact: a checker judges only the formats it has a CHECKER
#: for, and several standard ones -- ``date-time`` above all, the format an
#: endpoint author is likeliest to write, and ``uri`` -- need optional
#: packages this tree does not install. Observed against jsonschema 4.25.1,
#: which registers ``date``, ``time``, ``uuid``, ``email``, ``ipv4``,
#: ``ipv6``, ``regex`` and the JSON-pointer pair here. That is an install, not
#: an intent, so it is pinned by tests rather than by this comment:
#: ``TestFormatIsCheckedOnlyWhereThisTreeHasAChecker``.
_FORMAT_CHECKER = jsonschema.FormatChecker()


@dataclass(frozen=True)
class _ParamRule:
    """One param's compiled constraint set."""

    required: bool
    #: Everything but the bounds, judged on the value as it arrived.
    shape: jsonschema.Draft202012Validator
    #: ``minimum`` / ``maximum`` only, judged on the value as an exact
    #: decimal. Separate because these two compare rather than describe.
    bounds: jsonschema.Draft202012Validator


class ParamChecker:
    """Judges a resolved value table against the params an operation declares.

    Built once per operation through :meth:`for_params` and called once per
    page. It answers one question -- does this table satisfy what the endpoint
    document declared -- and answers it for EVERY param before raising, so an
    author fixing a document sees the whole list rather than discovering the
    next failure on the next run.

    Names in the table that the document does not declare are not this class's
    business: the request build decides which values it has a place to put,
    and a value nothing binds is refused there. Judging it here would give the
    same defect two owners and two messages.
    """

    def __init__(self, rules: Mapping[str, _ParamRule], *, endpoint: str) -> None:
        """Take a prepared rule table; :meth:`for_params` is the way in."""
        self._rules = dict(rules)
        self._endpoint = endpoint

    @classmethod
    def for_params(
        cls, declared: Mapping[str, Param], *, endpoint: str
    ) -> ParamChecker:
        """Compile the checker for one operation's declared params.

        Every fragment is put through ``check_schema`` here rather than being
        trusted, because the contract types ``pattern`` as a plain ``str`` and
        a string that is not a regex is a contract-valid document. Compiled
        lazily, that document raises a raw ``re.error`` out of the validator on
        the first page of a read that has already committed rows; compiled
        here, it is a refusal at build time with the endpoint named.
        """
        rules: dict[str, _ParamRule] = {}
        for name, param in declared.items():
            unusable = _unusable_bound(param)
            if unusable is not None:
                raise RequestSpecError(
                    f"param {name!r} for endpoint {endpoint!r} declares "
                    f"{unusable}, which orders against nothing: no value could "
                    f"satisfy it and none could fail it"
                )
            fragment = _shape_fragment(param)
            bounds = _bounds_fragment(param)
            try:
                for candidate in (fragment, bounds):
                    jsonschema.Draft202012Validator.check_schema(
                        candidate, format_checker=_FORMAT_CHECKER
                    )
            except jsonschema.exceptions.SchemaError as err:
                raise RequestSpecError(
                    f"param {name!r} for endpoint {endpoint!r} declares "
                    f"constraints that are not a usable schema: {err.message}. "
                    f"No value of this param could be judged"
                ) from err
            rules[name] = _ParamRule(
                # A param a loop owns is exempt from ``required``, and only
                # from that: the pagination and replication loops decide when
                # their param is in flight, and page one of a cursor scheme
                # legitimately carries no cursor. Holding the author's
                # ``required`` against the engine's own loop would report a
                # defect on page one of a correct document, and report it to
                # the one person who cannot fix it.
                #
                # It stops at ``required`` on purpose. Absence is the loop's
                # to decide; a VALUE is not -- once the loop has set one, the
                # author's declaration is the only statement about what that
                # param may carry, and a token breaking it is as wrong as any
                # other value breaking it.
                required=param.required and param.controlled_by is None,
                shape=jsonschema.Draft202012Validator(
                    fragment, format_checker=_FORMAT_CHECKER
                ),
                bounds=jsonschema.Draft202012Validator(
                    bounds, format_checker=_FORMAT_CHECKER
                ),
            )
        return cls(rules, endpoint=endpoint)

    def check(self, values: Mapping[str, Any]) -> None:
        """Refuse unless every declared param is present and admissible.

        Both halves, for a caller holding every value the run can produce:
        the declared defaults resolved against a real connection, the
        stream's filters, and the replication cursor. Only such a caller can
        read absence as a defect -- see :meth:`check_values` for the one that
        cannot.

        Walks the DECLARED params, not the resolved ones, because the absence
        of a required name is the failure that matters most and absence is
        only visible from the declaration side.

        A param that resolved to nothing -- missing from the table, or present
        as ``None`` -- is the same state twice: the request build sends
        neither, so both are "the param is not in this request".

        ``None`` and nothing else. An empty string, an empty list and an
        empty object are VALUES: the binding emits them, so the param is in
        the request, and whether an empty one is admissible is what
        ``minLength`` and ``minItems`` are for -- a required header sent as
        ``""`` is a thing an endpoint may legitimately declare. Reading
        emptiness as absence here was compensating for ``url_encode``
        answering ``""`` for an unresolved input, which is fixed where it
        happens: that function now answers ``None``, so this one has a single
        signal to read.
        """
        _refuse(
            [
                _missing(name, endpoint=self._endpoint)
                for name, rule in self._rules.items()
                if rule.required and values.get(name) is None
            ]
            + self._unmet_in(values)
        )

    def check_values(self, values: Mapping[str, Any]) -> None:
        """Refuse unless every value PRESENT is one its declaration admits.

        Says nothing about absence, because two callers cannot read absence
        the same way. A page carries the loop's values for that page and no
        others, and the conformance kit compiles a read with no connection at
        all -- its rule, which predates this module, is that a value only a
        run supplies is deferred rather than reported ("deferred on the value,
        never on the declaration"). Judging presence from either would refuse
        a correct document: page one of a cursor scheme carries no cursor, and
        a definition-only compile carries no connection parameter.

        What each of them CAN answer is whether the value it does hold is one
        the declaration admits, and that is the same question on every page
        and in the kit as in a run. :meth:`check` is presence and this.
        """
        _refuse(self._unmet_in(values))

    def _unmet_in(self, values: Mapping[str, Any]) -> list[str]:
        """Every declared param whose present value breaks its own schema.

        A param that resolved to nothing carries no value to judge, so it is
        skipped rather than failed against a ``type`` it was never going to
        carry. A name in *values* the document does not declare is not this
        class's business: the request build decides which values it has a
        place to put, and a value nothing binds is refused there.
        """
        problems: list[str] = []
        for name, rule in self._rules.items():
            value = values.get(name)
            if value is None:
                continue
            unorderable = _non_finite_problem(name, value, endpoint=self._endpoint)
            if unorderable is not None:
                # Reported instead of validated, not as well: the comparisons
                # below are what cannot be trusted for this value.
                problems.append(unorderable)
                continue
            problems.extend(
                _unmet(name, err, endpoint=self._endpoint)
                for err in rule.shape.iter_errors(_as_json_value(value))
            )
            problems.extend(
                _unmet(name, err, endpoint=self._endpoint)
                for err in rule.bounds.iter_errors(_as_exact_decimal(value))
            )
        return problems


def _refuse(problems: list[str]) -> None:
    """Report every problem at once, or return having found none.

    Joined rather than raised as an ExceptionGroup: every member is the same
    defect class with the same handling (``RequestSpecError``, non-retryable,
    fails the stream), so a group would give a caller nothing to branch on and
    cost every log reader a traceback tree. The surrounding request build
    reports the same way.
    """
    if problems:
        raise RequestSpecError("; ".join(problems))


def _non_finite_problem(name: str, value: Any, *, endpoint: str) -> str | None:
    """Why this value cannot be compared to anything, or ``None``.

    ``NaN`` compares false against every bound, so a schema declaring
    ``minimum: 1, maximum: 100`` accepts it: the keyset loop would advance on
    a garbage cursor having satisfied every constraint the author wrote. It
    arrives without anyone authoring one -- a provider body carrying the
    non-standard ``NaN`` / ``Infinity`` literals decodes to Python floats,
    and the keyset strategy walks the last record's value into the next
    page's param. ``Decimal("NaN")`` is worse: comparing it raises
    ``InvalidOperation`` out of the validator, a builtin escaping a module
    whose whole error vocabulary is ``RequestSpecError``.

    So it is named here rather than left to the schema, which cannot refuse
    what it cannot order.
    """
    if isinstance(value, Decimal):
        finite = value.is_finite()
    elif isinstance(value, float):
        finite = math.isfinite(value)
    else:
        return None
    if finite:
        return None
    return (
        f"param {name!r} for endpoint {endpoint!r} resolved to {value!r}, which "
        f"orders against nothing: every declared bound would 'hold' for it and "
        f"the request would go out carrying it"
    )


def _as_json_value(value: Any) -> Any:
    """Render a value in the JSON type model the declared ``type`` is written in.

    One conversion, and only one: an exactly integral ``Decimal`` becomes an
    ``int``. JSON Schema's ``integer`` is an ``isinstance`` test, so a
    ``Decimal("9901")`` -- which is what a keyset param carries after a
    response parsed with ``parse_float=Decimal`` -- would otherwise fail a
    param correctly declared ``type: integer``, on page two of a read whose
    page one had already committed rows.

    Everything else is handed over untouched, and that is the point.
    ``Decimal`` is a ``numbers.Number``, so the validator judges it as a
    ``number`` and compares it against ``minimum`` / ``maximum`` at full
    precision. Converting to ``float`` first would round the JUDGEMENT --
    ``Decimal("100.0000000000000000001")`` passing ``maximum: 100`` -- and it
    would round it exactly where the digits were being kept on purpose.
    """
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return int(value)
    return value


def _unusable_bound(param: Param) -> str | None:
    """Return the declared bound that cannot order anything, or ``None``.

    ``json.loads`` accepts the non-standard ``NaN`` and ``Infinity``
    literals and the contract types both bounds as a plain float, so
    ``"minimum": NaN`` is a document that parses. It compiles as a schema
    too -- it is a number -- and then every comparison against it raises
    ``InvalidOperation`` out of the validator, a builtin escaping a module
    whose whole error vocabulary is ``RequestSpecError``.

    Refused here, once per operation, rather than per value: it is a fact
    about the document, true before any request is built, and the author is
    the only one who can act on it.
    """
    declared = _declared_keywords(param)
    for keyword in _ORDERING_KEYWORDS:
        bound = declared.get(keyword)
        if bound is not None and not math.isfinite(bound):
            return f"{keyword}={bound!r}"
    return None


def _declared_keywords(param: Param) -> dict[str, Any]:
    """Every constraint the param actually declares, under its JSON Schema name.

    The one place this module reads a ``Param`` attribute by a name from
    ``_KEYWORDS``, which is what lets the consumption census see the whole
    table as the claim: a second such site in this module and it can no longer
    tell which table binds which read. The two fragments below filter this
    result rather than reading the param again.

    A key the param leaves unset is omitted rather than emitted as null: a
    fragment carrying ``"pattern": null`` is not a schema, and an omitted
    keyword is exactly JSON Schema's own spelling of "unconstrained".
    """
    declared = {
        keyword: getattr(param, attribute) for attribute, keyword in _KEYWORDS.items()
    }
    return {keyword: value for keyword, value in declared.items() if value is not None}


def _shape_fragment(param: Param) -> dict[str, Any]:
    """Render everything the param says about a value's SHAPE, as JSON Schema.

    ``type`` is always present -- the contract requires it -- and it is
    enforced as declared, with no widening for a value that merely looks like
    the declared type. A ``"5"`` under ``type: integer`` is refused rather than
    read as 5, for two reasons. The declared type is already load-bearing
    elsewhere in this package (``cdk.api.query_style`` settles a collection's
    wire spelling from it and refuses a value of the other shape), so accepting
    a string here would make one field mean two things depending on which
    reader looked at it. And the bounds are defined over numbers only: a string
    admitted under ``type: integer`` sails past every range the author
    declared, which turns this module into the silent pass it exists to remove.
    A config expression yielding a string for an integer param is a defect in
    the config and is fixed there. JSON's own leniency is kept as-is: ``1.0``
    satisfies ``integer`` because JSON Schema says a number with a zero
    fractional part is one, and the contract's vocabulary is JSON's.
    """
    return {
        keyword: value
        for keyword, value in _declared_keywords(param).items()
        if keyword not in _COMPARING_KEYWORDS
    }


def _bounds_fragment(param: Param) -> dict[str, Any]:
    """Render ``minimum`` / ``maximum`` for comparison against exact decimals.

    Its own fragment because these two ORDER a value where every other keyword
    describes one, and ordering is the only thing here that two number models
    disagree about. The contract types both as ``StrictFloat``, so the author's
    ``0.1`` reaches the engine as the binary float nearest to it -- which is
    NOT ``0.1``. A keyset cursor arrives as a ``Decimal`` parsed from the
    provider's own decimal text, and Python compares the two exactly:
    ``Decimal("0.1") < 0.1`` is true. A cursor sitting exactly on a declared
    fractional bound would be refused, on page two of a read whose page one had
    already committed rows.

    So the bound is put back into decimal text (``Decimal(str(...))``, which
    recovers the literal the author wrote for every float Python reprs) and the
    value is put into the same model by :func:`_as_exact_decimal`. Both sides
    from text, one model, no comparison across two.
    """
    return {
        keyword: _exact_members(value) if keyword == "enum" else Decimal(str(value))
        for keyword, value in _declared_keywords(param).items()
        if keyword in _COMPARING_KEYWORDS
    }


def _exact_members(members: Any) -> Any:
    """Put an ``enum``'s numeric members into the model its values are compared in.

    Equality has the float/decimal problem ordering has: a declared ``0.1``
    is the binary float nearest it, a keyset value is a ``Decimal`` parsed
    from decimal text, and the two are not equal. Non-numeric members are
    untouched -- a string is a string in either model.
    """
    if not isinstance(members, list):
        return members
    return [_as_exact_decimal(m) for m in members]


def _as_exact_decimal(value: Any) -> Any:
    """Put a number into the decimal model the bounds are compared in.

    A ``float`` goes through ``str`` for the same reason the bound does: the
    author and the provider both wrote decimal text, and ``str`` is what
    recovers it. A ``Decimal`` is already exact. Everything else is handed over
    untouched -- the bounds keywords ignore a non-number, which is what makes
    it safe for this fragment to carry no ``type`` of its own.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    return value


def _missing(name: str, *, endpoint: str) -> str:
    """Say what an omitted required param costs, not just that it is omitted."""
    return (
        f"param {name!r} for endpoint {endpoint!r} is declared required and "
        f"resolved to no value. The request would be sent without it, and a "
        f"provider that was being narrowed by this param answers the full "
        f"collection instead -- a 200 the read accepts, commits and reports as "
        f"a successful page"
    )


def _unmet(
    name: str, error: jsonschema.exceptions.ValidationError, *, endpoint: str
) -> str:
    """Name the param, the keyword it broke, and what was declared -- not the value.

    The value is deliberately absent, and so is jsonschema's own message,
    which embeds it. A param carries whatever its declaration resolves to,
    and secret-valued params are a supported request shape: a bearer token,
    an API key, an opaque continuation token. This message becomes a
    ``RequestSpecError``, which fails the stream and is logged -- and the
    worker's log redactor masks DSN-shaped credentials only, so anything
    rendered here reaches the run log as written.

    What is left is enough to act on: which param, which endpoint, which
    keyword, what the document declared, and the shape of what arrived. For
    the length and size keywords the measured size is reported too, since a
    count is not the content.
    """
    # The bound is repr'd as the author wrote it, not as the Decimal this
    # module compares it in: `maximum=100.0` is the document, and
    # `maximum=Decimal('100.0')` is an implementation detail the author
    # cannot act on.
    declared = error.validator_value
    if isinstance(declared, Decimal):
        declared = float(declared)
    if isinstance(declared, list):
        declared = [float(m) if isinstance(m, Decimal) else m for m in declared]
    measured = ""
    if error.validator in _SIZE_KEYWORDS:
        try:
            measured = f", size {len(error.instance)}"
        except TypeError:
            measured = ""
    return (
        f"param {name!r} for endpoint {endpoint!r} declares "
        f"{error.validator}={declared!r}, and the value it resolved to "
        f"({type(error.instance).__name__}{measured}) does not satisfy it. "
        f"The value itself is not shown: a param may carry a credential or an "
        f"opaque token, and this message is logged"
    )
