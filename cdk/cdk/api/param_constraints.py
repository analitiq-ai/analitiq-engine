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
handed to a JSON Schema implementation rather than re-checked here. A hand
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

from collections.abc import Mapping
from dataclasses import dataclass
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
    "minimum": "minimum",
    "maximum": "maximum",
    "min_length": "minLength",
    "max_length": "maxLength",
    "min_items": "minItems",
    "max_items": "maxItems",
}

#: Format is annotation-only unless a checker is attached, and even attached it
#: only judges the formats it has a checker FOR. Observed against jsonschema
#: 4.25.1: ``format: "int64"`` on a string validates clean, as does the
#: standard ``date-time`` (its checker needs an optional dependency this tree
#: does not install), while ``date`` and ``uuid`` do fail a bad value. That is
#: the behaviour this module wants and the reason the checker is attached at
#: all: ``Param.format`` is a free-form ``str`` in the contract, providers name
#: their own (``int64``, ``money``, ``xero-date``), and a name outside JSON
#: Schema's registry is the author saying something to a reader -- refusing it
#: would fail documents for being descriptive. So a format is enforced where
#: the library knows it and ignored where it does not.
_FORMATS = jsonschema.FormatChecker()


@dataclass(frozen=True)
class _ParamRule:
    """One param's compiled constraint set."""

    required: bool
    validator: jsonschema.Draft202012Validator


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
            fragment = _fragment(param)
            try:
                jsonschema.Draft202012Validator.check_schema(
                    fragment, format_checker=_FORMATS
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
                # the one person who cannot fix it. A value the loop DOES set
                # is judged like any other.
                required=param.required and param.controlled_by is None,
                validator=jsonschema.Draft202012Validator(
                    fragment, format_checker=_FORMATS
                ),
            )
        return cls(rules, endpoint=endpoint)

    def check(self, values: Mapping[str, Any]) -> None:
        """Refuse the request unless every declared param is satisfied.

        Walks the DECLARED params, not the resolved ones, because the absence
        of a required name is the failure that matters most and absence is
        only visible from the declaration side.

        A param that resolved to nothing -- missing from the table, or present
        as ``None`` -- is the same state twice: the request build sends
        neither, so both are "the param is not in this request". Required, that
        is a refusal; optional, there is no value to hold to the remaining
        constraints and the param is skipped rather than failed against a
        ``type`` it was never going to carry.
        """
        problems: list[str] = []
        for name, rule in self._rules.items():
            if values.get(name) is None:
                if rule.required:
                    problems.append(_missing(name, endpoint=self._endpoint))
                continue
            problems.extend(
                _unmet(name, err, endpoint=self._endpoint)
                for err in rule.validator.iter_errors(values[name])
            )
        if problems:
            # Joined rather than raised as an ExceptionGroup: every member is
            # the same defect class with the same handling (RequestSpecError,
            # non-retryable, fails the stream), so a group would give a caller
            # nothing to branch on and cost every log reader a traceback tree.
            # The surrounding request build reports the same way.
            raise RequestSpecError("; ".join(problems))


def _fragment(param: Param) -> dict[str, Any]:
    """Render one ``Param``'s declared constraints as a JSON Schema fragment.

    Every key the param leaves unset is omitted rather than emitted as null: a
    fragment carrying ``"pattern": null`` is not a schema, and an omitted
    keyword is exactly JSON Schema's own spelling of "unconstrained".

    ``type`` is always present -- the contract requires it -- and it is
    enforced as declared, with no widening for a value that merely looks like
    the declared type. A ``"5"`` under ``type: integer`` is refused rather than
    read as 5, for two reasons. The declared type is already load-bearing
    elsewhere in this package (``cdk.api.query_style`` settles a collection's
    wire spelling from it and refuses a value of the other shape), so accepting
    a string here would make one field mean two things depending on which
    reader looked at it. And ``minimum``/``maximum`` are defined over numbers
    only: a string admitted under ``type: integer`` sails past every range the
    author declared, which turns this module into the silent pass it exists to
    remove. A config expression yielding a string for an integer param is a
    defect in the config and is fixed there. JSON's own leniency is kept as-is:
    ``1.0`` satisfies ``integer`` because JSON Schema says a number with a zero
    fractional part is one, and the contract's vocabulary is JSON's.
    """
    fragment: dict[str, Any] = {}
    for attribute, keyword in _KEYWORDS.items():
        declared = getattr(param, attribute)
        if declared is not None:
            fragment[keyword] = declared
    return fragment


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
    """Name the param, the keyword it broke, what was declared and what came."""
    return (
        f"param {name!r} for endpoint {endpoint!r} declares "
        f"{error.validator}={error.validator_value!r} and resolved to "
        f"{error.instance!r}: {error.message}"
    )
