"""What the constraints a param declares actually refuse.

``Param`` has carried ``required``, ``enum``, ``format``, ``pattern``, the two
ranges, the two lengths and the two item counts since the contract's first
release, and the request build read none of them: a document could declare a
required, bounded, enumerated param and the engine would send anything, or
nothing. So the tests below ask, per keyword, what now goes out and what is
refused -- and in particular that an absent required param is a refusal rather
than a request that quietly widens the read.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import textwrap
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from analitiq.contracts.endpoints import Param

from cdk.api.exceptions import RequestSpecError
from cdk.api.param_constraints import ParamChecker

pytestmark = pytest.mark.unit


def _param(**declared: Any) -> Param:
    """One declared param, parsed as the operation's params are.

    Parsed through the model rather than constructed, so the aliases the
    contract spells (``in``, ``minLength``, ``minItems``) are exercised the way
    an endpoint document reaches the engine.
    """
    return Param.model_validate(
        {"in": "query", "type": "string", "required": False, **declared}
    )


def _checker(**declared: Param) -> ParamChecker:
    """A checker over the named params, built the way an operation builds one."""
    return ParamChecker.for_params(declared, endpoint="items")


def _refusal(checker: ParamChecker, values: dict[str, Any]) -> str:
    """The message a refused value table produces."""
    with pytest.raises(RequestSpecError) as raised:
        checker.check(values)
    return str(raised.value)


class TestRequired:
    """A required param that resolves to nothing is the expensive omission."""

    def test_absent_name_is_refused(self) -> None:
        message = _refusal(_checker(since=_param(required=True)), {})
        assert "'since'" in message
        assert "'items'" in message
        assert "required" in message

    def test_present_as_none_is_refused(self) -> None:
        """``None`` is not sent either, so it is the same state as absence."""
        message = _refusal(_checker(since=_param(required=True)), {"since": None})
        assert "'since'" in message

    def test_optional_absent_passes(self) -> None:
        _checker(since=_param(required=False)).check({})

    def test_optional_none_is_not_held_to_its_type(self) -> None:
        """An optional param that resolved to nothing is left out, not typed."""
        _checker(since=_param(required=False, type="integer")).check({"since": None})

    def test_required_and_present_passes(self) -> None:
        _checker(since=_param(required=True)).check({"since": "2024-01-01"})


class TestType:
    """The declared type is enforced as declared -- no widening for lookalikes."""

    @pytest.mark.parametrize(
        ("kind", "value"),
        [
            ("string", "x"),
            ("integer", 5),
            # JSON's own rule, kept: a number with a zero fractional part IS an
            # integer, and the contract's type vocabulary is JSON's.
            ("integer", 5.0),
            ("number", 1.5),
            ("boolean", True),
            ("array", ["a"]),
            ("object", {"a": 1}),
        ],
    )
    def test_declared_type_passes(self, kind: str, value: Any) -> None:
        # A query param typed array or object must declare its serialization --
        # the contract refuses one that does not, so the fixture supplies it.
        container = (
            {"style": "form", "explode": True} if kind in ("array", "object") else {}
        )
        _checker(field=_param(type=kind, **container)).check({"field": value})

    def test_numeric_string_is_refused_under_integer(self) -> None:
        """A string is not narrowed: every declared range would skip it."""
        message = _refusal(_checker(limit=_param(type="integer")), {"limit": "5"})
        assert "'limit'" in message
        assert "type='integer'" in message

    def test_integer_is_refused_under_string(self) -> None:
        message = _refusal(_checker(name=_param(type="string")), {"name": 5})
        assert "type='string'" in message


class TestEnum:
    def test_declared_member_passes(self) -> None:
        _checker(status=_param(enum=["open", "closed"])).check({"status": "open"})

    def test_value_outside_the_enum_is_refused(self) -> None:
        message = _refusal(
            _checker(status=_param(enum=["open", "closed"])), {"status": "void"}
        )
        assert "enum=['open', 'closed']" in message
        # The value is named by TYPE, never rendered -- see TestAValueIsNeverLogged.
        assert "'void'" not in message
        assert "(str)" in message


class TestFormat:
    def test_known_format_refuses_a_bad_value(self) -> None:
        message = _refusal(
            _checker(on=_param(format="date")), {"on": "the-first-of-never"}
        )
        assert "format='date'" in message

    def test_known_format_passes_a_good_value(self) -> None:
        _checker(on=_param(format="date")).check({"on": "2024-01-01"})

    def test_unknown_format_is_an_annotation(self) -> None:
        """``Param.format`` is free-form: providers name formats JSON Schema does
        not define (``int64``, ``money``), and those must describe rather than
        refuse. Observed against jsonschema 4.25.1 with a ``FormatChecker``
        attached: a format with no registered checker validates clean.
        """
        checker = _checker(cursor=_param(format="int64"), issued=_param(format="money"))
        checker.check({"cursor": "not-an-int64", "issued": "not-money"})


class TestPattern:
    def test_matching_value_passes(self) -> None:
        _checker(sku=_param(pattern="^SKU-[0-9]+$")).check({"sku": "SKU-42"})

    def test_non_matching_value_is_refused(self) -> None:
        message = _refusal(_checker(sku=_param(pattern="^SKU-[0-9]+$")), {"sku": "42"})
        assert "pattern='^SKU-[0-9]+$'" in message

    def test_unusable_pattern_is_refused_at_build_time(self) -> None:
        """Compiled lazily it would raise ``re.error`` mid-read, from a document
        the contract accepts -- ``pattern`` is typed as a plain string.
        """
        with pytest.raises(RequestSpecError) as raised:
            _checker(sku=_param(pattern="["))
        assert "'sku'" in str(raised.value)
        assert "'items'" in str(raised.value)


class TestRanges:
    def test_within_the_range_passes(self) -> None:
        _checker(limit=_param(type="integer", minimum=1, maximum=100)).check(
            {"limit": 50}
        )

    def test_below_the_minimum_is_refused(self) -> None:
        message = _refusal(
            _checker(limit=_param(type="integer", minimum=1)), {"limit": 0}
        )
        assert "minimum=1.0" in message

    def test_above_the_maximum_is_refused(self) -> None:
        message = _refusal(
            _checker(limit=_param(type="integer", maximum=100)), {"limit": 500}
        )
        assert "maximum=100.0" in message
        assert "500" not in message
        assert "(int)" in message


class TestLengths:
    def test_within_the_lengths_passes(self) -> None:
        _checker(q=_param(minLength=2, maxLength=8)).check({"q": "abcd"})

    def test_below_the_minimum_length_is_refused(self) -> None:
        message = _refusal(_checker(q=_param(minLength=2)), {"q": "a"})
        assert "minLength=2" in message

    def test_above_the_maximum_length_is_refused(self) -> None:
        message = _refusal(_checker(q=_param(maxLength=8)), {"q": "abcdefghij"})
        assert "maxLength=8" in message


class TestItemCounts:
    def _tags(self, **declared: Any) -> Param:
        """An array param, which the contract requires a style and explode on."""
        return _param(type="array", style="form", explode=True, **declared)

    def test_within_the_counts_passes(self) -> None:
        _checker(tags=self._tags(minItems=1, maxItems=3)).check({"tags": ["a", "b"]})

    def test_too_few_items_is_refused(self) -> None:
        message = _refusal(_checker(tags=self._tags(minItems=1)), {"tags": []})
        assert "minItems=1" in message

    def test_too_many_items_is_refused(self) -> None:
        message = _refusal(
            _checker(tags=self._tags(maxItems=2)), {"tags": ["a", "b", "c"]}
        )
        assert "maxItems=2" in message


class TestUnconstrained:
    def test_a_param_declaring_only_its_type_passes_anything_of_it(self) -> None:
        _checker(note=_param()).check({"note": "anything at all"})

    def test_an_undeclared_name_is_not_this_checker_s_business(self) -> None:
        """The request build refuses a value nothing binds; two owners would
        mean two messages for one defect.
        """
        _checker(note=_param()).check({"note": "x", "stowaway": object()})

    def test_no_params_at_all_passes(self) -> None:
        _checker().check({})


class TestAggregation:
    def test_every_unmet_constraint_is_reported_at_once(self) -> None:
        checker = _checker(
            since=_param(required=True),
            limit=_param(type="integer", maximum=100),
            status=_param(enum=["open"]),
        )
        message = _refusal(checker, {"limit": 500, "status": "void"})
        # Each named with the reason it failed, not merely named: a checker
        # reporting "missing" for all three would satisfy the names alone.
        assert "'since'" in message and "declared required" in message
        assert "'limit'" in message and "maximum=100" in message
        assert "'status'" in message and "enum=['open']" in message

    def test_several_keywords_on_one_param_are_all_reported(self) -> None:
        message = _refusal(
            _checker(q=_param(minLength=5, pattern="^SKU-")), {"q": "42"}
        )
        assert "pattern=" in message
        assert "minLength=" in message


class TestARequiredParamMustReachTheWire:
    """Present in the table is not the same as present in the request.

    An empty array under exploded `form`, and an empty object under
    `deepObject`, serialize to no query pairs at all -- so the required
    narrowing is absent from the request however present it looks here, and
    the provider answers the whole collection. The same empty array under
    `explode: false` sends `ids=` and does reach, which is why this asks the
    serializer rather than keeping a list of shapes.
    """

    @pytest.mark.parametrize(
        ("declared", "value"),
        [
            ({"type": "array", "style": "form", "explode": True}, []),
            ({"type": "object", "style": "deepObject", "explode": True}, {}),
            ({"type": "object", "style": "form", "explode": True}, {}),
        ],
    )
    def test_a_value_serializing_to_no_pairs_does_not_satisfy_required(
        self, declared: dict[str, Any], value: Any
    ) -> None:
        message = _refusal(
            _checker(ids=_param(required=True, **declared)), {"ids": value}
        )
        assert "'ids'" in message and "required" in message

    def test_an_empty_array_that_still_sends_satisfies_required(self) -> None:
        _checker(
            ids=_param(required=True, type="array", style="form", explode=False)
        ).check({"ids": []})

    def test_a_populated_collection_satisfies_required(self) -> None:
        _checker(
            ids=_param(required=True, type="array", style="form", explode=True)
        ).check({"ids": [1]})

    def test_a_scalar_query_param_is_unaffected(self) -> None:
        _checker(q=_param(required=True, type="string")).check({"q": ""})


class TestABoundMustBeAbleToOrder:
    """A bound that orders against nothing is refused with the document.

    `json.loads` accepts the non-standard `NaN`/`Infinity` literals and the
    contract types both bounds as a plain float, so such a document parses
    and compiles as a schema. Every comparison against it then raised
    `InvalidOperation` -- a builtin escaping a module whose whole error
    vocabulary is `RequestSpecError`.
    """

    @pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
    def test_a_non_finite_bound_is_refused_at_build_time(self, literal: str) -> None:
        declared = json.loads(
            '{"in":"query","type":"number","required":false,"minimum":' + literal + "}"
        )
        with pytest.raises(RequestSpecError, match="orders against nothing"):
            _checker(x=Param.model_validate(declared))

    def test_a_finite_bound_still_compiles(self) -> None:
        _checker(x=_param(type="number", minimum=1, maximum=100)).check_values(
            {"x": 50}
        )

    def test_an_inverted_interval_is_refused_at_build_time(self) -> None:
        """`check_schema` accepts `minimum: 10, maximum: 1` -- it validates each
        keyword's shape, never whether two of them can hold at once. Left to the
        values it surfaces on whichever page first carries one, which for a param
        a loop owns is page two, after page one has committed.
        """
        with pytest.raises(RequestSpecError, match="an interval no value is inside"):
            _checker(x=_param(type="number", minimum=10, maximum=1))

    @pytest.mark.parametrize(
        ("floor", "ceiling", "declared"),
        [
            ("minLength", "maxLength", {"type": "string"}),
            (
                "minItems",
                "maxItems",
                {"type": "array", "style": "form", "explode": True},
            ),
        ],
    )
    def test_an_inverted_size_interval_is_refused_at_build_time(
        self, floor: str, ceiling: str, declared: dict[str, Any]
    ) -> None:
        # An author who can invert the numeric pair can invert either size
        # pair, and the answer must not depend on which one they chose.
        with pytest.raises(RequestSpecError, match="an interval no value is inside"):
            _checker(x=_param(**declared, **{floor: 10, ceiling: 1}))

    def test_a_single_point_interval_is_usable(self) -> None:
        # `minimum == maximum` admits exactly one value, which is a narrowing
        # an author may legitimately write.
        checker = _checker(x=_param(type="number", minimum=5, maximum=5))
        checker.check_values({"x": 5})
        assert "maximum" in _refusal(checker, {"x": 6})


class TestAValueIsNeverLogged:
    """A refusal says which param and what was declared, never what arrived.

    Secret-valued params are a supported request shape -- a bearer token, an
    API key, an opaque continuation token -- and this message becomes a
    `RequestSpecError` that fails the stream and is logged. The worker's log
    redactor masks DSN-shaped credentials only, so anything rendered here
    reaches the run log as written.
    """

    #: Stands in for a credential without being shaped like one. A realistic
    #: key literal here is a real finding for the repository's secret scan --
    #: which cannot tell a test fixture from a leak, and should not try.
    _CANARY = "canary-value-that-must-never-be-logged"

    def test_a_secret_value_does_not_reach_the_message(self) -> None:
        secret = self._CANARY
        message = _refusal(
            _checker(token=_param(type="string", pattern="^tok_")), {"token": secret}
        )
        assert secret not in message
        assert "token" in message and "pattern=" in message

    def test_jsonschemas_own_message_is_not_passed_through(self) -> None:
        # jsonschema renders the instance in `err.message` too, so the
        # refusal is built rather than forwarded.
        message = _refusal(
            _checker(q=_param(type="string", enum=["a"])), {"q": self._CANARY}
        )
        assert self._CANARY not in message
        assert "is not one of" not in message

    def test_a_size_failure_reports_the_size_not_the_content(self) -> None:
        message = _refusal(
            _checker(q=_param(type="string", maxLength=4)), {"q": self._CANARY}
        )
        assert self._CANARY not in message
        assert "size 38" in message


class TestABoundIsComparedInOneNumberModel:
    """A value sitting exactly on a declared bound passes, from either side.

    The contract types `minimum`/`maximum` as floats, so an author's `0.1`
    reaches the engine as the binary float nearest it -- which is not `0.1`.
    A keyset cursor arrives as a Decimal parsed from the provider's decimal
    text. Compared across the two models, `Decimal("0.1") < 0.1` is true and a
    cursor exactly on its declared floor is refused on page two of a read
    whose page one had already committed.
    """

    def test_a_decimal_on_its_declared_minimum_passes(self) -> None:
        _checker(x=_param(type="number", minimum=0.1)).check_values(
            {"x": Decimal("0.1")}
        )

    def test_a_decimal_on_its_declared_maximum_passes(self) -> None:
        _checker(x=_param(type="number", maximum=0.3)).check_values(
            {"x": Decimal("0.3")}
        )

    def test_a_float_on_its_declared_bound_still_passes(self) -> None:
        # The other side of the same coin: putting the BOUND into decimal
        # text without doing the same to the value would swap which of the
        # two is wrongly refused.
        checker = _checker(x=_param(type="number", minimum=0.1, maximum=0.3))
        checker.check_values({"x": 0.1})
        checker.check_values({"x": 0.3})

    def test_a_nested_number_is_normalised_too(self) -> None:
        # "One number model" is a claim about the whole value or it is not a
        # claim: an enum of arrays holds its numbers a level down, and the
        # response parser supplies Decimal at that same depth.
        declared = {"type": "array", "style": "form", "explode": True}
        checker = _checker(ids=_param(enum=[[0.1], [0.2]], **declared))
        checker.check_values({"ids": [Decimal("0.1")]})
        checker.check_values({"ids": [0.1]})
        assert "enum=" in _refusal(checker, {"ids": [Decimal("0.9")]})

    def test_a_nested_number_in_an_object_member_is_normalised(self) -> None:
        checker = _checker(
            f=_param(type="object", style="deepObject", explode=True, enum=[{"v": 0.1}])
        )
        checker.check_values({"f": {"v": Decimal("0.1")}})

    def test_a_decimal_past_the_bound_is_still_refused(self) -> None:
        # Exactness cuts both ways: the digits that make the boundary pass
        # are the digits that make one ulp past it fail.
        message = _refusal(
            _checker(x=_param(type="number", maximum=100)),
            {"x": Decimal("100.0000000000000000001")},
        )
        # Reported as the author wrote the bound, not as the Decimal the
        # comparison happens in.
        assert "maximum=100.0" in message

    def test_a_non_finite_value_orders_against_nothing_and_is_refused(self) -> None:
        # NaN compares false against every bound, so a param declaring both
        # would "satisfy" them and the keyset loop would advance on it.
        for value in (float("nan"), Decimal("NaN"), Decimal("Infinity")):
            assert "orders against nothing" in _refusal(
                _checker(x=_param(type="number", minimum=1, maximum=100)), {"x": value}
            )


class TestTheEnforcedFormatSetIsFixed:
    """Which formats are enforced is this module's decision, not the venv's.

    `FormatChecker()` inherits whatever checkers happen to be registered in
    the interpreter, and several standard formats register theirs only when an
    optional package is installed. A connector pulling `rfc3339-validator` in
    for its own reasons would have `date-time` enforced inside its worker and
    nowhere else, so the same document would pass in one consumer and fail in
    another.
    """

    def test_the_registry_is_exactly_what_is_declared(self) -> None:
        from cdk.api.param_constraints import _ENFORCED_FORMATS, _FORMAT_CHECKER

        assert sorted(_FORMAT_CHECKER.checkers) == sorted(_ENFORCED_FORMATS)

    def test_every_enforced_format_has_a_checker_in_a_bare_install(self) -> None:
        """The declared set must hold in the install the extras actually produce.

        `FormatChecker(formats=...)` indexes the registry eagerly, so naming a
        format whose checker jsonschema registers only when an optional package
        is present is not a weaker check -- it is a `KeyError` while importing
        this module, and `analitiq-cdk[api]` that cannot be imported at all.
        The developer venv hides it: something else pulls `jsonpointer` and
        `idna` in, and the same wheel then fails in a connector's.

        Run in a subprocess with those packages blocked, because jsonschema
        registers its checkers at import time and this process has already
        imported it.
        """
        blocked = (
            "jsonpointer",
            "idna",
            "rfc3339_validator",
            "rfc3987",
            "uri_template",
            "webcolors",
            "isoduration",
            "fqdn",
        )
        script = textwrap.dedent(
            f"""
            import sys

            class Blocker:
                def find_spec(self, name, path=None, target=None):
                    if name.split(".")[0] in {blocked!r}:
                        raise ImportError(name)
                    return None

            sys.meta_path.insert(0, Blocker())
            from cdk.api.param_constraints import _ENFORCED_FORMATS, _FORMAT_CHECKER

            assert sorted(_FORMAT_CHECKER.checkers) == sorted(_ENFORCED_FORMATS), (
                sorted(_FORMAT_CHECKER.checkers)
            )
            """
        )
        completed = subprocess.run(  # noqa: S603
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            check=False,
            cwd=Path(__file__).resolve().parents[4],
            env={**os.environ, "PYTHONPATH": "cdk"},
        )
        assert completed.returncode == 0, completed.stderr

    def test_an_enforced_format_refuses_a_bad_value(self) -> None:
        assert "format='date'" in _refusal(
            _checker(day=_param(format="date")), {"day": "not-a-date"}
        )

    def test_time_is_annotation_because_its_checker_is_the_wrong_draft(
        self,
    ) -> None:
        """`time` is the one name always registered under semantics the
        contract's reader would not expect.

        The Draft 2020-12 checker needs `rfc3339-validator`; the one that is
        always there is Draft 3's `strptime("%H:%M:%S")`, which refuses
        `12:00:00Z`. Enforcing it would refuse a valid time under a rule
        nobody wrote.
        """
        _checker(at=_param(format="time")).check_values({"at": "12:00:00Z"})

    def test_a_format_outside_the_set_is_annotation_only(self) -> None:
        # `date-time` is a standard format this engine does not enforce, and
        # `money` is a provider's own. Both describe rather than refuse, and
        # neither changes if a package appears in the environment.
        checker = _checker(at=_param(format="date-time"), amount=_param(format="money"))
        checker.check_values({"at": "not-a-timestamp", "amount": "not-money"})


class TestTheDocumentedSetIsThePinnedOne:
    """`docs/source-config.md` tells connector authors which formats refuse.

    Prose is a copy of a decision made in code, and this copy already drifted
    once: it promised `json-pointer` enforcement that the module deliberately
    does not do, so an author would have written a document expecting a
    refusal that never comes. A copy that cannot be generated has to be
    pinned, or it drifts again the next time the set changes.
    """

    def test_the_docs_name_exactly_the_enforced_formats(self) -> None:
        from cdk.api.param_constraints import _ENFORCED_FORMATS

        doc = (
            Path(__file__).resolve().parents[4] / "docs" / "source-config.md"
        ).read_text()
        promised = re.search(
            r"`format` is enforced for the formats the engine has a checker "
            r"for \(([^)]*)\)",
            doc,
        )
        assert promised is not None, "the docs no longer state which formats refuse"
        named = re.findall(r"`([^`]+)`", promised.group(1))
        assert sorted(named) == sorted(_ENFORCED_FORMATS)


class TestReuse:
    def test_one_checker_judges_page_after_page(self) -> None:
        """Built once per operation and called per page, so it must carry no
        state from the call before it -- including a call that refused.
        """
        checker = _checker(limit=_param(type="integer", minimum=1, maximum=100))
        checker.check({"limit": 50})
        assert "maximum" in _refusal(checker, {"limit": 500})
        checker.check({"limit": 1})
        assert "minimum" in _refusal(checker, {"limit": 0})
        checker.check({"limit": 100})
