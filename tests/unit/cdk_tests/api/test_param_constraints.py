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
        assert "'void'" in message


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
        assert "500" in message


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
        assert "'since'" in message
        assert "'limit'" in message
        assert "'status'" in message

    def test_several_keywords_on_one_param_are_all_reported(self) -> None:
        message = _refusal(
            _checker(q=_param(minLength=5, pattern="^SKU-")), {"q": "42"}
        )
        assert "pattern=" in message
        assert "minLength=" in message


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
