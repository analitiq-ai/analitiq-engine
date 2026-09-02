"""What a declared param may carry, judged in isolation from any request.

``ParamRules`` is where the nine JSON-Schema value keywords a param may
declare, plus ``required``, become a verdict on one resolved value. The
read path and the conformance kit both drive it through
``ParamTable``/``RequestBuilder`` (see ``test_request.py`` and
``test_read_path.py``); this file is the unit underneath both -- compiling
a declaration, judging a value against it, and the number model that lets a
``Decimal`` off the wire and a ``float`` an author wrote compare equal.
"""

from __future__ import annotations

import decimal
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import pytest
from analitiq.contracts.endpoints import Param

from cdk.api.exceptions import RequestSpecError
from cdk.api.param_rules import ParamRules, normalize_numbers

pytestmark = pytest.mark.unit


def _params(declared: Mapping[str, Any]) -> dict[str, Param]:
    """The operation's declared params, parsed as the caller receives them.

    Parsed rather than handed to ``ParamRules.compile`` as dicts, exactly
    as ``test_request.py``'s own ``_params`` helper: a test writing the
    wire's JSON Schema keys (``minLength``, ``in``, ...) against code
    reading the contract model's attributes would otherwise agree with
    itself and with nothing else.
    """
    return {name: Param.model_validate(spec) for name, spec in declared.items()}


def _rules(declared: Mapping[str, Any]) -> ParamRules:
    return ParamRules.compile(_params(declared), endpoint="items")


class TestCompile:
    def test_a_non_finite_declared_bound_is_refused_once_at_compile(self) -> None:
        # Decidable from the document alone: no comparison can order NaN,
        # so this is caught here rather than re-discovered on every page.
        with pytest.raises(RequestSpecError, match="no comparison can order"):
            _rules(
                {
                    "amt": {
                        "in": "query",
                        "type": "number",
                        "required": False,
                        "minimum": float("nan"),
                    }
                }
            )

    def test_the_compiled_schema_is_reused_not_rebuilt_per_value(self) -> None:
        # Compiled once for the whole read: a page one hundred pays nothing
        # page one did not. Proven by judging two different values against
        # the same ParamRules object without recompiling.
        rules = _rules(
            {
                "amt": {
                    "in": "query",
                    "type": "integer",
                    "required": False,
                    "minimum": 0.0,
                }
            }
        )
        rules.check_admissible({"amt": 1})
        rules.check_admissible({"amt": 2})


class TestCheckAdmissibleNeverQuotesTheValue:
    def test_a_refused_value_is_named_by_size_not_by_content(self) -> None:
        # Params carry bearer tokens and API keys; a refusal renders the
        # measurement, never the instance.
        rules = _rules(
            {
                "token": {
                    "in": "query",
                    "type": "string",
                    "required": False,
                    "maxLength": 3,
                }
            }
        )
        with pytest.raises(RequestSpecError) as caught:
            rules.check_admissible({"token": "secret-token"})
        message = str(caught.value)
        assert "secret-token" not in message
        assert "12" in message

    def test_an_unmeasured_keyword_names_neither_the_value_nor_a_size(self) -> None:
        # ``pattern`` is not a measured keyword, so a refusal for it only
        # reports the type and the declared keyword.
        rules = _rules(
            {
                "token": {
                    "in": "query",
                    "type": "string",
                    "required": False,
                    "pattern": "^[0-9]+$",
                }
            }
        )
        with pytest.raises(RequestSpecError) as caught:
            rules.check_admissible({"token": "s3cret"})
        message = str(caught.value)
        assert "s3cret" not in message
        assert "measured" not in message


class TestTheNumberModel:
    def test_a_decimal_on_its_declared_minimum_is_admitted(self) -> None:
        # A value read back off the wire arrives as an exact Decimal; the
        # declared bound is a StrictFloat. Both enter the same model so a
        # cursor sitting exactly on the bound is not refused.
        rules = _rules(
            {
                "amt": {
                    "in": "query",
                    "type": "number",
                    "required": False,
                    "minimum": 0.1,
                }
            }
        )
        rules.check_admissible({"amt": Decimal("0.1")})

    def test_a_decimal_just_under_the_minimum_is_refused(self) -> None:
        rules = _rules(
            {
                "amt": {
                    "in": "query",
                    "type": "number",
                    "required": False,
                    "minimum": 0.1,
                }
            }
        )
        with pytest.raises(RequestSpecError, match="minimum"):
            rules.check_admissible({"amt": Decimal("0.05")})

    def test_an_exactly_integral_decimal_admits_a_declared_integer(self) -> None:
        # jsonschema's "integer" is an isinstance test and refuses
        # Decimal(9901) outright; a keyset cursor on an integer id has to
        # narrow to int first.
        rules = _rules({"id": {"in": "query", "type": "integer", "required": False}})
        rules.check_admissible({"id": Decimal("9901")})

    def test_an_enum_of_arrays_matches_decimals_arriving_at_depth(self) -> None:
        # Recursive: an enum of arrays matches element by element, and
        # normalising only the outermost number looks right in a test and
        # fails on real data.
        rules = _rules(
            {
                "combo": {
                    "in": "body",
                    "type": "array",
                    "required": False,
                    "enum": [[1, 0.1]],
                }
            }
        )
        rules.check_admissible({"combo": [Decimal("1"), Decimal("0.1")]})

    def test_a_plain_float_still_compares_against_a_fractional_maximum(self) -> None:
        # Not every value arrives as a Decimal: a plain float an earlier
        # expression produced has to compare correctly too.
        rules = _rules(
            {
                "amt": {
                    "in": "query",
                    "type": "number",
                    "required": False,
                    "maximum": 0.1,
                }
            }
        )
        rules.check_admissible({"amt": 0.1})


class TestNonFiniteRuntimeValues:
    def test_a_runtime_nan_float_is_refused(self) -> None:
        rules = _rules(
            {
                "amt": {
                    "in": "query",
                    "type": "number",
                    "required": False,
                    "minimum": 1.0,
                }
            }
        )
        with pytest.raises(RequestSpecError, match="non-finite"):
            rules.check_admissible({"amt": float("nan")})

    def test_a_runtime_nan_decimal_is_refused_without_leaking_invalidoperation(
        self,
    ) -> None:
        # Decimal('NaN') satisfies no comparison and raises
        # decimal.InvalidOperation from inside the validator library if it
        # reaches one; the non-finite check has to intercept it first, so
        # the caller only ever catches RequestSpecError.
        rules = _rules(
            {
                "amt": {
                    "in": "query",
                    "type": "number",
                    "required": False,
                    "minimum": 1.0,
                }
            }
        )
        try:
            rules.check_admissible({"amt": Decimal("NaN")})
        except RequestSpecError:
            pass
        except decimal.InvalidOperation:
            pytest.fail("decimal.InvalidOperation escaped check_admissible")
        else:
            pytest.fail("a non-finite Decimal must be refused")


class TestCheckRequired:
    def test_a_required_param_resolving_to_nothing_is_refused_by_name(self) -> None:
        rules = _rules({"account": {"in": "query", "type": "string", "required": True}})
        with pytest.raises(RequestSpecError) as caught:
            rules.check_required({})
        message = str(caught.value)
        assert "'account'" in message
        assert "'items'" in message

    def test_a_present_required_value_is_not_refused(self) -> None:
        rules = _rules({"account": {"in": "query", "type": "string", "required": True}})
        rules.check_required({"account": "acme"})

    def test_an_absent_optional_value_is_not_refused(self) -> None:
        rules = _rules(
            {"account": {"in": "query", "type": "string", "required": False}}
        )
        rules.check_required({})

    def test_a_controlled_param_absent_is_exempt_from_required(self) -> None:
        # Its loop sets it, and a cursor strategy deliberately sends no
        # token on the first request -- correctly absent here even though
        # the declaration says required.
        rules = _rules(
            {
                "page": {
                    "in": "query",
                    "type": "integer",
                    "required": True,
                    "controlled_by": "pagination",
                }
            }
        )
        rules.check_required({})

    def test_a_controlled_param_is_not_exempt_from_its_other_keywords(self) -> None:
        # The exemption stops at presence: once the loop has set a value,
        # the author's declaration is the only statement about what that
        # param may carry -- which is what catches a corrupted resume
        # marker.
        rules = _rules(
            {
                "page": {
                    "in": "query",
                    "type": "integer",
                    "required": True,
                    "controlled_by": "pagination",
                    "minimum": 0.0,
                    "maximum": 100.0,
                }
            }
        )
        rules.check_required({})  # absent is fine
        with pytest.raises(RequestSpecError, match="maximum"):
            rules.check_admissible({"page": 500})


class TestCheckAdmissibleIgnoresAbsence:
    def test_a_none_value_is_not_judged(self) -> None:
        # None alone is absence; check_admissible answers only whether a
        # PRESENT value is one its declaration admits.
        rules = _rules(
            {
                "amt": {
                    "in": "query",
                    "type": "number",
                    "required": False,
                    "minimum": 1.0,
                }
            }
        )
        rules.check_admissible({"amt": None})

    def test_an_empty_string_is_a_value_and_is_judged(self) -> None:
        # "" is not absence -- the binding emits it, so the param is in the
        # request, and whether an empty one is admissible is exactly what
        # minLength exists to answer.
        rules = _rules(
            {
                "token": {
                    "in": "query",
                    "type": "string",
                    "required": False,
                    "minLength": 1,
                }
            }
        )
        with pytest.raises(RequestSpecError, match="minLength"):
            rules.check_admissible({"token": ""})


class TestNormalizeNumbers:
    def test_a_float_re_enters_through_its_own_decimal_spelling(self) -> None:
        # 0.1 becomes Decimal("0.1"), the text the author wrote -- not the
        # binary float nearest it.
        assert normalize_numbers(0.1) == Decimal("0.1")

    def test_an_exactly_integral_decimal_narrows_to_int(self) -> None:
        assert normalize_numbers(Decimal("9901")) == 9901
        assert isinstance(normalize_numbers(Decimal("9901")), int)

    def test_a_fractional_decimal_stays_a_decimal(self) -> None:
        # Narrowing to float first would round the judgement it is about
        # to be used for.
        assert normalize_numbers(Decimal("0.10")) == Decimal("0.10")
        assert isinstance(normalize_numbers(Decimal("0.10")), Decimal)

    def test_a_bool_is_left_alone(self) -> None:
        # bool is an int subclass, but jsonschema already tells the two
        # apart; touching it here would only break that.
        assert normalize_numbers(True) is True
        assert normalize_numbers(False) is False

    def test_it_recurses_into_mappings_and_sequences(self) -> None:
        normalized = normalize_numbers({"a": [0.1, {"b": Decimal("2")}]})
        assert normalized == {"a": [Decimal("0.1"), {"b": 2}]}

    def test_a_non_numeric_value_passes_through_unchanged(self) -> None:
        assert normalize_numbers("abc") == "abc"
        assert normalize_numbers(None) is None


class TestNumbersTooLargeToRenderAsAnInt:
    """A provider can put a number in a body that CPython will not render.

    ``normalize_numbers`` narrows an integral ``Decimal`` to ``int`` so a
    ``type: integer`` param admits a keyset cursor. Unbounded, that same
    narrowing turns ``Decimal("1E+1000000")`` into a million-digit ``int``
    -- which costs minutes to build and then raises a bare ``ValueError``
    the moment anything renders it, out of the one module whose whole error
    vocabulary is ``RequestSpecError``.
    """

    _DECLARED: Mapping[str, Any] = {
        "cursor": {"in": "query", "type": "number", "required": False, "maximum": 100.0}
    }

    def test_a_number_past_the_render_limit_stays_a_decimal(self) -> None:
        assert isinstance(normalize_numbers(Decimal("1E+100000")), Decimal)

    def test_an_ordinary_integral_decimal_still_narrows(self) -> None:
        # The narrowing the cap protects must survive it: a real id is
        # nowhere near the limit.
        assert normalize_numbers(Decimal("9901")) == 9901
        assert isinstance(normalize_numbers(Decimal("9901")), int)

    def test_it_is_refused_in_words_rather_than_raising_a_builtin(self) -> None:
        rules = _rules(self._DECLARED)
        try:
            rules.check_admissible({"cursor": Decimal("1E+1000000")})
        except RequestSpecError as err:
            assert "maximum" in str(err)
        except ValueError:  # pragma: no cover - the regression this pins
            pytest.fail("a bare ValueError escaped check_admissible")
        else:  # pragma: no cover - the value is over the declared maximum
            pytest.fail("a number over the declared maximum must be refused")


class TestAnUnenforcedFormatSaysSo:
    def test_a_format_outside_the_enforced_set_is_announced(self, caplog) -> None:
        # ``Param.format`` is an open string, so ``datetime`` for
        # ``date-time`` is a typo no contract check catches. It stays an
        # annotation -- JSON Schema's own default -- but silence would make
        # it indistinguishable from a constraint that held.
        with caplog.at_level("WARNING"):
            rules = _rules(
                {
                    "when": {
                        "in": "query",
                        "type": "string",
                        "required": False,
                        "format": "datetime",
                    }
                }
            )
        assert "datetime" in caplog.text
        assert "does not enforce" in caplog.text
        rules.check_admissible({"when": "not a moment at all"})

    def test_an_enforced_format_is_checked_and_announces_nothing(self, caplog) -> None:
        with caplog.at_level("WARNING"):
            rules = _rules(
                {
                    "when": {
                        "in": "query",
                        "type": "string",
                        "required": False,
                        "format": "date-time",
                    }
                }
            )
        assert caplog.text == ""
        with pytest.raises(RequestSpecError, match="format"):
            rules.check_admissible({"when": "not a moment at all"})
