"""What a declared body is encoded as, and which media types are allowed.

``request.content_type`` (contract 1.0.0rc23) is the author's statement of
what the provider takes. The contract constrains it to the SHAPE of a media
type only -- it cannot know which encoders ship here -- so this is where
the closed set is kept honest.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from cdk.api.body import (
    FORM_CONTENT_TYPE,
    JSON_CONTENT_TYPE,
    media_type,
    unsupported_media_type,
)
from cdk.api.exceptions import RequestSpecError
from cdk.api.http import encode_body

pytestmark = pytest.mark.unit


class TestMediaTypeSelection:
    @pytest.mark.parametrize(
        ("declared", "selected"),
        [
            (None, JSON_CONTENT_TYPE),
            ("application/json", JSON_CONTENT_TYPE),
            ("application/json; charset=utf-8", JSON_CONTENT_TYPE),
            ("APPLICATION/JSON", JSON_CONTENT_TYPE),
            (" application/x-www-form-urlencoded ", FORM_CONTENT_TYPE),
        ],
    )
    def test_parameters_and_case_do_not_change_the_encoder(
        self, declared: str | None, selected: str
    ) -> None:
        """The parameters describe the bytes, not which bytes to produce."""
        assert media_type(declared) == selected
        assert unsupported_media_type(declared) is None

    def test_an_unsupported_type_names_what_is_supported(self) -> None:
        problem = unsupported_media_type("application/xml")
        assert problem is not None
        assert "application/xml" in problem
        assert JSON_CONTENT_TYPE in problem and FORM_CONTENT_TYPE in problem


class TestJsonIsStillTheDefault:
    def test_no_declared_type_encodes_as_json(self) -> None:
        assert encode_body({"a": 1}) == b'{"a":1}'

    def test_a_decimal_keeps_its_exact_digits(self) -> None:
        # Precision is the one thing that cannot be recovered downstream.
        assert encode_body({"n": Decimal("1.10")}) == b'{"n":"1.10"}'

    def test_a_datetime_is_handled_by_the_encoder_itself(self) -> None:
        moment = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        assert b"2026-01-02T03:04:05" in encode_body({"at": moment})


class TestFormEncoding:
    def test_a_flat_object_becomes_form_pairs(self) -> None:
        body = {"grant_type": "client_credentials", "scope": "read write"}
        assert (
            encode_body(body, FORM_CONTENT_TYPE)
            == b"grant_type=client_credentials&scope=read+write"
        )

    def test_the_declared_parameters_still_select_form(self) -> None:
        assert encode_body({"a": "b"}, f"{FORM_CONTENT_TYPE}; charset=utf-8") == b"a=b"

    @pytest.mark.parametrize(
        ("value", "encoded"),
        [
            (True, b"flag=true"),
            (False, b"flag=false"),
            (Decimal("1.10"), b"flag=1.10"),
            (7, b"flag=7"),
        ],
    )
    def test_a_scalar_renders_as_it_does_everywhere_else(
        self, value: object, encoded: bytes
    ) -> None:
        """A boolean in its JSON spelling, not Python's ``True``."""
        assert encode_body({"flag": value}, FORM_CONTENT_TYPE) == encoded

    def test_a_container_field_is_refused_rather_than_guessed(self) -> None:
        # `a[0]`, `a.0` and repeated keys are all somebody's convention and
        # none of them is the provider's by default.
        with pytest.raises(RequestSpecError, match="flat name/value pairs"):
            encode_body({"items": [1, 2]}, FORM_CONTENT_TYPE)

    def test_a_body_that_is_not_an_object_is_refused(self) -> None:
        with pytest.raises(RequestSpecError, match="name/value pairs"):
            encode_body([1, 2], FORM_CONTENT_TYPE)


class TestAnUnsupportedTypeNeverReachesTheWire:
    def test_encoding_refuses_rather_than_sending_json_under_a_lying_header(
        self,
    ) -> None:
        """The plan check should have caught it; this is what makes that true.

        Falling back to JSON would hand the provider bytes that do not match
        the type they arrived under, and the failure would surface as
        whatever that provider does with a malformed body.
        """
        with pytest.raises(RequestSpecError, match="cannot encode"):
            encode_body({"a": 1}, "application/xml")
