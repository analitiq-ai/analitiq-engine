"""Every rule about a URL the API path applies.

The URL rules are the part the landed strategy adapter dropped: without
them a provider-supplied ``next_url`` sends the connection's baked-in auth
headers to any host a response body names.
"""

from __future__ import annotations

from typing import Any

import pytest

from cdk.api.urls import follow_url, join_url, redact_credentials, same_origin
from cdk.exceptions import TransportSpecError
from cdk.transport_factory import require_http_base_url

from .fakes import BASE_URL

pytestmark = pytest.mark.unit


class TestJoinUrl:
    def test_both_segments_survive(self) -> None:
        # urljoin would drop the base's own path on a leading slash.
        assert join_url("https://x/api/v1", "/items") == "https://x/api/v1/items"
        assert join_url("https://x/api/v1/", "items") == "https://x/api/v1/items"


class TestFollowUrl:
    def test_a_relative_target_joins_the_current_page(self) -> None:
        # A query-only link continues from the endpoint path, not the
        # connection root.
        assert (
            follow_url(f"{BASE_URL}/v1/items", "?page=2", origin=BASE_URL)
            == f"{BASE_URL}/v1/items?page=2"
        )

    def test_an_absolute_same_origin_target_is_followed(self) -> None:
        target = f"{BASE_URL}/v1/items?page=2"
        assert follow_url(f"{BASE_URL}/v1/items", target, origin=BASE_URL) == target

    def test_equivalent_origin_spellings_are_the_same_origin(self) -> None:
        target = "https://API.example.test:443/v1/items?page=2"
        assert follow_url(f"{BASE_URL}/v1/items", target, origin=BASE_URL) == target

    def test_a_cross_origin_target_is_refused(self) -> None:
        # The session sends the connection's auth headers on every request.
        with pytest.raises(ValueError, match="leaves the connection's origin"):
            follow_url(
                f"{BASE_URL}/v1/items", "https://evil.test/steal", origin=BASE_URL
            )

    def test_an_uppercase_scheme_still_classifies_as_absolute(self) -> None:
        with pytest.raises(ValueError, match="leaves the connection's origin"):
            follow_url(f"{BASE_URL}/v1/items", "HTTPS://evil.test/x", origin=BASE_URL)

    def test_a_protocol_relative_target_is_refused(self) -> None:
        with pytest.raises(ValueError, match="leaves the connection's origin"):
            follow_url(f"{BASE_URL}/v1/items", "//evil.test/x", origin=BASE_URL)

    def test_a_non_string_target_is_refused(self) -> None:
        with pytest.raises(ValueError, match="expected a URL string"):
            follow_url(f"{BASE_URL}/v1", {"href": "/x"}, origin=BASE_URL)

    @pytest.mark.parametrize(
        ("base", "target", "shared"),
        [
            ("https://a.test", "https://A.test:443", True),
            ("http://a.test:80", "http://a.test", True),
            ("https://a.test", "http://a.test", False),
            ("https://a.test", "https://a.test:8443", False),
            ("https://a.test", "https://b.test", False),
        ],
    )
    def test_same_origin_normalizes_case_and_default_ports(
        self, base: str, target: str, shared: bool
    ) -> None:
        """The normalization is yarl's ``origin()``, not a rule of ours."""
        assert same_origin(base, target) is shared


class TestNoRefusalLogsAPassword:
    """The credential rule is the LAST of four, and the other three quote.

    A URL carrying `user:pass@` that fails an earlier one -- a non-http
    scheme, a query string, an authority yarl cannot parse -- never reaches
    the rule that exists because it carries credentials, so those messages
    are where the password would actually escape. `connect()` logs and
    re-wraps them.
    """

    #: Distinctive on purpose: the refusals talk ABOUT credentials, so
    #: asserting on the word "secret" would match their own prose. This is
    #: a string only the declared value could put in a message.
    PASSWORD = "hunter2-do-not-log"

    @pytest.mark.parametrize(
        ("declared", "refusal"),
        [
            (f"https://user:{PASSWORD}@h/v1?t=a", "no query or fragment"),
            (f"https://user:{PASSWORD}@h/v1#f", "no query or fragment"),
            (f"ftp://user:{PASSWORD}@h", "absolute http(s) URL"),
            (f"https://user:{PASSWORD}@[bad", "not a URL an HTTP client can open"),
            (f"https://user:{PASSWORD}@h", "no credentials"),
        ],
    )
    def test_the_password_is_absent_whichever_rule_fires(
        self, declared: str, refusal: str
    ) -> None:
        with pytest.raises(TransportSpecError) as caught:
            require_http_base_url(declared)
        message = str(caught.value)
        assert refusal in message
        assert self.PASSWORD not in message


class TestRedactCredentials:
    """The one site standing between a declared URL and a log line."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("https://user:pass@h/v1", "https://h/v1"),
            ("https://user@h/v1", "https://h/v1"),
            ("https://h/v1", "https://h/v1"),
            ("https://h/v1?a=1", "https://h/v1?a=1"),
        ],
    )
    def test_userinfo_is_removed_and_the_rest_is_left_alone(
        self, value: str, expected: str
    ) -> None:
        assert redact_credentials(value) == expected

    @pytest.mark.parametrize(
        "value",
        [{"ref": "connection.parameters.host"}, None, 42],
    )
    def test_anything_that_is_not_a_string_comes_back_untouched(
        self, value: Any
    ) -> None:
        """A declaration is often an expression, and this is not a parser."""
        assert redact_credentials(value) is value

    @pytest.mark.parametrize(
        "value", ["https://user:pass@[bad", "https://user@", "https://u:p@h:notaport"]
    )
    def test_an_unparseable_url_that_could_carry_credentials_is_not_shown(
        self, value: str
    ) -> None:
        """Unparseable means the credentials cannot be found and removed.

        ``https://user:secret@[bad`` raises in yarl, and the refusal that
        reports it runs BEFORE the credential rule -- so echoing the value
        would put the password in a message ``connect()`` logs and re-wraps.
        Nothing can be split off safely, so nothing is shown; the exception
        yarl raised still says what was wrong with it.
        """
        assert redact_credentials(value) == "<unparseable url>"
        assert "p@" not in redact_credentials(value)

    @pytest.mark.parametrize(
        "value", ["https://api.example.test:abc", "https://[abc", "https://:443"]
    )
    def test_an_unparseable_url_with_no_userinfo_is_still_shown(
        self, value: str
    ) -> None:
        """Userinfo cannot exist without an ``@``, so there is nothing to hide.

        A typo'd port is the common failure here, and hiding the value
        would leave the author with a refusal that does not say which URL.
        """
        assert redact_credentials(value) == value
