"""Every rule about a URL the API path applies.

The URL rules are the part the landed strategy adapter dropped: without
them a provider-supplied ``next_url`` sends the connection's baked-in auth
headers to any host a response body names.
"""

from __future__ import annotations

from typing import Any

import pytest

from cdk.api.urls import (
    declared_origins,
    follow_url,
    join_url,
    redact_credentials,
    require_declared_origin,
)
from cdk.exceptions import TransportSpecError
from cdk.transport_factory import require_http_base_url

from .fakes import BASE_URL

pytestmark = pytest.mark.unit

#: The declared set a single-transport connector has: one origin.
ONE_ORIGIN = declared_origins([BASE_URL])

#: A connector declaring a second transport -- the file-download shape,
#: one system serving records and documents from two hosts.
TWO_ORIGINS = declared_origins([BASE_URL, "https://files.example.test/v2"])

REFUSAL = "leaves its transport's origin"


class TestJoinUrl:
    def test_both_segments_survive(self) -> None:
        # urljoin would drop the base's own path on a leading slash.
        assert join_url("https://x/api/v1", "/items") == "https://x/api/v1/items"
        assert join_url("https://x/api/v1/", "items") == "https://x/api/v1/items"


class TestDeclaredOriginContainment:
    """The guard both roles apply: a URL has to land on a declared origin."""

    def test_a_url_on_a_declared_origin_passes(self) -> None:
        require_declared_origin(f"{BASE_URL}/v1/items", origins=ONE_ORIGIN)

    def test_a_second_declared_transport_widens_the_set(self) -> None:
        # One system, two origins: the file-download shape a single-origin
        # pin refused while the connector declares both.
        require_declared_origin(
            "https://files.example.test/v2/doc.pdf", origins=TWO_ORIGINS
        )
        with pytest.raises(ValueError, match=REFUSAL):
            require_declared_origin(
                "https://files.example.test/v2/doc.pdf", origins=ONE_ORIGIN
            )

    def test_a_url_off_every_declared_origin_is_refused(self) -> None:
        with pytest.raises(ValueError, match=REFUSAL):
            require_declared_origin("https://evil.test/steal", origins=TWO_ORIGINS)

    def test_the_set_is_built_from_base_urls_not_origins(self) -> None:
        """A transport's base URL carries a path; the guard compares origins."""
        assert declared_origins(["https://a.test/api/v1"]) == frozenset(
            ["https://a.test"]
        )


class TestFollowUrl:
    def test_a_relative_target_joins_the_current_page(self) -> None:
        # A query-only link continues from the endpoint path, not the
        # connection root.
        assert (
            follow_url(f"{BASE_URL}/v1/items", "?page=2", origins=ONE_ORIGIN)
            == f"{BASE_URL}/v1/items?page=2"
        )

    def test_an_absolute_same_origin_target_is_followed(self) -> None:
        target = f"{BASE_URL}/v1/items?page=2"
        assert follow_url(f"{BASE_URL}/v1/items", target, origins=ONE_ORIGIN) == target

    def test_equivalent_origin_spellings_are_the_same_origin(self) -> None:
        target = "https://API.example.test:443/v1/items?page=2"
        assert follow_url(f"{BASE_URL}/v1/items", target, origins=ONE_ORIGIN) == target

    def test_a_link_onto_another_declared_transport_is_followed(self) -> None:
        # The provider hands the read a link to the host it serves
        # documents from, which this connector declares a transport for.
        target = "https://files.example.test/v2/export/1"
        assert follow_url(f"{BASE_URL}/v1/items", target, origins=TWO_ORIGINS) == target

    def test_a_cross_origin_target_is_refused(self) -> None:
        # The session sends the connection's auth headers on every request.
        with pytest.raises(ValueError, match=REFUSAL):
            follow_url(
                f"{BASE_URL}/v1/items", "https://evil.test/steal", origins=ONE_ORIGIN
            )

    def test_an_uppercase_scheme_still_classifies_as_absolute(self) -> None:
        with pytest.raises(ValueError, match=REFUSAL):
            follow_url(
                f"{BASE_URL}/v1/items", "HTTPS://evil.test/x", origins=ONE_ORIGIN
            )

    def test_a_protocol_relative_target_is_refused(self) -> None:
        with pytest.raises(ValueError, match=REFUSAL):
            follow_url(f"{BASE_URL}/v1/items", "//evil.test/x", origins=ONE_ORIGIN)

    def test_a_non_string_target_is_refused(self) -> None:
        with pytest.raises(ValueError, match="expected a URL string"):
            follow_url(f"{BASE_URL}/v1", {"href": "/x"}, origins=ONE_ORIGIN)

    @pytest.mark.parametrize(
        ("declared", "target", "permitted"),
        [
            ("https://a.test", "https://A.test:443", True),
            ("http://a.test:80", "http://a.test", True),
            ("https://a.test", "http://a.test", False),
            ("https://a.test", "https://a.test:8443", False),
            ("https://a.test", "https://b.test", False),
        ],
    )
    def test_membership_normalizes_case_and_default_ports(
        self, declared: str, target: str, permitted: bool
    ) -> None:
        """The normalization is yarl's ``origin()``, not a rule of ours.

        Asked of the guard rather than of a comparison helper, because the
        set the guard is built from and the URL it is probed with have to
        normalize the same way -- which is the whole reason one reduction
        serves both.
        """
        origins = declared_origins([declared])
        if permitted:
            require_declared_origin(target, origins=origins)
        else:
            with pytest.raises(ValueError, match=REFUSAL):
                require_declared_origin(target, origins=origins)


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
