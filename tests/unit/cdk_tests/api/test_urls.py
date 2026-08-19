"""Every rule about a URL the API path applies.

The URL rules are the part the landed strategy adapter dropped: without
them a provider-supplied ``next_url`` sends the connection's baked-in auth
headers to any host a response body names.
"""

from __future__ import annotations

import pytest

from cdk.api.urls import follow_url, join_url, same_origin

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
