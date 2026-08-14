"""Every rule about a URL the API path applies, with no HTTP client.

Separate from :mod:`cdk.api.http` because the rules and the round trip
have different dependency sets and different audiences. ``urllib.parse``
is all a URL rule needs, and the refusal to follow a provider-supplied
link off the connection's origin is a rule the conformance kit must
certify by *executing* it -- from an install that carries the
``conformance`` extra and no HTTP client at all. A kit that could not
import the rule would have to copy it, and a copied rule drifts from the
one the engine runs, which is the single failure mode the kit exists to
prevent.
"""

from __future__ import annotations

from urllib.parse import SplitResult, urljoin, urlsplit

__all__ = ["ORIGIN_REFUSAL_MARKER", "follow_url", "join_url", "same_origin"]

#: The stable fragment of the off-origin refusal below. Exported so a
#: caller recognizing THIS refusal (the conformance kit's origin guard)
#: matches the raise site's own words instead of a copied string that
#: drifts.
ORIGIN_REFUSAL_MARKER = "leaves the connection's origin"

#: Scheme defaults, so ``https://host:443`` and ``https://host`` compare as
#: the one origin they are.
_DEFAULT_PORTS = {"http": 80, "https": 443}


def join_url(base: str, path: str) -> str:
    """Append *path* to *base*, keeping both segments.

    Not ``urljoin``: it treats a leading ``/`` on the path as
    absolute-path-relative and drops the base's own path segment
    (``/api/v1`` + ``/Foo`` -> ``/Foo``, not ``/api/v1/Foo``).
    """
    return base.rstrip("/") + "/" + path.lstrip("/")


def same_origin(base: SplitResult, target: SplitResult) -> bool:
    """Whether two split URLs share scheme, host and effective port.

    Compares normalized parts -- case-insensitive scheme and host, default
    ports made explicit -- so ``https://api.example.test:443`` and
    ``https://API.example.test`` count as the origin they are.
    """
    base_scheme = base.scheme.lower()
    target_scheme = target.scheme.lower()
    return (
        base_scheme == target_scheme
        and (base.hostname or "").lower() == (target.hostname or "").lower()
        and (base.port or _DEFAULT_PORTS.get(base_scheme))
        == (target.port or _DEFAULT_PORTS.get(target_scheme))
    )


def follow_url(current: str, target: str, *, origin: str) -> str:
    """Resolve a provider-supplied next-page URL against the page it came from.

    An absolute target must stay on the connection's origin. The session
    sends the connection's default headers -- auth included -- on every
    request, so following a URL a response body named to another host would
    hand those credentials to it.

    A relative target resolves against the CURRENT page URL per RFC 3986,
    so a query-only link like ``?page=2`` continues from the endpoint path
    instead of the connection root.

    Classification is by parsing, never by string prefix: a target carrying
    any scheme or authority is absolute whatever its case, and the origin
    check then also rejects non-HTTP schemes and ambiguous
    protocol-relative URLs loudly.
    """
    if not isinstance(target, str):
        raise ValueError(
            f"next_url resolved to a {type(target).__name__}, expected a URL string"
        )
    parsed = urlsplit(target)
    if not parsed.scheme and not parsed.netloc:
        return urljoin(current, target)
    root = urlsplit(origin)
    if not same_origin(root, parsed):
        raise ValueError(
            f"next_url {target!r} {ORIGIN_REFUSAL_MARKER} "
            f"{root.scheme}://{root.netloc}; refusing to send the "
            f"connection's headers to another host"
        )
    return target
