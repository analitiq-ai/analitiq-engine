"""Every rule about a URL the API path applies, asked of ``yarl``.

``yarl`` is what ``aiohttp`` builds every request URL with, so it is the
authority on what a URL means -- not a second opinion this module could
form. Joining a path, deciding whether two URLs share an origin, resolving
a provider's next link: each is one call here, and what the engine sends
is what the library says.

Separate from :mod:`cdk.api.http` because these rules need no HTTP client
and no network. The conformance kit certifies them by *executing* them,
from an install that carries no transport -- which is why ``yarl`` is a
core dependency rather than one riding in behind ``aiohttp``. A kit that
could not import it would have to copy the rules, and a copied rule is
always one rule behind the client that actually sends the request.
"""

from __future__ import annotations

from yarl import URL

# Defined in the core transport module, not here: the base-url refusals
# that must not log a password are core, and core cannot import this
# package. Re-exported so the URL rules still read as one surface.
from ..transport_factory import redact_credentials

__all__ = [
    "ORIGIN_REFUSAL_MARKER",
    "follow_url",
    "join_url",
    "redact_credentials",
    "same_origin",
]

#: The stable fragment of the off-origin refusal below. Exported so a
#: caller recognizing THIS refusal (the conformance kit's origin guard)
#: matches the raise site's own words instead of a copied string that
#: drifts.
ORIGIN_REFUSAL_MARKER = "leaves the connection's origin"


def join_url(base: str, path: str) -> str:
    """Append *path* to *base*, keeping both segments.

    Not ``urljoin``: that treats a leading ``/`` as absolute-path-relative
    and drops the base's own path (``/api/v1`` + ``/Foo`` -> ``/Foo``), so
    the leading slash is stripped before the path is handed over.

    ``encoded=True`` because *path* arrives already encoded --
    :func:`~cdk.api.request.substitute_path` percent-encodes every value it
    substitutes, so a segment holding ``a/b`` is already ``a%2Fb``. Letting
    yarl encode it again sends ``a%252Fb``, which is a different resource.
    """
    return str(URL(base).joinpath(path.lstrip("/"), encoded=True))


def same_origin(base: str, target: str) -> bool:
    """Whether two URLs share scheme, host and effective port.

    ``URL.origin()`` is the normalization -- case-folded host, default port
    made explicit -- so ``https://API.example.test:443`` and
    ``https://api.example.test`` are the one origin they are. Compared as
    strings because two ``origin()`` results that render identically still
    compare unequal when one was spelled with its default port.
    """
    return str(URL(base).origin()) == str(URL(target).origin())


def follow_url(current: str, target: str, *, origin: str) -> str:
    """Resolve a provider-supplied next-page URL against the page it came from.

    ``URL.join`` is RFC 3986 resolution, the same one the client applies:
    a relative target continues from the CURRENT page (so ``?page=2`` keeps
    the endpoint path), an absolute one replaces it, and a
    protocol-relative ``//host/p`` takes the current scheme -- which the
    origin check below then judges like any other absolute target.

    That check is the reason this function exists at all. The session sends
    the connection's default headers, credentials included, on every
    request, so following a link a response body pointed at another host
    would hand them to it.

    A target that carries its own scheme goes back verbatim: it is the
    provider's own string, and re-rendering it could only differ from what
    the provider asked for. Anything else -- relative, or the schemeless
    ``//host/path`` -- goes back resolved, because that is the URL the
    request will actually go to.
    """
    if not isinstance(target, str):
        raise ValueError(
            f"next_url resolved to a {type(target).__name__}, expected a URL string"
        )
    link = URL(target)
    resolved = str(URL(current).join(link))
    if not same_origin(origin, resolved):
        raise ValueError(
            f"next_url {target!r} {ORIGIN_REFUSAL_MARKER} "
            f"{URL(origin).origin()}; refusing to send the connection's "
            f"headers to another host"
        )
    return target if link.scheme else resolved
