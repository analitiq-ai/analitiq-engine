"""Which media types a body can be sent as, and how the non-JSON one encodes.

``request.content_type`` (contract 1.0.0rc23) is the author's statement of
what the provider takes. The contract constrains it to the shape of a media
type and no further -- it cannot know which ones this engine can actually
encode -- so the closed set lives here, and a media type outside it is
refused at plan time, where the message can still name what IS supported.

Free of the HTTP client on purpose. The conformance kit certifies a
declared media type from an install that carries no transport, and
``orjson`` counts as transport in this package (see
``tests/unit/cdk_tests/api/test_package_surface.py``) -- so the JSON
encoder stays in :mod:`cdk.api.http` with the round trip, and what lives
here is the vocabulary both sides share plus the encoder that needs
nothing.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from email.message import Message
from typing import Any
from urllib.parse import urlencode

from .exceptions import RequestSpecError

__all__ = [
    "FORM_CONTENT_TYPE",
    "JSON_CONTENT_TYPE",
    "SUPPORTED_CONTENT_TYPES",
    "encode_form",
    "media_type",
]

#: The media type the engine sends when an endpoint declares none.
JSON_CONTENT_TYPE = "application/json"

#: The media type a form-encoded provider takes.
FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"

#: Every media type the engine can turn a declared body into. Closed and
#: engine-owned: the contract types ``content_type`` as a media-type string
#: because a document cannot know which encoders ship here.
SUPPORTED_CONTENT_TYPES = frozenset({JSON_CONTENT_TYPE, FORM_CONTENT_TYPE})


#: The character encoding both encoders emit. ``orjson`` writes UTF-8
#: directly and ``urlencode`` percent-encodes UTF-8 bytes, so this is not a
#: preference -- it is what the engine produces.
_ENCODED_CHARSET = "utf-8"


def _parsed(content_type: str) -> Message:
    """Parse a media type the way the stdlib does.

    ``email.message.Message`` is the parser for this header: it lowercases,
    strips whitespace, unquotes a parameter value and normalizes the
    charset name. Splitting on ``;`` by hand gets the first of those and
    silently misses the rest.
    """
    parsed = Message()
    parsed["content-type"] = content_type
    return parsed


def media_type(content_type: str | None) -> str:
    """Return the media type *content_type* selects, without its parameters.

    ``application/json; charset=utf-8`` selects the same encoder as
    ``application/json``: the parameters describe the bytes, not which
    bytes to produce. They still go out -- what the author declared is what
    is sent -- so only the selection drops them.
    """
    if content_type is None:
        return JSON_CONTENT_TYPE
    return _parsed(content_type).get_content_type()


def unsupported_media_type(content_type: str | None) -> str | None:
    """Why the engine cannot encode a body as *content_type*, or ``None``.

    Two things are judged, because the parameters are sent verbatim and a
    provider is entitled to honour them. The media type has to be one there
    is an encoder for; and a declared ``charset`` has to be the one that
    encoder emits, because both write UTF-8 and nothing here transcodes.
    ``application/json; charset=utf-16`` would otherwise ship UTF-8 bytes
    under a header promising UTF-16, and a provider that believed the header
    would decode a different payload.
    """
    if content_type is None:
        return None
    selected = media_type(content_type)
    if selected not in SUPPORTED_CONTENT_TYPES:
        return (
            f"request.content_type {content_type!r} selects {selected!r}, "
            f"which this engine cannot encode a body as. It encodes "
            f"{', '.join(sorted(SUPPORTED_CONTENT_TYPES))}"
        )
    charset = _parsed(content_type).get_content_charset()
    if charset is not None and charset != _ENCODED_CHARSET:
        return (
            f"request.content_type {content_type!r} declares charset "
            f"{charset!r}, and the engine encodes every body as "
            f"{_ENCODED_CHARSET}. The parameter is sent as declared, so a "
            f"provider honouring it would decode different bytes than were "
            f"sent. Declare {_ENCODED_CHARSET} or leave the charset off"
        )
    return None


def _form_value(name: str, value: Any) -> str:
    """Render one form field, or refuse a value a form cannot carry.

    A form body is a flat sequence of name/value pairs: there is no nesting
    and no typing. A container therefore has no form spelling the engine
    could choose without inventing one -- ``a[0]``, ``a.0`` and repeated
    keys are all somebody's convention and none of them is the provider's
    by default.

    The scalars render as they do everywhere else a value leaves this
    package: a boolean in its JSON spelling rather than Python's, and a
    ``Decimal`` as its exact digits.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (Mapping, list, tuple, set)):
        raise RequestSpecError(
            f"request.body field {name!r} is a {type(value).__name__}, and "
            f"{FORM_CONTENT_TYPE} carries only flat name/value pairs -- there "
            f"is no one way to spell a container in a form, so how it nests "
            f"is the endpoint's to declare. Flatten the field, or declare a "
            f"content_type that carries structure"
        )
    if isinstance(value, (Decimal, datetime, date)):
        return str(value)
    return "" if value is None else str(value)


def encode_form(data: Any) -> bytes:
    """Serialise a body as ``application/x-www-form-urlencoded``."""
    if not isinstance(data, Mapping):
        raise RequestSpecError(
            f"a {FORM_CONTENT_TYPE} body must be an object of name/value "
            f"pairs; this one is a {type(data).__name__}"
        )
    return urlencode(
        [(str(name), _form_value(str(name), value)) for name, value in data.items()]
    ).encode("ascii")
