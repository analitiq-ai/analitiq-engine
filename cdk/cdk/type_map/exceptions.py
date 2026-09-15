"""Exceptions raised by the type-map subsystem."""


class TypeMapError(Exception):
    """Base class for type-map errors."""


class InvalidTypeMapError(TypeMapError):
    """Raised when a type-map file or rule is structurally invalid."""


class TypeMapNotFoundError(InvalidTypeMapError):
    """Raised when a *required* type-map file is absent (not malformed).

    A distinct subclass so callers can tell "this connector simply ships no
    type-map" (fine for API connectors) apart from "the type-map is present but
    broken" (a hard error that must never be silently downgraded). Subclasses
    :class:`InvalidTypeMapError` so existing broad handlers still catch it.
    """


class MissingEncodingError(TypeMapError):
    """Raised when a field needs a decode/encode declaration and has none.

    A field whose wire shape is not a direct cast into (read) or out of
    (write) its declared ``arrow_type`` must name the catalog entry that
    bridges the two. No catalog entry -- including ``iso8601``/``epoch`` --
    is applied implicitly; this is the enforcement point for that rule.
    """

    def __init__(
        self, field_name: str, arrow_type: str, *, direction: str, key: str
    ) -> None:
        self.field_name = field_name
        self.arrow_type = arrow_type
        self.direction = direction
        self.key = key
        super().__init__(
            f"field {field_name!r} declares arrow_type {arrow_type!r}, whose "
            f"{direction} wire shape is not a direct cast, but declares no "
            f"{key!r}; name a catalog entry (or 'code' with a connector.py "
            f"override) so the {direction} is not left to an implicit default"
        )


class UnmappedTypeError(TypeMapError):
    """Raised when a native type has no matching rule in the connector's type-map.

    The engine must never silently default to ``Utf8`` (or any other type).
    This exception carries the unmapped input so operators can see exactly
    what the source reported. ``direction`` is ``"forward"`` for read-map
    misses (native -> canonical, via ``to_arrow_type``) and ``"reverse"`` for
    write-map misses (canonical -> native, via ``to_native_type``).
    """

    def __init__(self, connector_slug: str, direction: str, value: str) -> None:
        self.connector_slug = connector_slug
        self.direction = direction
        self.value = value
        super().__init__(
            f"No {direction} type-map rule for {value!r} in connector "
            f"{connector_slug!r}"
        )
