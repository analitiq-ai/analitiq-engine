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