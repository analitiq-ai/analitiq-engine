"""Exceptions raised by the type-map subsystem."""


class TypeMapError(Exception):
    """Base class for type-map errors."""


class InvalidTypeMapError(TypeMapError):
    """Raised when a type-map file or rule is structurally invalid."""


class UnmappedTypeError(TypeMapError):
    """Raised when a native type has no matching rule in the connector's type-map.

    The engine must never silently default to ``Utf8`` (or any other type).
    This exception carries the unmapped input so operators can see exactly
    what the source reported. ``direction`` is always ``"forward"`` today;
    the field is kept as a structured attribute so future destination-side
    files (authored from the canonical end) can report ``"reverse"`` misses
    without changing the type.
    """

    def __init__(self, connector_slug: str, direction: str, value: str) -> None:
        self.connector_slug = connector_slug
        self.direction = direction
        self.value = value
        super().__init__(
            f"No {direction} type-map rule for {value!r} in connector "
            f"{connector_slug!r}"
        )


class InvalidSSLModeMapError(TypeMapError):
    """Raised when an ssl-mode-map file is structurally invalid."""


class UnmappedSSLModeError(TypeMapError):
    """Raised when a native SSL mode has no mapping to a canonical value."""

    def __init__(self, connector_slug: str, value: str) -> None:
        self.connector_slug = connector_slug
        self.value = value
        super().__init__(
            f"No ssl-mode-map entry for {value!r} in connector "
            f"{connector_slug!r}"
        )