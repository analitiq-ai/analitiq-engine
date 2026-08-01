"""Errors the file connector family raises."""

from __future__ import annotations

from collections.abc import Mapping


class StorageBackendNotBuiltError(RuntimeError):
    """A registered file-based kind whose storage backend does not exist yet.

    Deliberately not a ``ValueError``: nothing the operator writes in the
    connection can satisfy it, so it must not read as a config error.

    The built table arrives from the caller rather than being read back
    from the family surface: the error lives below that surface, and an
    error type that imports its own raiser is a cycle.
    """

    def __init__(self, kind: str, built: Mapping[str, type]) -> None:
        self.kind = kind
        self.backend = f"{kind} storage backend"
        names = ", ".join(f"{n} ({c.__name__})" for n, c in sorted(built.items()))
        super().__init__(
            f"destination kind {kind!r} is registered but the {self.backend} "
            f"that writes its files does not exist yet: it is planned, not "
            f"misconfigured, so no connection setting enables it. Built "
            f"storage backends: {names}. Point this destination at a built "
            f"kind until the {self.backend} ships."
        )
