"""Storage backends for file-based destinations.

The destination registry routes every file-based connector kind to
``FileDestinationHandler``; this module turns that kind into the backend
that performs the write. Kinds are registered ahead of their backends
(``s3`` is a planned destination alongside ``nosql`` and ``document``), so
a kind that reaches here with no backend is unbuilt, not misconfigured,
and must say so in one message the operator can act on.
"""

from .base import BaseStorageBackend
from .local import LocalFileStorage

__all__ = [
    "BaseStorageBackend",
    "LocalFileStorage",
    "StorageBackendNotBuiltError",
    "get_storage_backend",
]

# Connector kind -> the storage backend serving it. The backend's own name
# (``LocalFileStorage.storage_type``) is not always the kind: the ``file``
# kind writes through the local filesystem backend.
_BACKENDS: dict[str, type[BaseStorageBackend]] = {
    "file": LocalFileStorage,
}


class StorageBackendNotBuiltError(RuntimeError):
    """A registered file-based kind whose storage backend does not exist yet.

    Deliberately not a ``ValueError``: nothing the operator writes in the
    connection can satisfy it, so it must not read as a config error.
    """

    def __init__(self, kind: str) -> None:
        self.kind = kind
        self.backend = f"{kind} storage backend"
        built = ", ".join(
            f"{name} ({cls.__name__})" for name, cls in sorted(_BACKENDS.items())
        )
        super().__init__(
            f"destination kind {kind!r} is registered but the {self.backend} "
            f"that writes its files does not exist yet: it is planned, not "
            f"misconfigured, so no connection setting enables it. Built "
            f"storage backends: {built}. Point this destination at a built "
            f"kind until the {self.backend} ships."
        )


def get_storage_backend(kind: str) -> BaseStorageBackend:
    """Instantiate the storage backend serving the connector *kind*.

    Args:
        kind: File-based connector kind from the connection's runtime.

    Returns:
        Instance of the backend registered for the kind.

    Raises:
        StorageBackendNotBuiltError: If the kind has no backend yet.
    """
    backend_class = _BACKENDS.get(kind.lower())
    if backend_class is None:
        raise StorageBackendNotBuiltError(kind)

    return backend_class()
