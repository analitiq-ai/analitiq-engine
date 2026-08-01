"""The CDK's file connector family: one class, one storage backend per kind.

The destination registry routes every file-based connector kind to
``GenericFileConnector``; this module turns that kind into the backend
that performs the write. Kinds are registered ahead of their backends
(``s3`` is a planned destination alongside ``nosql`` and ``document``), so
a kind that reaches here with no backend is unbuilt, not misconfigured,
and must say so in one message the operator can act on.

Registering ``s3`` ahead of its backend is what makes a misdirected ``s3``
destination refuse *by name* instead of quietly writing to the local disk.
No object-store client is involved: this family speaks the local
filesystem only, and pulls no cloud SDK until an object-store backend
ships.
"""

from .backend import BaseStorageBackend
from .exceptions import StorageBackendNotBuiltError
from .local_backend import LocalFileStorage

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
        raise StorageBackendNotBuiltError(kind, _BACKENDS)

    return backend_class()
