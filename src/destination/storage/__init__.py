"""Storage backends for file-based destinations."""

from .base import BaseStorageBackend
from .local import LocalFileStorage

__all__ = ["BaseStorageBackend", "LocalFileStorage"]


def get_storage_backend(storage_type: str) -> BaseStorageBackend:
    """
    Factory function to get storage backend by type.

    Args:
        storage_type: Storage type (local, s3, file)

    Returns:
        Instance of the appropriate storage backend

    Raises:
        ValueError: If storage type is not supported
    """
    backends = {
        "local": LocalFileStorage,
        "file": LocalFileStorage,  # Alias
    }

    backend_class = backends.get(storage_type.lower())
    if backend_class is None:
        supported = ", ".join(backends.keys())
        raise ValueError(
            f"Unsupported storage type: {storage_type}. "
            f"Supported types: {supported}"
        )

    return backend_class()
