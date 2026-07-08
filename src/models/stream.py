"""Stream configuration models used at engine runtime.

Two engine-side types live here: :class:`WriteMode`, the destination
write-mode enum, and :class:`EndpointRef`, the structured reference to an
endpoint definition. Identity is id-based: connections, streams, and
pipelines are all keyed by their ``*_id`` field (= directory name on disk).

:class:`EndpointRef` delegates all shape/validation to the published
contract (``k2m.models.stream.validate_endpoint_ref``) -- it is a thin,
frozen, hashable runtime handle over the validated ref, not a second
definition of its shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from k2m.models.stream import (
    ConnectionEndpointRef,
    validate_endpoint_ref,
)


class WriteMode(str, Enum):
    """Destination write modes (database)."""

    INSERT = "insert"
    UPSERT = "upsert"


# ---------------------------------------------------------------------------
# Endpoint reference
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DatabaseObject:
    """Verbatim provider-native object locator for a ``connection``-scoped
    endpoint (stream contract: ``endpoint_ref.database_object``).

    Frozen so an :class:`EndpointRef` carrying one stays hashable.
    """

    schema: str
    name: str
    catalog: str | None = None
    object_type: str | None = None

    def to_dict(self) -> dict[str, str]:
        payload = {"schema": self.schema, "name": self.name}
        if self.catalog is not None:
            payload["catalog"] = self.catalog
        if self.object_type is not None:
            payload["object_type"] = self.object_type
        return payload


@dataclass(frozen=True)
class EndpointRef:
    """Structured reference to an endpoint definition.

    Two contract variants, discriminated by ``scope``:

    - ``scope="connector"``: ``{scope, connection_id, endpoint_id}``. The
      ``endpoint_id`` is the client-authored connector-registry key. Resolve
      by looking up the connection's ``connector_id`` and loading
      ``connectors/<connector_id>/definition/endpoints/<endpoint_id>.json``.
    - ``scope="connection"``: ``{scope, connection_id, database_object}``.
      ``database_object`` is the verbatim provider-native locator; the
      ``endpoint_id`` is a server-derived opaque handle over it (never
      client-authored) and is filled in by the contract validator.

    Frozen so instances are hashable and usable as dict keys.
    """

    scope: str
    connection_id: str
    endpoint_id: str
    database_object: DatabaseObject | None = None

    @classmethod
    def from_dict(cls, data: Any) -> EndpointRef:
        """Validate and construct from a dict (or pass-through if already typed).

        Shape and cross-field rules -- including derivation of a
        ``connection``-scoped ``endpoint_id`` from ``database_object`` -- are
        enforced by the published contract via ``validate_endpoint_ref``.
        """
        if isinstance(data, EndpointRef):
            return data
        parsed = validate_endpoint_ref(data)
        database_object = None
        if isinstance(parsed, ConnectionEndpointRef):
            obj = parsed.database_object
            database_object = DatabaseObject(
                schema=obj.schema_,
                name=obj.name,
                catalog=obj.catalog,
                object_type=obj.object_type,
            )
        return cls(
            scope=parsed.scope,
            connection_id=parsed.connection_id,
            endpoint_id=parsed.endpoint_id,
            database_object=database_object,
        )

    def __str__(self) -> str:
        return f"{self.scope}:{self.connection_id}/{self.endpoint_id}"

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "scope": self.scope,
            "connection_id": self.connection_id,
            "endpoint_id": self.endpoint_id,
        }
        if self.database_object is not None:
            payload["database_object"] = self.database_object.to_dict()
        return payload
