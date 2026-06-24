"""Stream configuration models used at engine runtime.

Two engine-side types live here: :class:`WriteMode`, the destination
write-mode enum, and :class:`EndpointRef`, the structured reference to an
endpoint definition. Identity is id-based: connections, streams, and
pipelines are all keyed by their ``*_id`` field (= directory name on disk).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict


class WriteMode(str, Enum):
    """Destination write modes (database)."""

    INSERT = "insert"
    UPSERT = "upsert"


# ---------------------------------------------------------------------------
# Endpoint reference
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EndpointRef:
    """Structured reference to an endpoint definition.

    Contract shape (stream document):

        {
          "scope":         "connector" | "connection",
          "connection_id": "<connection id selected in the pipeline>",
          "endpoint_id":   "<stable endpoint id from endpoint discovery>",
          # plus optional "x-*" extension metadata
        }

    Resolution rules:

    - ``scope="connector"``: look up the connection by ``connection_id``,
      read its ``connector_id``, then load
      ``connectors/<connector_id>/definition/endpoints/<endpoint_id>.json``.
    - ``scope="connection"``: load
      ``connections/<connection_id>/definition/endpoints/<endpoint_id>.json``.

    Frozen so instances are hashable and usable as dict keys.
    """

    scope: str
    connection_id: str
    endpoint_id: str

    _VALID_SCOPES = ("connector", "connection")

    def __post_init__(self) -> None:
        if self.scope not in self._VALID_SCOPES:
            raise ValueError(
                f"EndpointRef.scope must be one of {self._VALID_SCOPES}, got {self.scope!r}"
            )
        if not self.connection_id:
            raise ValueError("EndpointRef.connection_id cannot be empty")
        if not self.endpoint_id:
            raise ValueError("EndpointRef.endpoint_id cannot be empty")

    def __str__(self) -> str:
        return f"{self.scope}:{self.connection_id}/{self.endpoint_id}"

    @classmethod
    def from_dict(cls, data: Any) -> "EndpointRef":
        """Validate and construct from a dict (or pass-through if already typed).

        Accepts ``x-*`` extension keys verbatim per the stream contract —
        they are not loaded onto the dataclass but do not trigger an
        unknown-key error either.
        """
        if isinstance(data, EndpointRef):
            return data
        if not isinstance(data, dict):
            raise TypeError(
                "endpoint_ref must be an object with keys "
                "{'scope','connection_id','endpoint_id'} (plus optional 'x-*' "
                f"extensions), got {type(data).__name__}"
            )
        required = {"scope", "connection_id", "endpoint_id"}
        unknown = {
            k for k in set(data) - required if not k.startswith("x-")
        }
        if unknown:
            raise ValueError(
                f"endpoint_ref has unknown keys {sorted(unknown)}; allowed: "
                f"{sorted(required)} plus optional 'x-*' extension keys"
            )
        missing = required - set(data)
        if missing:
            raise ValueError(
                f"endpoint_ref is missing required keys {sorted(missing)}"
            )
        return cls(
            scope=data["scope"],
            connection_id=data["connection_id"],
            endpoint_id=data["endpoint_id"],
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            "scope": self.scope,
            "connection_id": self.connection_id,
            "endpoint_id": self.endpoint_id,
        }
