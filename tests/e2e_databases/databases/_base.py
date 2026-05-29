"""Base abstraction for a database the matrix can talk to.

A ``DatabaseSpec`` answers everything the orchestrator needs:

- How to spin up an instance (docker compose service name, or None for
  embedded / cloud databases that don't need a container).
- How to seed the canonical table.
- How to read the table back to assert correctness.
- How to translate the canonical schema into the database's native column
  types (used to write the endpoint JSON the engine validates).
- Which DIP connector slug to point connections at; ``None`` means there is
  no DIP connector yet and the matrix should skip pairs that involve this
  DB.

Implementations live next to this file. Concrete subclasses opt into
either ``LocalContainerSpec`` (Docker), ``EmbeddedSpec`` (SQLite/DuckDB),
or ``CloudSpec`` (Snowflake/BigQuery/Redshift).
"""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Literal, Mapping, Optional

from tests.e2e_databases.seeds import SeedRow

# A database participates in a pair under one of two roles. Source and
# destination are always distinct instances (distinct containers, even for the
# same DB type), so role selects which one a spec method talks to.
Role = Literal["source", "destination"]


@dataclass(frozen=True)
class ColumnSpec:
    """One column in the canonical seed table, expressed in the DB's native + Arrow vocab."""

    name: str
    native_type: str
    arrow_type: str
    nullable: bool


@dataclass(frozen=True)
class ConnectionDescriptor:
    """Where this database lives, from both sides.

    The engine runs inside Docker and reaches DB containers by their
    network hostnames. The pytest process runs on the host and reaches
    the same containers via published ports on ``localhost``. Cloud
    DBs have the same address from both sides.
    """

    # As seen by the engine container.
    engine_host: str
    engine_port: int

    # As seen by the pytest process on the host.
    host_address: str
    host_port: int

    database: str
    username: str
    password: str
    schema: Optional[str] = None
    # Extra connection.parameters keys the connector expects (ssl_mode, etc).
    extra_parameters: Mapping[str, object] = field(default_factory=dict)
    # Extra secrets/credentials keys (most DBs only need ``password``).
    extra_secrets: Mapping[str, str] = field(default_factory=dict)


class DatabaseSpec(abc.ABC):
    """Everything the matrix needs to know about one database."""

    # Identifier the matrix uses in test IDs and workspace paths.
    slug: str

    # DIP connector folder name (matches ``connectors/<dip_connector_id>``).
    # ``None`` means "no DIP connector yet — skip pairs involving this DB".
    dip_connector_id: Optional[str]

    # ``True`` if this DB only runs as a managed cloud service.
    is_cloud: bool = False

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if not getattr(cls, "slug", ""):
            raise TypeError(
                f"{cls.__name__} must define a non-empty 'slug' class attribute"
            )

    @abc.abstractmethod
    def columns(self) -> List[ColumnSpec]:
        """Canonical column definitions in this DB's native types."""

    @abc.abstractmethod
    def connection(self, role: Role) -> ConnectionDescriptor:
        """Connection details for the given role.

        Local-container specs return different docker hostnames per role so
        source and destination point at distinct instances.
        """

    @abc.abstractmethod
    def up(self, role: Role) -> None:
        """Bring this DB up for the given role. No-op for embedded / cloud."""

    @abc.abstractmethod
    def down(self, role: Role) -> None:
        """Tear this DB down. No-op for embedded / cloud unless we created data."""

    @abc.abstractmethod
    def seed(self, role: Role, rows: Iterable[SeedRow]) -> None:
        """Drop the seed table if it exists, recreate it, and insert ``rows``."""

    @abc.abstractmethod
    def upsert_rows(self, role: Role, rows: Iterable[SeedRow]) -> None:
        """Insert-or-update ``rows`` into the seed table by primary key.

        Used between incremental syncs to mutate the source (the delta) and to
        plant the destination sentinel, without dropping the existing table.
        """

    @abc.abstractmethod
    def prepare_destination(self) -> None:
        """Drop the seed table at the destination if present, so the run starts clean."""

    @abc.abstractmethod
    def read_destination(self) -> List[SeedRow]:
        """Read the seed table back from the destination, ordered by primary key."""

    # ---- introspection helpers used by the orchestrator -----------------

    @property
    def available(self) -> Optional[str]:
        """Return a non-None reason if this DB cannot run; None if it is usable.

        Used to surface ``pytest.skip`` messages. Default: only check that the
        DIP connector folder exists at the project root.
        """
        if self.dip_connector_id is None:
            return f"no DIP connector published for {self.slug}"
        connectors_root = Path(__file__).resolve().parents[3] / "connectors"
        if not (connectors_root / self.dip_connector_id).is_dir():
            return (
                f"DIP connector {self.dip_connector_id!r} not present under "
                f"{connectors_root} — run the DIP registry pull first"
            )
        return None
