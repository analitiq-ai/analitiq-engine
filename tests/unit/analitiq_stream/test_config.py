"""Unit tests for config module."""

import pytest

from src.config import (
    load_connection,
    load_connector_definition,
    resolve_endpoint_ref,
    validate_artifact,
)
from src.config.endpoint_resolver import ConnectionLookup
from src.config.exceptions import (
    ConnectionConfigError,
    ConnectorNotFoundError,
    EndpointNotFoundError,
)
from analitiq.contracts.endpoint_identity import derive_db_endpoint_id

from src.models.stream import EndpointRef


class TestConfig:
    """Test suite for configuration module."""

    @pytest.mark.unit
    def test_config_module_exports(self):
        """Test that config module exports expected attributes."""
        from src import config

        expected_exports = [
            "validate_artifact",
            "resolve_endpoint_ref",
            "load_connection",
            "load_connector_definition",
            "PathBasedConfigLoader",
        ]

        for attr in expected_exports:
            assert hasattr(config, attr), f"config module should have attribute: {attr}"


class TestPipelineConfigValidator:
    """Test suite for pipeline config validation."""

    @pytest.fixture
    def valid_pipeline(self):
        return {
            "$schema": "https://schemas.analitiq.ai/pipeline/latest.json",
            "display_name": "Test Pipeline",
            "status": "active",
            "connections": {
                "source": "00000000-0000-0000-0000-000000000001",
                "destinations": ["00000000-0000-0000-0000-000000000002"],
            },
            "streams": ["00000000-0000-0000-0000-000000000003"],
            "schedule": {"type": "manual", "timezone": "UTC"},
        }

    @pytest.mark.unit
    def test_valid_pipeline_passes(self, valid_pipeline):
        validate_artifact("pipeline", valid_pipeline)
        assert valid_pipeline["display_name"] == "Test Pipeline"

    @pytest.mark.unit
    def test_missing_connections_fails(self, valid_pipeline):
        del valid_pipeline["connections"]
        with pytest.raises(Exception, match="connections"):
            validate_artifact("pipeline", valid_pipeline)

    @pytest.mark.unit
    def test_missing_source_fails(self, valid_pipeline):
        del valid_pipeline["connections"]["source"]
        with pytest.raises(Exception, match="source"):
            validate_artifact("pipeline", valid_pipeline)

    @pytest.mark.unit
    def test_empty_destinations_fails(self, valid_pipeline):
        valid_pipeline["connections"]["destinations"] = []
        with pytest.raises(Exception, match="destinations|minItems|too short"):
            validate_artifact("pipeline", valid_pipeline)


class TestConnectionConfigValidator:
    """Test suite for connection config validation."""

    @pytest.mark.unit
    def test_valid_connection_passes(self):
        # A minimal connection that satisfies the published contract:
        # connector_id is the only required authored field.
        config = {
            "$schema": "https://schemas.analitiq.ai/connection/latest.json",
            "connector_id": "postgresql",
        }
        validate_artifact("connection", config)

    @pytest.mark.unit
    def test_invalid_connection_raises(self):
        with pytest.raises(ValueError):
            validate_artifact("connection", {})


class TestEndpointRefModel:
    """Test suite for the EndpointRef dataclass."""

    @pytest.mark.unit
    def test_from_dict_connector(self):
        ref = EndpointRef.from_dict(
            {
                "scope": "connector",
                "connection_id": "pipedrive",
                "endpoint_id": "deals",
            }
        )
        assert ref.scope == "connector"
        assert ref.connection_id == "pipedrive"
        assert ref.endpoint_id == "deals"

    @pytest.mark.unit
    def test_from_dict_connection(self):
        # A connection-scoped ref carries database_object; endpoint_id is
        # server-derived from it (never client-authored).
        ref = EndpointRef.from_dict(
            {
                "scope": "connection",
                "connection_id": "prod-postgres",
                "database_object": {"schema": "public", "name": "users"},
            }
        )
        assert ref.scope == "connection"
        assert ref.connection_id == "prod-postgres"
        assert ref.database_object is not None
        assert ref.database_object.schema == "public"
        assert ref.database_object.name == "users"
        assert ref.endpoint_id == derive_db_endpoint_id(None, "public", "users")

    @pytest.mark.unit
    def test_connection_scope_requires_database_object(self):
        """The old ``{scope, connection_id, endpoint_id}`` connection shape is
        no longer valid: connection refs must carry database_object."""
        with pytest.raises(ValueError):
            EndpointRef.from_dict(
                {
                    "scope": "connection",
                    "connection_id": "x",
                    "endpoint_id": "public_users",
                }
            )

    @pytest.mark.unit
    def test_from_dict_passes_through_existing_instance(self):
        original = EndpointRef(scope="connector", connection_id="x", endpoint_id="y")
        assert EndpointRef.from_dict(original) is original

    @pytest.mark.unit
    def test_invalid_scope_raises(self):
        with pytest.raises(ValueError):
            EndpointRef.from_dict(
                {
                    "scope": "unknown",
                    "connection_id": "x",
                    "endpoint_id": "y",
                }
            )

    @pytest.mark.unit
    def test_missing_required_field_raises(self):
        with pytest.raises(ValueError):
            EndpointRef.from_dict({"scope": "connector"})

    @pytest.mark.unit
    def test_unknown_keys_raise(self):
        """endpoint_ref is closed (additionalProperties: false in the
        contract); any extra key, including ``x-*``, is rejected."""
        with pytest.raises(ValueError):
            EndpointRef.from_dict(
                {
                    "scope": "connector",
                    "connection_id": "x",
                    "endpoint_id": "y",
                    "extra": "z",
                }
            )

    @pytest.mark.unit
    def test_empty_connection_id_raises(self):
        with pytest.raises(ValueError):
            EndpointRef.from_dict(
                {
                    "scope": "connector",
                    "connection_id": "",
                    "endpoint_id": "y",
                }
            )

    @pytest.mark.unit
    def test_empty_endpoint_id_raises(self):
        with pytest.raises(ValueError):
            EndpointRef.from_dict(
                {
                    "scope": "connector",
                    "connection_id": "x",
                    "endpoint_id": "",
                }
            )

    @pytest.mark.unit
    def test_non_dict_input_raises(self):
        with pytest.raises(ValueError):
            EndpointRef.from_dict("connector:x/y")

    @pytest.mark.unit
    def test_to_dict_roundtrip(self):
        d = {"scope": "connector", "connection_id": "x", "endpoint_id": "y"}
        assert EndpointRef.from_dict(d).to_dict() == d

    @pytest.mark.unit
    def test_str_canonical_form(self):
        ref = EndpointRef(scope="connection", connection_id="conn", endpoint_id="name")
        assert str(ref) == "connection:conn/name"

    @pytest.mark.unit
    def test_hashable_for_dict_keys(self):
        ref1 = EndpointRef(scope="connector", connection_id="x", endpoint_id="y")
        ref2 = EndpointRef(scope="connector", connection_id="x", endpoint_id="y")
        assert hash(ref1) == hash(ref2)
        cache = {ref1: "value"}
        assert cache[ref2] == "value"


class TestBatchWriteResultInvariant:
    """``success`` is a derived property of ``status`` — the dataclass is
    frozen and ``success`` is not constructor-settable. Status is the
    single source of truth, so the two cannot drift."""

    @pytest.mark.unit
    def test_success_derived_from_status(self):
        from cdk.base_handler import BatchWriteResult
        from src.grpc.generated.analitiq.v1 import AckStatus

        assert (
            BatchWriteResult(
                status=AckStatus.ACK_STATUS_SUCCESS, records_written=3
            ).success
            is True
        )
        assert (
            BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE, records_written=0
            ).success
            is False
        )
        assert (
            BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE, records_written=0
            ).success
            is False
        )
        assert (
            BatchWriteResult(
                status=AckStatus.ACK_STATUS_ALREADY_COMMITTED, records_written=5
            ).success
            is True
        )

    @pytest.mark.unit
    def test_success_is_not_constructor_kwarg(self):
        """Removing the ``success`` constructor arg prevents the
        success/status drift bug entirely."""
        from cdk.base_handler import BatchWriteResult
        from src.grpc.generated.analitiq.v1 import AckStatus

        with pytest.raises(TypeError):
            BatchWriteResult(
                success=True,  # type: ignore[call-arg]
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
            )

    @pytest.mark.unit
    def test_negative_records_written_raises(self):
        from cdk.base_handler import BatchWriteResult
        from src.grpc.generated.analitiq.v1 import AckStatus

        with pytest.raises(ValueError, match="non-negative"):
            BatchWriteResult(status=AckStatus.ACK_STATUS_SUCCESS, records_written=-1)

    @pytest.mark.unit
    def test_frozen_rejects_mutation(self):
        from dataclasses import FrozenInstanceError

        from cdk.base_handler import BatchWriteResult
        from src.grpc.generated.analitiq.v1 import AckStatus

        r = BatchWriteResult(status=AckStatus.ACK_STATUS_SUCCESS, records_written=1)
        with pytest.raises(FrozenInstanceError):
            r.records_written = 99  # type: ignore[misc]

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "status",
        ["ACK_STATUS_RETRYABLE_FAILURE", "ACK_STATUS_FATAL_FAILURE"],
    )
    def test_cursor_on_failure_raises(self, status):
        """A failure result must not carry a cursor — the engine persists
        the cursor as the checkpoint, so accepting one would advance the
        checkpoint past a failed batch (#129)."""
        from cdk.base_handler import BatchWriteResult
        from cdk.types import Cursor
        from src.grpc.generated.analitiq.v1 import AckStatus

        with pytest.raises(ValueError, match="committed_cursor must be None"):
            BatchWriteResult(
                status=getattr(AckStatus, status),
                records_written=0,
                committed_cursor=Cursor(token=b"x"),
            )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "status",
        ["ACK_STATUS_SUCCESS", "ACK_STATUS_ALREADY_COMMITTED"],
    )
    def test_cursor_on_success_allowed(self, status):
        from cdk.base_handler import BatchWriteResult
        from cdk.types import Cursor
        from src.grpc.generated.analitiq.v1 import AckStatus

        result = BatchWriteResult(
            status=getattr(AckStatus, status),
            records_written=1,
            committed_cursor=Cursor(token=b"x"),
        )
        assert result.committed_cursor == Cursor(token=b"x")

    @pytest.mark.unit
    def test_failed_record_ids_stored_as_tuple(self):
        """A caller-supplied list is normalized to a tuple, completing the
        frozen dataclass's immutability (#129)."""
        from cdk.base_handler import BatchWriteResult
        from src.grpc.generated.analitiq.v1 import AckStatus

        r = BatchWriteResult(
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            records_written=0,
            failed_record_ids=["a", "b"],
        )
        assert r.failed_record_ids == ("a", "b")
        assert isinstance(r.failed_record_ids, tuple)


class TestEnumWireAlignment:
    """The CDK-native ``AckStatus`` / ``WriteMode`` integer values mirror the
    proto enums 1:1 — the load-bearing invariant the entire wire <-> CDK
    translation in ``server.py`` rests on (``WriteMode(msg.write_mode)`` and
    ``status=result.status`` are identity only while the ints agree). If the
    proto enum is ever renumbered, this fails loudly instead of silently
    mistranslating a status or write mode.
    """

    @pytest.mark.unit
    def test_ack_status_values_match_proto(self):
        from cdk.types import AckStatus as CdkAckStatus
        from src.grpc.generated.analitiq.v1 import AckStatus as ProtoAckStatus

        # Proto enums are protobuf ``EnumTypeWrapper`` (not iterable): names via
        # ``.keys()``, value via ``getattr``/attribute access (returns the int).
        for member in CdkAckStatus:
            assert int(member) == getattr(
                ProtoAckStatus, member.name
            ), f"AckStatus.{member.name} drifted from proto"
        # Both enums enumerate the same member names — neither side has an
        # extra value the other lacks.
        assert {m.name for m in CdkAckStatus} == set(ProtoAckStatus.keys())

    @pytest.mark.unit
    def test_write_mode_values_match_proto(self):
        from cdk.types import WriteMode as CdkWriteMode
        from src.grpc.generated.analitiq.v1 import WriteMode as ProtoWriteMode

        for member in CdkWriteMode:
            assert int(member) == getattr(
                ProtoWriteMode, member.name
            ), f"WriteMode.{member.name} drifted from proto"
        assert {m.name for m in CdkWriteMode} == set(ProtoWriteMode.keys())


class TestEndpointRefResolver:
    """Test suite for endpoint reference resolution."""

    @pytest.fixture
    def lookup(self):
        return ConnectionLookup(
            directory_by_id={"wise": "wise", "prod-pg": "prod-pg"},
            connector_id_by_id={"wise": "wise", "prod-pg": "postgresql"},
        )

    @pytest.mark.unit
    def test_resolve_connector_endpoint(self, tmp_path, lookup):
        """Test resolving a public connector endpoint."""
        endpoint_dir = tmp_path / "connectors" / "wise" / "definition" / "endpoints"
        endpoint_dir.mkdir(parents=True)
        endpoint_file = endpoint_dir / "transfers.json"
        endpoint_file.write_text('{"endpoint": "/v1/transfers", "method": "GET"}')

        paths = {
            "connectors": tmp_path / "connectors",
            "connections": tmp_path / "connections",
        }
        result = resolve_endpoint_ref(
            {"scope": "connector", "connection_id": "wise", "endpoint_id": "transfers"},
            paths,
            lookup,
        )
        assert result["endpoint"] == "/v1/transfers"

    @pytest.mark.unit
    def test_resolve_connection_endpoint(self, tmp_path, lookup):
        """Test resolving a private connection endpoint (under definition/).

        A connection ref carries database_object; the endpoint_id is derived
        from it, and the bundle writes the doc under that derived handle.
        """
        derived_id = derive_db_endpoint_id(None, "public", "users")
        endpoint_dir = tmp_path / "connections" / "prod-pg" / "definition" / "endpoints"
        endpoint_dir.mkdir(parents=True)
        endpoint_file = endpoint_dir / f"{derived_id}.json"
        endpoint_file.write_text('{"endpoint": "public/users", "method": "DATABASE"}')

        paths = {
            "connectors": tmp_path / "connectors",
            "connections": tmp_path / "connections",
        }
        result = resolve_endpoint_ref(
            {
                "scope": "connection",
                "connection_id": "prod-pg",
                "database_object": {"schema": "public", "name": "users"},
            },
            paths,
            lookup,
        )
        assert result["method"] == "DATABASE"

    @pytest.mark.unit
    def test_resolve_accepts_endpoint_ref_instance(self, tmp_path, lookup):
        endpoint_dir = tmp_path / "connectors" / "wise" / "definition" / "endpoints"
        endpoint_dir.mkdir(parents=True)
        (endpoint_dir / "transfers.json").write_text('{"endpoint": "/v1/transfers"}')

        paths = {
            "connectors": tmp_path / "connectors",
            "connections": tmp_path / "connections",
        }
        ref = EndpointRef(
            scope="connector", connection_id="wise", endpoint_id="transfers"
        )
        assert resolve_endpoint_ref(ref, paths, lookup)["endpoint"] == "/v1/transfers"

    @pytest.mark.unit
    def test_resolve_missing_endpoint_raises(self, tmp_path, lookup):
        """Test that missing endpoint file raises EndpointNotFoundError."""
        paths = {
            "connectors": tmp_path / "connectors",
            "connections": tmp_path / "connections",
        }
        with pytest.raises(EndpointNotFoundError):
            resolve_endpoint_ref(
                {
                    "scope": "connector",
                    "connection_id": "wise",
                    "endpoint_id": "nonexistent",
                },
                paths,
                lookup,
            )


class TestConnectionLoader:
    """Test suite for connection loading."""

    @pytest.mark.unit
    def test_load_connection(self, tmp_path):
        conn_dir = tmp_path / "my-api"
        conn_dir.mkdir()
        (conn_dir / "connection.json").write_text(
            '{"connector_slug": "wise", "host": "https://api.wise.com"}'
        )

        result = load_connection("my-api", tmp_path)
        assert result["connector_slug"] == "wise"

    @pytest.mark.unit
    def test_load_missing_connection_raises(self, tmp_path):
        with pytest.raises(ConnectionConfigError):
            load_connection("nonexistent", tmp_path)

    @pytest.mark.unit
    def test_load_connector_definition(self, tmp_path):
        connector_dir = tmp_path / "wise" / "definition"
        connector_dir.mkdir(parents=True)
        (connector_dir / "connector.json").write_text(
            '{"connector_type": "api", "slug": "wise"}'
        )

        result = load_connector_definition("wise", tmp_path)
        assert result["connector_type"] == "api"

    @pytest.mark.unit
    def test_load_missing_connector_raises(self, tmp_path):
        with pytest.raises(ConnectorNotFoundError):
            load_connector_definition("nonexistent", tmp_path)
