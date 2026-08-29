"""Unit tests for the typed resolved-runtime models and their invariants."""

from typing import Annotated, Literal
from unittest.mock import MagicMock

import pytest
from analitiq.contracts.pipelines.config import ErrorHandling as ContractErrorHandling
from analitiq.contracts.pipelines.config import Runtime as ContractRuntime
from analitiq.contracts.stream import Replication, StreamSource
from pydantic import BaseModel, Field, TypeAdapter, create_model

from src.engine.mapping import MappingDocument
from src.engine.pipeline_config_prep import _parse_replication, _parse_runtime_config
from src.models.resolved import (
    BatchingConfig,
    ErrorHandlingConfig,
    PipelineConnections,
    ReplicationConfig,
    ResolvedPipeline,
    ResolvedStream,
    RuntimeConfig,
    _contract_literals,
    _variant_literals,
    with_effective_safety_window,
)
from src.models.state import ReplicationConfig as StateReplicationConfig


class TestBatchingConfig:
    def test_defaults(self):
        cfg = BatchingConfig()
        assert cfg.batch_size == 1000

    @pytest.mark.parametrize("batch_size", [0, -1])
    def test_rejects_non_positive_batch_size(self, batch_size):
        with pytest.raises(ValueError, match="batch_size must be positive"):
            BatchingConfig(batch_size=batch_size)


class TestContractLiterals:
    """The reader fails loud rather than narrowing a boundary to nothing."""

    def test_reads_a_literal_field(self):
        class Model(BaseModel):
            kind: Literal["a", "b"]

        assert _contract_literals(Model, "kind") == {"a", "b"}

    @pytest.mark.parametrize(
        "annotation", [str, int, str | None, list[str], Literal[1, 2]]
    )
    def test_rejects_a_field_that_is_not_a_string_literal(self, annotation):
        # A contract that stops declaring an enum here must break the engine
        # loudly; an empty vocabulary would reject every value instead.
        model = create_model("Model", kind=(annotation, ...))
        with pytest.raises(RuntimeError, match="not a Literal of strings"):
            _contract_literals(model, "kind")

    def test_rejects_a_renamed_or_dropped_field(self):
        # A renamed field is at least as likely as a retyped one, and must
        # reach the same explanation rather than a bare KeyError.
        model = create_model("Model", kind=(Literal["a"], ...))
        with pytest.raises(RuntimeError, match="does not declare a 'method' field"):
            _contract_literals(model, "method")

    def test_rejects_a_variant_that_is_not_a_model(self):
        # An Optional variant puts NoneType in the union; reading model_fields
        # off it must explain, not raise AttributeError.
        with pytest.raises(RuntimeError, match="does not declare"):
            _contract_literals(type(None), "method")


class TestVariantLiterals:
    """The union reader states which shape it could not read."""

    def test_reads_an_annotated_discriminated_union(self):
        class A(BaseModel):
            kind: Literal["a"]

        class B(BaseModel):
            kind: Literal["b"]

        annotated = Annotated[A | B, Field(discriminator="kind")]
        assert _variant_literals(annotated, "kind") == {"a", "b"}

    def test_reads_a_bare_union(self):
        # The contract wraps its unions in Annotated today; the reader does not
        # depend on that, so dropping the discriminator is not a silent break.
        class A(BaseModel):
            kind: Literal["a"]

        class B(BaseModel):
            kind: Literal["b"]

        assert _variant_literals(A | B, "kind") == {"a", "b"}

    @pytest.mark.parametrize("shape", ["plain_model", "annotated_single"])
    def test_rejects_an_annotation_that_is_not_a_union(self, shape):
        # The likeliest shape change: the contract collapses the union to one
        # model. Before the Annotated strip was explicit this raised a bare
        # unpack ValueError naming neither the contract nor the cause.
        class A(BaseModel):
            kind: Literal["a"]

        annotation = A if shape == "plain_model" else Annotated[A, Field()]
        with pytest.raises(RuntimeError, match="no longer a union"):
            _variant_literals(annotation, "kind")


class TestErrorHandlingConfig:
    def test_defaults(self):
        cfg = ErrorHandlingConfig()
        assert cfg.strategy == "fail"
        assert cfg.max_retries == 3
        assert cfg.retry_delay_seconds == 5

    def test_vocabulary_is_the_one_the_engine_has_handling_for(self):
        # Deriving means a contract that gains a strategy widens what this
        # boundary accepts on its own. That is the right runtime behavior -- a
        # contract-valid pipeline is never rejected here -- but the strategy
        # dispatch in StreamProcessor would then reject it mid-run, on the
        # first failed batch, rather than at config time. This is the only
        # place the set is written down, so a contract that adds one fails
        # here and gets a code path before it can reach a pipeline.
        assert _contract_literals(ContractErrorHandling, "strategy") == {
            "fail",
            "dlq",
            "skip",
        }

    @pytest.mark.parametrize(
        "strategy", sorted(_contract_literals(ContractErrorHandling, "strategy"))
    )
    def test_accepts_every_contract_strategy(self, strategy):
        # Must accept the published pipeline contract enum exactly, so a
        # contract-valid pipeline is never rejected at this boundary.
        assert ErrorHandlingConfig(strategy=strategy).strategy == strategy

    def test_rejects_unknown_strategy(self):
        with pytest.raises(ValueError, match="Unknown error strategy"):
            ErrorHandlingConfig(strategy="retry-forever")

    def test_rejects_negative_max_retries(self):
        with pytest.raises(ValueError, match="max_retries must be non-negative"):
            ErrorHandlingConfig(max_retries=-1)

    def test_rejects_negative_retry_delay(self):
        with pytest.raises(ValueError, match="retry_delay_seconds must be non-neg"):
            ErrorHandlingConfig(retry_delay_seconds=-1)


class TestRuntimeConfig:
    def test_defaults_compose_sub_configs(self):
        cfg = RuntimeConfig()
        assert isinstance(cfg.batching, BatchingConfig)
        assert isinstance(cfg.error_handling, ErrorHandlingConfig)
        assert cfg.buffer_size == 5000

    def test_rejects_non_positive_buffer(self):
        with pytest.raises(ValueError, match="buffer_size must be positive"):
            RuntimeConfig(buffer_size=0)

    def test_composes_typed_sub_configs(self):
        cfg = RuntimeConfig(
            batching=BatchingConfig(batch_size=250),
            error_handling=ErrorHandlingConfig(strategy="dlq"),
            buffer_size=4096,
        )
        assert cfg.batching.batch_size == 250
        assert cfg.error_handling.strategy == "dlq"
        assert cfg.buffer_size == 4096


class TestPipelineConnections:
    def test_holds_source_and_destinations(self):
        conns = PipelineConnections(source="src", destinations=["a", "b"])
        assert conns.source == "src"
        assert conns.destinations == ["a", "b"]

    def test_rejects_empty_source(self):
        with pytest.raises(ValueError, match="source cannot be empty"):
            PipelineConnections(source="", destinations=["a"])


class TestResolvedModelGuards:
    def _pipeline(self, pipeline_id="p1"):
        return ResolvedPipeline(
            pipeline_id=pipeline_id,
            name="n",
            display_name=None,
            connections=PipelineConnections(source="src", destinations=["d"]),
        )

    def test_resolved_pipeline_accepts_valid_id(self):
        assert self._pipeline().pipeline_id == "p1"

    def test_resolved_pipeline_rejects_empty_id(self):
        with pytest.raises(ValueError, match="pipeline_id cannot be empty"):
            self._pipeline(pipeline_id="")

    def _stream(self, stream_id="s1"):
        return ResolvedStream(
            stream_id=stream_id,
            stream_version=1,
            source=MagicMock(),
            destinations=[MagicMock()],
            mapping=MappingDocument(),
        )

    def test_resolved_stream_rejects_empty_id(self):
        with pytest.raises(ValueError, match="stream_id cannot be empty"):
            self._stream(stream_id="")


def _runtime_block(block: dict) -> ContractRuntime:
    """A pipeline's runtime block as the contract validates it."""
    return ContractRuntime.model_validate(block)


def _source_block(block: dict) -> StreamSource:
    """A stream's source block as the contract validates it.

    Connection-scoped (a database table): the contract reserves
    ``tie_breaker_fields`` for database sources.
    """
    return StreamSource.model_validate(
        {
            "endpoint_ref": {
                "scope": "connection",
                "connection_id": "c",
                "database_object": {"schema": "public", "name": "t"},
            },
            **block,
        }
    )


class TestParseRuntimeConfig:
    def test_empty_block_yields_defaults(self):
        cfg = _parse_runtime_config(_runtime_block({}))
        assert cfg.batching.batch_size == 1000
        assert cfg.error_handling.strategy == "fail"
        assert cfg.buffer_size == 5000

    def test_partial_block_merges_with_defaults(self):
        cfg = _parse_runtime_config(_runtime_block({"batching": {"batch_size": 50}}))
        assert cfg.batching.batch_size == 50
        assert cfg.buffer_size == 5000  # untouched keys keep their defaults

    def test_full_block_is_typed(self):
        cfg = _parse_runtime_config(
            _runtime_block(
                {
                    "batching": {"batch_size": 200},
                    "error_handling": {
                        "strategy": "dlq",
                        "max_retries": 5,
                        "retry_delay_seconds": 1,
                    },
                    "buffer_size": 1234,
                }
            )
        )
        assert cfg.batching.batch_size == 200
        assert cfg.error_handling.strategy == "dlq"
        assert cfg.error_handling.max_retries == 5
        assert cfg.buffer_size == 1234

    def test_invalid_value_fails_loud(self):
        # Now validated against the contract model, so an out-of-enum strategy
        # is rejected by the contract (authority) before the engine type.
        with pytest.raises(ValueError, match="strategy"):
            _runtime_block({"error_handling": {"strategy": "nope"}})

    def test_retired_batching_key_is_rejected_not_ignored(self):
        # max_concurrent_batches was dropped from the contract (issue #436).
        # A pipeline still declaring it must fail here rather than have the key
        # silently dropped: the author asked for something the engine no longer
        # offers, and a silent drop reads as if the request was honoured.
        with pytest.raises(ValueError, match="max_concurrent_batches"):
            _runtime_block(
                {"batching": {"batch_size": 200, "max_concurrent_batches": 4}}
            )

    def test_out_of_range_max_retries_fails_loud(self):
        # The contract caps max_retries (le=5); the parser enforces it.
        with pytest.raises(ValueError, match="max_retries"):
            _runtime_block({"error_handling": {"max_retries": 9}})

    def test_omitted_fields_use_engine_defaults_not_contract(self, monkeypatch):
        """Omitted runtime fields fall through to the engine's (env-overridable)
        defaults, never the contract model's own defaults.

        Guards the ``model_fields_set`` author-intent signal (infra #938):
        ``retry_delay_seconds`` in particular must not be forwarded from the
        contract's injected default when the author omitted it, or the engine's
        precedence (pipeline > env > engine default) breaks.
        """
        monkeypatch.setenv("ANALITIQ_RETRY_DELAY_SECONDS", "42")
        monkeypatch.setenv("ANALITIQ_ERROR_STRATEGY", "skip")
        cfg = _parse_runtime_config(_runtime_block({}))
        # engine env values, not the contract's retry_delay=5 / strategy='dlq'
        assert cfg.error_handling.retry_delay_seconds == 42
        assert cfg.error_handling.strategy == "skip"

    def test_null_retry_delay_takes_the_contract_resolved_value(self, monkeypatch):
        # An explicit null is resolved by the CONTRACT model itself
        # (`RetryErrorHandlingBase._default_retry_delay` replaces it before the
        # engine can observe it), so it reaches the engine as an author-set 5
        # -- unlike an omitted key, which stays out of `model_fields_set` and
        # falls through to the engine's env-overridable default. The env
        # override makes the two paths discriminating.
        monkeypatch.setenv("ANALITIQ_RETRY_DELAY_SECONDS", "42")
        cfg = _parse_runtime_config(
            _runtime_block({"error_handling": {"retry_delay_seconds": None}})
        )
        assert cfg.error_handling.retry_delay_seconds == 5


class TestReplicationConfig:
    def test_vocabulary_equals_the_published_contract_enum(self):
        # Two genuinely independent readings: the engine walks each variant's
        # method literal, this reads the discriminator mapping pydantic renders
        # into the published schema. A reader that visited only the first
        # variant would pass every other test in this class.
        published = TypeAdapter(Replication).json_schema()["discriminator"]["mapping"]
        assert _variant_literals(Replication, "method") == set(published)

    def test_vocabulary_is_the_one_the_engine_has_handling_for(self):
        # Same reason as the error-strategy canary: deriving lets a contract
        # widen this boundary on its own, and the engine branches on the
        # method, so a new one needs a code path before a pipeline can use it.
        assert _variant_literals(Replication, "method") == {
            "full_refresh",
            "incremental",
        }

    @pytest.mark.parametrize("method", sorted(_variant_literals(Replication, "method")))
    def test_accepts_every_contract_method(self, method):
        assert ReplicationConfig(method=method).method == method

    def test_rejects_unknown_method(self):
        with pytest.raises(ValueError, match="Unknown replication method"):
            ReplicationConfig(method="cdc")

    def test_rejects_non_string_cursor_field(self):
        # The contract is string|null; a legacy list must fail loud here, not
        # reach compute_max_cursor as an opaque TypeError.
        with pytest.raises(ValueError, match="cursor_field must be a string or None"):
            ReplicationConfig(method="incremental", cursor_field=["updated_at"])

    def test_optional_fields_default_absent(self):
        cfg = ReplicationConfig(method="full_refresh")
        assert cfg.cursor_field is None
        assert cfg.tie_breaker_fields is None

    def test_holds_cursor_and_tie_breakers(self):
        cfg = ReplicationConfig(
            method="incremental",
            cursor_field="updated_at",
            tie_breaker_fields=["id", "seq"],
        )
        assert cfg.cursor_field == "updated_at"
        assert cfg.tie_breaker_fields == ["id", "seq"]


class TestParseReplication:
    def test_absent_replication_yields_none(self):
        assert _parse_replication(_source_block({"primary_keys": ["id"]})) is None

    def test_null_replication_yields_none(self):
        assert _parse_replication(_source_block({"replication": None})) is None

    def test_incremental_block_is_typed(self):
        cfg = _parse_replication(
            _source_block(
                {
                    "replication": {
                        "method": "incremental",
                        "cursor_field": "updated_at",
                        "tie_breaker_fields": ["id"],
                    }
                }
            )
        )
        assert cfg.method == "incremental"
        assert cfg.cursor_field == "updated_at"
        assert cfg.tie_breaker_fields == ["id"]

    def test_full_refresh_without_cursor(self):
        cfg = _parse_replication(
            _source_block({"replication": {"method": "full_refresh"}})
        )
        assert cfg.method == "full_refresh"
        assert cfg.cursor_field is None

    def test_missing_method_fails_loud(self):
        # method is contract-required; the contract model rejects a block that
        # omits it (a malformed block must not pass silently).
        with pytest.raises(ValueError, match="method"):
            _source_block({"replication": {"cursor_field": "updated_at"}})


class TestEffectiveSafetyWindow:
    """The engine fills the safety window before a config crosses the boundary.

    It is operational policy a connector never declares, so a connector
    treats an absent value as a wiring defect rather than inventing a
    default -- which is how the number came to exist in three places.
    """

    def test_an_incremental_stream_gets_the_engines_default(self):
        filled = with_effective_safety_window(
            {"replication": {"method": "incremental", "cursor_field": "updated_at"}}
        )
        assert filled["replication"]["safety_window_seconds"] == (
            StateReplicationConfig.safety_window_seconds
        )

    def test_an_authored_window_is_left_alone(self):
        filled = with_effective_safety_window(
            {"replication": {"method": "incremental", "safety_window_seconds": 900}}
        )
        assert filled["replication"]["safety_window_seconds"] == 900

    def test_a_full_refresh_stream_gets_nothing(self):
        source = {"replication": {"method": "full_refresh"}}
        assert with_effective_safety_window(source) == source

    def test_a_stream_with_no_replication_block_is_untouched(self):
        source = {"endpoint_ref": {"scope": "connector"}}
        assert with_effective_safety_window(source) is source

    def test_the_caller_s_dict_is_not_mutated(self):
        # The raw stream_source travels to more than one consumer; filling
        # in place would edit what the engine's own typed view was built
        # from.
        source = {"replication": {"method": "incremental"}}
        with_effective_safety_window(source)
        assert "safety_window_seconds" not in source["replication"]
