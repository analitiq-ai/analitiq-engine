
# Pipeline Config Structure


├── 1. PIPELINE METADATA
│   ├── pipeline_id: "uuid-string"
│   ├── name: "Pipeline Name"
│   ├── version: "1.0"
│   └── description: "Optional description"
│
├── 2. ENGINE CONFIGURATION
│   ├── batch_size: 1000
│   ├── max_concurrent_batches: 10
│   ├── buffer_size: 10000
│   └── schedule
│       ├── type: "interval"
│       ├── interval_minutes: 60
│       └── timezone: "UTC"
│
├── 3. STREAMS CONFIGURATION
│   └── {stream_id}
│       ├── stream_id: "uuid-string"
│       ├── name: "Stream Name"
│       ├── description: "Stream description"
│       └── source
│           ├── endpoint_id: "source-endpoint-uuid"
│           ├── base_url: "https://api.source.com"
│           ├── headers: {"Authorization": "Bearer token"}
│           ├── replication_method: "incremental"
│           ├── cursor_field: "updated_at"
│           ├── cursor_mode: "inclusive"
│           ├── safety_window_seconds: 120
│           ├── primary_key: ["id"]
│           └── tie_breaker_fields: ["id"]
│       └── destination
│           ├── endpoint_id: "dest-endpoint-uuid"
│           ├── base_url: "https://api.dest.com"
│           ├── headers: {"Authorization": "Bearer token"}
│           ├── refresh_mode: "upsert"
│           ├── batch_support: true
│           └── batch_size: 100
│       └── mapping
│           ├── field_mappings
│           │   ├── "source_field": "target_field"
│           │   └── "nested.field": {"target": "flat_field", "transformations": ["to_string"]}
│           ├── computed_fields
│           │   ├── "sync_timestamp": "now()"
│           │   └── "record_type": "transaction"
│           └── transformations
│               ├── type: "field_mapping"
│               ├── mappings: {...}
│               └── conditions: {...}
│
└── 4. OPERATIONAL CONFIGURATION
├── error_handling
│   ├── strategy: "dlq"
│   ├── retry_failed_records: true
│   ├── max_retries: 3
│   └── backoff_multiplier: 2.0
├── monitoring
│   ├── metrics_enabled: true
│   ├── log_level: "INFO"
│   ├── checkpoint_interval: 100
│   └── progress_monitoring: "enabled"
├── validation (optional)
│   ├── rules
│   │   ├── field: "user_id"
│   │   ├── type: "not_null"
│   │   ├── error_action: "dlq"
│   │   └── custom_message: "User ID required"
│   └── error_thresholds
│       ├── max_error_rate: 0.05
│       └── window_size: 1000
└── state_management
├── state_dir: "/path/to/state"
├── logs_dir: "/path/to/logs"
├── dlq_dir: "/path/to/deadletter"
└── checkpoint_frequency: 100

## Multi-Environment Configuration Loading

The framework supports configuration loading from multiple backends:

### Environment Configuration

**Local Environment (`ENV=local`):**
```
Local Filesystem
├── pipelines/{pipeline_id}.json
├── hosts/{host_id}.json
└── endpoints/{endpoint_id}.json
```

**AWS Environment (`ENV=dev|prod`):**
```
S3 Bucket (s3://analitiq-config/)
├── pipelines/{pipeline_id}.json
├── hosts/{host_id}.json
└── endpoints/{endpoint_id}.json
```

### PipelineConfigPrep Class

```python
from analitiq_stream.core.pipeline_config_prep import PipelineConfigPrep

# Auto-detect environment from ENV variables
prep = PipelineConfigPrep()
config = prep.create_config()

# Or specify explicit settings
settings = PipelineConfigPrepSettings(
    env="prod",
    pipeline_id="my-pipeline",
    s3_config_bucket="my-config-bucket"
)
prep = PipelineConfigPrep(settings)
config = prep.create_config()
```

### Environment Variables

**Local:**
- `ENV=local`
- `PIPELINE_ID=pipeline-uuid`
- `LOCAL_CONFIG_MOUNT=/path/to/config`

**AWS:**
- `ENV=prod` (or `dev`)
- `PIPELINE_ID=pipeline-uuid`
- `AWS_REGION=eu-central-1`
- `S3_CONFIG_BUCKET=analitiq-config`
- `USE_SECRETS_MANAGER=true` (optional)

Pipeline Configuration Construction Tree

Multi-Environment Pipeline Configuration Loading
│
├── 1. ENVIRONMENT DETECTION
│   ├── Local Mode (ENV=local) ──► Local filesystem access
│   ├── AWS Mode (ENV=dev/prod) ──► S3 + optional Secrets Manager
│   └── Environment validation ──► Check credentials & paths
│
├── 2. LOAD INDIVIDUAL CONFIGS
│   ├── pipeline_config.json ──► Basic pipeline metadata
│   ├── endpoints/{endpoint_id}.json ──► Endpoint schemas (src/dst)
│   ├── hosts/{host_id}.json ──► Host credentials (src/dst)
│   └── validation_config.json ──► Validation rules (optional)
│
├── 3. MERGE CONFIGS PER STREAM
│   ├── Load host credentials by host_id from src/dst sections
│   ├── Load endpoint schemas by endpoint_id from stream configs
│   ├── Merge: stream config + endpoint schema + host credentials
│   └── Expand Environment Variables (${VAR_NAME} → actual values)
│
├── 4. VALIDATE CONFIGS
│   ├── Pipeline-level validation (pipeline_id, engine_config, etc.)
│   ├── Source/Destination validation (UUIDs, refresh_modes, etc.)
│   └── Transformation validation (field_mappings, computed_fields, etc.)
│
├── 5. BUILD UNIFIED CONFIG
│   ├── Stream-level configs ──► Per-stream source + destination + mappings
│   ├── Pipeline-level metadata ──► pipeline_id, name, version, engine_config
│   ├── Error handling & monitoring ──► DLQ settings, logging config
│   └── Optional validation rules ──► Field validation, error actions
│
└── 6. INSTANTIATE PIPELINE
└── PipelineConfig(config=unified_config) ──► Validated Pydantic model
