# Design Review: Connectors-as-Code Architecture

## Problem Statement

The current architecture tightly couples configuration loading (from DynamoDB) with the core engine module. Every DynamoDB schema change requires code changes in this repo, making it difficult to maintain as a standalone module.

**Goals:**
1. Decouple connector/endpoint definitions from the core engine
2. Allow connectors to be stored in public repositories (like Airbyte/Singer catalog)
3. Users pull connector repos and reference them in their pipeline configs
4. Engine loads configs from user-specified paths at runtime
5. Handle secrets via late-binding (resolve at use time, not load time)

**Confirmed Requirements:**
- Deployment: AWS Batch only
- Connectors: Stored in Git repositories, users pull/clone them
- Secrets: Late-binding with pluggable resolvers
- Priority: Full decoupling (standalone module with no cloud knowledge)

---

## Target Architecture: Connectors-as-Code

Similar to open-source SaaS products like Airbyte and Singer, connectors and their endpoints are defined in public repositories that users can pull into their local environment or deployment.

```
+-------------------------------------------------------------------+
|  CONNECTOR CATALOG (public GitHub repos)                          |
|  - Connector templates with auth schemas                          |
|  - Endpoint definitions with API specs                            |
|  - Versioned and community-maintained                             |
+-------------------------------------------------------------------+
         |
         | git clone / git pull
         v
+-------------------------------------------------------------------+
|  USER'S ENVIRONMENT                                               |
|  connectors/              <-- Pulled from catalog                 |
|  ├── wise/                                                        |
|  ├── sevdesk/                                                     |
|  └── postgres/                                                    |
|                                                                   |
|  my_pipelines/            <-- User-defined                        |
|  ├── pipelines/                                                   |
|  ├── streams/                                                     |
|  └── secrets/                                                     |
+-------------------------------------------------------------------+
         |
         | References passed at runtime
         v
+-------------------------------------------------------------------+
|  CORE ENGINE (this repo - analitiq-core)                          |
|  - Receives path references in pipeline config                    |
|  - Loads connector/endpoint JSONs from specified paths            |
|  - Validates via Pydantic models                                  |
|  - Resolves secrets via pluggable SecretsResolver                 |
|  - Executes ETL pipeline                                          |
+-------------------------------------------------------------------+
```

---

## Complete Directory Structure

### Full Project Layout

```
my_project/
│
├── connectors/                           # CONNECTOR CATALOG (cloned from GitHub)
│   │
│   ├── wise/                             # API Connector: Wise (TransferWise)
│   │   ├── connector.json                # Host/connection template
│   │   └── endpoints/
│   │       ├── transfers.json            # Endpoint: GET /v1/transfers
│   │       ├── balances.json             # Endpoint: GET /v1/balances
│   │       └── profiles.json             # Endpoint: GET /v1/profiles
│   │
│   ├── sevdesk/                          # API Connector: SevDesk
│   │   ├── connector.json                # Host/connection template
│   │   └── endpoints/
│   │       ├── vouchers.json             # Endpoint: POST /Voucher
│   │       ├── contacts.json             # Endpoint: GET /Contact
│   │       └── invoices.json             # Endpoint: GET /Invoice
│   │
│   ├── postgres/                         # Database Connector: PostgreSQL
│   │   ├── connector.json                # Host/connection template
│   │   └── endpoints/
│   │       └── generic_table.json        # Generic table endpoint (schema + table)
│   │
│   ├── mysql/                            # Database Connector: MySQL
│   │   ├── connector.json
│   │   └── endpoints/
│   │       └── generic_table.json
│   │
│   └── stripe/                           # API Connector: Stripe
│       ├── connector.json
│       └── endpoints/
│           ├── charges.json
│           ├── customers.json
│           └── subscriptions.json
│
├── pipelines/                            # USER-DEFINED PIPELINES
│   ├── wise_to_postgres.json             # Pipeline: Wise -> PostgreSQL
│   ├── wise_to_sevdesk.json              # Pipeline: Wise -> SevDesk
│   └── stripe_to_postgres.json           # Pipeline: Stripe -> PostgreSQL
│
├── streams/                              # USER-DEFINED STREAMS (with mapping)
│   ├── wise_transfers.json               # Stream: Wise transfers -> PostgreSQL
│   ├── wise_balances.json                # Stream: Wise balances -> PostgreSQL
│   └── stripe_charges.json               # Stream: Stripe charges -> PostgreSQL
│
├── .secrets/                              # USER CREDENTIALS (gitignored)
│   ├── wise_credentials.json             # Wise API token
│   ├── sevdesk_credentials.json          # SevDesk API token
│   ├── postgres_credentials.json         # PostgreSQL password
│   └── stripe_credentials.json           # Stripe API key
│
├── state/                                # RUNTIME: Pipeline state (auto-created)
│   └── {pipeline_id}/
│       └── streams/
│           └── stream.{stream_id}/
│               └── partition-default.json
│
├── logs/                                 # RUNTIME: Logs (auto-created)
│   └── {pipeline_id}/
│       ├── pipeline.log
│       └── {stream_id}/
│           └── stream.log
│
├── deadletter/                           # RUNTIME: Failed records (auto-created)
│   └── {pipeline_id}/
│       └── {stream_id}/
│           └── dlq_*.jsonl
│
└── metrics/                              # RUNTIME: Execution metrics (auto-created)
    └── client_id={client_id}/
        └── year=YYYY/
            └── month=MM/
                └── day=DD/
                    └── {run_id}.json
```

### Connector Catalog (Public GitHub Repository)

This would be a separate public repo that users clone/pull:

```
analitiq-connectors/                      # https://github.com/analitiq-ai/connectors
│
├── README.md                             # Catalog documentation
├── CONTRIBUTING.md                       # How to add new connectors
│
├── wise/
│   ├── connector.json                    # Base URL, auth schema, rate limits
│   ├── README.md                         # Connector documentation
│   └── endpoints/
│       ├── transfers.json
│       ├── balances.json
│       └── profiles.json
│
├── sevdesk/
│   ├── connector.json
│   ├── README.md
│   └── endpoints/
│       ├── vouchers.json
│       ├── contacts.json
│       └── invoices.json
│
├── stripe/
│   ├── connector.json
│   └── endpoints/
│       ├── charges.json
│       ├── customers.json
│       └── subscriptions.json
│
├── postgres/
│   ├── connector.json                    # Connection params, SSL, pool settings
│   ├── README.md
│   └── endpoints/
│       └── generic_table.json            # Template for any table
│
├── mysql/
│   ├── connector.json
│   └── endpoints/
│       └── generic_table.json
│
└── snowflake/
    ├── connector.json
    └── endpoints/
        └── generic_table.json
```

### User's Minimal Setup

For a simple Wise -> PostgreSQL sync, user only needs:

```
my_wise_sync/
│
├── analitiq-connectors/                  # git clone analitiq-connectors
│   ├── wise/
│   ├── postgres/
│   └── ...
│
├── pipelines/
│   └── wise_to_postgres.json             # User creates this
│
├── streams/
│   └── transfers.json                    # User creates this (with mapping)
│
└── .secrets/
    ├── wise.json                         # {"WISE_API_TOKEN": "..."}
    └── postgres.json                     # {"DB_PASSWORD": "..."}
```

---

## Configuration Format

### Connector Template (`connectors/wise/connector.json`)

Defines how to connect to a service (auth schema, base URL, connection parameters):

```json
{
  "connector_id": "wise",
  "name": "Wise (TransferWise)",
  "type": "api",
  "version": "1.0.0",
  "host": "https://api.wise.com",
  "auth": {
    "type": "bearer_token",
    "token_placeholder": "${WISE_API_TOKEN}"
  },
  "headers": {
    "Content-Type": "application/json",
    "Accept": "application/json"
  },
  "rate_limit": {
    "max_requests": 60,
    "time_window": 60
  },
  "timeout": 30,
  "max_connections": 10
}
```

### Endpoint Definition (`connectors/wise/endpoints/transfers.json`)

Defines a specific API endpoint with schema and pagination:

```json
{
  "endpoint_id": "wise_transfers",
  "connector": "wise",
  "name": "Transfers",
  "endpoint": "/v1/transfers",
  "method": "GET",
  "replication_filter_mapping": {
    "created": "createdDateStart"
  },
  "pagination": {
    "type": "offset",
    "params": {
      "limit_param": "limit",
      "offset_param": "offset",
      "default_limit": 100
    }
  },
  "response_schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "array",
    "items": {
      "type": "object",
      "properties": {
        "id": { "type": "integer" },
        "created": { "type": "string", "format": "date-time" },
        "targetValue": { "type": "number" }
      }
    }
  }
}
```

### Pipeline Configuration (`pipelines/wise_to_postgres.json`)

References connectors and endpoints by path:

```json
{
  "pipeline_id": "b0c2f9d0-3b2a-4a7e-8c86-1b9c6c2d7b15",
  "name": "Wise to PostgreSQL Sync",
  "version": 1,

  "connectors_path": "./connectors",

  "connections": {
    "source": {
      "wise_api": {
        "connector": "wise",
        "credentials": "./secrets/wise_credentials.json"
      }
    },
    "destinations": [
      {
        "postgres_db": {
          "connector": "postgres",
          "credentials": "./secrets/postgres_credentials.json"
        }
      }
    ]
  },

  "streams_path": "./streams",

  "runtime": {
    "batching": { "batch_size": 100 },
    "error_handling": { "strategy": "dlq" },
    "logging": { "log_level": "INFO" }
  }
}
```

### Stream Configuration (`streams/transfers_stream.json`)

User-defined stream with source, destination, and mapping:

```json
{
  "stream_id": "f1a2b3c4-d5e6-7890-abcd-ef1234567891",
  "pipeline_id": "b0c2f9d0-3b2a-4a7e-8c86-1b9c6c2d7b15",
  "name": "Wise Transfers to PostgreSQL",

  "source": {
    "connection_ref": "wise_api",
    "endpoint": "wise/endpoints/transfers.json",
    "primary_key": ["id"],
    "replication": {
      "method": "incremental",
      "cursor_field": ["created"]
    }
  },

  "destinations": [
    {
      "connection_ref": "postgres_db",
      "endpoint": "postgres/endpoints/generic_table.json",
      "endpoint_override": {
        "schema": "wise_data",
        "table": "transfers"
      },
      "write": {
        "mode": "upsert",
        "conflict_keys": ["wise_id"]
      }
    }
  ],

  "mapping": {
    "assignments": [
      {
        "target": { "path": ["wise_id"], "type": "integer" },
        "value": { "kind": "field", "path": ["id"] }
      },
      {
        "target": { "path": ["amount"], "type": "decimal" },
        "value": { "kind": "field", "path": ["targetValue"] }
      },
      {
        "target": { "path": ["created_at"], "type": "timestamp" },
        "value": { "kind": "field", "path": ["created"] }
      }
    ]
  }
}
```

### User Credentials (`secrets/wise_credentials.json`)

User-specific credentials that override connector placeholders:

```json
{
  "WISE_API_TOKEN": "your-actual-api-token-here"
}
```

---

## Engine Loading Flow

When the engine starts, it:

1. **Receives pipeline path** (e.g., `./pipelines/wise_to_postgres.json`)
2. **Loads pipeline config** and extracts:
   - `connectors_path` - base path for connector catalog
   - `connections` - connector references with credential paths
   - `streams_path` - path to stream definitions
3. **For each connection:**
   - Load connector template from `{connectors_path}/{connector}/connector.json`
   - Load credentials from specified path
   - Merge credentials into connector config (late-binding)
4. **For each stream:**
   - Load stream config from `{streams_path}/{stream}.json`
   - Load source endpoint from `{connectors_path}/{endpoint_path}`
   - Load destination endpoint(s) from their paths
   - Apply `endpoint_override` if specified
5. **Execute pipeline** with fully resolved configs

---

## Late-Binding Secrets Design

### Why Late Binding?

- Secrets resolved **at connection time**, not at config load time
- Shorter exposure window in memory
- Pluggable backends (local files, S3, Secrets Manager)
- Easier testing with mock resolvers

### Core Abstractions

**1. SecretsResolver Protocol** (`src/secrets/protocol.py`)
```python
class SecretsResolver(Protocol):
    async def resolve(
        self,
        connection_id: str,
        *,
        client_id: Optional[str] = None,
        keys: Optional[list[str]] = None,
    ) -> Dict[str, str]:
        """Resolve secrets for a connection."""
        ...

    async def close(self) -> None:
        """Clean up resources."""
        ...
```

**2. ConnectionConfig Wrapper** (`src/secrets/config_wrapper.py`)
```python
class ConnectionConfig:
    """Wraps connection config with lazy secret resolution."""

    def __init__(self, config: Dict, connection_id: str, resolver: SecretsResolver):
        self._raw_config = config  # Contains ${placeholder} values
        self._resolver = resolver
        self._resolved_config = None

    async def resolve(self) -> Dict[str, Any]:
        """Resolve all ${placeholder} values just-in-time."""
        if self._resolved_config is None:
            secrets = await self._resolver.resolve(self.connection_id)
            self._resolved_config = self._expand_placeholders(self._raw_config, secrets)
        return self._resolved_config
```

**3. Resolver Implementations**
- `LocalFileSecretsResolver` - reads from `{config_dir}/.secrets/{connection_id}`

### Integration Points

**Modified connector flow:**
```python
# src/connectors/api.py
class APIConnector(BaseConnector):
    async def connect(self, config: Dict | ConnectionConfig):
        # Resolve secrets just-in-time
        resolved_config = await self._resolve_config(config)
        # Now use resolved_config with actual secret values
        self.headers = resolved_config["headers"]  # "Bearer actual-token"
```

**Resolution timing:**
| Stage | Secrets Status |
|-------|---------------|
| Config loaded from S3 | Unresolved (`${token}`) |
| Pipeline.__init__() | Unresolved |
| connector.connect() | **RESOLVED** |
| connector.read_batches() | Already resolved |

---

## Implementation Plan

### Phase 1: Create Path-Based Config Loader

Create new `src/config/` package for path-based loading:

```
src/config/
├── __init__.py
├── loader.py             # PathBasedConfigLoader class
├── models.py             # Config path models
└── exceptions.py         # ConfigNotFoundError, etc.
```

**PathBasedConfigLoader** (`src/config/loader.py`):
```python
class PathBasedConfigLoader:
    """Loads configs from user-specified paths."""

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    def load_pipeline(self, pipeline_path: str) -> Dict[str, Any]:
        """Load pipeline config and extract paths."""
        pipeline = self._load_json(pipeline_path)
        self.connectors_path = pipeline.get("connectors_path", "./connectors")
        self.streams_path = pipeline.get("streams_path", "./streams")
        return pipeline

    def load_connector(self, connector_name: str) -> Dict[str, Any]:
        """Load connector template from {connectors_path}/{connector}/connector.json"""
        path = self.connectors_path / connector_name / "connector.json"
        return self._load_json(path)

    def load_endpoint(self, endpoint_ref: str) -> Dict[str, Any]:
        """Load endpoint from {connectors_path}/{endpoint_ref}"""
        path = self.connectors_path / endpoint_ref
        return self._load_json(path)

    def load_credentials(self, credentials_path: str) -> Dict[str, Any]:
        """Load user credentials from specified path."""
        return self._load_json(credentials_path)

    def load_streams(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """Load all streams for a pipeline from {streams_path}."""
        streams = []
        for stream_file in (self.streams_path).glob("*.json"):
            stream = self._load_json(stream_file)
            if stream.get("pipeline_id") == pipeline_id:
                streams.append(stream)
        return streams
```

### Phase 2: Add Secrets Module (New Files)

Create `src/secrets/` package:
```
src/secrets/
├── __init__.py
├── protocol.py           # SecretsResolver protocol
├── exceptions.py         # SecretNotFoundError, etc.
├── config_wrapper.py     # ConnectionConfig wrapper
├── safe_logging.py       # Logging filters to mask secrets
└── resolvers/
    ├── __init__.py
    ├── local.py          # LocalFileSecretsResolver (reads from credentials path)
    ├── s3.py             # S3SecretsResolver (for cloud deployment)
    └── memory.py         # InMemorySecretsResolver (for testing)
```

### Phase 3: Refactor PipelineConfigPrep

Replace DynamoDB loading with path-based loading:

```python
class PipelineConfigPrep:
    def __init__(
        self,
        pipeline_path: str,
        base_path: Optional[str] = None,
        secrets_resolver: Optional[SecretsResolver] = None,
    ):
        """
        Initialize config prep with path references.

        Args:
            pipeline_path: Path to pipeline JSON (e.g., "./pipelines/wise_to_postgres.json")
            base_path: Base path for resolving relative paths (defaults to pipeline's parent dir)
            secrets_resolver: Optional resolver for late-binding secrets
        """
        self.loader = PathBasedConfigLoader(base_path or Path(pipeline_path).parent)
        self.secrets_resolver = secrets_resolver

    def create_config(self) -> Tuple[PipelineConfig, List[StreamConfig], Dict, Dict]:
        """
        Load and assemble complete configuration.

        1. Load pipeline config (extracts connectors_path, streams_path)
        2. For each connection: load connector template + merge credentials
        3. Load streams, for each stream:
           - Load source endpoint from connectors_path
           - Load destination endpoint(s)
           - Apply endpoint_override if specified
        4. Return assembled configs
        """
```

### Phase 4: Modify Connectors for Late Binding

Files to modify:
- `src/connectors/base.py` - Add `_resolve_config()` helper
- `src/connectors/api.py` - Call `_resolve_config()` in `connect()`
- `src/connectors/database/database_connector.py` - Same pattern

### Phase 5: Update Entry Points

The framework uses **different entry points** for local development vs cloud deployment, but both ultimately use the same path-based config loading:

```
LOCAL:
  User creates files manually
  python -m src.runner --pipeline ./pipelines/my_pipeline.json
        |
        v
  PipelineConfigPrep (path-based loading)
        |
        v
  Engine executes pipeline

CLOUD (AWS Batch):
  docker/entrypoint.py
        |
        v
  docker/config_fetcher.py
  (queries DynamoDB + S3, writes to /tmp/config/)
        |
        v
  python -m src.runner --pipeline /tmp/config/pipelines/{id}.json
        |
        v
  PipelineConfigPrep (same path-based loading)
        |
        v
  Engine executes pipeline
```

**Local development:**
```bash
# Users manually create pipelines/, streams/, .secrets/ files
# Then run directly with path reference
python -m src.runner --pipeline ./pipelines/wise_to_postgres.json

# Or with environment variable
PIPELINE_PATH=./pipelines/wise_to_postgres.json python -m src.runner
```

**Cloud (AWS Batch) - docker/entrypoint.py:**
```python
import subprocess
import os

# Step 1: Fetch configs from DynamoDB/S3 and write to local filesystem
subprocess.run([
    "python", "docker/config_fetcher.py",
    "--output-dir", "/tmp/config"
], check=True)

# Step 2: Run the pipeline using path-based loading (same as local)
pipeline_id = os.getenv("PIPELINE_ID")
subprocess.run([
    "python", "-m", "src.runner",
    "--pipeline", f"/tmp/config/pipelines/{pipeline_id}.json"
], check=True)
```

This keeps the core engine completely cloud-agnostic - it only knows about local file paths.

### Phase 6: Remove DynamoDB Code

Delete DynamoDB-specific methods from `PipelineConfigPrep`:
- `_load_dynamodb_pipeline()`
- `_load_dynamodb_streams()`
- `_load_dynamodb_connection()`
- `_load_dynamodb_connector()`
- `_load_dynamodb_endpoint()`
- `_deserialize_dynamodb_item()`

Keep only path-based loading logic.

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/config/` (NEW) | Create path-based config loader module |
| `src/secrets/` (NEW) | Create secrets module with resolvers |
| `src/core/pipeline_config_prep.py` | Replace DynamoDB loading with path-based loading |
| `src/connectors/base.py` | Add `_resolve_config()` helper method |
| `src/connectors/api.py` | Use `_resolve_config()` in `connect()` |
| `src/connectors/database/database_connector.py` | Use `_resolve_config()` in `connect()` |
| `docker/config_fetcher.py` (NEW) | Script to fetch configs from DynamoDB/S3 for cloud deployments |
| `docker/entrypoint.py` | Invoke `config_fetcher.py` first, then run pipeline with local path |
| `src/runner.py` | Accept pipeline path as argument |
| `cli.py` | Add `--pipeline` argument |

---

## Testing Strategy

1. **Unit tests for config loader:**
   - Test path resolution
   - Test connector/endpoint loading
   - Test credential merging

2. **Unit tests for secrets module:**
   - Use `InMemorySecretsResolver` to mock secrets
   - Test placeholder expansion
   - Test error handling (missing secrets, access denied)

3. **Integration tests:**
   - Test connectors with `ConnectionConfig` wrappers
   - Verify secrets are resolved at `connect()` time
   - Verify secrets are masked in logs

4. **E2E tests:**
   - Test full flow with example directory structure
   - Test with different connector catalogs

---

## Cloud Deployment (AWS Batch)

For cloud deployment, a **config fetcher script** is bundled with the Docker image and runs as the first step before the pipeline executes:

### Config Fetcher Script (`docker/config_fetcher.py`)

A standalone script that:

1. **Reads environment variables** (`PIPELINE_ID`, `CLIENT_ID`, `AWS_REGION`, table names)
2. **Queries DynamoDB** for pipeline, streams, client connections, connectors, and endpoints
3. **Fetches secrets from S3** for each connection
4. **Writes everything to local filesystem** in the standard directory structure

The script writes to `/tmp/config/`:
```
/tmp/config/
  pipelines/{pipeline_id}.json
  streams/{stream_id}.json
  client_connections/{connection_id}.json
  connector_endpoints/{endpoint_id}.json
  .secrets/{connection_id}
```

### Batch Container Execution Flow

1. **Entrypoint invokes config fetcher** - `docker/config_fetcher.py` runs first
2. **Config fetcher populates local files** - queries DynamoDB/S3, writes to `/tmp/config/`
3. **Entrypoint invokes runner** - `python -m src.runner --pipeline /tmp/config/pipelines/{id}.json`
4. **Engine uses path-based loading** - same `PipelineConfigPrep` as local development

### Benefits

- **Core engine stays cloud-agnostic** - only knows about local file paths
- **Single codebase** - same engine code runs locally and in cloud
- **DynamoDB schema changes isolated** - only affects `config_fetcher.py`, not the engine
- **Testable** - config fetcher can be unit tested separately

### Environment Variables

See `CLAUDE.md` for the complete list of required environment variables (DynamoDB table names, S3 bucket patterns, etc.).