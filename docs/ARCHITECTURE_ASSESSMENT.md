# Architecture Assessment: Connectors-as-Code Design

**Date**: 2025-01-05
**Document Reviewed**: `docs/ARCHITECTURE_DECOUPLING_PLAN.md`
**Comparison Targets**: Singer, Airbyte, Fivetran

---

## Executive Summary

The proposed design is a solid foundation that addresses the immediate decoupling goals, but has gaps when compared to mature platforms like Singer, Airbyte, and Fivetran. The design correctly identifies key principles (connector catalogs, late-binding secrets, path-based loading) but misses several critical patterns that make ELT frameworks robust and user-friendly.

---

## Comparison with Industry Standards

### 1. Connector Protocol

| Platform | Approach | Your Design |
|----------|----------|-------------|
| **Singer** | JSON messages via stdout (SCHEMA, RECORD, STATE, ACTIVATE_VERSION) | None defined |
| **Airbyte** | Airbyte Protocol with spec/check/discover/read/write commands | None defined |
| **Fivetran** | Proprietary SDK with request/response contract | None defined |

**Gap**: Your design tightly couples connectors to the engine code. Singer/Airbyte connectors are independent processes/containers that communicate via a defined protocol.

**Recommendation**: Define a connector interface protocol:

```python
class ConnectorProtocol:
    async def spec() -> ConnectorSpec        # Return config schema
    async def check(config) -> ConnectionStatus  # Test connection
    async def discover(config) -> Catalog    # Discover available streams
    async def read(config, catalog, state) -> Iterator[Message]
    async def write(config, catalog, messages) -> Iterator[Message]
```

---

### 2. Schema/Catalog Discovery

| Platform | Approach | Your Design |
|----------|----------|-------------|
| **Singer** | `tap --discover` emits JSON catalog with all streams/schemas | Static endpoint files |
| **Airbyte** | `discover` command returns AirbyteCatalog | Static endpoint files |
| **Fivetran** | Auto-discovery + schema drift handling | Static endpoint files |

**Gap**: Your design requires users to manually define endpoint schemas. Modern platforms auto-discover what's available.

**Example flow (Airbyte):**

```
1. User configures connection (API key)
2. Platform calls discover() -> returns ["transfers", "balances", "profiles"]
3. User selects which streams to sync
4. Platform generates stream configs automatically
```

**Your flow:**

```
1. User clones connector repo (has endpoint JSONs)
2. User manually creates stream config referencing endpoint
3. User manually writes field mappings
```

**Recommendation**: Add a discovery phase:

```python
# In connector.json
{
  "connector_id": "wise",
  "discover_endpoint": "/v1/available-resources",  # or introspection logic
  ...
}

# Or implement discover() in connector code
async def discover(config: Dict) -> List[StreamDefinition]:
    """Return available streams/schemas from the source."""
```

---

### 3. Stream Selection vs Stream Definition

| Platform | Approach | Your Design |
|----------|----------|-------------|
| **Singer** | Catalog with `selected: true/false` per stream | User writes stream files |
| **Airbyte** | ConfiguredCatalog with user selections | User writes stream files |
| **Fivetran** | UI toggle per table/endpoint | User writes stream files |

**Gap**: Users must write detailed stream configs instead of selecting from discovered streams.

**Recommendation**: Separate "available streams" (from discover) from "selected streams" (user choice):

```json
// discovered_catalog.json (auto-generated)
{
  "streams": [
    {"name": "transfers", "schema": {...}, "supported_sync_modes": ["incremental", "full_refresh"]},
    {"name": "balances", "schema": {...}, "supported_sync_modes": ["full_refresh"]}
  ]
}

// configured_streams.json (user creates)
{
  "streams": [
    {"stream": "transfers", "sync_mode": "incremental", "destination_sync_mode": "append_dedup"}
  ]
}
```

---

### 4. Connector Isolation and Versioning

| Platform | Approach | Your Design |
|----------|----------|-------------|
| **Singer** | Each tap/target is a separate pip package | In-process, shared code |
| **Airbyte** | Each connector is a Docker container | In-process, shared code |
| **Fivetran** | Cloud-hosted, managed versions | Git-cloned, no version pinning |

**Gap**: No isolation between connectors. A buggy connector can crash the engine. No version pinning mechanism.

**Risks:**

- Dependency conflicts between connectors
- No rollback mechanism
- Breaking changes in connector repo affect all users immediately

**Recommendation**: Add version pinning at minimum:

```json
// pipelines/wise_to_postgres.json
{
  "connections": {
    "source": {
      "wise_api": {
        "connector": "wise",
        "connector_version": "1.2.0",
        "credentials": "./secrets/wise.json"
      }
    }
  }
}
```

For stronger isolation (future), consider subprocess execution or Docker containers.

---

### 5. Connection Testing

| Platform | Approach | Your Design |
|----------|----------|-------------|
| **Singer** | N/A (manual testing) | N/A |
| **Airbyte** | `check` command validates credentials | N/A |
| **Fivetran** | "Test Connection" button | N/A |

**Gap**: No way to validate credentials/connectivity before running a pipeline.

**Recommendation**: Add a `check` capability:

```bash
python -m src.runner check --connection wise_api --pipeline ./pipelines/wise.json
# Output: Connection successful (latency: 120ms)
# Or: Connection failed: 401 Unauthorized
```

---

### 6. Normalization and Type Mapping

| Platform | Approach | Your Design |
|----------|----------|-------------|
| **Singer** | Targets handle type conversion | User-defined mappings |
| **Airbyte** | Optional normalization (nested JSON to tables) | User-defined mappings |
| **Fivetran** | Aggressive auto-normalization | User-defined mappings |

**Current design**: User must manually define every field mapping:

```json
{
  "mapping": {
    "assignments": [
      {"target": {"path": ["wise_id"], "type": "integer"}, "value": {"kind": "field", "path": ["id"]}},
      {"target": {"path": ["amount"], "type": "decimal"}, "value": {"kind": "field", "path": ["targetValue"]}}
    ]
  }
}
```

**Airbyte approach**: Auto-map with optional transformations:

```yaml
sync_mode: incremental
destination_sync_mode: append_dedup
# All fields auto-mapped, types auto-inferred
# User only specifies exceptions
```

**Recommendation**: Support auto-mapping with optional overrides:

```json
{
  "mapping": {
    "mode": "auto",
    "type_inference": true,
    "overrides": [
      {"source": "targetValue", "target": "amount", "type": "decimal"}
    ],
    "exclude": ["internal_field"]
  }
}
```

---

### 7. State Management

| Platform | Approach | Your Design |
|----------|----------|-------------|
| **Singer** | STATE messages emitted by tap, persisted by target | Per-stream/partition state |
| **Airbyte** | State passed between runs, versioned | Per-stream/partition state |
| **Fivetran** | Managed internally | Per-stream/partition state |

**Assessment**: Your state management design is solid and aligns with industry practice. Good use of:

- Per-stream/partition checkpoints
- Bookmarks with cursor tracking
- Safety window for late-arriving data

---

### 8. Error Handling and DLQ

| Platform | Approach | Your Design |
|----------|----------|-------------|
| **Singer** | Fails on error (no DLQ) | DLQ with retry metadata |
| **Airbyte** | Configurable error handling | DLQ strategy option |
| **Fivetran** | Managed with alerting | DLQ with S3 storage |

**Assessment**: Your DLQ design is actually more sophisticated than Singer's. Good features:

- Configurable strategy (dlq vs fail-fast)
- S3 storage with partitioning
- Retry metadata preservation

---

### 9. Secret Management

| Platform | Approach | Your Design |
|----------|----------|-------------|
| **Singer** | Config file with secrets (plaintext) | Late-binding with resolvers |
| **Airbyte** | Docker env vars or managed secrets | Late-binding with resolvers |
| **Fivetran** | Managed (encrypted at rest) | Pluggable resolvers |

**Assessment**: Your late-binding design is actually **better** than Singer's approach:

- Secrets resolved at connection time, not load time
- Pluggable backends (local, S3, Secrets Manager)
- Shorter exposure window

**Suggestion**: Add safe logging to mask secrets automatically:

```python
# Already mentioned in design - good!
src/secrets/safe_logging.py  # Logging filters to mask secrets
```

---

## Summary: Gaps vs Strengths

### Critical Gaps

| Gap | Impact | Priority |
|-----|--------|----------|
| No connector protocol | Tight coupling, no isolation | High |
| No schema discovery | Manual work for users | High |
| No connection testing | Poor UX, debugging difficulty | Medium |
| No version pinning | Breaking changes affect all users | Medium |
| Manual field mapping required | Tedious for large schemas | Medium |

### Strengths (Keep These)

| Strength | Notes |
|----------|-------|
| Late-binding secrets | Better than Singer's plaintext approach |
| Path-based config | Simple, testable, git-friendly |
| Per-stream/partition state | Proper incremental sync support |
| DLQ with metadata | Better than Singer's fail-fast |
| Cloud/local parity | Same engine, different config loaders |

---

## Recommended Additions

### 1. Minimal Connector Protocol

```python
# src/connectors/protocol.py
from typing import Protocol, Dict, List, Any
from pydantic import BaseModel


class ConnectorSpec(BaseModel):
    connection_spec: Dict[str, Any]  # JSON Schema for credentials
    supported_modes: List[str]  # ["incremental", "full_refresh"]


class StreamSchema(BaseModel):
    name: str
    schema: Dict[str, Any]
    supported_sync_modes: List[str]
    source_defined_cursor: bool = False
    default_cursor_field: List[str] | None = None


class ConnectorProtocol(Protocol):
    def spec(self) -> ConnectorSpec:
        """Return the connector's configuration specification."""
        ...

    async def check(self, config: Dict) -> bool:
        """Validate credentials and connectivity."""
        ...

    async def discover(self, config: Dict) -> List[StreamSchema]:
        """Discover available streams and their schemas."""
        ...

    # read/write already exist in your BaseConnector
```

### 2. Auto-Mapping Mode

```json
{
  "mapping": {
    "mode": "auto",
    "rename_strategy": "snake_case",
    "type_inference": true,
    "overrides": [
      {"source": "targetValue", "target": "amount", "type": "decimal"}
    ],
    "exclude": ["_internal", "debug_info"]
  }
}
```

### 3. CLI Commands

```bash
# Discover available streams
python -m src.runner discover --connection wise_api

# Test connection
python -m src.runner check --connection wise_api

# Generate stream config from discovery
python -m src.runner generate-stream --source wise_api --stream transfers

# Validate configuration without running
python -m src.runner validate --pipeline ./pipelines/wise_to_postgres.json
```

### 4. Connector Version Pinning

```json
{
  "connectors_path": "./connectors",
  "connector_versions": {
    "wise": "1.2.0",
    "postgres": "2.0.1"
  }
}
```

---

## Final Assessment

| Aspect | Score | Notes |
|--------|-------|-------|
| **Decoupling Goal** | Achieved | Successfully separates cloud config from engine |
| **Singer Parity** | Partial | Missing protocol, discovery |
| **Airbyte Parity** | Partial | Missing isolation, discovery, check |
| **Production Readiness** | Partial | Good foundation, needs CLI tooling |
| **Developer Experience** | Needs Work | Manual mapping is tedious |

**Bottom Line**: The design successfully achieves the stated decoupling goals and has some innovations (late-binding secrets, sophisticated DLQ). However, to compete with modern ELT platforms, it needs:

1. Schema discovery
2. Connection testing
3. Auto-mapping capability
4. Version management

---

## Recommended Implementation Order

The original implementation plan should be modified to include these additions:

### Phase 0: Define Connector Protocol (NEW)

Before Phase 1, establish the connector protocol:

1. Create `src/connectors/protocol.py` with `ConnectorProtocol` abstract class
2. Define `ConnectorSpec`, `StreamSchema`, `ConnectionStatus` models
3. Add `check()` and `discover()` methods to `BaseConnector`
4. Update existing connectors to implement the protocol

### Phase 1-6: As Originally Planned

Proceed with the original phases for path-based loading and secrets.

### Phase 7: CLI Tooling (NEW)

Add developer experience improvements:

1. `check` command for connection testing
2. `discover` command for schema discovery
3. `generate-stream` command to scaffold stream configs
4. `validate` command for config validation

### Phase 8: Auto-Mapping (NEW)

Reduce manual configuration burden:

1. Implement `mode: "auto"` in mapping config
2. Add type inference from JSON Schema
3. Support rename strategies (snake_case, camelCase, etc.)
4. Allow selective overrides and exclusions
