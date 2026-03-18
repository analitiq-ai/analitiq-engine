# Source Configuration Reference

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ENV` | Yes | `local`, `dev`, `prod` |
| `PIPELINE_ID` | Yes | Pipeline UUID |
| `ORG_ID` | Yes | Org UUID |
| `LOCAL_CONFIG_MOUNT` | Yes | Config directory path |
| `AWS_REGION` | No | Default: `eu-central-1` |
| `LOG_LEVEL` | No | Default: `INFO` |

## Pipeline Configuration

**File:** `pipelines/{pipeline_id}.json`

```json
{
  "connections": {
    "source": { "<alias>": "<connection_uuid>" }
  }
}
```

## Stream Source Configuration

**File:** `streams/{stream_id}.json`

| Field | Required | Description |
|-------|----------|-------------|
| `source.connection_ref` | Yes | Connection alias from pipeline |
| `source.endpoint_id` | Yes | Source endpoint UUID |
| `source.primary_key` | No | List of primary key fields |
| `source.replication.method` | No | `incremental` or `full` (default: `incremental`) |
| `source.replication.cursor_field` | No | Token path to cursor field (e.g., `["updated_at"]`) |
| `source.replication.safety_window_seconds` | No | Offset for late-arriving data |
| `source.replication.tie_breaker_fields` | No | Fields for ordering when cursor values match |
| `source.source_schema_fingerprint` | No | SHA256 hash of source schema |
| `source.parameters` | No | Connector-specific parameters |

## Connection Configuration

**File:** `connections/{connection_id}.json` or `.secrets/{connection_id}`

### Database Connection

| Field | Required | Description |
|-------|----------|-------------|
| `provider` / `driver` | Yes | `postgresql`, `mysql`, etc. |
| `host` | Yes | Database hostname |
| `port` | Yes | Database port |
| `database` / `dbname` | Yes | Database name |
| `username` / `user` | Yes | Database user |
| `password` | Yes | Database password (supports `${VAR}`) |
| `ssl_mode` | No | `prefer`, `require`, `verify-ca` or `verify-full` |
| `connection_timeout` | No | Timeout in seconds |
| `max_connections` | No | Max pool size |
| `min_connections` | No | Min pool size |

### API Connection

| Field | Required | Description |
|-------|----------|-------------|
| `provider` / `type` | Yes | Set to `api` |
| `base_url` / `host` | Yes | Base API URL |
| `headers` | No | HTTP headers (supports `${VAR}`) |
| `auth.type` | No | `bearer_token`, `api_key`, `basic` |
| `auth.token` | No | Bearer token |
| `auth.api_key` | No | API key |
| `auth.username` / `password` | No | Basic auth credentials |
| `timeout` | No | Request timeout (default: 30) |
| `max_connections` | No | Max concurrent connections (default: 10) |
| `rate_limit.max_requests` | No | Requests per time window |
| `rate_limit.time_window` | No | Time window in seconds |

## Endpoint Configuration

**File:** `connectors/{connector_name}/endpoints/{endpoint_id}.json`

### Database Endpoint

| Field | Required | Description |
|-------|----------|-------------|
| `schema` | No | Database schema (default: `public`) |
| `table` | Yes | Table name |

### API Endpoint

| Field | Required | Description |
|-------|----------|-------------|
| `endpoint` | Yes | API path (e.g., `/v1/transfers`) |
| `method` | No | HTTP method (default: `GET`) |
| `pagination.type` | No | `offset`, `cursor`, `page`, `time`, `link` |
| `pagination.params.limit_param` | No | Query param for page size |
| `pagination.params.max_limit` | No | Maximum page size |
| `pagination.params.offset_param` | No | Query param for offset |
| `pagination.params.cursor_param` | No | Query param for cursor |
| `pagination.params.next_cursor_field` | No | Response field with next cursor |
| `pagination.params.page_param` | No | Query param for page number |
| `pagination.params.start_param` | No | Start time param (time-based) |
| `pagination.params.end_param` | No | End time param (time-based) |
| `replication_filter_mapping` | No | Maps cursor field to API param |
| `filters.<name>.type` | No | Filter data type |
| `filters.<name>.required` | No | Whether filter is required |
| `filters.<name>.operators` | No | `eq`, `in`, `gte`, `lte`, etc. |