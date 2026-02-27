# Secrets and Credential Expansion

Analitiq Stream uses a `${placeholder}` pattern to keep credentials out of pipeline configuration files. Connection configs contain placeholders like `${password}`, and actual secret values live in separate JSON files that are merged at connection time.

## How It Works

### 1. Connection config has placeholders

In the consolidated pipeline file (`pipelines/{pipeline_id}.json`), connections contain `${placeholder}` values instead of real credentials:

```json
{
  "connections": [
    {
      "connection_id": "prod-postgres",
      "connector_id": "pg-connector",
      "host": "${DB_HOST}",
      "port": 5432,
      "database": "${DB_NAME}",
      "username": "${DB_USER}",
      "password": "${DB_PASSWORD}"
    }
  ]
}
```

### 2. Secrets file provides the values

A matching secrets file at `.secrets/{connection_id}.json` contains a flat key-value dictionary:

```json
{
  "DB_HOST": "db.example.com",
  "DB_NAME": "analytics",
  "DB_USER": "admin",
  "DB_PASSWORD": "Example1234"
}
```

The keys in the secrets file must match the placeholder names exactly. `${DB_PASSWORD}` in the config maps to `"DB_PASSWORD"` in the secrets file.

### 3. Expansion happens at connection time

Secrets are **not** expanded when the pipeline config is loaded. They are expanded lazily, just before a connection is established. This is handled by `ConnectionConfig.resolve()` in `src/secrets/config_wrapper.py`.

The flow:

1. `PipelineConfigPrep.create_config()` loads the pipeline and wraps each connection in a `ConnectionConfig` object (placeholders still intact)
2. When a component needs to connect, it calls `await resolved_connection.resolve_config()`
3. `ConnectionConfig.resolve()` fetches secrets via `SecretsResolver.resolve(connection_id)` and expands all `${placeholder}` values

After expansion, the config becomes:

```json
{
  "connection_id": "prod-postgres",
  "connector_id": "pg-connector",
  "host": "db.example.com",
  "port": 5432,
  "database": "analytics",
  "username": "admin",
  "password": "Example1234"
}
```

### 4. Missing placeholders fail immediately

If a `${placeholder}` has no matching key in the secrets file, a `PlaceholderExpansionError` is raised. The connection is never established with raw placeholders. There is no silent passthrough.

## Secrets File Location

Secrets files are located via the `secrets` path in `analitiq.yaml`:

```yaml
paths:
  pipelines: "./pipelines"
  secrets: "./.secrets"
```

The resolver searches for secrets in this order:

1. `{secrets_dir}/{connection_id}.json`
2. `{secrets_dir}/{connection_id}` (no extension)
3. `{secrets_dir}/{client_id}/{connection_id}.json` (multi-tenant)
4. `{secrets_dir}/{client_id}/{connection_id}` (multi-tenant, no extension)

## Secrets File Format

Each secrets file is a flat JSON object mapping placeholder names to values:

```json
{
  "API_TOKEN": "bearer-token-here",
  "API_SECRET": "secret-value"
}
```

All values are converted to strings during expansion. Nested objects are not supported inside the secrets file itself, but `${placeholder}` values can appear anywhere in the connection config, including nested dicts and lists:

```json
{
  "headers": {
    "Authorization": "Bearer ${API_TOKEN}",
    "X-Custom": "${CUSTOM_HEADER}"
  },
  "params": ["${PARAM_1}", "${PARAM_2}"]
}
```

## Cloud Mode

In `dev`/`prod` environments, `docker/config_fetcher.py` fetches secrets from AWS and writes them to the local secrets directory before the pipeline runs. `PipelineConfigPrep` then reads from the same local path regardless of environment.

## SecretsResolver Protocol

Secret fetching is abstracted behind the `SecretsResolver` protocol (`src/secrets/protocol.py`). Implementations:

| Resolver | Backend | Usage |
|----------|---------|-------|
| `LocalFileSecretsResolver` | `.secrets/{connection_id}.json` | Default for all environments |
| `S3SecretsResolver` | S3 bucket | Available for direct S3 access |
| `InMemorySecretsResolver` | In-memory dict | Testing |

## Security Best Practices

1. **Never commit secrets files** to version control. Add `.secrets/` to `.gitignore`.
2. **Use restrictive file permissions** (`chmod 600`) on secrets files.
3. **Use different secrets** per environment (local, dev, prod).
4. **Call `clear_resolved()`** on `ConnectionConfig` after establishing a connection to remove secrets from memory.
5. **Rotate credentials regularly** and update the secrets files.

### .gitignore

```
.secrets/
```