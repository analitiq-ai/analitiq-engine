# Field Mapping, Transformations & Validation

**Scope:** this doc owns the assignment syntax, the expression AST, the
validation rules, and the transformation registry
(`src/transformations/registry.py`). For Arrow type-system and
schema-contract internals see
[`pyarrow-and-destinations.md`](pyarrow-and-destinations.md).

Streams declare their record-shape transformation under `mapping` in
`pipelines/{id}/streams/{stream_id}.json`. The implementation lives in
`src/engine/data_transformer.py` (`AssignmentTransformer`).

## Overview

Each target field is built by exactly one **assignment**:

```
Target Field (path + type)  ←  Value (const | expr AST)  ←  Optional validation
```

Expressions are a **structured AST** (JSON), not source strings. End
users edit assignments through a UI; the engine evaluates the AST per
record. Stream-level `source_to_generic` and
`generic_to_destination` blocks describe canonical typing for the
destination's schema contract and are independent of the assignment AST.

## Stream `mapping` Shape

```json
{
  "mapping": {
    "source_schema_id": "wise.transfers.v1",
    "target_schema_id": "postgres.public_wise_transfers.v1",
    "assignments": [
      {
        "value": { "kind": "expr", "expr": { "op": "get", "path": ["created"] } },
        "target": { "type": "datetime", "nullable": true, "path": ["created"] }
      }
    ],
    "defaults": { "on_error": "dlq" },

    "source_to_generic": {
      "created": { "generic_type": "datetime" }
    },
    "generic_to_destination": {
      "my-postgres": {
        "created": { "destination_type": "datetime", "nullable": true }
      }
    }
  }
}
```

- `assignments[]` is an ordered list, evaluated top to bottom. Each
  assignment's `get` reads only from the **source record** — the
  in-progress result is threaded through evaluation as `partial_result`
  but no current `op` consults it, so earlier assignments are **not**
  visible to later ones. Treat every assignment as a pure function of
  the source record.
- `source_to_generic` / `generic_to_destination` are consumed by the
  destination's `SchemaContract` for vectorized Arrow casting (see
  `cdk/cdk/schema_contract.py`; details in
  [`pyarrow-and-destinations.md`](pyarrow-and-destinations.md)).
- `defaults.on_error` provides a stream-level default for assignments
  that do not override `validate.on_error`.

## Assignments

### Direct field copy

```json
{
  "value": { "kind": "expr", "expr": { "op": "get", "path": ["targetValue"] } },
  "target": { "path": ["amount"], "type": "decimal", "nullable": false }
}
```

### Constant

```json
{
  "value": { "kind": "const", "const": { "type": "string", "value": "100" } },
  "target": { "path": ["status"], "type": "string", "nullable": false }
}
```

### Nested object constant

```json
{
  "value": {
    "kind": "const",
    "const": {
      "type": "object",
      "value": { "id": "5936402", "objectName": "CheckAccount" }
    }
  },
  "target": { "path": ["checkAccount"], "type": "object", "nullable": false }
}
```

### Pipeline of functions

```json
{
  "value": {
    "kind": "expr",
    "expr": {
      "op": "pipe",
      "args": [
        { "op": "get", "path": ["email"] },
        { "op": "fn", "name": "trim",  "version": 1, "args": [] },
        { "op": "fn", "name": "lower", "version": 1, "args": [] }
      ]
    }
  },
  "target": { "path": ["email"], "type": "string", "nullable": true }
}
```

### Conditional

```json
{
  "value": {
    "kind": "expr",
    "expr": {
      "op": "if",
      "args": [
        { "op": "eq", "args": [
          { "op": "get", "path": ["is_active"] },
          { "op": "const", "value": true }
        ]},
        { "op": "const", "value": "active" },
        { "op": "const", "value": "inactive" }
      ]
    }
  },
  "target": { "path": ["status"], "type": "string", "nullable": false }
}
```

## Expression AST

Implemented `op` values (see `_evaluate_expression` in
`data_transformer.py`):

| `op` | Description |
|------|-------------|
| `get` | Read from the source record at `path` (token array) |
| `const` | Inline literal `value` |
| `pipe` | First arg is the seed, remaining args are `fn` nodes applied left-to-right |
| `fn` | Apply function `name@version` to the value flowing through `pipe` |
| `if` | Three-arg ternary: `[condition, then, else]` |
| `eq`, `neq` | Equality |
| `gt`, `gte`, `lt`, `lte` | Comparison |
| `and`, `or`, `not` | Boolean logic |
| `concat` | String concatenation of evaluated args (None args dropped) |
| `coalesce` | First non-null evaluated arg |

Unknown `op` values log a warning and resolve to `null` rather than
raising — keep authoring tooling honest by validating against this list.

## Function Catalog

Built-in functions (`AssignmentTransformer.FUNCTION_CATALOG`):

| Name | Version | Purpose |
|------|---------|---------|
| `iso_to_date` | 1 | ISO-8601 timestamp → `YYYY-MM-DD` string |
| `iso_to_datetime` | 1 | ISO-8601 → datetime (timezone-aware) |
| `iso_to_timestamp` | 1 | ISO-8601 → epoch timestamp |
| `trim`, `lower`, `upper` | 1 | String normalization |
| `to_int`, `to_float`, `to_string` | 1 | Type coercion |
| `abs` | 1 | Numeric absolute value |
| `now` | 1 | Current UTC datetime |
| `default` | 1 | Substitute fallback when input is null |
| `coalesce` | 1 | First non-null value |

Function versions are pinned in the AST (`"version": 1`) so the catalog
can evolve without rewriting existing mappings. New versions must be
registered alongside the existing ones; do not silently rewrite v1
behaviour.

## Validation

```json
"validate": {
  "rules": [
    { "type": "not_null", "message": "id must be present" },
    { "type": "min_length", "value": 1 }
  ],
  "on_error": "dlq"
}
```

Implemented rule types (`_validate_value`):

| `type` | Required keys | Notes |
|--------|---------------|-------|
| `not_null` (alias `required`) | — | Fails when the value is None |
| `min_length` | `value` | Compares against `len(str(value))` |
| `max_length` | `value` | Same |
| `pattern` | `value` | Python `re.match` against `str(value)` |
| `range` | `min` and/or `max` | Numeric comparison |
| `in_list` | `value` | Value must be in the supplied list |

`on_error` actions:

- `dlq` — record is sent to the dead-letter queue, processing continues
- `quarantine` — record is parked for review, processing continues
- `skip_record` — drop the record, processing continues
- `default_value` — substitute the rule's `default` and continue
- `stop_stream` — abort the current stream

A stream-level `defaults.on_error` is used when an assignment does not
specify `validate.on_error`.

## Type Coercion

`target.type` drives a JSON-compatible coercion step before the value is
written into the result (`_coerce_type`):

- `string` → `str(value)`
- `integer` → `int(float(value))` (returns the original value if
  parsing fails)
- `decimal`, `number` → `float(value)`
- `boolean` → bool, with `"true"`/`"false"` string parsing

`datetime`, `date`, and `time` values are intentionally **not** coerced
in the engine — they pass through as JSON-friendly strings and are
materialized by the destination handler's Arrow-based schema contract
(`SchemaContract`, `cdk/cdk/schema_contract.py`), which preserves
precision. This is deliberate: coercing to native
Python `datetime` here would lose information during gRPC serialization.

## Versioning Strategy

End-user mappings need to remain stable for years. The contract is:

1. Functions are versioned (`name@version`).
2. Every `fn` AST node stores its `version`.
3. New behavior ships as a new version; the old version stays
   executable.
4. When deprecating a version, provide an automatic AST migration plus
   fixtures, and require fixtures to pass before the migrated mapping
   is enabled.

## Stream Fixtures (Recommended)

Fixtures detect schema/type drift and regressions when users edit
mappings:

```json
{
  "tests": [
    {
      "name": "maps basic transaction",
      "input": {
        "created": "2025-01-01T10:00:00Z",
        "targetValue": 12.34
      },
      "expect": {
        "valueDate": "2025-01-01",
        "amount": 12.34
      }
    }
  ]
}
```

Fixtures are not yet enforced by the runtime; treat them as authoring
discipline that pays off the moment an upstream payload changes.

## See Also

- [`source-config.md`](source-config.md) — stream file layout and
  source section
- [`destination-config.md`](destination-config.md) — destination side
- [`engine-architecture.md`](engine-architecture.md) — module map
- [`pyarrow-and-destinations.md`](pyarrow-and-destinations.md) — Arrow type system, schema contract
