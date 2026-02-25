# Field Mapping, Transformations & Validation Guide (Aligned Model)

This guide documents the **assignment-based mapping system** used in Analitiq Stream for low-code SaaS integrations. It consolidates **field mapping, transformations, and validation** into a single, schema-aware structure that is safe for end users to edit.

---

## Overview

This approach provides:

- **Unified configuration**: every target field is built by a single rule (“assignment”).
- **UI-friendly editing**: tokenized paths, typed targets, safe expression AST (no code strings).
- **Schema-aware validation**: field pickers, type checks, required-field enforcement.
- **Stable behavior over time**: versioned transforms/functions and explicit policies.

---

## Core Concepts

### 1) Assignments (field-centric mapping)

A mapping is an **ordered list of assignments**, each producing exactly one target field/path in the destination payload.

Conceptually:

```
Target Field (path + type) ← Value (const or expression AST) ← Optional validation/error handling
```

### 2) Expressions are structured AST (JSON), not strings

End users should not edit raw script strings. Expressions are represented as a constrained AST with a small set of `op` nodes (e.g., `get`, `fn`, `pipe`, `if`).

### 3) Schemas are explicit (or pinned via endpoint versioning)

- If your `endpoint_id` is versioned and immutable, it effectively pins schema.
- Otherwise, include `source_schema_id` and `target_schema_id` to drive the UI and validation.

### 4) Transforms are catalog functions (typed + versioned)

Transformations are implemented via `op: fn` nodes referencing a **function catalog** with strict typing, parameter metadata, and versioning.

---

## Configuration Structure

### Stream mapping structure (recommended)

```json
{
  "mapping": {
    "source_schema_id": "wise.transactions.v3",
    "target_schema_id": "sevdesk.checkAccountTransaction.v2",
    "assignments": [
      {
        "target": { "path": ["valueDate"], "type": "date", "nullable": false },
        "value": {
          "kind": "expr",
          "expr": {
            "op": "pipe",
            "args": [
              { "op": "get", "path": ["created"] },
              { "op": "fn", "name": "iso_to_date", "version": 1, "args": [] }
            ]
          }
        },
        "validate": {
          "rules": [{ "type": "not_null" }],
          "on_error": "dlq"
        }
      }
    ],
    "defaults": {
      "on_error": "dlq"
    }
  }
}
```

Key points:
- **No `field_mappings` vs `computed_fields` split**. Constants and computed values are both assignments.
- `target.path` uses **token arrays** (safe for nesting).
- `value.kind` is typically `const` or `expr`.

---

## Assignments

### A) Direct mapping (no transforms)

Use `get` to copy a source field to a target field:

```json
{
  "target": { "path": ["amount"], "type": "decimal", "nullable": false },
  "value": { "kind": "expr", "expr": { "op": "get", "path": ["targetValue"] } }
}
```

### B) Constant fields

Set a constant (replaces the need for a separate `computed_fields` section):

```json
{
  "target": { "path": ["status"], "type": "string", "nullable": false },
  "value": { "kind": "const", "const": { "type": "string", "value": "100" } }
}
```

### C) Nested object constants

Avoid JSON-in-strings; use typed object constants:

```json
{
  "target": { "path": ["checkAccount"], "type": "object", "nullable": false },
  "value": {
    "kind": "const",
    "const": {
      "type": "object",
      "value": { "id": "5936402", "objectName": "CheckAccount" }
    }
  }
}
```

### D) Transform chains (via `pipe` + `fn`)

Example: trim + lowercase:

```json
{
  "target": { "path": ["email"], "type": "string", "nullable": true },
  "value": {
    "kind": "expr",
    "expr": {
      "op": "pipe",
      "args": [
        { "op": "get", "path": ["email"] },
        { "op": "fn", "name": "trim", "version": 1, "args": [] },
        { "op": "fn", "name": "lower", "version": 1, "args": [] }
      ]
    }
  }
}
```

### E) Conditional mapping (optional)

Example: map `is_active` to a status:

```json
{
  "target": { "path": ["status"], "type": "string", "nullable": false },
  "value": {
    "kind": "expr",
    "expr": {
      "op": "if",
      "args": [
        {
          "op": "eq",
          "args": [
            { "op": "get", "path": ["is_active"] },
            { "op": "const", "value": true }
          ]
        },
        { "op": "const", "value": "active" },
        { "op": "const", "value": "inactive" }
      ]
    }
  }
}
```

---

## Validation Rules & Error Handling

### Validation block (per assignment)

```json
"validate": {
  "rules": [{ "type": "not_null" }],
  "on_error": "dlq"
}
```

### Recommended error actions (normalized)

- `dlq` — send record to dead-letter processing/storage
- `skip_record` — drop the record and continue
- `stop_stream` — fail the stream run immediately
- `default_value` — substitute a configured fallback value
- `quarantine` — store record for review and continue

### Defaults + overrides

- Pipeline default: `engine_config.error_handling.default_action`
- Stream override: `runtime_overrides.error_handling.default_action`
- Field override: `validate.on_error` (or `on_error`)

---

## Execution / Runtime Policies (where they belong)

Execution concerns should live under a dedicated section (pipeline defaults, stream overrides), for example:

- retry/backoff
- rate limits
- DLQ/quarantine configuration
- expression evaluation limits (`max_ops`, `max_depth`, `max_string_len`)
- batching defaults
- concurrency

This keeps mapping logic purely declarative and UI-editable.

---

## Function Catalog (Transforms as First-Class, Parameterized)

The UI should render transforms from a catalog with:

- `name`, `label`, `description`
- `input_types`, `output_type`
- `params` with metadata (types, defaults, picklists)
- `version`

Example catalog entry:

```json
{
  "name": "iso_to_date",
  "label": "ISO Timestamp → Date",
  "description": "Converts an ISO-8601 timestamp into YYYY-MM-DD.",
  "version": 1,
  "input_types": ["string", "datetime"],
  "output_type": "date",
  "params": []
}
```

### Minimum function catalog for most integrations

**Data access**
- `get(path)` (often represented as an `op`, not a function)
- `coalesce(a,b,...)`
- `default(value, fallback)`

**Text**
- `concat`
- `trim`, `lower`, `upper`
- `replace` (restricted)
- `substring`
- `format` (limited templates)

**Numbers / money**
- `to_number`, `round`, `abs`
- `multiply`, `divide`
- `currency_convert` (optional; requires service)

**Dates**
- `parse_date`, `format_date`
- `add_days`, `start_of_day`
- `to_timezone`

**Logic**
- `if(cond, a, b)`
- comparisons: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`
- boolean: `and`, `or`, `not`
- set membership: `in(list)`

---

## Versioning Strategy (Critical for Low-Code)

End-user mappings must remain stable for years.

### 1) Version functions/transforms

Example: `iso_to_date@1` vs `iso_to_date@2`

### 2) Store the function version in each AST node

```json
{ "op": "fn", "name": "iso_to_date", "version": 1, "args": [] }
```

### 3) Deprecation and migrations

When deprecating:
- keep old versions executable for a period
- provide automatic migration (rewrite AST nodes from v1 → v2)
- surface migration notes and require fixtures to pass before enabling the updated mapping

---

## Stream Tests / Fixtures (Recommended)

Fixtures are critical because they:
- catch schema/type drift early
- reduce regressions when users edit mappings
- make failures actionable (exact input + expected output)
- enable CI-style validation when connectors or runtimes change

Recommended structure:

```json
{
  "tests": [
    {
      "name": "maps basic transaction",
      "input": {
        "created": "2025-01-01T10:00:00Z",
        "targetValue": 12.34,
        "details": { "reference": "X" }
      },
      "expect": {
        "valueDate": "2025-01-01",
        "amount": 12.34,
        "paymtPurpose": "X"
      }
    }
  ]
}
```

---

## Recommended Best Practices (Checklist)

1) Prefer **assignments** over split `field_mappings` + `computed_fields`.
2) Keep expressions as **AST**, not strings.
3) Use **tokenized paths** for nested fields.
4) Make schemas explicit (or pin via versioned `endpoint_id`) for UI + validation.
5) Use a **typed, versioned function catalog**; reference versions in AST nodes.
6) Centralize execution concerns under `engine_config`, with stream/field overrides.
7) Normalize error actions: `dlq`, `skip_record`, `stop_stream`, `default_value`, `quarantine`.
8) Add fixtures/tests to catch regressions and speed debugging.  
