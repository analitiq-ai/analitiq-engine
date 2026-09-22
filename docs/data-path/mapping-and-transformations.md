# Field Mapping, Transformations & Validation

**Scope:** this doc owns the assignment syntax, the path grammar, the
expression AST, the validation rules, the function catalog, and the
conversion matrix. For Arrow type-system and schema-contract internals see
[`arrow-and-transport-strategy.md`](arrow-and-transport-strategy.md).

Streams declare their record-shape transformation under `mapping` in
`pipelines/{id}/streams/{stream_id}.json`. The implementation lives in
`src/engine/mapping.py`: the contract's validated `StreamMapping` is
compiled once by `compile_mapping` into vectorized `pyarrow.compute`, and
applied to each Arrow batch.

## Overview

Each target field is built by exactly one **assignment**:

```
Target Field (path + arrow_type)  ←  Value (constant | expression AST)  ←  Optional validation
```

Expressions are a **structured AST** (JSON), not source strings. End
users edit assignments through a UI; the engine compiles the AST once per
stream and evaluates it as vectorized Arrow compute over each batch -- the
data never leaves Arrow.

The mapping document is validated by the published contract when the stream
is loaded; the engine compiles exactly the expression forms and conversion
functions that contract declares. That match is checked when the engine
starts, so a contract release that adds a form or a function fails the engine
at startup rather than on the first batch that carries it.

## Paths Are Token Arrays

A source path is an ordered array of field-name tokens, outermost first, and
nothing splits a string anywhere along the route from the document to the
batch read:

- `["address", "city"]` reads `city` nested under `address`.
- `["address.city"]` reads one top-level field whose name contains a dot.

A **target** path is different: it is a single segment naming one field on the
destination record root. Nesting under that field is declared with
`arrow_type: "Object"` plus `properties` (or `"List"` plus `items`), which is
also what builds the Arrow struct.

## Stream `mapping` Shape

```json
{
  "mapping": {
    "assignments": [
      {
        "value": {
          "kind": "expression",
          "expression": { "op": "get", "path": ["created"] }
        },
        "target": {
          "path": "created",
          "arrow_type": "Timestamp(MICROSECOND, UTC)",
          "nullable": true
        }
      }
    ]
  }
}
```

`assignments[]` is an ordered list, evaluated top to bottom. Each assignment
compiles to a closure typed `Callable[[pa.RecordBatch], pa.Array]`, so the
only input any assignment can read is the **source batch** — no in-progress
result is threaded through evaluation at all, and earlier assignments are
therefore **not** visible to later ones. Treat every assignment as a pure
function of the source record.

## Assignments

`value.kind` is required and discriminates the two value shapes; there is no
default.

### Direct field copy

```json
{
  "value": {
    "kind": "expression",
    "expression": { "op": "get", "path": ["targetValue"] }
  },
  "target": { "path": "amount", "arrow_type": "Decimal128(12, 2)", "nullable": false }
}
```

### Constant

```json
{
  "value": {
    "kind": "constant",
    "constant": { "arrow_type": "Utf8", "value": "100" }
  },
  "target": { "path": "status", "arrow_type": "Utf8", "nullable": false }
}
```

The column is built at the **target**'s `arrow_type`; the constant's own
`arrow_type` declares the literal's JSON kind for authoring tools.

### Nested object constant

```json
{
  "value": {
    "kind": "constant",
    "constant": {
      "arrow_type": "Object",
      "value": { "id": "5936402", "objectName": "CheckAccount" },
      "properties": {
        "id": { "arrow_type": "Utf8" },
        "objectName": { "arrow_type": "Utf8" }
      }
    }
  },
  "target": {
    "path": "checkAccount",
    "arrow_type": "Object",
    "nullable": false,
    "properties": {
      "id": { "arrow_type": "Utf8" },
      "objectName": { "arrow_type": "Utf8" }
    }
  }
}
```

### Conversion pipe

```json
{
  "value": {
    "kind": "expression",
    "expression": {
      "op": "pipe",
      "args": [
        { "op": "get", "path": ["id"] },
        { "op": "fn", "name": "to_string" }
      ]
    }
  },
  "target": { "path": "reference", "arrow_type": "Utf8", "nullable": false }
}
```

## Expressions

The contract publishes two expression forms:

| `op` | Description |
|------|-------------|
| `get` | Read from the source record at `path` (token array) |
| `pipe` | `args[0]` is a `get` seed; every later arg is an `fn` stage applied left-to-right |

An `fn` stage names a conversion function. The contract closes the names
over the functions the conversion matrix declares for `explicit`
conversions. A failure while evaluating an expression fails the entire
batch and surfaces as a transform-stage stream failure.

### Vectorized evaluation: known divergences

The transform is a single Arrow-native path (`compile_mapping`); each stage is
a `pyarrow.compute` kernel applied to a whole column. A few differences from
Python's per-value behaviour are inherent to vectorized evaluation and are
accepted:

- **`to_string` of a temporal** uses Arrow's ISO formatting, which can differ in
  notation/precision from Python's `str(datetime)`.
- **Validation `pattern`** runs on Arrow's RE2 engine (anchored `^(?:...)`),
  which supports standard regex but not Python-only features such as lookaround.

## Function Catalog

| Name | Purpose |
|------|---------|
| `to_string` | Format a value as a string; booleans render as `True`/`False` |

## Validation

```json
"validate": {
  "rules": [
    { "type": "not_null", "field": ["id"], "message": "id must be present" },
    { "type": "min_length", "field": ["address", "city"], "value": 1 }
  ]
}
```

A rule's `field` is an ordered token array addressing the mapped output it
guards. The first token names any `target.path` the mapping declares — rules
are authored per assignment but grade the record the assignments build
together — and each later token names a field declared under that target's
`properties`, descending through `items` for a `List` (a row fails when any
of its list elements does). The contract refuses a token that resolves to
nothing (RULE-STRM-015); a token is one field name, so a `.` inside it is part
of that name, never nesting.

Implemented rule types, each compiled to a vectorized boolean mask over the
batch. A null value is exempt from every rule except `not_null`:

| `type` | `value` | Notes |
|--------|---------|-------|
| `not_null` (alias `required`) | omitted | Fails where the value is null |
| `min_length` | number | Unicode length of the value as a string |
| `max_length` | number | Same |
| `pattern` | string | Anchored regex match (`^(?:pattern)`) against the value as a string |
| `range` | object with `min` and/or `max` | Numeric comparison |
| `in_list` | array | Value must be in the supplied list |

Validation is **batch-wide**: if any row fails any rule, the whole batch is
rejected with a `ValidationFailure` naming the column and the offending rows.
The transform does not route individual records. What the stream does with the
rejected batch is decided by the failed rule's **effective strategy**: the
assignment's `validate.error_handling.strategy` when declared, else the
pipeline's `runtime.error_handling.strategy`. `fail` stops the stream, `dlq`
dead-letters the source rows and continues, `skip` drops them and continues.
When rules under different strategies fail on the same batch, the strictest
one wins (`fail` over `dlq` over `skip`). The batch never reaches the
destination (see [`engine-architecture.md`](../architecture/engine-architecture.md)).

The override's `max_retries` and `retry_delay_seconds` are not read: a rule is
a pure function of the batch, so a retry would fail the same rows the same way.

A mapping defect on the batch — an expression that cannot be evaluated, a
rejected conversion, a null in a non-nullable column — fails the stream with a
`TransformationError` whatever the strategies say.

## Type Conversion

A field's `target.arrow_type` is the type the engine builds the post-transform
column to. Whether a given `source arrow_type → target arrow_type` conversion is
permitted is decided by a single declarative policy — the **conversion matrix**
(`cdk/cdk/type_map/conversions.py`) — consulted identically at every build
boundary (the transform retype and the destination
`SchemaContract.cast_arrow_batch`), so a conversion can never be accepted on one
boundary and rejected on another. Each pair resolves to one mode:

- `identity` — same type, passthrough.
- `auto` — lossless, applied implicitly: a width widening (`Int32 → Int64`),
  numeric interconversion (`Int64 → Float64`), and parsing the JSON-string
  numbers an API source ships (`"1" → Int64`, `"1.5" → Float64`), which both
  build paths already perform.
- `explicit` — permitted only with a declared conversion function. Formatting a
  scalar as a string (`Int64 → Utf8`, `Boolean → Utf8`, `Float64 → Utf8`,
  `Timestamp → Utf8`) is a notation choice, not a free widening, so the mapping
  must wire `to_string`. A boundary that still sees the raw scalar fails loud,
  naming the function — the destination no longer silently stringifies an int.
  This gate applies to a scalar leaf *inside* a nested (`Object`/`List`) target
  too: an `Int64 → Utf8` struct leaf fails loud, not a silent per-child cast.
- `forbidden` — never permitted: nested and `Json` conversions (`Object → Int64`)
  and every cross-`conversion_kind` pair outside the stable-cast allowlist
  (`Binary → Int64`, `Duration → Date32`, `Utf8 → Date32`). The published
  grid lists only casts the runtime performs identically on every supported
  pyarrow version, so it never promises a conversion that cannot run.

`runtime_checked` marks a permitted conversion a per-row guard may still reject
(a narrowing that overflows, a string that will not parse); the build runs with
`safe=True` so a bad row fails loud rather than truncating.

The same policy is published as a generated artifact
(`cdk/cdk/type_map/conversion_matrix.json`, built from the `ARROW_FAMILIES`
table by `build_conversion_matrix()`) so the mapping authoring UI offers
exactly the conversions the engine accepts and auto-wires the function an `explicit`
conversion needs. The artifact states its own version in a top-level `version` field, filled from
`CONVERSION_MATRIX_VERSION`, so a consumer holding the bytes can name the policy
it got; the publisher reads that field rather than assigning a version. The frontend consumes it as the `@analitiq-ai/conversion-matrix`
npm package (`packages/conversion-matrix/`), which regenerates from that artifact
on every build and is republished to GitHub Packages whenever the grid changes.

`datetime`, `date`, and `time` columns are built and carried as typed Arrow
values, materialized by the destination's Arrow schema contract
(`SchemaContract`, `cdk/cdk/schema_contract.py`), which preserves precision
across the gRPC boundary.

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

- [`source-config.md`](../config/source-config.md) — stream file layout and
  source section
- [`destination-config.md`](../config/destination-config.md) — destination side
- [`engine-architecture.md`](../architecture/engine-architecture.md) — module map
- [`arrow-and-transport-strategy.md`](arrow-and-transport-strategy.md) — Arrow type system, schema contract
