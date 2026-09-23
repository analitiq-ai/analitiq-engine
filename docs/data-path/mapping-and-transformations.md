# Field Mapping, Transformations & Validation

**Scope:** this doc owns how the engine compiles and applies a stream's
mapping at runtime, and the conversion matrix. For Arrow type-system and
schema-contract internals see
[`arrow-and-transport-strategy.md`](arrow-and-transport-strategy.md).

The implementation lives in `src/engine/mapping.py`: `compile_mapping` compiles
the stream's contract `StreamMapping` once into vectorized `pyarrow.compute`,
which is applied to each Arrow batch. The engine compiles exactly the
expression forms and conversion functions the contract declares; that match
is checked at startup, so a contract release that adds one fails the engine
at startup rather than on the first batch that carries it.

## Overview

Each target field is built by exactly one **assignment**:

```
Target Field (path + arrow_type)  ←  Value (constant | expression)  ←  Optional validation
```

The engine compiles each expression once per stream and evaluates it as
vectorized Arrow compute over each batch -- the data never leaves Arrow.

## Source Paths

Nothing splits a source path token on a dot anywhere along the route from the
document to the batch read:

- `["address", "city"]` reads `city` nested under `address`.
- `["address.city"]` reads one top-level field whose name contains a dot.

## Assignments

Assignments are evaluated top to bottom. Each assignment compiles to a closure
typed `Callable[[pa.RecordBatch], pa.Array]`, so the only input any assignment
can read is the **source batch** — no in-progress result is threaded through
evaluation at all, and earlier assignments are therefore **not** visible to
later ones. Every assignment is a pure function of the source record.

A column is built at the **target**'s `arrow_type`.

## Expression Evaluation

A single expression error fails the entire batch and surfaces as a
transform-stage stream failure.

### Vectorized evaluation: known divergences

The transform is a single Arrow-native path (`compile_mapping`); each op is a
`pyarrow.compute` kernel applied to a whole column. A few differences are
inherent to typed, vectorized evaluation:

- **Typed intermediates.** Every sub-expression produces a typed Arrow column,
  so a value cannot change type mid-expression the way an untyped Python value
  could.
- **`to_string` of a temporal** uses Arrow's ISO formatting, which can differ in
  notation/precision from Python's `str(datetime)`.
- **Validation `pattern`** runs on Arrow's RE2 engine (anchored `^(?:...)`),
  which supports standard regex but not Python-only features such as lookaround.

## Function Catalog

The engine ships one function kernel, `to_string`.

## Validation

A rule on a field under a `List` fails the row when any of its list elements
fails.

Each rule type compiles to a vectorized boolean mask over the batch. A null
value is exempt from every rule except `not_null` and `required`:

| `type` | Semantics |
|--------|-----------|
| `not_null` (alias `required`) | Fails where the value is null |
| `min_length` | Unicode length of the value as a string |
| `max_length` | Same |
| `pattern` | Anchored regex match (`^(?:pattern)`) against the value as a string |
| `range` | Numeric comparison |
| `in_list` | Value must be in the supplied list |

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
  naming the function.
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

## See Also

- [`destination-config.md`](../config/destination-config.md) — destination side
- [`engine-architecture.md`](../architecture/engine-architecture.md) — module map
- [`arrow-and-transport-strategy.md`](arrow-and-transport-strategy.md) — Arrow type system, schema contract
