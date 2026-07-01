# @analitiq-ai/conversion-matrix

The Analitiq engine's Arrow type-conversion policy grid, published as data.

The grid says, for every `source -> target` arrow_type family pair, whether the
engine permits the conversion and which mapping function an `explicit`
conversion requires. The mapping authoring UI reads it to offer exactly the
conversions the engine accepts and to auto-wire the function an `explicit`
conversion needs.

## Source of truth

The grid is generated from `cdk/cdk/type_map/conversion_matrix.json` in the
[engine repo](https://github.com/analitiq-ai/analitiq-engine), which is itself
generated from the canonical policy in `cdk/cdk/type_map/conversions.py` and
pinned to it by the engine's conformance test. This package regenerates its
data on every build, so it cannot drift from the engine. CI publishes a new
version only when the grid content changes.

## Install

Published to GitHub Packages. Point the `@analitiq-ai` scope at the registry
(e.g. in `.npmrc`):

```
@analitiq-ai:registry=https://npm.pkg.github.com
```

```
npm install @analitiq-ai/conversion-matrix
```

## Usage

```ts
import { conversionMatrix, getConversion, arrowFamilies } from "@analitiq-ai/conversion-matrix";

getConversion("Int64", "Utf8");
// { mode: "explicit", fn: "to_string", runtime_checked: false }

getConversion("Int32", "Int64");
// { mode: "auto", fn: null, runtime_checked: false }
```

- `mode`: `identity` | `auto` | `explicit` | `forbidden`
- `fn`: the mapping function an `explicit` conversion must declare (`null` otherwise)
- `runtime_checked`: a permitted conversion a per-row guard may still reject

The raw grid is also shipped as JSON:

```ts
import matrix from "@analitiq-ai/conversion-matrix/conversion_matrix.json" with { type: "json" };
```
