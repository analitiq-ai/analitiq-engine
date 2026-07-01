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
pinned to it by the engine's conformance test. This package regenerates its data
from that artifact on every build, so it cannot drift from the engine.

## Install

Published to **GitHub Packages**, like every other `@analitiq-ai/*` package.
GitHub Packages requires authentication to install, so a consumer needs a
`.npmrc` with both the scope->registry mapping and a token (a `GITHUB_TOKEN` in
CI, or a personal access token with the `read:packages` scope locally):

```
@analitiq-ai:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

This is the same setup already used to consume `@analitiq-ai/contracts`. With it
in place:

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

getConversion("Nope", "Utf8");
// undefined  (unknown family)
```

`getConversion` is the safe accessor: it returns `undefined` for any family not
in the grid, including prototype names, so a caller can trust a truthy result.

- `mode`: `identity` | `auto` | `explicit` | `forbidden`
- `fn`: the mapping function an `explicit` conversion must declare (`null` otherwise)
- `runtime_checked`: a permitted conversion a per-row guard may still reject

The raw grid is also shipped as JSON:

```ts
import matrix from "@analitiq-ai/conversion-matrix/conversion_matrix.json" with { type: "json" };
```

## Publishing (maintainers)

The engine repo owns and publishes this package; consumers only pin it. CI
(`.github/workflows/conversion-matrix.yml`) runs the pipeline:

1. **Build** reproducibly from the committed lockfile (`npm ci`) and pinned
   TypeScript, regenerating the grid from the engine artifact.
2. **Verify** (every PR) that the packaged grid is byte-identical to the engine
   source.
3. **Publish** (push to `main`) only when the built tarball content changed:
   `scripts/publish-if-changed.mjs` hashes the exact `npm pack` file set and
   compares it to the digest recorded on the last published version, so an
   engine release that changes nothing we ship never cuts a new version.
4. **Version** auto-bumps the patch off the last published release; the package
   publishes scoped with `access: "public"`.

The published data always matches the engine commit that produced it.
