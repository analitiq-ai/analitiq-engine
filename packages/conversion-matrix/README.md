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

A **private** package on **GitHub Packages**, like every other `@analitiq-ai/*`
package (the `analitiq-ai` org does not allow public packages). A consumer needs
read access to the package (granted per repo/team, the same way
`@analitiq-ai/contracts` is) and a `.npmrc` with the scope->registry mapping and
a token (a `GITHUB_TOKEN` in CI, or a personal access token with the
`read:packages` scope locally):

```
@analitiq-ai:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

With that in place:

```
npm install @analitiq-ai/conversion-matrix
```

## Usage

```ts
import { conversionMatrix, getConversion, arrowFamilies } from "@analitiq-ai/conversion-matrix";

getConversion("Int64", "Utf8");
// { mode: "explicit", fn: "to_string", runtime_checked: false }

getConversion("Int32", "Int64");
// { mode: "auto", fn: null, runtime_checked: true }

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

## Versioned JSON on S3

For consumers that cannot (or should not) pull a private npm package, the raw
grid is also published to S3 as versioned JSON, under a bucket/prefix
configured outside this repo:

```
{prefix}/v{version}/conversion_matrix.json   immutable, one object per grid version
{prefix}/latest.json                         {version, sha256, commit, publishedAt}
```

Pin a version by fetching its immutable object; discover the current one via
`latest.json`. Versions here are **independent of the npm package version**:
the npm digest covers the shipped TS helpers too, while an S3 version is cut
only when the grid content itself changes.

CI publishes with short-lived GitHub OIDC credentials (`sync-s3` job in
`.github/workflows/conversion-matrix.yml`). The role ARN, region, bucket, and
prefix come from the variables `CONVERSION_MATRIX_S3_ROLE_ARN`,
`CONVERSION_MATRIX_S3_REGION`, `CONVERSION_MATRIX_S3_BUCKET`, and
`CONVERSION_MATRIX_S3_PREFIX` (default `conversion-matrix`). All four must be
**repository-scoped** variables — an environment-scoped variable is invisible
to the job's gate and would skip the sync silently. The job runs once the
role ARN is set; the remaining variables are then required and fail loud when
missing. The assumed role needs `s3:GetObject` and `s3:PutObject` on the
prefix and `s3:ListBucket` on the bucket (so a missing manifest reads as
absence rather than Forbidden); it needs no delete permissions. The sync
reconciles against `latest.json` (sha256 compare, patch-bump on change,
manifest written last as the commit point), so re-runs and partial failures
converge without cutting spurious versions.

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
4. **Version** auto-bumps the patch off the last published release. The package
   is private on GitHub Packages (the `analitiq-ai` org does not allow public
   packages), like `@analitiq-ai/contracts`.

The published data always matches the engine commit that produced it.

> **One-time setup:** the first publish creates the package private. So the
> frontend can install it, an admin grants the consuming repo(s) read access to
> the package once (package -> Package settings -> Manage Actions access -> add
> repo with Read), the same grant `@analitiq-ai/contracts` uses. The workflow's
> `GITHUB_TOKEN` publishes new versions but cannot grant this access.
