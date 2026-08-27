# The Consumption Manifest

A published, versioned statement of **which contract fields the engine's path
actually reads**, per resource. It exists so the repository that owns the
contract can fail its own build when it declares a field nothing consumes —
instead of learning years later that it shipped one.

## Why

Every guard either side operates pins a *value*: an enum set against the
models, the validator pin against the shipped package, the vendored arrow
grammar's sha256 against the published object. Not one asserts
**reachability**.

The cost of that gap, measured: `request.transport_ref` was declared in
api-endpoint `1.0.0`, survived structurally unchanged through `16.0.0`, was
rendered into every published JSON Schema, taught in authoring prose, written
into shipped connectors — and read by nothing, the whole time. Three more
surfaces of the same class came out of the same review
([#451](https://github.com/analitiq-ai/analitiq-engine/pull/451)). All four
failed *silently*: the request went out, just not the one the author wrote.

None of them was found by review. They were found by something finally
**executing** the contract instead of reading it.

## What the artifact says

One resource today, `api-endpoint`, rooted at
`analitiq.contracts.endpoints.ApiEndpointDoc`:

```json
{
  "version": "1.0.0",
  "contract_models": "1.0.0rc23",
  "resources": {
    "api-endpoint": {
      "root_model": "ApiEndpointDoc",
      "consumed": {
        "GetReadRequest": ["headers", "method", "path", "path_params", "query", "transport_ref"],
        "...": ["..."]
      }
    }
  }
}
```

Claims are **(model, field) pairs**, not dotted document paths. A `stop_when`
predicate nests into itself, so `operations.read.pagination.stop_when` alone
spells infinitely many paths and no finite list of them could be complete. A
field belongs to a model and the models are finite, so the pair is the only
spelling that closes. The name is the **wire** name — what the JSON Schema
renders and what an author writes (`$schema`, not `schema_url`).

`contract_models` states the published contract package the claims were read
against: a pair means nothing without the model tree it came from, and a
consumer pinned elsewhere has to be able to see that.

## How it is derived

Never written down. A hand-kept list would rot exactly like the fields it
exists to catch.

`tools/consumption_manifest/` drives the engine over a corpus:

1. each corpus case's document is validated through `ApiEndpointDoc` — a case
   the contract would reject could claim anything;
2. it is serialised by the engine's own `dump_endpoint_document`, with every
   node that corresponds to a contract model wrapped in a `dict` subclass that
   records the keys looked up on it;
3. `GenericAPIConnector` — the class a pipeline runs — reads and writes the
   document for real against a scripted session;
4. what it looked up is the claim; what it never looked at is not claimed.

Two rules keep the record honest:

- **Only engine frames count.** The conformance kit reads endpoint documents to
  build its probes; a field only the checker reads must not be claimed.
- **Only census fields count.** The resolver decides whether a node is an
  expression by asking it for `ref`/`template`/`literal`, so it probes keys on
  models that have no such field. Those lookups are real and mean nothing.

Reading a field **in order to refuse it** is still reading it.
`request.headers_remove` is contract-valid and the engine reads it only to
refuse the document — the connection's default headers live on a shared session
and no per-request instruction can delete one. That is a loud failure with the
author told, not the silent class this artifact exists to catch, so it is
claimed. A corpus case declares such an outcome with `refused`.

### The corpus is the other half of the claim

A field no corpus case declares is unclaimed for a completely different reason
than a field the engine ignored, and a consumer cannot tell the two apart. So
`tests/unit/contracts/test_consumption_manifest.py` asserts the corpus declares
**every** field in the census. A new contract field turns that test red until a
case declares it; only then can the manifest say whether anything reads it.

Regenerate with:

```shell
python -m tools.consumption_manifest
```

The pinning test fails when the committed bytes are not what the engine
produces today, so a change to what the path reads cannot land as a stale
artifact.

## Versioning and publication

`CONSUMPTION_MANIFEST_VERSION` lives beside the builder and is carried inside
the artifact, exactly as `GRAMMAR_VERSION` and `CONVERSION_MATRIX_VERSION` are.
Bump it in the same commit as any change to what the builder emits: minor when
the change is purely additive for a consumer (a new claim only relaxes the
gate), major when a claim is withdrawn or the shape changes. There is no patch
tier — a consumer either reads the same claims or it does not.

Publication is the established path (`.github/workflows/conversion-matrix.yml`,
`packages/conversion-matrix/scripts/sync-contracts-to-s3.mjs`): an immutable
`v{version}/consumption_manifest.json` object under the
`consumption-manifest/` prefix, plus a mutable `latest.json` naming the current
version and its sha256. The publisher reads the version from the bytes and
refuses to republish changed content under an already-published one;
`check-version-bump.mjs` moves that refusal into the pull request.

## What it does not say

- **Not every unclaimed field is a defect.** Some are read by the validator
  (`$schema`) or exist for authoring prose. Those are marked authoring-only on
  the contract side; the manifest states only what the engine reads, and the
  contract repository accounts for the remainder. An unclaimed, unmarked field
  is the failure.
- **It is per-resource.** `api-endpoint` is the pilot because the api path is
  where the class was found. A further resource is an entry in `RESOURCE_ROOTS`
  plus corpus cases — never a new mechanism.

The consuming half is
[analitiq-ai/claude-code-plugins#129](https://github.com/analitiq-ai/claude-code-plugins/issues/129).
