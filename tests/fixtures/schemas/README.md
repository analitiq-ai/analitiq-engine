# Vendored published JSON Schemas

Byte-for-byte snapshots of the published Analitiq contract schemas from
`https://schemas.analitiq.ai/{kind}/latest.json`, used by tests that point
`ANALITIQ_SCHEMA_BASE_URL` at this directory (as a `file://` URL) so real
contract validation runs offline and deterministically.

The umbrella `endpoint` kind is intentionally absent: it is not published
(the live URL returns 403) and nothing validates against it — the engine
dispatches to `api-endpoint` / `database-endpoint` by the document's
`$schema` URL.

Refresh with:

```shell
for kind in connector connection pipeline stream api-endpoint database-endpoint; do
  curl -fsS "https://schemas.analitiq.ai/$kind/latest.json" \
    -o "tests/fixtures/schemas/$kind/latest.json"
done
```
