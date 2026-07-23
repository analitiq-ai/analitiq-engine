// Tests for planSync() — the pure decision core of scripts/sync-matrix-to-s3.mjs.
// The AWS-facing shell around it is exercised only in CI; the version/skip
// decisions and the fail-loud paths for corrupt manifests are pinned here.
import { test } from "node:test";
import assert from "node:assert/strict";
import { planSync } from "../scripts/sync-matrix-to-s3.mjs";

const SHA_A = "a".repeat(64);
const SHA_B = "b".repeat(64);

const manifest = (fields) =>
  JSON.stringify({ version: "1.2.3", sha256: SHA_A, commit: "abc", ...fields });

test("no manifest yet publishes 1.0.0", () => {
  assert.deepEqual(planSync(null, SHA_A), { action: "publish", version: "1.0.0" });
});

test("matching sha256 skips", () => {
  assert.deepEqual(planSync(manifest({}), SHA_A), { action: "skip" });
});

test("changed sha256 patch-bumps the manifest version", () => {
  assert.deepEqual(planSync(manifest({}), SHA_B), { action: "publish", version: "1.2.4" });
});

test("a matching sha256 skips even with a malformed version", () => {
  // The version is only needed to cut a new one; an up-to-date grid never
  // aborts the publish job over a field it does not use.
  assert.deepEqual(planSync(manifest({ version: "not-semver" }), SHA_A), { action: "skip" });
});

test("unparseable manifest aborts", () => {
  assert.throws(() => planSync("{not json", SHA_A), /not valid JSON/);
});

test("manifest without a usable version aborts instead of guessing", () => {
  assert.throws(() => planSync(manifest({ version: "v1.2.3" }), SHA_B), /no usable version/);
  assert.throws(() => planSync(manifest({ version: undefined }), SHA_B), /no usable version/);
  assert.throws(() => planSync(JSON.stringify({ sha256: SHA_A }), SHA_B), /no usable version/);
});
