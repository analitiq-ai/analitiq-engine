// Tests for the pure decision core of scripts/sync-matrix-to-s3.mjs: planSync()
// and the absence classifier manifestAbsent(). The AWS-facing shell around them
// runs only in CI, and only once the S3 repository variables are configured;
// the version/skip decisions and the fail-loud paths are pinned here.
import { test } from "node:test";
import assert from "node:assert/strict";
import { manifestAbsent, planSync } from "../scripts/sync-matrix-to-s3.mjs";

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
  // aborts the sync job over a field it does not use.
  assert.deepEqual(planSync(manifest({ version: "not-semver" }), SHA_A), { action: "skip" });
});

test("unparseable manifest aborts", () => {
  assert.throws(() => planSync("{not json", SHA_A), /not valid JSON/);
});

test("manifest that parses to a non-object aborts with context", () => {
  assert.throws(() => planSync("null", SHA_A), /not a JSON object/);
  assert.throws(() => planSync("42", SHA_A), /not a JSON object/);
  assert.throws(() => planSync("[]", SHA_A), /not a JSON object/);
});

test("manifest without a usable version aborts instead of guessing", () => {
  assert.throws(() => planSync(manifest({ version: "v1.2.3" }), SHA_B), /no usable version/);
  assert.throws(() => planSync(manifest({ version: undefined }), SHA_B), /no usable version/);
  assert.throws(() => planSync(JSON.stringify({ sha256: SHA_A }), SHA_B), /no usable version/);
});

test("only NoSuchKey classifies as an absent manifest", () => {
  // GetObject on a missing key (role has s3:ListBucket) — the one absence case.
  assert.equal(
    manifestAbsent(
      "An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist."
    ),
    true
  );
});

test("every other AWS failure re-throws instead of reading as first publish", () => {
  // A broadened match here (e.g. any "404" or "error") would let a mistyped
  // bucket or a permissions problem reset published versioning to 1.0.0.
  for (const output of [
    "An error occurred (NoSuchBucket) when calling the GetObject operation: The specified bucket does not exist",
    "An error occurred (404) when calling the HeadObject operation: Not Found",
    "An error occurred (403) when calling the GetObject operation: Forbidden",
    "An error occurred (AccessDenied) when calling the GetObject operation: Access Denied",
    "An error occurred (ExpiredToken) when calling the GetObject operation: The provided token has expired.",
    "Could not connect to the endpoint URL",
    "",
  ]) {
    assert.equal(manifestAbsent(output), false, output || "(empty output)");
  }
});
