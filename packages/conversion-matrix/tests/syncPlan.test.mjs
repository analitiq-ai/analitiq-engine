// Tests for the pure decision core of scripts/sync-contracts-to-s3.mjs: planSync()
// and the absence classifier manifestAbsent(). The AWS-facing shell around them
// runs only in CI, and only once the S3 repository variables are configured;
// the version/skip decisions and the fail-loud paths are pinned here.
import { test } from "node:test";
import assert from "node:assert/strict";
import { manifestAbsent, planSync } from "../scripts/sync-contracts-to-s3.mjs";

const SHA_A = "a".repeat(64);
const SHA_B = "b".repeat(64);

const manifest = (fields) =>
  JSON.stringify({ version: "1.2.3", sha256: SHA_A, commit: "abc", ...fields });

test("no manifest yet publishes the version the artifact declares", () => {
  assert.deepEqual(planSync(null, SHA_A, "2.1.0"), { action: "publish", version: "2.1.0" });
});

test("matching sha256 skips", () => {
  assert.deepEqual(planSync(manifest({}), SHA_A, "1.2.3"), { action: "skip" });
});

test("changed sha256 publishes the declared version", () => {
  assert.deepEqual(planSync(manifest({}), SHA_B, "1.3.0"), { action: "publish", version: "1.3.0" });
});

test("a matching sha256 skips even with a malformed manifest version", () => {
  // The manifest version is only consulted to rule on a new one; an
  // up-to-date artifact never aborts over a field the decision does not use.
  assert.deepEqual(planSync(manifest({ version: "not-semver" }), SHA_A, "1.2.3"), {
    action: "skip",
  });
});

test("an artifact with no usable version aborts before anything else", () => {
  // The publisher reads this field and cannot assign one, so a malformed
  // artifact must never reach S3 — not even on a first publish.
  // "01.2.3" would normalise to 1.2.3 under a bare \\d+ and publish at a key
  // semver parsers reject; a non-string that stringifies to semver likewise.
  for (const declared of [
    undefined,
    "",
    "v1.2.3",
    "1.2",
    "1.2.3-rc1",
    123,
    "01.2.3",
    "1.02.3",
    "1.2.03",
    ["1.2.3"],
  ]) {
    assert.throws(
      () => planSync(null, SHA_A, declared),
      /artifact declares no usable version/,
      String(declared)
    );
  }
});

test("changed content at an unbumped version aborts instead of overwriting", () => {
  // The versioned object is immutable; republishing different bytes under the
  // published version would silently change what a pinned consumer resolves.
  assert.throws(() => planSync(manifest({}), SHA_B, "1.2.3"), /still 1\.2\.3/);
});

test("a declared version older than the published one aborts", () => {
  assert.throws(() => planSync(manifest({}), SHA_B, "1.2.2"), /older than the published/);
  assert.throws(() => planSync(manifest({}), SHA_B, "0.9.9"), /older than the published/);
});

test("version ordering compares parts numerically, not as text", () => {
  assert.deepEqual(planSync(manifest({ version: "1.9.0" }), SHA_B, "1.10.0"), {
    action: "publish",
    version: "1.10.0",
  });
  assert.throws(() => planSync(manifest({ version: "1.10.0" }), SHA_B, "1.9.0"), /older than/);
});

test("unparseable manifest aborts", () => {
  assert.throws(() => planSync("{not json", SHA_A, "1.2.3"), /not valid JSON/);
});

test("manifest that parses to a non-object aborts with context", () => {
  assert.throws(() => planSync("null", SHA_A, "1.2.3"), /not a JSON object/);
  assert.throws(() => planSync("42", SHA_A, "1.2.3"), /not a JSON object/);
  assert.throws(() => planSync("[]", SHA_A, "1.2.3"), /not a JSON object/);
});

test("manifest without a usable version aborts instead of guessing", () => {
  assert.throws(() => planSync(manifest({ version: "v1.2.3" }), SHA_B, "1.2.4"), /latest\.json on S3 has no usable version/);
  assert.throws(() => planSync(manifest({ version: undefined }), SHA_B, "1.2.4"), /latest\.json on S3 has no usable version/);
  assert.throws(() => planSync(JSON.stringify({ sha256: SHA_A }), SHA_B, "1.2.4"), /latest\.json on S3 has no usable version/);
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
  // bucket or a permissions problem read as "nothing published yet", which
  // skips the ordering check and overwrites the immutable object already at
  // the declared version.
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
