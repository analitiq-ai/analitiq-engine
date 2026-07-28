// Tests for the pure decision core of scripts/check-version-bump.mjs. The git
// shell around it only supplies the base branch's copy of the artifact.
import { test } from "node:test";
import assert from "node:assert/strict";
import { checkVersionBump } from "../scripts/check-version-bump.mjs";

const artifact = (version, body) => JSON.stringify({ version, families: body });

test("an unchanged artifact passes and reports its version", () => {
  const text = artifact("1.1.0", {});
  assert.deepEqual(checkVersionBump(text, text), { changed: false, version: "1.1.0" });
});

test("a bumped artifact passes and reports the new version", () => {
  assert.deepEqual(checkVersionBump(artifact("1.1.0", {}), artifact("1.2.0", { a: 1 })), {
    changed: true,
    version: "1.2.0",
  });
});

test("changed content at an unbumped version fails", () => {
  assert.throws(
    () => checkVersionBump(artifact("1.1.0", {}), artifact("1.1.0", { a: 1 })),
    /content changed but version is still 1\.1\.0/
  );
});

test("an artifact new on this branch has nothing to compare against", () => {
  assert.deepEqual(checkVersionBump(null, artifact("1.0.0", {})), {
    changed: false,
    version: "1.0.0",
  });
});

test("a base copy with no version counts as changed, not as an error", () => {
  // The base predates versioned artifacts, so anything this branch declares is
  // a change from it.
  const base = JSON.stringify({ families: {} });
  assert.deepEqual(checkVersionBump(base, artifact("1.1.0", {})), {
    changed: true,
    version: "1.1.0",
  });
});

test("an artifact with no usable version fails whether or not it changed", () => {
  for (const declared of [undefined, "", "v1.1.0", "1.1", "1.1.0-rc1"]) {
    const head = artifact(declared, {});
    assert.throws(() => checkVersionBump(head, head), /declares no usable version/, String(declared));
    assert.throws(
      () => checkVersionBump(artifact("1.0.0", {}), head),
      /declares no usable version/,
      String(declared)
    );
  }
});
