// Every ARTIFACTS entry must point at a real, git-tracked, valid-JSON file.
// A stale entry (renamed source, or a generated file swallowed by the blanket
// *.json gitignore) passes every other test and fails only in the post-merge
// sync job — this is the pre-merge net for exactly that.
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { ARTIFACTS, CHANNELS, artifactsFor, parseVersion } from "../scripts/sync-contracts-to-s3.mjs";
import { matrixVersion } from "../dist/index.js";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

for (const { prefix, path } of ARTIFACTS) {
  const sourcePath = join(repoRoot, path);

  test(`${prefix}: source file is valid JSON`, () => {
    assert.doesNotThrow(() => JSON.parse(readFileSync(sourcePath, "utf8")));
  });

  test(`${prefix}: source file declares a plain-semver version`, () => {
    // The publisher reads this field instead of assigning one, so an artifact
    // that lost it cannot be published at all.
    const { version } = JSON.parse(readFileSync(sourcePath, "utf8"));
    assert.notEqual(parseVersion(version), null, `version: ${JSON.stringify(version)}`);
  });

  test(`${prefix}: source file is tracked by git`, () => {
    // ls-files --error-unmatch exits non-zero for untracked paths, catching a
    // generated artifact that exists locally but never made it into a commit.
    assert.doesNotThrow(() =>
      execFileSync("git", ["ls-files", "--error-unmatch", sourcePath], {
        cwd: repoRoot,
        stdio: "ignore",
      })
    );
  });
}

test("every artifact publishes through exactly one known channel", () => {
  // A channel no workflow invokes is an artifact nothing ever publishes; the
  // two channels here are the two workflows that call the sync script.
  assert.deepEqual([...CHANNELS].sort(), ["cdk-release", "main"]);
  assert.deepEqual(
    CHANNELS.flatMap((channel) => artifactsFor(channel).map((a) => a.prefix)).sort(),
    ARTIFACTS.map((a) => a.prefix).sort()
  );
  assert.throws(() => artifactsFor("nightly"), /unknown channel "nightly"/);
});

test("the built package reports the engine artifact's own version", () => {
  // matrixVersion is public API and documented as equal to the artifact's
  // version; nothing else compares the built value against the source.
  const [matrix] = ARTIFACTS.filter((a) => a.prefix === "conversion-matrix");
  const { version } = JSON.parse(readFileSync(join(repoRoot, matrix.path), "utf8"));
  assert.equal(matrixVersion, version);
});
