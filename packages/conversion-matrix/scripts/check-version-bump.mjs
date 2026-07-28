// Pre-merge net: an artifact whose bytes changed on this branch must also
// carry a changed `version`.
//
// The publisher (sync-contracts-to-s3.mjs) refuses to republish changed
// content under an already-published version, but it runs post-merge — an
// unbumped change would land on main and strand the artifact unpublished.
// This check moves the same failure into the pull request.
//
// It asserts the version *changed*, not that it increased: the base branch is
// not the published history (a bucket can be behind main, or ahead of it after
// a revert), so only the publisher, holding the manifest, can rule on
// ordering. Here the question is whether the author bumped at all.

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { ARTIFACTS, parseVersion } from "./sync-contracts-to-s3.mjs";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
// Repo-relative, forward-slashed: this is a git pathspec, not a filesystem path.
const typeMapPath = "cdk/cdk/type_map";

/**
 * Verdict for one artifact, from the base branch's copy (raw text, or null
 * when the branch adds the artifact) and this branch's copy.
 *
 * Returns {changed, version}. Throws when this branch's artifact declares no
 * plain-semver version, or when its bytes moved while its version did not.
 */
export function checkVersionBump(baseText, headText) {
  const version = JSON.parse(headText).version;
  if (parseVersion(version) === null) {
    throw new Error(
      `declares no usable version (got ${JSON.stringify(version)}); the ` +
        `publisher reads this field and cannot assign one`
    );
  }
  if (baseText === null || baseText === headText) return { changed: false, version };
  // A malformed or absent base version compares unequal, which is the right
  // answer: anything this branch declares is a change from it.
  if (JSON.parse(baseText).version === version) {
    throw new Error(
      `content changed but version is still ${version}; bump the version ` +
        `constant in the CDK and regenerate the artifact`
    );
  }
  return { changed: true, version };
}

/** The artifact as of *ref*, or null when *ref* does not have that path. */
function readAtRef(ref, path) {
  try {
    return execFileSync("git", ["show", `${ref}:${path}`], {
      cwd: repoRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (err) {
    const output = `${err.stdout ?? ""}${err.stderr ?? ""}`;
    if (output.includes("does not exist") || output.includes("exists on disk, but not in")) {
      return null;
    }
    throw err;
  }
}

function main() {
  const baseRef = process.argv[2];
  if (!baseRef) throw new Error("usage: check-version-bump.mjs <base-ref>");
  for (const { prefix, file } of ARTIFACTS) {
    let verdict;
    try {
      verdict = checkVersionBump(
        readAtRef(baseRef, `${typeMapPath}/${file}`),
        readFileSync(join(repoRoot, "cdk", "cdk", "type_map", file), "utf8")
      );
    } catch (err) {
      throw new Error(`${prefix}: ${err.message}`, { cause: err });
    }
    console.log(
      verdict.changed
        ? `${prefix} changed and declares v${verdict.version}`
        : `${prefix} unchanged against ${baseRef} (v${verdict.version})`
    );
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main();
}
