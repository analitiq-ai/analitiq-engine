// Pre-merge net: an artifact whose bytes changed on this branch must also
// carry a higher `version`.
//
// The publisher (sync-contracts-to-s3.mjs) refuses to republish changed
// content under an already-published version, and refuses a version older than
// the published one. It runs post-merge, so either mistake lands on main and
// strands the artifact unpublished. This check moves both failures into the
// pull request.
//
// It rules on ordering against the base branch even though the base is not the
// published history. It can, because the publisher requires a strict increase
// over whatever the bucket holds: a head version below the base's is
// unpublishable no matter where the bucket sits, since the bucket is either at
// the base's version or behind it, and being behind only lowers the floor. What
// this check cannot do is confirm a version is high ENOUGH — the bucket may
// have moved ahead of main — so the publisher still has the last word.

import { execFileSync } from "node:child_process";
import { readFileSync, realpathSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { ARTIFACTS, compareVersions, parseVersion } from "./sync-contracts-to-s3.mjs";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

/**
 * Verdict for one artifact, from the base branch's copy (raw text, or null
 * when the branch adds the artifact) and this branch's copy.
 *
 * Returns one of four statuses: "added" (the base has no copy), "unchanged",
 * "bumped" (compared, and the version increased), or "uncomparable" (the base
 * copy states no orderable version). Every status that did NOT compare is kept
 * distinct from the ones that did, so a caller cannot report a skipped check
 * as a passed one.
 *
 * Throws when this branch's artifact declares no plain-semver version, or when
 * its bytes moved without the version increasing.
 */
export function checkVersionBump(baseText, headText) {
  const version = JSON.parse(headText).version;
  if (parseVersion(version) === null) {
    throw new Error(
      `declares no usable version (got ${JSON.stringify(version)}); the ` +
        `publisher reads this field and cannot assign one`
    );
  }
  if (baseText === null) return { status: "added", version };
  if (baseText === headText) return { status: "unchanged", version };

  // A base that does not parse, or whose version is absent or malformed, gives
  // nothing to order against — it predates versioned artifacts, or it is
  // corrupt. Neither is this author's fault and neither should fail them, but
  // the result is an UNCOMPARED artifact and says so: the publisher, which
  // holds the real published version, is then the only thing enforcing the
  // increase.
  let baseVersion = null;
  let reason = "declares no orderable version";
  try {
    baseVersion = parseVersion(JSON.parse(baseText).version);
  } catch (err) {
    reason = `does not parse (${err.message})`;
  }
  if (baseVersion === null) return { status: "uncomparable", version, reason };

  if (compareVersions(parseVersion(version), baseVersion) <= 0) {
    throw new Error(
      `content changed but version ${version} does not increase on the base ` +
        `branch's ${baseVersion.join(".")}; bump the version constant in the ` +
        `CDK and regenerate the artifact`
    );
  }
  return { status: "bumped", version };
}

/**
 * The artifact as of *ref*, or null when *ref* does not contain that path.
 *
 * `git show` reports a bad ref and an absent path the same way — a non-zero
 * exit with a prose message — so absence is established with `ls-tree`, where
 * it is an exit-0 empty listing and every non-zero exit is unambiguously a
 * real failure. Matching git's wording would make the gate fail OPEN the day
 * that wording changes.
 */
function readAtRef(ref, path) {
  const git = (args) =>
    execFileSync("git", args, {
      cwd: repoRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
  // `:/` is git's top-level pathspec magic. Without it the pathspec resolves
  // against the process cwd, where a miss is an exit-0 empty listing -- the
  // same signal as a genuinely absent path, so the gate would switch itself
  // off silently rather than fail.
  if (git(["ls-tree", "--name-only", ref, "--", `:/${path}`]).trim() === "") return null;
  return git(["show", `${ref}:${path}`]);
}

function main() {
  const baseRef = process.argv[2];
  if (!baseRef) throw new Error("usage: check-version-bump.mjs <base-ref>");
  for (const { prefix, path } of ARTIFACTS) {
    let verdict;
    try {
      verdict = checkVersionBump(
        readAtRef(baseRef, path),
        readFileSync(join(repoRoot, path), "utf8")
      );
    } catch (err) {
      throw new Error(`${prefix}: ${err.message}`, { cause: err });
    }
    const report = {
      added: `absent in ${baseRef}, added here at v${verdict.version} (not compared)`,
      unchanged: `unchanged against ${baseRef} (v${verdict.version})`,
      bumped: `changed and declares v${verdict.version}`,
      uncomparable:
        `declares v${verdict.version}; the copy in ${baseRef} ` +
        `${verdict.reason} (not compared — only the publisher can order this)`,
    };
    console.log(`${prefix} ${report[verdict.status]}`);
  }
}

// realpathSync on both sides: `resolve` does not follow symlinks, so a
// checkout reached through one makes the two spellings differ and main()
// silently never runs -- the gate then reports success having checked nothing.
if (process.argv[1] && realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url))) {
  main();
}
