// Publish @analitiq-ai/conversion-matrix to GitHub Packages only when the built
// package content changed.
//
// The gate hashes the whole built dist/ -- the grid data AND the package's own
// code/types -- so a grid change or a fix to getConversion both cut a new
// version, while an engine release that changes nothing we ship does not. The
// registry is the source of truth: the digest is recorded on each published
// version (analitiqPackageSha256) and compared on the next run.
//
// Versions patch-bump off the last published release, EXCEPT when package.json
// declares one higher than that -- then the declared version wins. The package
// re-exports the engine artifact verbatim (dist/conversion_matrix.json is a
// documented entry point), so an artifact shape change is a breaking change
// here too, and a patch bump would walk consumers' semver ranges straight into
// it. Declaring the version in package.json is how that break is announced.

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  compareVersions,
  parseVersion,
  planVersionedPublish,
} from "./sync-contracts-to-s3.mjs";

const pkgRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const pkgName = "@analitiq-ai/conversion-matrix";

const npm = (args) =>
  execFileSync("npm", args, { cwd: pkgRoot, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();

// A never-published package (or field) makes `npm view` exit with E404; that is
// the only "absent" case. Every other failure -- auth, network, 5xx, rate limit
// -- must abort the run, not be misread as "nothing published yet" (which would
// cut a needless version or publish over an existing one).
const npmViewOrAbsent = (args) => {
  try {
    return npm(args);
  } catch (err) {
    const out = `${err.stdout ?? ""}${err.stderr ?? ""}`;
    if (out.includes("E404") || out.includes("404 Not Found")) return "";
    throw err;
  }
};

// Fields this script writes back onto package.json. They are excluded from the
// digest below, which would otherwise depend on its own previous output.
const SELF_WRITTEN_FIELDS = [
  "version",
  "analitiqPackageSha256",
  "analitiqMatrixVersion",
  "analitiqMatrixSha256",
];

// Digest of the exact tarball contents npm will publish -- enumerated from
// `npm pack` so it stays honest as the file set evolves (dist, README, LICENSE,
// package.json metadata). Any other shipped change cuts a version; a no-op
// rebuild does not.
function shipDigest() {
  const listing = JSON.parse(npm(["pack", "--dry-run", "--json"]));
  const paths = listing[0].files.map((f) => f.path).sort();
  const hash = createHash("sha256");
  for (const path of paths) {
    hash.update(path);
    hash.update("\0");
    if (path === "package.json") {
      const pkg = JSON.parse(readFileSync(join(pkgRoot, path), "utf8"));
      for (const field of SELF_WRITTEN_FIELDS) delete pkg[field];
      hash.update(Buffer.from(JSON.stringify(pkg)));
    } else {
      hash.update(readFileSync(join(pkgRoot, path)));
    }
    hash.update("\0");
  }
  return hash.digest("hex");
}

const currentSha = shipDigest();
const publishedSha = npmViewOrAbsent(["view", pkgName, "analitiqPackageSha256"]);

if (publishedSha && publishedSha === currentSha) {
  console.log(`package unchanged (sha256 ${currentSha.slice(0, 12)}); nothing to publish`);
  process.exit(0);
}

// This package re-exports the engine artifact verbatim, so it is a publishing
// channel for it and must hold the same version invariant the S3 sync does --
// enforced here rather than only in the pull-request gate, which a direct push
// to main or a merge whose final bytes were never gated would bypass. Without
// it npm can ship a changed grid under an unchanged matrixVersion while the
// S3 sync refuses the same bytes, leaving one channel's invariant broken.
const artifact = readFileSync(join(pkgRoot, "dist", "conversion_matrix.json"));
const artifactSha = createHash("sha256").update(artifact).digest("hex");
planVersionedPublish(
  npmViewOrAbsent(["view", pkgName, "analitiqMatrixVersion"]) || null,
  npmViewOrAbsent(["view", pkgName, "analitiqMatrixSha256"]) || null,
  artifactSha,
  JSON.parse(artifact).version
);

const declaredVersion = npm(["pkg", "get", "version"]).replace(/"/g, "");
if (parseVersion(declaredVersion) === null) {
  throw new Error(`package.json declares no plain-semver version (got ${declaredVersion})`);
}

const lastVersion = npmViewOrAbsent(["view", pkgName, "version"]);
let nextVersion;
if (!lastVersion) {
  nextVersion = declaredVersion;
} else if (parseVersion(lastVersion) === null) {
  // A published version this script cannot order against is a state it must
  // not guess at. `npm version patch` does not rescue it: on a prerelease it
  // DROPS the suffix rather than incrementing (0.3.0-rc.1 -> 0.3.0), which
  // would promote an rc to its final release and discard a declared breaking
  // version at the same time.
  throw new Error(
    `${pkgName}@${lastVersion} is published but is not plain semver; ` +
      `set package.json to the version this release should carry`
  );
} else if (compareVersions(parseVersion(declaredVersion), parseVersion(lastVersion)) > 0) {
  // A deliberate bump in package.json -- the maintainer is announcing a change
  // consumers' ranges must not cross silently. Honour it verbatim.
  nextVersion = declaredVersion;
} else {
  npm(["pkg", "set", `version=${lastVersion}`]);
  npm(["--no-git-tag-version", "version", "patch"]);
  nextVersion = npm(["pkg", "get", "version"]).replace(/"/g, "");
}

npm([
  "pkg",
  "set",
  `version=${nextVersion}`,
  `analitiqPackageSha256=${currentSha}`,
  `analitiqMatrixVersion=${JSON.parse(artifact).version}`,
  `analitiqMatrixSha256=${artifactSha}`,
]);
console.log(`publishing ${pkgName}@${nextVersion} (package sha256 ${currentSha.slice(0, 12)})`);
execFileSync("npm", ["publish"], { cwd: pkgRoot, stdio: "inherit" });
