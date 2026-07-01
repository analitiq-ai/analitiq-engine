// Publish @analitiq-ai/conversion-matrix to GitHub Packages only when the built
// package content changed, patch-bumping off the last published release.
//
// The gate hashes the whole built dist/ -- the grid data AND the package's own
// code/types -- so a grid change or a fix to getConversion both cut a new
// version, while an engine release that changes nothing we ship does not. The
// registry is the source of truth: the digest is recorded on each published
// version (analitiqPackageSha256) and compared on the next run.

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

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

// Digest of everything we ship: each dist file's path and raw bytes, in a stable
// order. Captures grid, compiled code, and types; a no-op rebuild is unchanged.
function distDigest() {
  const dir = join(pkgRoot, "dist");
  const hash = createHash("sha256");
  for (const name of readdirSync(dir).sort()) {
    hash.update(name);
    hash.update("\0");
    hash.update(readFileSync(join(dir, name)));
    hash.update("\0");
  }
  return hash.digest("hex");
}

const currentSha = distDigest();
const publishedSha = npmViewOrAbsent(["view", pkgName, "analitiqPackageSha256"]);

if (publishedSha && publishedSha === currentSha) {
  console.log(`package unchanged (sha256 ${currentSha.slice(0, 12)}); nothing to publish`);
  process.exit(0);
}

const lastVersion = npmViewOrAbsent(["view", pkgName, "version"]);
let nextVersion;
if (lastVersion) {
  npm(["pkg", "set", `version=${lastVersion}`]);
  npm(["--no-git-tag-version", "version", "patch"]);
  nextVersion = npm(["pkg", "get", "version"]).replace(/"/g, "");
} else {
  nextVersion = npm(["pkg", "get", "version"]).replace(/"/g, "");
}

npm(["pkg", "set", `version=${nextVersion}`, `analitiqPackageSha256=${currentSha}`]);
console.log(`publishing ${pkgName}@${nextVersion} (package sha256 ${currentSha.slice(0, 12)})`);
execFileSync("npm", ["publish"], { cwd: pkgRoot, stdio: "inherit" });
