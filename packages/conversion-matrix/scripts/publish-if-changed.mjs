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
import { readFileSync } from "node:fs";
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

// Digest of the exact tarball contents npm will publish -- enumerated from
// `npm pack` so it stays honest as the file set evolves (dist, README, LICENSE,
// package.json metadata). package.json is hashed without the two fields this
// script mutates (version, analitiqPackageSha256), which would otherwise make
// the digest depend on its own output. Any other shipped change cuts a version;
// a no-op rebuild does not.
function shipDigest() {
  const listing = JSON.parse(npm(["pack", "--dry-run", "--json"]));
  const paths = listing[0].files.map((f) => f.path).sort();
  const hash = createHash("sha256");
  for (const path of paths) {
    hash.update(path);
    hash.update("\0");
    if (path === "package.json") {
      const pkg = JSON.parse(readFileSync(join(pkgRoot, path), "utf8"));
      delete pkg.version;
      delete pkg.analitiqPackageSha256;
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
