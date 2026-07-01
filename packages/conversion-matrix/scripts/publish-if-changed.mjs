// Publish @analitiq-ai/conversion-matrix to GitHub Packages only when the grid
// content changed, bumping the patch version off the last published release.
//
// The registry is the source of truth: we record the grid's sha256 on each
// published version (analitiqMatrixSha256) and skip publishing when it is
// unchanged, so an engine release that leaves the grid untouched never cuts a
// new package version.

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const pkgRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const pkgName = "@analitiq-ai/conversion-matrix";

const sha256 = (s) => createHash("sha256").update(s).digest("hex");
const npm = (args) =>
  execFileSync("npm", args, { cwd: pkgRoot, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
// Registry reads for a never-published field/package exit non-zero; treat as absent.
const npmOrEmpty = (args) => {
  try {
    return npm(args);
  } catch {
    return "";
  }
};

const currentSha = sha256(readFileSync(join(pkgRoot, "dist", "conversion_matrix.json"), "utf8"));
const publishedSha = npmOrEmpty(["view", pkgName, "analitiqMatrixSha256"]);

if (publishedSha && publishedSha === currentSha) {
  console.log(`grid unchanged (sha256 ${currentSha.slice(0, 12)}); nothing to publish`);
  process.exit(0);
}

const lastVersion = npmOrEmpty(["view", pkgName, "version"]);
let nextVersion;
if (lastVersion) {
  npm(["pkg", "set", `version=${lastVersion}`]);
  npm(["--no-git-tag-version", "version", "patch"]);
  nextVersion = npm(["pkg", "get", "version"]).replace(/"/g, "");
} else {
  nextVersion = npm(["pkg", "get", "version"]).replace(/"/g, "");
}

npm(["pkg", "set", `version=${nextVersion}`, `analitiqMatrixSha256=${currentSha}`]);
console.log(`publishing ${pkgName}@${nextVersion} (grid sha256 ${currentSha.slice(0, 12)})`);
execFileSync("npm", ["publish"], { cwd: pkgRoot, stdio: "inherit" });
