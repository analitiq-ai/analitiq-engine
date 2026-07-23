// Sync the engine's conversion grid to S3 as versioned JSON.
//
// Layout under s3://$CONVERSION_MATRIX_S3_BUCKET/$CONVERSION_MATRIX_S3_PREFIX/:
//   v{version}/conversion_matrix.json   immutable, one object per grid version
//   latest.json                         mutable manifest {version, sha256, commit, publishedAt}
//
// S3 is its own source of truth: the grid's sha256 is compared against the
// manifest and a patch version is cut only when the grid content changed. This
// is deliberately independent of the npm package version — that digest also
// covers the shipped TS code, so a helper fix bumps npm without touching the
// grid consumers pin here.
//
// The manifest is written last: it is the commit point. A run that dies after
// uploading the versioned object leaves an orphan no consumer can discover;
// the next run recomputes the same next version and overwrites it.

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const pkgRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = resolve(pkgRoot, "..", "..");
const gridPath = join(repoRoot, "cdk", "cdk", "type_map", "conversion_matrix.json");

/**
 * Decide what this run must do, from the manifest currently on S3 (raw JSON
 * text, or null when none exists yet) and the sha256 of the local grid.
 *
 * Returns {action: "skip"} when the published grid already matches, or
 * {action: "publish", version} with the version to cut. A manifest that does
 * not parse, or whose version is not plain semver, aborts: guessing a version
 * on top of corrupt state could overwrite a published object.
 */
export function planSync(manifestText, currentSha) {
  if (manifestText === null) return { action: "publish", version: "1.0.0" };
  let manifest;
  try {
    manifest = JSON.parse(manifestText);
  } catch (err) {
    throw new Error(`latest.json on S3 is not valid JSON: ${err.message}`);
  }
  if (manifest.sha256 === currentSha) return { action: "skip" };
  const parts = /^(\d+)\.(\d+)\.(\d+)$/.exec(manifest.version ?? "");
  if (!parts) {
    throw new Error(
      `latest.json on S3 has no usable version (got ${JSON.stringify(manifest.version)})`
    );
  }
  const next = `${parts[1]}.${parts[2]}.${Number(parts[3]) + 1}`;
  return { action: "publish", version: next };
}

const aws = (args, opts = {}) =>
  execFileSync("aws", args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], ...opts });

// A manifest that has never been written is the only "absent" case (404 /
// NoSuchKey). Every other failure — credentials, missing bucket, network —
// must abort the run, not be misread as a first publish (which would reset
// versioning to 1.0.0 over an existing history).
function fetchManifestOrAbsent(url) {
  try {
    return aws(["s3", "cp", url, "-"]);
  } catch (err) {
    const out = `${err.stdout ?? ""}${err.stderr ?? ""}`;
    if (out.includes("(404)") || out.includes("NoSuchKey") || out.includes("Not Found")) {
      return null;
    }
    throw err;
  }
}

function requireEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not set`);
  return value;
}

function main() {
  const bucket = requireEnv("CONVERSION_MATRIX_S3_BUCKET");
  const prefix = process.env.CONVERSION_MATRIX_S3_PREFIX || "conversion-matrix";
  // The commit of the checked-out tree the grid was read from — the workflow
  // checks out main's tip, so GITHUB_SHA (the triggering commit) can be stale.
  const commit = execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: repoRoot,
    encoding: "utf8",
  }).trim();
  const base = `s3://${bucket}/${prefix}`;

  const grid = readFileSync(gridPath);
  const currentSha = createHash("sha256").update(grid).digest("hex");
  const plan = planSync(fetchManifestOrAbsent(`${base}/latest.json`), currentSha);

  if (plan.action === "skip") {
    console.log(`grid unchanged on S3 (sha256 ${currentSha.slice(0, 12)}); nothing to sync`);
    return;
  }

  console.log(`syncing grid v${plan.version} to ${base} (sha256 ${currentSha.slice(0, 12)})`);
  aws(
    [
      "s3", "cp", gridPath, `${base}/v${plan.version}/conversion_matrix.json`,
      "--content-type", "application/json",
      "--cache-control", "public, max-age=31536000, immutable",
    ],
    { stdio: "inherit" }
  );
  const manifest = JSON.stringify(
    {
      version: plan.version,
      sha256: currentSha,
      commit,
      publishedAt: new Date().toISOString(),
    },
    null,
    2
  );
  aws(
    [
      "s3", "cp", "-", `${base}/latest.json`,
      "--content-type", "application/json",
      "--cache-control", "no-cache",
    ],
    { input: manifest, stdio: ["pipe", "inherit", "inherit"] }
  );
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main();
}
