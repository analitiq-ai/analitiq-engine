// Sync the engine's published contract artifacts to S3 as versioned JSON.
//
// Layout under s3://$CONVERSION_MATRIX_S3_BUCKET/{prefix}/, one prefix per
// artifact in ARTIFACTS:
//   v{version}/{artifact}.json   immutable, one object per published version
//   latest.json                  mutable manifest {version, sha256, commit, publishedAt}
//
// The version is carried by the artifact itself (its top-level `version`
// field, generated from the CDK's GRAMMAR_VERSION / CONVERSION_MATRIX_VERSION,
// or cdk.__version__ for the consumption manifest) so a consumer holding the
// bytes can name the vocabulary it got. This script reads that version; it
// never assigns one. Each artifact's sha256 is compared against its own
// manifest, and content that changed without a version bump aborts rather
// than overwriting an immutable published object.
//
// Each artifact names the CHANNEL that publishes it, and one run syncs one
// channel (`node sync-contracts-to-s3.mjs <channel>`):
//   main         on push to main -- the artifact bumps its own version
//                constant whenever its content changes, so any commit is
//                publishable;
//   cdk-release  at a cdk-v* tag -- the artifact's version is
//                cdk.__version__, which moves once per release while the
//                content moves with every PR, so a push to main would abort
//                on an unbumped change.
//
// Versioning here is deliberately independent of the npm package version —
// that digest also covers the shipped TS code, so a helper fix bumps npm
// without touching what consumers pin here.
//
// The manifest is written last: it is the commit point. A run that dies after
// uploading the versioned object leaves an orphan no consumer can discover;
// the next run rewrites that same key and then commits the manifest. The bytes
// cannot differ between the two runs — the version is part of the content, so
// an artifact that moved in between declares a different version and lands on
// a different key. Only a consumer that probed a version no manifest ever
// referenced could observe that window.

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const pkgRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = resolve(pkgRoot, "..", "..");

// Prefixes are fixed, not configurable: on a shared bucket whose publisher
// role is not prefix-scoped, a typo'd prefix variable would silently start a
// parallel version history. A constant removes that channel entirely.
// Exported so the test suite can assert each source file exists and is
// tracked — a stale entry otherwise surfaces only in the post-merge sync job.
export const ARTIFACTS = [
  {
    prefix: "conversion-matrix",
    path: "cdk/cdk/type_map/conversion_matrix.json",
    channel: "main",
  },
  {
    prefix: "arrow-type-grammar",
    path: "cdk/cdk/type_map/arrow_type_grammar.json",
    channel: "main",
  },
  {
    prefix: "contract-consumption",
    path: "cdk/cdk/contract_consumption.json",
    channel: "cdk-release",
  },
];

export const CHANNELS = Object.freeze([...new Set(ARTIFACTS.map((a) => a.channel))]);

/** The artifacts one channel publishes; an unknown channel is a usage error. */
export function artifactsFor(channel) {
  if (!CHANNELS.includes(channel)) {
    throw new Error(`unknown channel ${JSON.stringify(channel)}; one of: ${CHANNELS.join(", ")}`);
  }
  return ARTIFACTS.filter((a) => a.channel === channel);
}

// Each component is `0` or a leading-zero-free number, as semver requires.
// A bare \d+ would accept "01.2.3" and normalise it to 1.2.3, so the object
// would publish at a key many semver parsers refuse to read back.
const SEMVER = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;

/**
 * The three numeric parts of a plain semver string, or null if it is not one.
 *
 * Non-strings are rejected outright rather than coerced: `["1.2.3"]` stringifies
 * to a matching `1.2.3`, and would then be interpolated into the S3 key.
 */
export function parseVersion(value) {
  if (typeof value !== "string") return null;
  const parts = SEMVER.exec(value);
  return parts === null ? null : parts.slice(1, 4).map(Number);
}

/** -1, 0 or 1, comparing two parsed versions part by part. */
export function compareVersions(left, right) {
  for (let i = 0; i < 3; i += 1) {
    if (left[i] !== right[i]) return left[i] < right[i] ? -1 : 1;
  }
  return 0;
}

/**
 * The version invariant, independent of the channel enforcing it: an artifact
 * declares a plain-semver version; identical bytes are already published; and
 * different bytes require a strictly higher version than the one recorded
 * against the published copy.
 *
 * Returns {action: "skip"} or {action: "publish", version}, and throws for
 * every state that would publish bytes under a version that lies about them.
 * `publishedVersion` / `publishedSha` are null when nothing is published yet.
 *
 * A declared version OLDER than the published one is refused by default: the
 * npm channel has one `latest` and no other place for it. A channel that
 * keeps one immutable object per version (S3) passes `allowOlder` and gets
 * {action: "backfill", version} instead -- the older release's object can
 * still be written, and the manifest is left pointing at the newer one. That
 * is how two CDK tags approved out of order both end up published.
 *
 * Shared rather than restated: every channel that ships these bytes -- S3, the
 * npm package -- has to hold the same line, and a second copy of the rule is
 * how one channel ends up with the invariant broken while the other refuses.
 */
export function planVersionedPublish(
  publishedVersion,
  publishedSha,
  currentSha,
  declaredVersion,
  { allowOlder = false } = {}
) {
  const declared = parseVersion(declaredVersion);
  if (declared === null) {
    throw new Error(
      `artifact declares no usable version (got ${JSON.stringify(declaredVersion)})`
    );
  }
  if (publishedSha === null && publishedVersion === null) {
    return { action: "publish", version: declaredVersion };
  }
  if (publishedSha === currentSha) return { action: "skip" };
  const published = parseVersion(publishedVersion);
  if (published === null) {
    throw new Error(
      `the published copy has no usable version (got ${JSON.stringify(publishedVersion)})`
    );
  }
  const order = compareVersions(declared, published);
  if (order === 0) {
    throw new Error(
      `artifact content changed but its version is still ${declaredVersion}, ` +
        `already published with a different sha256; bump the version constant ` +
        `in the CDK and regenerate`
    );
  }
  if (order < 0) {
    if (allowOlder) return { action: "backfill", version: declaredVersion };
    throw new Error(
      `artifact declares version ${declaredVersion}, older than the published ` +
        `${publishedVersion}; a revert must be published as a new higher version`
    );
  }
  return { action: "publish", version: declaredVersion };
}

/**
 * Whether a backfill may write its versioned object, given that object's
 * current text on S3 (null when absent) and the bytes to publish.
 *
 * Absent: write it. Identical: nothing to do. Different: two builds claim the
 * same version with different bytes, which is the one thing a versioned
 * channel must never let through.
 */
export function planBackfill(objectText, currentSha, declaredVersion) {
  if (objectText === null) return { action: "publish-object", version: declaredVersion };
  const publishedSha = createHash("sha256").update(objectText).digest("hex");
  if (publishedSha === currentSha) return { action: "skip" };
  throw new Error(
    `v${declaredVersion} is already published with different bytes (sha256 ` +
      `${publishedSha.slice(0, 12)} vs ${currentSha.slice(0, 12)}); a version ` +
      `is immutable once published`
  );
}

/**
 * The same decision for the S3 channel, whose published state arrives as the
 * raw text of its latest.json manifest (or null when none exists yet).
 *
 * A manifest that does not parse aborts: guessing on top of corrupt state
 * could overwrite a published object.
 */
export function planSync(manifestText, currentSha, declaredVersion) {
  if (manifestText === null) {
    return planVersionedPublish(null, null, currentSha, declaredVersion);
  }
  let manifest;
  try {
    manifest = JSON.parse(manifestText);
  } catch (err) {
    throw new Error(`latest.json on S3 is not valid JSON: ${err.message}`);
  }
  if (manifest === null || typeof manifest !== "object" || Array.isArray(manifest)) {
    throw new Error("latest.json on S3 is not a JSON object");
  }
  try {
    return planVersionedPublish(
      manifest.version ?? null,
      manifest.sha256 ?? null,
      currentSha,
      declaredVersion,
      { allowOlder: true }
    );
  } catch (err) {
    throw new Error(err.message.replace("the published copy", "latest.json on S3"), {
      cause: err,
    });
  }
}

const aws = (args, opts = {}) =>
  execFileSync("aws", args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], ...opts });

/**
 * True when the AWS CLI output says the manifest object itself does not exist.
 *
 * NoSuchKey is the only absence signal: with the prefixes constant, it means
 * a never-written manifest — the first publish — or an out-of-band deletion
 * of latest.json, which the publisher role cannot cause (no delete
 * permission). Everything else — NoSuchBucket, AccessDenied, ExpiredToken,
 * network errors — must abort the run, not be misread as a first publish.
 * A first publish skips the ordering check entirely and writes the declared
 * version, so misreading absence would overwrite the immutable object already
 * published at that version and repoint latest.json at it.
 */
export function manifestAbsent(cliOutput) {
  return cliOutput.includes("NoSuchKey");
}

// GetObject via s3api rather than `aws s3 cp`: `s3 cp` probes with HeadObject,
// whose bodyless 404 cannot say WHAT is missing — a mistyped bucket would read
// as "first publish". GetObject's error carries the real code, so absence can
// be matched on NoSuchKey alone. NOTE: this distinction only exists when the
// role grants s3:ListBucket — without it S3 masks a missing key as
// AccessDenied and the first publish dead-ends on a Forbidden fetch.
function fetchObjectOrAbsent(bucket, key) {
  const dir = mkdtempSync(join(tmpdir(), "engine-contracts-"));
  const outfile = join(dir, basename(key));
  try {
    // Only the AWS call is classified. A read failure on the file the CLI just
    // wrote is a local fault, and must not be fed to an AWS error classifier
    // that could only ever conclude "no manifest, publish as if first".
    try {
      aws(["s3api", "get-object", "--bucket", bucket, "--key", key, outfile]);
    } catch (err) {
      if (manifestAbsent(`${err.stdout ?? ""}${err.stderr ?? ""}`)) return null;
      throw err;
    }
    return readFileSync(outfile, "utf8");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function requireEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not set`);
  return value;
}

function syncArtifact(bucket, commit, { prefix, path }) {
  const sourcePath = join(repoRoot, path);
  const file = basename(path);
  const base = `s3://${bucket}/${prefix}`;
  const content = readFileSync(sourcePath);
  const currentSha = createHash("sha256").update(content).digest("hex");
  const manifestText = fetchObjectOrAbsent(bucket, `${prefix}/latest.json`);
  let plan;
  try {
    plan = planSync(manifestText, currentSha, JSON.parse(content).version);
    if (plan.action === "backfill") {
      const objectText = fetchObjectOrAbsent(bucket, `${prefix}/v${plan.version}/${file}`);
      plan = { ...planBackfill(objectText, currentSha, plan.version), backfill: true };
    }
  } catch (err) {
    // Several artifacts can sync in one run; without the prefix the operator
    // cannot tell which one refused to publish.
    throw new Error(`${prefix}: ${err.message}`, { cause: err });
  }

  if (plan.action === "skip") {
    console.log(`${prefix} unchanged on S3 (sha256 ${currentSha.slice(0, 12)}); nothing to sync`);
    return;
  }

  console.log(`syncing ${prefix} v${plan.version} to ${base} (sha256 ${currentSha.slice(0, 12)})`);
  aws(
    [
      "s3", "cp", sourcePath, `${base}/v${plan.version}/${file}`,
      "--content-type", "application/json",
      "--cache-control", "public, max-age=31536000, immutable",
    ],
    { stdio: "inherit" }
  );
  if (plan.backfill) {
    // An older release published after a newer one: its object is written,
    // latest.json keeps naming the newer version.
    console.log(`${prefix} v${plan.version} backfilled; latest.json unchanged`);
    return;
  }
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

function main() {
  const channel = process.argv[2];
  if (!channel) throw new Error(`usage: sync-contracts-to-s3.mjs <${CHANNELS.join("|")}>`);
  const artifacts = artifactsFor(channel);
  const bucket = requireEnv("CONVERSION_MATRIX_S3_BUCKET");
  // The commit of the checked-out tree the artifacts were read from — the
  // workflow checks out main's tip, so GITHUB_SHA (the triggering commit) can
  // be stale.
  const commit = execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: repoRoot,
    encoding: "utf8",
  }).trim();
  for (const artifact of artifacts) {
    syncArtifact(bucket, commit, artifact);
  }
}

// realpathSync on both sides: `resolve` does not follow symlinks, so a
// checkout reached through one (macOS /var, a symlinked CI workspace) makes
// the two spellings differ and main() silently never runs -- the job then
// succeeds having published nothing.
if (process.argv[1] && realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url))) {
  main();
}
