// Tests for scripts/check-version-bump.mjs: the pure verdict core, and the
// git plumbing it rests on driven against a real throwaway repository. The
// plumbing matters as much as the core here — every way it can misreport
// absence turns the gate off while printing success.
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { checkVersionBump } from "../scripts/check-version-bump.mjs";

const artifact = (version, body) => JSON.stringify({ version, families: body });

const scriptPath = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "..",
  "scripts",
  "check-version-bump.mjs"
);

test("an unchanged artifact passes and reports its version", () => {
  const text = artifact("1.1.0", {});
  assert.deepEqual(checkVersionBump(text, text), { status: "unchanged", version: "1.1.0" });
});

test("a bumped artifact passes and reports the new version", () => {
  assert.deepEqual(checkVersionBump(artifact("1.1.0", {}), artifact("1.2.0", { a: 1 })), {
    status: "bumped",
    version: "1.2.0",
  });
});

test("changed content at an unbumped version fails", () => {
  assert.throws(
    () => checkVersionBump(artifact("1.1.0", {}), artifact("1.1.0", { a: 1 })),
    /version 1\.1\.0 does not increase on the base branch's 1\.1\.0/
  );
});

test("changed content at a LOWER version fails", () => {
  // A plain revert of a bump lands here. The version differs, so a
  // changed-not-increased rule would pass it and the publisher would then
  // abort post-merge with "older than the published" — the exact stranding
  // this gate exists to prevent.
  assert.throws(
    () => checkVersionBump(artifact("1.2.0", {}), artifact("1.1.0", { a: 1 })),
    /version 1\.1\.0 does not increase on the base branch's 1\.2\.0/
  );
});

test("ordering compares parts numerically, not as text", () => {
  assert.deepEqual(checkVersionBump(artifact("1.9.0", {}), artifact("1.10.0", { a: 1 })), {
    status: "bumped",
    version: "1.10.0",
  });
  assert.throws(
    () => checkVersionBump(artifact("1.10.0", {}), artifact("1.9.0", { a: 1 })),
    /does not increase/
  );
});

test("an artifact new on this branch is reported as added, not as unchanged", () => {
  // "added" and "unchanged" must not collapse: one means compared-and-equal,
  // the other means never compared.
  assert.deepEqual(checkVersionBump(null, artifact("1.0.0", {})), {
    status: "added",
    version: "1.0.0",
  });
});

test("a base with no orderable version reports uncomparable, not bumped", () => {
  // Nothing to order against, so nothing was checked -- and it must not be
  // reported as a verified increase. The publisher, which holds the real
  // published version, is then the only thing enforcing the increase.
  for (const base of [JSON.stringify({ families: {} }), '{"version": "v1", "families": {}}']) {
    const verdict = checkVersionBump(base, artifact("1.1.0", {}));
    assert.equal(verdict.status, "uncomparable");
    assert.equal(verdict.version, "1.1.0");
    assert.match(verdict.reason, /declares no orderable version/);
  }
});

test("an unparseable base blames the base, not this branch's artifact", () => {
  // JSON.parse on the base must not throw a SyntaxError that main() then
  // relabels as a problem with the artifact the author actually changed.
  const verdict = checkVersionBump("{not json", artifact("1.1.0", {}));
  assert.equal(verdict.status, "uncomparable");
  assert.match(verdict.reason, /does not parse/);
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

// --- the git plumbing -------------------------------------------------------

const git = (cwd, args) => execFileSync("git", args, { cwd, encoding: "utf8" });

const scriptsDir = dirname(scriptPath);

/**
 * A throwaway repo laid out like this one, with the real scripts copied in.
 *
 * The CLI resolves the repo root from its own location, so it has to run from
 * a copy inside the fixture; running the checked-out script against a foreign
 * cwd would read this repository's artifacts instead.
 */
function withRepo(run) {
  // The temp dir is deliberately NOT realpath'd: on macOS it arrives through a
  // /var -> /private/var symlink, so these runs also pin that the script's
  // entry-point guard survives a symlinked checkout instead of silently
  // skipping main() and reporting success.
  const dir = mkdtempSync(join(tmpdir(), "version-bump-"));
  try {
    git(dir, ["init", "-q", "-b", "main"]);
    git(dir, ["config", "user.email", "t@t"]);
    git(dir, ["config", "user.name", "t"]);
    mkdirSync(join(dir, "cdk", "cdk", "type_map"), { recursive: true });
    const fixtureScripts = join(dir, "packages", "conversion-matrix", "scripts");
    mkdirSync(fixtureScripts, { recursive: true });
    for (const name of ["check-version-bump.mjs", "sync-contracts-to-s3.mjs"]) {
      copyFileSync(join(scriptsDir, name), join(fixtureScripts, name));
    }
    run(dir, join(fixtureScripts, "check-version-bump.mjs"));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

const runCli = (cli, ref) =>
  execFileSync("node", [cli, ref], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });

test("a bad base ref aborts instead of reporting the artifact as absent", () => {
  // The failure mode this replaces: classifying git's prose as "not in that
  // ref" turns the gate into a no-op that prints success.
  withRepo((dir, cli) => {
    writeFileSync(join(dir, "cdk", "cdk", "type_map", "conversion_matrix.json"), artifact("1.0.0", {}));
    writeFileSync(join(dir, "cdk", "cdk", "type_map", "arrow_type_grammar.json"), artifact("1.0.0", {}));
    git(dir, ["add", "-A"]);
    git(dir, ["commit", "-qm", "init"]);
    assert.throws(() => runCli(cli, "no-such-ref"), (err) => {
      assert.match(`${err.stdout ?? ""}${err.stderr ?? ""}`, /no-such-ref/);
      return true;
    });
  });
});

test("a path absent at the base ref is reported as added, not compared", () => {
  withRepo((dir, cli) => {
    writeFileSync(join(dir, "placeholder"), "x");
    git(dir, ["add", "-A"]);
    git(dir, ["commit", "-qm", "no artifacts yet"]);
    const base = git(dir, ["rev-parse", "HEAD"]).trim();
    writeFileSync(join(dir, "cdk", "cdk", "type_map", "conversion_matrix.json"), artifact("2.0.0", {}));
    writeFileSync(join(dir, "cdk", "cdk", "type_map", "arrow_type_grammar.json"), artifact("1.1.0", {}));
    const out = runCli(cli, base);
    assert.match(out, /conversion-matrix absent in .* added here at v2\.0\.0 \(not compared\)/);
    assert.match(out, /arrow-type-grammar absent in .* added here at v1\.1\.0 \(not compared\)/);
  });
});

test("an unversioned base is reported as not compared, and names the artifact", () => {
  // The state of main before versioned artifacts existed. The check cannot
  // order against it, and must not print the line it uses for a verified bump.
  withRepo((dir, cli) => {
    for (const file of ["conversion_matrix.json", "arrow_type_grammar.json"]) {
      writeFileSync(join(dir, "cdk", "cdk", "type_map", file), JSON.stringify({ families: {} }));
    }
    git(dir, ["add", "-A"]);
    git(dir, ["commit", "-qm", "unversioned"]);
    const base = git(dir, ["rev-parse", "HEAD"]).trim();
    writeFileSync(join(dir, "cdk", "cdk", "type_map", "conversion_matrix.json"), artifact("2.0.0", {}));
    writeFileSync(join(dir, "cdk", "cdk", "type_map", "arrow_type_grammar.json"), artifact("1.1.0", {}));
    const out = runCli(cli, base);
    assert.match(out, /conversion-matrix declares v2\.0\.0; the copy in .* declares no orderable version \(not compared/);
    assert.match(out, /arrow-type-grammar declares v1\.1\.0; the copy in .* declares no orderable version \(not compared/);
    assert.doesNotMatch(out, /changed and declares/);
  });
});

test("the CLI names which artifact refused to publish", () => {
  withRepo((dir, cli) => {
    for (const file of ["conversion_matrix.json", "arrow_type_grammar.json"]) {
      writeFileSync(join(dir, "cdk", "cdk", "type_map", file), artifact("1.0.0", {}));
    }
    git(dir, ["add", "-A"]);
    git(dir, ["commit", "-qm", "init"]);
    const base = git(dir, ["rev-parse", "HEAD"]).trim();
    // Change the grammar's bytes without touching its version.
    writeFileSync(
      join(dir, "cdk", "cdk", "type_map", "arrow_type_grammar.json"),
      artifact("1.0.0", { changed: true })
    );
    assert.throws(() => runCli(cli, base), (err) => {
      const output = `${err.stdout ?? ""}${err.stderr ?? ""}`;
      assert.match(output, /arrow-type-grammar: content changed but version 1\.0\.0/);
      return true;
    });
  });
});
