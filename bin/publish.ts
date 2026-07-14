#!/usr/bin/env bun
/* eslint-disable no-console */

// Guarded publish path for the non-private `@intx/*` packages.
//
// By default this is a DRY RUN: it verifies the tree is in a publishable
// state, builds the compiled tarballs, proves they install and load, and
// prints the plan without uploading anything. Only `bin/publish --execute`
// runs `bun publish`, and only under the release credentials.
//
// The guards exist because the live 0.1.x packages shipped broken: they
// were published from a tree whose sibling versions were not yet the
// release version, so every internal dependency froze at an unpublished
// `0.0.0`. The two guards that prevent recurrence:
//
//   - version-sync: every non-private package's `version` equals the
//     release tag. `bin/release` establishes this; `bin/publish` refuses
//     to publish unless it already holds.
//   - internal-dependency expression: every internal `@intx/*` dependency
//     is written as `workspace:` or `catalog:`, which `bun pm pack`
//     rewrites to the concrete release version at pack time. A pinned
//     literal is the 0.1.x shape — it can lag the release — so it aborts.
//
// The published tarballs keep the repo-internal `intx-src` exports
// condition, which is inert for any consumer that does not explicitly pass
// `--conditions=intx-src` (no external consumer does). Rather than mutate
// tracked package manifests mid-publish to strip it — which would
// reintroduce the very "publish from a tree that isn't the committed
// tree" hazard that broke 0.1.x — the load smoke below asserts that
// default-condition resolution lands on `dist/`, not `src/`. That proves
// the condition stays inert on every dry run.
//
// The smoke test loads every package under each available runtime (Node,
// Bun, and Deno) and asserts the result against an expected matrix (see
// RUNTIME_INCOMPATIBLE). `@intx/tools-lsp` cannot load under Node or Deno
// because it depends on `vscode-jsonrpc/node`, a CommonJS subpath its
// dependency exposes without an `exports` map that neither loader can
// resolve, though Bun can — so those failures are *expected and asserted*.
// Any other failure, or tools-lsp unexpectedly loading, fails the matrix
// rather than hiding behind a fallback.
//
// `--execute` publishes leaf-first so a dependency is on the registry
// before any dependent. Partial-failure recovery is a known gap to close
// before `--execute` is ever run: if the run dies after publishing some
// prefix, npm rejects re-publishing those versions, so a resumable
// idempotent skip of already-published `name@version` is needed. This
// session only exercises the dry run.

import {
  cpSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type } from "arktype";

import { buildDist } from "./build-dist";
import { checkWorkspaceExports } from "./exports-shape";
import { checkWorkspaceMetadata } from "./publish-metadata";

const manifestSchema = type({
  name: "string",
  version: "string",
  "private?": "boolean",
  "dependencies?": { "[string]": "string" },
  "peerDependencies?": { "[string]": "string" },
  "optionalDependencies?": { "[string]": "string" },
});

/** A non-private package that `bin/publish` targets. */
export interface Target {
  name: string;
  /** Absolute path to the package directory. */
  dir: string;
  version: string;
  /** Distinct internal `@intx/*` dependency names across the
   *  resolution-relevant fields, for publish ordering. */
  internalDeps: string[];
  /** Every internal `@intx/*` dependency spec across the resolution-relevant
   *  fields, for the expression guard. */
  intxSpecs: { field: string; name: string; spec: string }[];
}

const INTX_FIELDS = [
  "dependencies",
  "peerDependencies",
  "optionalDependencies",
] as const;

/** Read every non-private package under `packages/`. */
export function readTargets(repoRoot: string): Target[] {
  const pkgsDir = join(repoRoot, "packages");
  const targets: Target[] = [];
  for (const entry of readdirSync(pkgsDir)) {
    const dir = join(pkgsDir, entry);
    const pjPath = join(dir, "package.json");
    let text: string;
    try {
      text = readFileSync(pjPath, "utf8");
    } catch (err) {
      // A directory without a package.json is not a package; skip it. Any
      // other read failure (permissions, I/O) must surface — silently
      // dropping a real package would publish from an incomplete tree.
      if (err instanceof Error && "code" in err && err.code === "ENOENT") {
        continue;
      }
      throw err;
    }
    const parsed = manifestSchema(JSON.parse(text));
    if (parsed instanceof type.errors) {
      throw new Error(`publish: ${pjPath} malformed: ${parsed.summary}`);
    }
    if (parsed.private === true) continue;
    const intxSpecs: Target["intxSpecs"] = [];
    for (const field of INTX_FIELDS) {
      const deps = parsed[field] ?? {};
      for (const [name, spec] of Object.entries(deps)) {
        if (name.startsWith("@intx/")) intxSpecs.push({ field, name, spec });
      }
    }
    const internalDeps = [...new Set(intxSpecs.map((s) => s.name))];
    targets.push({
      name: parsed.name,
      dir,
      version: parsed.version,
      internalDeps,
      intxSpecs,
    });
  }
  return targets.sort((a, b) => a.name.localeCompare(b.name));
}

/** Every target whose `version` is not the release version. */
export function checkVersionSync(
  targets: Target[],
  tagVersion: string,
): string[] {
  return targets
    .filter((t) => t.version !== tagVersion)
    .map(
      (t) =>
        `${t.name} is at ${t.version}, not the release version ${tagVersion}`,
    );
}

/** Every internal dependency pinned to a literal instead of `workspace:`/
 *  `catalog:`. A literal can lag the release version — the 0.1.x failure. */
export function checkInternalDepExpressions(targets: Target[]): string[] {
  const violations: string[] = [];
  for (const t of targets) {
    for (const { field, name, spec } of t.intxSpecs) {
      if (!spec.startsWith("workspace:") && !spec.startsWith("catalog:")) {
        violations.push(
          `${t.name}: ${field}.${name} is pinned to "${spec}"; internal ` +
            `dependencies must use workspace: or catalog: so pack rewrites ` +
            `them to the release version`,
        );
      }
    }
  }
  return violations;
}

/** Order targets leaf-first: a package appears after every internal
 *  dependency it imports, so publishing in order never references an
 *  unpublished sibling. Throws on a dependency cycle. */
export function topoSortLeafFirst(targets: Target[]): Target[] {
  const byName = new Map(targets.map((t) => [t.name, t]));
  const state = new Map<string, "visiting" | "done">();
  const ordered: Target[] = [];
  function visit(t: Target, path: string[]): void {
    const s = state.get(t.name);
    if (s === "done") return;
    if (s === "visiting") {
      throw new Error(
        `publish: dependency cycle detected: ${[...path, t.name].join(" -> ")}`,
      );
    }
    state.set(t.name, "visiting");
    for (const dep of t.internalDeps) {
      const d = byName.get(dep);
      if (d !== undefined) visit(d, [...path, t.name]);
    }
    state.set(t.name, "done");
    ordered.push(t);
  }
  for (const t of targets) visit(t, []);
  return ordered;
}

export const RUNTIMES = ["node", "bun", "deno"] as const;
export type Runtime = (typeof RUNTIMES)[number];

/** Runtimes whose results gate the publish — all three the distribution
 *  targets. Every available runtime here is asserted; a runtime that is not
 *  on PATH is skipped, not failed. */
export const ASSERTED_RUNTIMES: ReadonlySet<Runtime> = new Set([
  "node",
  "bun",
  "deno",
]);

/** Packages that are expected NOT to load under a runtime, with the reason.
 *  An entry loading, or a package outside the table failing, fails the
 *  matrix — the incompatibility is asserted, never silently absorbed. */
export const RUNTIME_INCOMPATIBLE: Record<
  string,
  Partial<Record<Runtime, string>>
> = {
  "@intx/tools-lsp": {
    node:
      "depends on vscode-jsonrpc/node, a CommonJS subpath with no exports " +
      "map; Node's ESM loader cannot resolve it, Bun can",
    deno:
      "depends on vscode-jsonrpc/node, a CommonJS subpath with no exports " +
      "map; Deno's node-compat loader cannot resolve it, Bun can",
  },
};

export type LoadResult = "load" | "fail";
export type ObservedMatrix = Record<
  string,
  Partial<Record<Runtime, LoadResult>>
>;

/** Compare observed loads against the expected matrix for the asserted
 *  runtimes only. Unavailable runtimes (absent from `observed[pkg]`) are
 *  skipped, not failed. */
export function assertMatrix(
  observed: ObservedMatrix,
  asserted: ReadonlySet<Runtime> = ASSERTED_RUNTIMES,
): string[] {
  const violations: string[] = [];
  for (const [pkg, byRuntime] of Object.entries(observed)) {
    for (const runtime of asserted) {
      const result = byRuntime[runtime];
      if (result === undefined) continue; // runtime unavailable
      const reason = RUNTIME_INCOMPATIBLE[pkg]?.[runtime];
      const expectFail = reason !== undefined;
      if (expectFail && result === "load") {
        violations.push(
          `${pkg} loaded under ${runtime} but is listed as incompatible ` +
            `(${reason}); remove the exception`,
        );
      } else if (!expectFail && result === "fail") {
        violations.push(
          `${pkg} failed to load under ${runtime} but was expected to load`,
        );
      }
    }
  }
  return violations;
}

/** Every internal `@intx/*` dependency in a packed manifest whose specifier
 *  is not the release version. `bun pm pack` rewrites `workspace:`/`catalog:`
 *  to a concrete version using `bun.lock`; a specifier that lands on anything
 *  else means the lockfile is stale. A plain `bun install` after a version
 *  bump does not refresh those records — `bin/release` regenerates them. */
export function checkPackedManifest(
  name: string,
  manifest: unknown,
  version: string,
): string[] {
  const parsed = manifestSchema(manifest);
  if (parsed instanceof type.errors) {
    throw new Error(
      `publish: packed manifest for ${name} malformed: ${parsed.summary}`,
    );
  }
  const violations: string[] = [];
  for (const field of INTX_FIELDS) {
    for (const [dep, spec] of Object.entries(parsed[field] ?? {})) {
      if (!dep.startsWith("@intx/")) continue;
      // `workspace:*` packs to a bare version; `workspace:^`/`~` pack to a
      // caret/tilde range. Both must resolve to the release version.
      const bare = spec.replace(/^[\^~]/, "");
      if (bare !== version) {
        violations.push(
          `${name}: packed ${field}.${dep} resolved to "${spec}", not the ` +
            `release version ${version}`,
        );
      }
    }
  }
  return violations;
}

// ---- effectful orchestration ----

function run(cmd: string[], cwd: string): void {
  const proc = Bun.spawnSync(cmd, { cwd, stdout: "pipe", stderr: "pipe" });
  if (proc.exitCode !== 0) {
    const detail = [
      proc.stdout.toString().trim(),
      proc.stderr.toString().trim(),
    ]
      .filter(Boolean)
      .join("\n");
    throw new Error(
      `publish: \`${cmd.join(" ")}\` failed in ${cwd}:\n${detail}`,
    );
  }
}

/** Fail fast, before the expensive dist build, if `bun.lock` is stale: pack
 *  one internal package and confirm `bun pm pack` rewrote its `@intx/*`
 *  dependencies to the release version. This exercises the actual pack-time
 *  rewrite rather than reimplementing bun.lock parsing. */
function assertLockfileFresh(targets: Target[], version: string): void {
  const rep = targets.find((t) => t.internalDeps.length > 0);
  if (rep === undefined) return;
  const scratch = mkdtempSync(join(tmpdir(), "intx-publish-guard-"));
  try {
    run(["bun", "pm", "pack", "--destination", scratch, "--quiet"], rep.dir);
    const tgz = readdirSync(scratch).find((f) => f.endsWith(".tgz"));
    if (tgz === undefined) {
      throw new Error(`publish: pack produced no tarball for ${rep.name}`);
    }
    const proc = Bun.spawnSync(
      ["tar", "-xzOf", join(scratch, tgz), "package/package.json"],
      { stdout: "pipe", stderr: "pipe" },
    );
    if (proc.exitCode !== 0) {
      throw new Error(
        `publish: could not read packed manifest for ${rep.name}:\n${proc.stderr
          .toString()
          .trim()}`,
      );
    }
    const violations = checkPackedManifest(
      rep.name,
      JSON.parse(proc.stdout.toString()),
      version,
    );
    if (violations.length > 0) {
      throw new Error(
        `publish: bun.lock is stale — pack emitted the wrong dependency ` +
          `versions:\n${violations.join("\n")}\nRe-run bin/release so ` +
          `bun.lock records the release versions.`,
      );
    }
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
}

function git(args: string[], repoRoot: string): string {
  const proc = Bun.spawnSync(["git", ...args], {
    cwd: repoRoot,
    stdout: "pipe",
    stderr: "pipe",
  });
  if (proc.exitCode !== 0) {
    throw new Error(
      `publish: \`git ${args.join(" ")}\` failed:\n${proc.stderr
        .toString()
        .trim()}`,
    );
  }
  return proc.stdout.toString().trim();
}

/** Abort unless the working tree is clean. */
function requireCleanTree(repoRoot: string): void {
  const dirty = git(["status", "--porcelain"], repoRoot);
  if (dirty !== "") {
    throw new Error(
      `publish: working tree is not clean; commit or stash first:\n${dirty}`,
    );
  }
}

/** The release version from the tag `HEAD` points at exactly. */
function releaseVersion(repoRoot: string): string {
  const tag = git(["describe", "--exact-match", "--tags", "HEAD"], repoRoot);
  const version = tag.startsWith("v") ? tag.slice(1) : tag;
  if (!/^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/.test(version)) {
    throw new Error(
      `publish: tag ${tag} does not name a semver release version`,
    );
  }
  return version;
}

/** Which of the candidate runtimes are on PATH. */
function availableRuntimes(): Runtime[] {
  return RUNTIMES.filter((rt) => {
    const proc = Bun.spawnSync([rt, "--version"], {
      stdout: "ignore",
      stderr: "ignore",
    });
    return proc.exitCode === 0;
  });
}

// Loads a package under Node, Bun, or Deno: resolves the package under
// default conditions, asserts the resolution lands on dist/ (never the inert
// intx-src -> src), then imports it. Deno exposes `process` via its
// node-compat layer, so one program serves all three.
const LOAD_CHECK = `
const pkg = process.argv[2];
try {
  const resolved = import.meta.resolve(pkg);
  if (!resolved.includes("/dist/")) {
    console.error("resolved to " + resolved + ", not dist/");
    process.exit(3);
  }
  await import(pkg);
  console.log("ok " + resolved);
} catch (err) {
  console.error(String(err && err.stack ? err.stack : err));
  process.exit(1);
}
`;

const RUNTIME_CMD: Record<Runtime, (script: string, pkg: string) => string[]> =
  {
    node: (script, pkg) => ["node", script, pkg],
    bun: (script, pkg) => ["bun", script, pkg],
    // `manual` trusts the npm-populated node_modules; `auto` rejects the
    // `file:` specifiers an `npm install <tarball>` records.
    deno: (script, pkg) => [
      "deno",
      "run",
      "--allow-all",
      "--node-modules-dir=manual",
      script,
      pkg,
    ],
  };

/** Load every target under every available runtime, recording pass/fail. */
function loadMatrix(
  consumer: string,
  scriptPath: string,
  targets: Target[],
  runtimes: Runtime[],
): ObservedMatrix {
  const observed: ObservedMatrix = {};
  for (const t of targets) {
    const row: Partial<Record<Runtime, LoadResult>> = {};
    observed[t.name] = row;
    for (const runtime of runtimes) {
      const proc = Bun.spawnSync(RUNTIME_CMD[runtime](scriptPath, t.name), {
        cwd: consumer,
        stdout: "pipe",
        stderr: "pipe",
      });
      const result: LoadResult = proc.exitCode === 0 ? "load" : "fail";
      row[runtime] = result;
      const reason = RUNTIME_INCOMPATIBLE[t.name]?.[runtime];
      const tag =
        result === "fail" && reason !== undefined ? "fail (expected)" : result;
      console.log(`    ${runtime.padEnd(5)} ${t.name.padEnd(40)} ${tag}`);
      if (result === "fail" && reason === undefined) {
        console.log(`      ${proc.stderr.toString().trim().split("\n")[0]}`);
      }
    }
  }
  return observed;
}

async function main(repoRoot: string, execute: boolean): Promise<void> {
  console.log(execute ? "publish: EXECUTE mode" : "publish: dry run");

  // Guards on the committed state — must hold before anything is built.
  requireCleanTree(repoRoot);
  const version = releaseVersion(repoRoot);
  console.log(`release version: ${version}`);

  const targets = readTargets(repoRoot);
  console.log(`targets: ${targets.length} non-private packages`);

  const versionViolations = checkVersionSync(targets, version);
  const depViolations = checkInternalDepExpressions(targets);
  const invariantErrors = [...versionViolations, ...depViolations];
  if (invariantErrors.length > 0) {
    throw new Error(
      `publish: version invariants violated:\n${invariantErrors.join("\n")}`,
    );
  }

  const shape = await checkWorkspaceExports(repoRoot);
  if (shape.violations.length > 0) {
    throw new Error(
      `publish: exports shape violations:\n${shape.violations.join("\n")}`,
    );
  }
  const metadata = await checkWorkspaceMetadata(repoRoot);
  if (metadata.violations.length > 0) {
    throw new Error(
      `publish: publish metadata violations:\n${metadata.violations.join("\n")}`,
    );
  }

  const ordered = topoSortLeafFirst(targets);
  console.log(
    `publish order (leaf-first): ${ordered.map((t) => t.name).join(", ")}`,
  );

  // Fail fast, before building 28 dist bundles, if the lockfile lags the bump.
  assertLockfileFresh(targets, version);

  const license = join(repoRoot, "LICENSE");
  const copiedLicenses: string[] = [];
  const scratch = mkdtempSync(join(tmpdir(), "intx-publish-"));
  try {
    // Emit compiled dist for every target.
    await buildDist(repoRoot);

    // Stage the LICENSE the files allowlist expects (not committed per-package).
    for (const t of ordered) {
      const dest = join(t.dir, "LICENSE");
      cpSync(license, dest);
      copiedLicenses.push(dest);
    }

    // Pack every target, then install the whole set together so internal
    // @intx/* dependencies resolve to the just-packed tarballs, never the
    // public registry (which still holds the broken 0.1.x).
    const tarballs = join(scratch, "tarballs");
    const consumer = join(scratch, "consumer");
    run(["mkdir", "-p", tarballs, consumer], repoRoot);
    for (const t of ordered) {
      run(["bun", "pm", "pack", "--destination", tarballs, "--quiet"], t.dir);
    }
    const tgz = readdirSync(tarballs)
      .filter((f) => f.endsWith(".tgz"))
      .map((f) => join(tarballs, f));
    console.log(`packed ${tgz.length} tarballs`);
    run(["npm", "init", "-y"], consumer);
    // No --silent: on failure the run() helper surfaces npm's own diagnostic
    // (which package at which version 404'd), which is the actionable detail.
    run(["npm", "install", "--no-audit", "--no-fund", ...tgz], consumer);

    // Load every package under each available runtime; assert the matrix.
    // A runtime not on PATH is skipped (assertMatrix ignores it), so the
    // printed runtime list is the record of what was actually verified.
    const scriptPath = join(consumer, "load-check.mjs");
    await Bun.write(scriptPath, LOAD_CHECK);
    const runtimes = availableRuntimes();
    console.log(`load smoke (runtimes: ${runtimes.join(", ")}):`);
    const observed = loadMatrix(consumer, scriptPath, ordered, runtimes);
    const matrixViolations = assertMatrix(observed);
    if (matrixViolations.length > 0) {
      throw new Error(
        `publish: load matrix failed:\n${matrixViolations.join("\n")}`,
      );
    }
    console.log(
      `load smoke: ok (${ordered.length} packages; asserted ${[...ASSERTED_RUNTIMES].join(", ")})`,
    );

    if (execute) {
      for (const t of ordered) {
        console.log(`publishing ${t.name}@${version}`);
        run(["bun", "publish"], t.dir);
      }
      console.log(
        `publish: published ${ordered.length} packages at ${version}`,
      );
    } else {
      console.log(
        `publish: dry run complete; would publish ${ordered.length} packages at ${version}`,
      );
    }
  } finally {
    for (const dest of copiedLicenses) rmSync(dest, { force: true });
    for (const t of ordered) {
      rmSync(join(t.dir, "dist"), { recursive: true, force: true });
    }
    rmSync(scratch, { recursive: true, force: true });
  }
}

if (import.meta.main) {
  if (import.meta.dirname === undefined) {
    throw new Error(
      "publish: import.meta.dirname is undefined; cannot locate the repository root",
    );
  }
  await main(
    join(import.meta.dirname, ".."),
    process.argv.includes("--execute"),
  );
}
