#!/usr/bin/env bun
/* eslint-disable no-console */

// Launcher-condition guard.
//
// The published `@intx/*` packages resolve to compiled `dist` by default;
// in this repo no `dist` is built, so every bun process that resolves
// `@intx/*` must pass `--conditions=intx-src` to reach the TypeScript
// source. `make` (via the `BUN` variable), `tsc` (customConditions), and
// vite set that structurally, in one place each. The surfaces where the
// flag is hand-written and can silently rot are the launchers invoked
// directly; this guard fails `make lint` when one runs an `@intx/*`
// importer without the flag:
//
//   1. `bin/*` bash wrappers — the flag rides on the `bun <script>.ts`
//      exec line.
//   2. `apps/*/bin` and `packages/*/bin` shebang binaries run by path
//      (e.g. the sidecar's workflow-child) — the flag rides on the
//      shebang, since `--conditions` is not inherited across a spawn.
//   3. `examples/*` and `apps/*` package.json `scripts` that `bun run` a
//      `.ts` — the flag rides on the script command.
//
// It never inspects the `bin/*.ts` entry points' own shebangs (inert under
// `bun x.ts`; they run through a wrapper that supplies the flag), make-
// invoked scripts (the `BUN` variable covers those), or `bin/lib` helpers
// (never a launcher target).
//
// The guard also catches the wrapper's other hazard: an extensionless
// wrapper `bin/<name>` and its `bin/<name>.ts` share a basename, so a
// top-level `bin/*.ts` importing `./<name>` resolves to the bash wrapper,
// not the `.ts`, and bun parses shell as TypeScript. It fails any such
// `./<name>` import and names the `bin/lib/` remedy.
//
// Scope boundary: the bun-invocation match covers the command positions
// launchers actually use (line start, after a separator, or `exec`). A
// `bun` invocation behind an env-assignment, `command`, or `time` prefix
// is deliberately not matched — no launcher uses those forms, and widening
// the match to every shell command position would reimplement shell
// parsing. Prose commands in docs are likewise out of scope.
//
// `checkLaunchers` runs the scans and is exported for tests, along with the
// pure `bunTsTarget`/`scanLauncher`/`importsIntx`/`shadowingImports` helpers;
// the CLI gate runs only when this file is the entry point.

import { existsSync, readFileSync, statSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import ts from "typescript";

const FLAG = "--conditions=intx-src";

// A `bun` command at a command position: line start, after a shell
// separator (`;`, `&`, `|`, `(`), or after `exec`. This excludes `bun`
// appearing inside a string (`log::info "running bun install"`) or as an
// argument (`command -v bun`).
const BUN_INVOCATION = /(?:^\s*|[;&|(]\s*|\bexec\s+)bun\b/;

// The condition flag as a whole token, in either `=` or space form.
const HAS_FLAG = /--conditions[= ]intx-src\b/;

/** True when a TypeScript source imports any `@intx/*` package directly.
 *  Imports are extracted with the TypeScript pre-processor, so an `@intx`
 *  mention inside a string or comment is not counted. */
export function importsIntx(code: string): boolean {
  for (const imported of ts.preProcessFile(code, true, true).importedFiles) {
    if (imported.fileName === "@intx" || imported.fileName.startsWith("@intx/"))
      return true;
  }
  return false;
}

/** The repo-root-relative `.ts` file a launcher line runs `bun` on, or null
 *  when the line does not invoke bun on a `.ts` target (`bun install`,
 *  `bun pm pack`, a `bun` mention inside a string). `wrapperRel` is the
 *  launcher's own repo-root-relative path, used to resolve the
 *  `$(dirname -- "${BASH_SOURCE[0]}")/x.ts` form against the wrapper's own
 *  directory. Throws when a `.ts` target is present but its path uses a form
 *  this guard cannot resolve — an unparsed launcher is an unguarded one. */
export function bunTsTarget(line: string, wrapperRel: string): string | null {
  if (!BUN_INVOCATION.test(line)) return null;
  // Strip the condition flag (either form) before locating the target: the
  // space form `--conditions intx-src` would otherwise leave a bare
  // `intx-src` token the target parser could misread. The flag's presence
  // is detected separately, by HAS_FLAG on the original line.
  const cleaned = line.replace(/--conditions[= ]intx-src/g, " ");
  const base = cleaned.match(/([\w.-]+\.ts)\b/)?.[1];
  if (base === undefined) return null;
  // `$(dirname -- "${BASH_SOURCE[0]}")/<name>.ts` -> the wrapper's own dir.
  if (cleaned.includes("BASH_SOURCE")) return join(dirname(wrapperRel), base);
  // `$REPODIR/<path>.ts` -> path relative to the repo root.
  const repoDir = cleaned.match(/\$REPODIR\/([\w./-]+\.ts)\b/)?.[1];
  if (repoDir !== undefined) return repoDir;
  // bare `bun [run] [--flags] <path>.ts` -> path relative to the repo root.
  const bare = cleaned.match(
    /bun\s+(?:run\s+)?(?:--[\w=.-]+\s+)*["']?([\w./-]+\.ts)\b/,
  )?.[1];
  if (bare !== undefined) return bare;
  throw new Error(
    `check-launchers: ${wrapperRel} runs bun on "${base}" but its path form is unrecognized: ${line.trim()}`,
  );
}

/** The single-segment relative import specifiers in `code` that would
 *  resolve to an extensionless launcher wrapper — a `./<name>` whose `<name>`
 *  is a wrapper basename — and so shadow the sibling `<name>.ts`. Only bare
 *  `./<name>` forms collide; `./lib/<name>` resolves elsewhere and an
 *  explicit `./<name>.ts` names the `.ts` directly, so neither is a hit. */
export function shadowingImports(
  code: string,
  wrapperNames: Set<string>,
): string[] {
  const hits: string[] = [];
  for (const imported of ts.preProcessFile(code, true, true).importedFiles) {
    const name = imported.fileName.match(/^\.\/([\w.-]+)$/)?.[1];
    if (name !== undefined && wrapperNames.has(name))
      hits.push(imported.fileName);
  }
  return hits;
}

export type LauncherCandidate = {
  line: string;
  target: string;
  hasFlag: boolean;
};

/** Every `bun <script>.ts` invocation in a launcher's text, with whether the
 *  condition flag is present. Comment lines (including the shebang) are
 *  skipped. */
export function scanLauncher(
  text: string,
  wrapperRel: string,
): LauncherCandidate[] {
  const candidates: LauncherCandidate[] = [];
  for (const raw of text.split("\n")) {
    if (/^\s*#/.test(raw)) continue;
    // Drop a trailing ` # ...` comment so a `.ts` mentioned there is not
    // read as a bun target.
    const line = raw.replace(/\s#.*$/, "");
    const target = bunTsTarget(line, wrapperRel);
    if (target === null) continue;
    candidates.push({
      line: line.trim(),
      target,
      hasFlag: HAS_FLAG.test(line),
    });
  }
  return candidates;
}

/** The `.ts` target a package.json `scripts` command runs `bun` on, relative
 *  to the package directory (scripts run with the package as cwd), or null
 *  when the command does not `bun run` a `.ts`. */
export function scriptBunTsTarget(cmd: string): string | null {
  if (!/\bbun\b/.test(cmd)) return null;
  const cleaned = cmd.replace(/--conditions[= ]intx-src/g, " ");
  return (
    cleaned.match(
      /\bbun\s+(?:run\s+)?(?:--[\w=.-]+\s+)*["']?([\w./-]+\.ts)\b/,
    )?.[1] ?? null
  );
}

export type LauncherReport = {
  violations: string[];
  launcherCount: number;
  guardedCount: number;
};

/** True when a top-level `bin/` file is a shell-script launcher (a shell
 *  shebang, no `.ts` extension). */
function isShellLauncher(abs: string): boolean {
  if (abs.endsWith(".ts") || !statSync(abs).isFile()) return false;
  const text = readFileSync(abs, "utf8");
  const nl = text.indexOf("\n");
  const shebang = nl === -1 ? text : text.slice(0, nl);
  return /^#!.*\b(?:bash|sh)\b/.test(shebang);
}

/** Scan every launcher surface (bin bash wrappers, app/package shebang
 *  binaries, and example/app package.json scripts) and require
 *  `--conditions=intx-src` on each `bun` invocation whose `.ts` target imports
 *  `@intx/*`; also fail any top-level `bin/*.ts` that imports a `./<name>` a
 *  wrapper shadows. Throws when a bin wrapper's bun target cannot be resolved
 *  to an existing file. */
export function checkLaunchers(repoRoot: string): LauncherReport {
  const violations: string[] = [];
  let launcherCount = 0;
  let guardedCount = 0;

  const entries = [...new Bun.Glob("bin/*").scanSync(repoRoot)].sort();

  // Pass 1: the wrapper set — every top-level shell-script launcher. Built
  // first because the shadowing check (pass 3) tests imports against it.
  const wrapperNames = new Set<string>();
  const launchers: string[] = [];
  for (const rel of entries) {
    if (!isShellLauncher(join(repoRoot, rel))) continue;
    wrapperNames.add(basename(rel));
    launchers.push(rel);
    launcherCount += 1;
  }

  // Pass 2: each launcher's bun `.ts` invocations must carry the flag when
  // their target imports `@intx/*`.
  for (const rel of launchers) {
    const text = readFileSync(join(repoRoot, rel), "utf8");
    for (const candidate of scanLauncher(text, rel)) {
      const targetAbs = join(repoRoot, candidate.target);
      if (!existsSync(targetAbs)) {
        throw new Error(
          `check-launchers: ${rel} runs bun on ${candidate.target}, which does not exist`,
        );
      }
      if (!importsIntx(readFileSync(targetAbs, "utf8"))) continue;
      guardedCount += 1;
      if (!candidate.hasFlag) {
        violations.push(
          `${rel}: execs \`bun ${candidate.target}\` (which imports @intx/*) ` +
            `without ${FLAG}. In-repo @intx/* has no dist, so bun selects the ` +
            `published \`default\` (compiled dist) and fails to resolve. Add ` +
            `${FLAG} to the exec line, matching bin/dev.`,
        );
      }
    }
  }

  // Pass 3: no top-level `bin/*.ts` may import a `./<name>` that a wrapper
  // shadows. Only top-level files collide — a `bin/lib/*.ts` import resolves
  // against `bin/lib`, where no wrapper lives.
  for (const rel of entries) {
    if (!rel.endsWith(".ts")) continue;
    const abs = join(repoRoot, rel);
    if (!statSync(abs).isFile()) continue;
    for (const spec of shadowingImports(
      readFileSync(abs, "utf8"),
      wrapperNames,
    )) {
      const name = spec.slice(2);
      violations.push(
        `${rel}: imports "${spec}", which resolves to the launcher wrapper ` +
          `bin/${name}, not bin/${name}.ts (bun would parse the shell wrapper ` +
          `as TypeScript). Move the shared code into bin/lib/ and import it ` +
          `from there.`,
      );
    }
  }

  // Pass 4: shebang binaries under `apps/*/bin` and `packages/*/bin` run by
  // path, so `--conditions` is not inherited and rides on the shebang alone.
  const shebangLaunchers = [
    ...new Bun.Glob("apps/*/bin/*").scanSync(repoRoot),
    ...new Bun.Glob("packages/*/bin/*").scanSync(repoRoot),
  ].sort();
  for (const rel of shebangLaunchers) {
    const abs = join(repoRoot, rel);
    if (!statSync(abs).isFile()) continue;
    const text = readFileSync(abs, "utf8");
    const nl = text.indexOf("\n");
    const shebang = nl === -1 ? text : text.slice(0, nl);
    if (!/^#!.*\bbun\b/.test(shebang)) continue;
    launcherCount += 1;
    if (!importsIntx(text)) continue;
    guardedCount += 1;
    if (!HAS_FLAG.test(shebang)) {
      violations.push(
        `${rel}: its bun shebang runs an @intx/* importer without ${FLAG}. ` +
          `This binary is executed by path, so the flag must ride on the ` +
          `shebang: #!/usr/bin/env -S bun ${FLAG}.`,
      );
    }
  }

  // Pass 5: package.json `scripts` under `examples/*` and `apps/*` that
  // `bun run` a `.ts` importing @intx/*. Scripts run with the package dir as
  // cwd, so the target resolves against that dir.
  const manifests = [
    ...new Bun.Glob("examples/*/package.json").scanSync(repoRoot),
    ...new Bun.Glob("apps/*/package.json").scanSync(repoRoot),
  ].sort();
  for (const rel of manifests) {
    const pkgDir = dirname(rel);
    const parsed: unknown = JSON.parse(
      readFileSync(join(repoRoot, rel), "utf8"),
    );
    const scripts =
      typeof parsed === "object" && parsed !== null && "scripts" in parsed
        ? (parsed as { scripts: unknown }).scripts
        : undefined;
    if (typeof scripts !== "object" || scripts === null) continue;
    for (const [scriptName, cmd] of Object.entries(scripts)) {
      if (typeof cmd !== "string") continue;
      const target = scriptBunTsTarget(cmd);
      if (target === null) continue;
      const targetAbs = join(repoRoot, pkgDir, target);
      if (!existsSync(targetAbs)) continue;
      if (!importsIntx(readFileSync(targetAbs, "utf8"))) continue;
      guardedCount += 1;
      if (!HAS_FLAG.test(cmd)) {
        violations.push(
          `${rel} script "${scriptName}" runs \`${cmd}\` (${target} imports ` +
            `@intx/*) without ${FLAG}, so it resolves @intx/* to a dist that ` +
            `is never built. Add the flag: bun ${FLAG} run ${target}.`,
        );
      }
    }
  }

  return { violations, launcherCount, guardedCount };
}

if (import.meta.main) {
  if (import.meta.dirname === undefined) {
    throw new Error(
      "check-launchers: import.meta.dirname is undefined; cannot locate the repository root",
    );
  }
  const repoRoot = join(import.meta.dirname, "..");
  const { violations, launcherCount, guardedCount } = checkLaunchers(repoRoot);
  if (violations.length > 0) {
    console.error(`check-launchers: ${violations.length} violation(s)\n`);
    for (const v of violations) console.error(`  - ${v}`);
    console.error(
      `\nEvery launcher that runs a bun target importing @intx/* must pass ` +
        `${FLAG} (make-invoked scripts get it from the Makefile's $(BUN) ` +
        `variable).`,
    );
    process.exit(1);
  }
  console.log(
    `check-launchers: ok (${launcherCount} launcher(s), ${guardedCount} @intx target(s) guarded)`,
  );
}
