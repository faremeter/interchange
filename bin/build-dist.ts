#!/usr/bin/env bun
/* eslint-disable no-console */

// Emit compiled ESM (`.js` + `.d.ts`) into each non-private workspace
// package's `dist/`, for npm distribution.
//
// The packages type-check with `noEmit` and are consumed inside the
// repo as `.ts` source (via the `intx-src` exports condition). For
// publication they must ship compiled JavaScript that runs on plain
// Node. This script produces that output per package:
//
//   1. Generate an ephemeral build tsconfig inside the package that
//      `extends` the package's own `tsconfig.json` — so each package's
//      real compiler options (including any that deviate from the base)
//      carry over — and overrides only the emit knobs.
//   2. Run `tsc -p` against it to emit `dist/*.js` and `dist/*.d.ts`.
//   3. Rewrite the emitted relative import specifiers to carry explicit
//      extensions (see `dist-rewrite`), which Node's ESM loader requires
//      and `tsc` under `moduleResolution: "bundler"` does not add.
//
// Packages emit independently: a package's `@intx/*` dependencies resolve
// to their `.ts` source through the exports map, not to a sibling `dist`,
// so emit order does not matter here. Leaf-first ordering is the concern
// of the publish path, not of emit.
//
// `dist/` is gitignored; nothing this script writes is committed. The
// generated build config is removed after each package, even on failure.
//
// Usage: `build-dist` emits every non-private package. `build-dist <name>
// [<name>...]` emits only the named packages (by `package.json#name`),
// for targeted verification.

import { rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { rewriteDistTree } from "./dist-rewrite";
import { readWorkspacePackages } from "./lib/packages";

const GENERATED_CONFIG = "tsconfig.dist.generated.json";

type Target = { name: string; dir: string };

/** Every non-private workspace package under `packages/*`, as
 *  `{ name, absolute dir }`, sorted by name for deterministic output. */
function publishTargets(repoRoot: string): Target[] {
  return readWorkspacePackages(repoRoot).map((p) => ({
    name: p.name,
    dir: p.dir,
  }));
}

/** The ephemeral per-package build config: inherit the package's own
 *  options, then override only what emit needs. `composite: false` keeps
 *  the emit a standalone one-shot (no `.tsbuildinfo`); `declarationMap`
 *  is off so no `.d.ts.map` litters `dist`; tests are excluded so they do
 *  not ship. */
function generatedConfig(): string {
  return JSON.stringify(
    {
      extends: "./tsconfig.json",
      compilerOptions: {
        noEmit: false,
        declaration: true,
        declarationMap: false,
        emitDeclarationOnly: false,
        composite: false,
        outDir: "./dist",
        rootDir: "./src",
      },
      include: ["src/**/*.ts"],
      exclude: ["**/*.test.ts"],
    },
    null,
    2,
  );
}

/** Emit and rewrite one package's `dist`. Throws on a `tsc` failure or an
 *  unresolved relative specifier so a broken emit stops the run. */
function emitPackage(repoRoot: string, target: Target): void {
  const configPath = join(target.dir, GENERATED_CONFIG);
  rmSync(join(target.dir, "dist"), { recursive: true, force: true });
  writeFileSync(configPath, generatedConfig());
  try {
    const tsc = Bun.spawnSync(
      [join(repoRoot, "node_modules", ".bin", "tsc"), "-p", configPath],
      { cwd: target.dir, stdout: "pipe", stderr: "pipe" },
    );
    if (tsc.exitCode !== 0) {
      const detail =
        tsc.stdout.toString().trim() || tsc.stderr.toString().trim();
      throw new Error(`build-dist: tsc failed for ${target.name}:\n${detail}`);
    }
    const { unresolved } = rewriteDistTree(join(target.dir, "dist"));
    if (unresolved.length > 0) {
      throw new Error(
        `build-dist: ${target.name} has ${unresolved.length} unresolved relative specifier(s):\n  ${unresolved.join("\n  ")}`,
      );
    }
  } finally {
    rmSync(configPath, { force: true });
  }
}

export async function buildDist(
  repoRoot: string,
  only?: readonly string[],
): Promise<Target[]> {
  const all = publishTargets(repoRoot);
  const selected =
    only && only.length > 0 ? all.filter((t) => only.includes(t.name)) : all;
  if (only && only.length > 0) {
    const missing = only.filter((n) => !all.some((t) => t.name === n));
    if (missing.length > 0) {
      throw new Error(
        `build-dist: no non-private package named ${missing.join(", ")}`,
      );
    }
  }
  for (const target of selected) emitPackage(repoRoot, target);
  return selected;
}

if (import.meta.main) {
  if (import.meta.dirname === undefined) {
    throw new Error(
      "build-dist: import.meta.dirname is undefined; cannot locate the repository root",
    );
  }
  const repoRoot = join(import.meta.dirname, "..");
  const emitted = await buildDist(repoRoot, process.argv.slice(2));
  for (const target of emitted) console.log(`  emitted ${target.name}/dist`);
  console.log(`build-dist: ok (${emitted.length} package(s))`);
}
