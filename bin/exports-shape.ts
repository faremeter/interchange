#!/usr/bin/env bun
/* eslint-disable no-console */

// Exports-shape guard for the published workspace packages.
//
// Every non-private `packages/*` ships compiled `dist` to npm while the
// repo consumes its TypeScript source. Each `exports` subpath therefore
// carries three conditions, in order:
//
//   "<subpath>": {
//     "intx-src": "./src/<stem>.ts",     // in-repo resolvers (see
//                                         //   tsconfig customConditions,
//                                         //   the BUN variable, vite)
//     "types":    "./dist/<stem>.d.ts",   // TypeScript for consumers
//     "default":  "./dist/<stem>.js"      // runtime for consumers
//   }
//
// The three targets must name the same logical module (`<stem>`), or an
// external consumer's types and runtime diverge. This module is both the
// one-time transform that produces that shape and the invariant check
// that keeps it true: a package added later with a hand-written two-key
// exports block would otherwise regress silently to publish semantics in
// the dev loop, where no `dist` exists.
//
// `expectedConditions` is the shared derivation used by both directions
// and is exported for tests. Run with no argument to check (non-zero on
// violations); run with `--fix` to rewrite every non-private package's
// `exports` in place. Private packages are left untouched.

import { join } from "node:path";
import { type } from "arktype";

import { readWorkspacePackages } from "./lib/packages";

const SRC_CONDITION = "intx-src";

// One exports subpath's condition object: condition name -> target path.
const conditionMap = type({ "[string]": "string" }).narrow((value, ctx) =>
  Array.isArray(value) ? ctx.mustBe("a non-array object") : true,
);
const manifestSchema = type({
  "name?": "string",
  "private?": "boolean",
  "exports?": type({ "[string]": conditionMap }).narrow((value, ctx) =>
    Array.isArray(value) ? ctx.mustBe("a non-array object") : true,
  ),
});
type Manifest = typeof manifestSchema.infer;

// The whole manifest as an opaque record, so a rewrite can replace
// `exports` while preserving every other field untouched.
const rawObjectSchema = type({ "[string]": "unknown" }).narrow((value, ctx) =>
  Array.isArray(value) ? ctx.mustBe("a non-array object") : true,
);

/** The source target (`./src/<stem>.ts`) a subpath resolves in the repo,
 *  taken from whichever condition names it. Pre-transform that is the
 *  `default`; post-transform it is `intx-src`. Returns null when no
 *  condition points at `./src/*.ts` (e.g. a hand-written entry to flag). */
function sourceTarget(conditions: Record<string, string>): string | null {
  for (const target of Object.values(conditions)) {
    if (/^\.\/src\/.+\.tsx?$/.test(target)) return target;
  }
  return null;
}

/** The canonical three-condition object for a `./src/<stem>.ts` target.
 *  Insertion order is the resolution order consumers see. */
export function expectedConditions(srcTarget: string): Record<string, string> {
  const match = srcTarget.match(/^\.\/src\/(.+)\.tsx?$/);
  if (match === null) {
    throw new Error(
      `exports-shape: expected a "./src/<stem>.ts" target, got ${JSON.stringify(srcTarget)}`,
    );
  }
  const stem = match[1];
  return {
    [SRC_CONDITION]: srcTarget,
    types: `./dist/${stem}.d.ts`,
    default: `./dist/${stem}.js`,
  };
}

/** True when a subpath's conditions already match the canonical shape for
 *  their own source target. */
function conforms(conditions: Record<string, string>): boolean {
  const src = sourceTarget(conditions);
  if (src === null) return false;
  const expected = expectedConditions(src);
  const expectedKeys = Object.keys(expected);
  const keys = Object.keys(conditions);
  // Insertion order is the resolution order consumers see (`types` must
  // precede `default`), so it is part of the shape, not just the key set.
  return (
    keys.length === expectedKeys.length &&
    keys.every((k, i) => k === expectedKeys[i] && conditions[k] === expected[k])
  );
}

async function readManifest(path: string): Promise<Manifest> {
  const parsed = manifestSchema(await Bun.file(path).json());
  if (parsed instanceof type.errors) {
    throw new Error(
      `exports-shape: ${path} is not a well-formed manifest: ${parsed.summary}`,
    );
  }
  return parsed;
}

export type ExportsReport = { violations: string[]; packageCount: number };

/** Check every non-private package's `exports` against the canonical
 *  three-condition shape. */
export async function checkWorkspaceExports(
  repoRoot: string,
): Promise<ExportsReport> {
  const violations: string[] = [];
  let packageCount = 0;
  for (const { manifestPath } of readWorkspacePackages(repoRoot)) {
    const manifest = await readManifest(manifestPath);
    packageCount += 1;
    const self = manifest.name ?? manifestPath;
    for (const [subpath, conditions] of Object.entries(
      manifest.exports ?? {},
    )) {
      const src = sourceTarget(conditions);
      if (src === null) {
        violations.push(
          `${self}: exports["${subpath}"] has no "./src/*.ts" target to anchor the shape`,
        );
        continue;
      }
      if (!conforms(conditions)) {
        violations.push(
          `${self}: exports["${subpath}"] is not the canonical shape ${JSON.stringify(expectedConditions(src))}`,
        );
      }
    }
  }
  return { violations, packageCount };
}

/** Rewrite every non-private package's `exports` to the canonical shape,
 *  in place, deriving each subpath's stem from its existing source target.
 *  Returns the packages changed. */
export async function fixWorkspaceExports(repoRoot: string): Promise<string[]> {
  const changed: string[] = [];
  for (const { manifestPath: path } of readWorkspacePackages(repoRoot)) {
    const manifest = await readManifest(path);
    if (manifest.exports === undefined) continue;
    const raw = rawObjectSchema(await Bun.file(path).json());
    if (raw instanceof type.errors) {
      throw new Error(
        `exports-shape: ${path} is not a well-formed manifest: ${raw.summary}`,
      );
    }
    const nextExports: Record<string, Record<string, string>> = {};
    let mutated = false;
    for (const [subpath, conditions] of Object.entries(manifest.exports)) {
      const src = sourceTarget(conditions);
      if (src === null) {
        throw new Error(
          `exports-shape: cannot fix ${manifest.name ?? path}: exports["${subpath}"] has no "./src/*.ts" target`,
        );
      }
      const expected = expectedConditions(src);
      nextExports[subpath] = expected;
      if (!conforms(conditions)) mutated = true;
    }
    if (!mutated) continue;
    raw["exports"] = nextExports;
    await Bun.write(path, JSON.stringify(raw, null, 2) + "\n");
    changed.push(manifest.name ?? path);
  }
  return changed;
}

if (import.meta.main) {
  if (import.meta.dirname === undefined) {
    throw new Error(
      "exports-shape: import.meta.dirname is undefined; cannot locate the repository root",
    );
  }
  const repoRoot = join(import.meta.dirname, "..");
  if (process.argv.includes("--fix")) {
    const changed = await fixWorkspaceExports(repoRoot);
    for (const name of changed) console.log(`  rewrote ${name}`);
    console.log(`exports-shape: rewrote ${changed.length} package(s)`);
  } else {
    const { violations, packageCount } = await checkWorkspaceExports(repoRoot);
    if (violations.length > 0) {
      console.error(`exports-shape: ${violations.length} violation(s)\n`);
      for (const v of violations) console.error(`  - ${v}`);
      console.error(
        `\nRun \`bun bin/exports-shape.ts --fix\` to rewrite exports to the canonical shape.`,
      );
      process.exit(1);
    }
    console.log(`exports-shape: ok (${packageCount} non-private package(s))`);
  }
}
