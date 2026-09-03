#!/usr/bin/env bun
/* eslint-disable no-console */

// Test-enumeration completeness guard.
//
// The `make test-unit`, `test-workflow`, and `test-core` passes enumerate
// their test paths POSITIVELY: each hands `bun test` an explicit list of
// directories and files rather than letting bun discover everything and
// filtering. They have to -- bun 1.2.22's `[test].pathIgnorePatterns` is
// documented but non-functional, so positive enumeration is the only
// mechanism that keeps the parallel-unsafe integration tests out of the
// fast unit pass (see CONVENTIONS.md's Testing section).
//
// The failure mode that buys is silent: a new `*.test.ts` that nobody
// adds to the list is never run by `make all`, and can sit red
// indefinitely without anything noticing -- which is exactly what
// happened to the run-event batching suite. This guard closes that hole.
// It fails `make lint` when a test file under `packages/` or `tests/` is
// not reachable from the Makefile's `test` or `test-load` targets, so an
// un-enumerated file surfaces at lint time instead of by accident.
//
// The Makefile is the single source of truth: the guard parses the
// enumerated paths out of the target recipes rather than duplicating the
// list, so the two cannot drift. A path argument that is a directory
// covers every test file beneath it (the fast pass enumerates whole
// dirs, e.g. `packages/`); a path argument that is a `*.test.ts` file
// covers itself.
//
// Scope: the universe is `*.test.ts` under `packages/` and `tests/`.
// `apps/` and `bin/` test files are covered by their whole-directory
// enumeration in the fast pass and are not re-checked here. There is no
// silent exclusion list -- if a test file must be excluded, enumerate the
// reason here explicitly; today there are none. `node_modules` and built
// `dist` trees are not source and are skipped.

import { readFileSync } from "node:fs";
import { join } from "node:path";

/** Source roots whose `*.test.ts` files must be enumerated. */
const TEST_ROOTS = ["packages", "tests"] as const;

/**
 * Makefile targets whose recipes enumerate the test suite. The `test`
 * target is a prerequisite-only aggregate with no recipe of its own, so
 * the enumerating recipes live on the three passes it depends on plus the
 * standalone load target. `bin/check-ci-test-jobs.ts` reuses this list to
 * assert every pass (except `test-load`) actually runs in CI.
 */
export const ENUMERATING_TARGETS = [
  "test-unit",
  "test-workflow",
  "test-core",
  "test-load",
] as const;

/**
 * The TAB-indented recipe lines of a Makefile target. A recipe is the run
 * of tab-prefixed lines immediately following the `target:` line; the
 * first line that is not tab-prefixed ends it (matching make's own rule).
 */
export function recipeLines(makefile: string, target: string): string[] {
  const header = new RegExp(`^${target}:(?:\\s|$)`);
  const lines = makefile.split("\n");
  const recipe: string[] = [];
  let inTarget = false;
  for (const line of lines) {
    if (header.test(line)) {
      inTarget = true;
      continue;
    }
    if (!inTarget) continue;
    if (line.startsWith("\t")) recipe.push(line);
    else break;
  }
  return recipe;
}

/**
 * The enumerated path arguments across the given targets' `bun test`
 * recipe lines. A path argument is any whitespace-delimited token that
 * contains a `/` -- the command and flag tokens (`$(BUN)`, `test`,
 * `--timeout`, the numeric timeout) never do, and every enumerated
 * directory or file does. Returned verbatim (a trailing slash marks a
 * directory), de-duplicated.
 */
export function enumeratedPaths(
  makefile: string,
  targets: readonly string[],
): string[] {
  const paths = new Set<string>();
  for (const target of targets) {
    for (const line of recipeLines(makefile, target)) {
      if (!/\bbun\b|\$\(BUN\)/.test(line) || !/\btest\b/.test(line)) continue;
      for (const token of line.trim().split(/\s+/)) {
        if (token.includes("/")) paths.add(token);
      }
    }
  }
  return [...paths];
}

/**
 * True when `file` (a repo-root-relative POSIX path) is covered by an
 * enumerated path: it equals an enumerated `*.test.ts` file, or it lives
 * beneath an enumerated directory argument.
 */
export function isCovered(
  file: string,
  enumerated: readonly string[],
): boolean {
  for (const path of enumerated) {
    if (path.endsWith(".test.ts")) {
      if (path === file) return true;
      continue;
    }
    const dir = path.endsWith("/") ? path : `${path}/`;
    if (file.startsWith(dir)) return true;
  }
  return false;
}

export type EnumerationReport = {
  unenumerated: string[];
  testFileCount: number;
  enumeratedPathCount: number;
};

/**
 * Compare every `*.test.ts` under the source roots against the Makefile's
 * enumerated test paths. Throws when the Makefile yields no enumerated
 * paths at all -- that means the recipe format changed out from under the
 * parser, and flagging every test file would be a false alarm; failing
 * loud forces the parser to be repaired instead.
 */
export function checkTestEnumeration(repoRoot: string): EnumerationReport {
  const makefile = readFileSync(join(repoRoot, "Makefile"), "utf8");
  const enumerated = enumeratedPaths(makefile, ENUMERATING_TARGETS);
  if (enumerated.length === 0) {
    throw new Error(
      "check-test-enumeration: parsed no enumerated paths from the " +
        `Makefile ${ENUMERATING_TARGETS.join("/")} targets; the recipe ` +
        "format changed and the guard can no longer validate coverage",
    );
  }

  const testFiles: string[] = [];
  for (const root of TEST_ROOTS) {
    for (const rel of new Bun.Glob(`${root}/**/*.test.ts`).scanSync(repoRoot)) {
      if (rel.includes("node_modules/") || rel.includes("/dist/")) continue;
      testFiles.push(rel);
    }
  }

  const unenumerated = testFiles
    .filter((file) => !isCovered(file, enumerated))
    .sort();
  return {
    unenumerated,
    testFileCount: testFiles.length,
    enumeratedPathCount: enumerated.length,
  };
}

if (import.meta.main) {
  if (import.meta.dirname === undefined) {
    throw new Error(
      "check-test-enumeration: import.meta.dirname is undefined; cannot locate the repository root",
    );
  }
  const repoRoot = join(import.meta.dirname, "..");
  const { unenumerated, testFileCount, enumeratedPathCount } =
    checkTestEnumeration(repoRoot);
  if (unenumerated.length > 0) {
    console.error(
      `check-test-enumeration: ${unenumerated.length} test file(s) not enumerated in the Makefile\n`,
    );
    for (const file of unenumerated) console.error(`  - ${file}`);
    console.error(
      `\nEvery *.test.ts under ${TEST_ROOTS.join(", ")} must be reachable ` +
        `from the Makefile's ${ENUMERATING_TARGETS.map((t) => `'${t}'`).join(" or ")} ` +
        "target. bun's pathIgnorePatterns is non-functional, so the suite is " +
        "enumerated positively; a file left off the list is never run by " +
        "`make all` and can sit red unnoticed. Add each file above to the " +
        "appropriate pass (fast unit, slow integration, or test-load).",
    );
    process.exit(1);
  }
  console.log(
    `check-test-enumeration: ok (${String(testFileCount)} test file(s) ` +
      `covered by ${String(enumeratedPathCount)} enumerated path(s))`,
  );
}
