import { afterEach, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import {
  checkTestEnumeration,
  enumeratedPaths,
  isCovered,
  recipeLines,
} from "./check-test-enumeration";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0))
    rmSync(root, { recursive: true, force: true });
});

function makeRepo(files: Record<string, string>): string {
  const root = mkdtempSync(join(tmpdir(), "check-test-enumeration-"));
  roots.push(root);
  for (const [rel, content] of Object.entries(files)) {
    const path = join(root, rel);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, content);
  }
  return root;
}

// A Makefile whose recipes enumerate a directory and files the way the real
// one does; tabs are literal so the parser sees real make recipe syntax (a
// run of tab-prefixed lines under the target header). This fixture drives the
// generic parser tests (recipeLines, enumeratedPaths), which take explicit
// target names, so its target names are illustrative rather than the real
// ones.
const MAKEFILE = [
  "test: FORCE",
  "\t$(BUN) test packages/ tests/covered-dir/",
  "\t$(BUN) test --timeout 120000 tests/wf/enumerated.test.ts tests/other/",
  "",
  "test-load: FORCE",
  "\t$(BUN) test --timeout 300000 tests/wf/load.test.ts",
  "",
].join("\n");

// checkTestEnumeration reads the module-level ENUMERATING_TARGETS, so its
// fixture must use the real target names: the `test` aggregate carries no
// recipe, and the enumerated passes live on test-unit/test-workflow/test-core
// plus the standalone test-load. This enumerates the same paths as MAKEFILE so
// the coverage assertions match.
const SPLIT_MAKEFILE = [
  "test: test-unit test-workflow test-core",
  "",
  "test-unit: FORCE",
  "\t$(BUN) test packages/ tests/covered-dir/",
  "",
  "test-workflow: FORCE",
  "\t$(BUN) test --timeout 120000 tests/wf/enumerated.test.ts tests/other/",
  "",
  "test-core: FORCE",
  "\t$(BUN) test --timeout 120000 tests/core/",
  "",
  "test-load: FORCE",
  "\t$(BUN) test --timeout 300000 tests/wf/load.test.ts",
  "",
].join("\n");

test("recipeLines returns a target's tab-indented lines and stops at the next target", () => {
  expect(recipeLines(MAKEFILE, "test")).toEqual([
    "\t$(BUN) test packages/ tests/covered-dir/",
    "\t$(BUN) test --timeout 120000 tests/wf/enumerated.test.ts tests/other/",
  ]);
  // The `test` header must not swallow the sibling `test-load` recipe.
  expect(recipeLines(MAKEFILE, "test-load")).toEqual([
    "\t$(BUN) test --timeout 300000 tests/wf/load.test.ts",
  ]);
});

test("enumeratedPaths keeps path tokens and drops the command, flags, and timeout", () => {
  expect(enumeratedPaths(MAKEFILE, ["test", "test-load"]).sort()).toEqual([
    "packages/",
    "tests/covered-dir/",
    "tests/other/",
    "tests/wf/enumerated.test.ts",
    "tests/wf/load.test.ts",
  ]);
});

test("isCovered matches a file under an enumerated directory and an exact file", () => {
  const enumerated = ["packages/", "tests/wf/enumerated.test.ts"];
  expect(isCovered("packages/a/b.test.ts", enumerated)).toBe(true);
  expect(isCovered("tests/wf/enumerated.test.ts", enumerated)).toBe(true);
  expect(isCovered("tests/wf/other.test.ts", enumerated)).toBe(false);
  // A directory match must be a whole path segment, not a substring prefix.
  expect(isCovered("packages-extra/x.test.ts", ["packages/"])).toBe(false);
});

test("checkTestEnumeration flags an un-enumerated test file and passes covered ones", () => {
  const root = makeRepo({
    Makefile: SPLIT_MAKEFILE,
    "packages/p/a.test.ts": "",
    "tests/covered-dir/x.test.ts": "",
    "tests/wf/enumerated.test.ts": "",
    "tests/wf/load.test.ts": "",
    "tests/wf/orphan.test.ts": "",
  });
  const report = checkTestEnumeration(root);
  expect(report.unenumerated).toEqual(["tests/wf/orphan.test.ts"]);
  expect(report.testFileCount).toBe(5);
});

test("checkTestEnumeration skips node_modules and dist test files", () => {
  const root = makeRepo({
    Makefile: SPLIT_MAKEFILE,
    "packages/p/node_modules/dep/x.test.ts": "",
    "packages/p/dist/built.test.ts": "",
  });
  const report = checkTestEnumeration(root);
  expect(report.unenumerated).toEqual([]);
  expect(report.testFileCount).toBe(0);
});

test("checkTestEnumeration throws when the Makefile yields no enumerated paths", () => {
  const root = makeRepo({
    Makefile: "test: FORCE\n\techo nothing-here\n",
    "tests/wf/orphan.test.ts": "",
  });
  expect(() => checkTestEnumeration(root)).toThrow(/no enumerated paths/);
});
