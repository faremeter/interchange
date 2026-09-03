import { afterEach, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { checkCiTestJobs } from "./check-ci-test-jobs";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0))
    rmSync(root, { recursive: true, force: true });
});

// Write a throwaway repo root carrying a single ci.yml at the path the guard
// reads. The guard resolves the enumerating targets from the module-level
// ENUMERATING_TARGETS, so a valid fixture must invoke test-unit/test-workflow/
// test-core (test-load is exempt) and gate every job behind "make all".
function makeRepo(ciYaml: string): string {
  const root = mkdtempSync(join(tmpdir(), "check-ci-test-jobs-"));
  roots.push(root);
  const path = join(root, ".github", "workflows", "ci.yml");
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, ciYaml);
  return root;
}

// A well-formed workflow: the three enumerated passes each run in a job, and
// the "make all" gate runs `always()` and needs every worker.
const VALID = `
jobs:
  static:
    steps:
      - run: make lint build build-admin-ui
  test-unit:
    steps:
      - run: make test-unit
  test-workflow:
    steps:
      - run: make test-workflow
  test-core:
    steps:
      - run: make test-core
  e2e:
    steps:
      - run: make test-e2e
  make-all:
    name: make all
    needs: [static, test-unit, test-workflow, test-core, e2e]
    if: \${{ always() }}
    steps:
      - run: "true"
`;

test("a well-formed workflow passes", () => {
  expect(checkCiTestJobs(makeRepo(VALID)).errors).toEqual([]);
});

test("flags a test pass that no job runs", () => {
  // test-core's job now runs test-unit instead, so nothing runs test-core.
  const yaml = VALID.replace(
    "      - run: make test-core",
    "      - run: make test-unit",
  );
  const errors = checkCiTestJobs(makeRepo(yaml)).errors;
  expect(errors).toHaveLength(1);
  expect(errors[0]).toContain('Makefile target "test-core"');
});

test("flags a job left out of the gate's needs", () => {
  const yaml = VALID.replace(
    "needs: [static, test-unit, test-workflow, test-core, e2e]",
    "needs: [static, test-unit, test-workflow, e2e]",
  );
  const errors = checkCiTestJobs(makeRepo(yaml)).errors;
  expect(errors).toHaveLength(1);
  expect(errors[0]).toContain(
    'CI job "test-core" is not in the "make all" gate',
  );
});

test("flags a gate that does not run always()", () => {
  const yaml = VALID.replace("    if: ${{ always() }}\n", "");
  const errors = checkCiTestJobs(makeRepo(yaml)).errors;
  expect(errors.some((e) => e.includes("if: always()"))).toBe(true);
});

test("flags a needs entry that names no real job", () => {
  const yaml = VALID.replace(
    "needs: [static, test-unit, test-workflow, test-core, e2e]",
    "needs: [static, test-unit, test-workflow, test-core, e2e, ghost]",
  );
  const errors = checkCiTestJobs(makeRepo(yaml)).errors;
  expect(errors.some((e) => e.includes('"ghost", which is not a job'))).toBe(
    true,
  );
});

test("flags a missing gate job", () => {
  // Rename the gate so no job produces the required "make all" context.
  const yaml = VALID.replace("    name: make all\n", "");
  const errors = checkCiTestJobs(makeRepo(yaml)).errors;
  expect(errors).toHaveLength(1);
  expect(errors[0]).toContain('required "make all" status check');
});
