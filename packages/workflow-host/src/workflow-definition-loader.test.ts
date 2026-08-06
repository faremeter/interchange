import { afterEach, describe, expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { loadWorkflowDefinitionFromClosure } from "./workflow-definition-loader";

// The workflow package the fixture entry modules import
// `@intx/workflow/definition` from. A materialized closure lays this out
// under the package's `node_modules/`; the fixtures below symlink it so
// the entry's bare-specifier import resolves the same way.
const WORKFLOW_PACKAGE_DIR = path.resolve(import.meta.dir, "../../workflow");

const createdDirs: string[] = [];

afterEach(async () => {
  while (createdDirs.length > 0) {
    const dir = createdDirs.pop();
    if (dir === undefined) continue;
    await fs.rm(dir, { recursive: true, force: true });
  }
});

interface ClosureFixtureSpec {
  /** Value written to `interchange.workflow`; omitted when `null`. */
  readonly workflowEntry: string | null;
  /** Relative path the entry source is written to under the package. */
  readonly entryRelPath?: string;
  /** Source of the entry module. */
  readonly entrySource?: string;
}

async function createClosureFixture(spec: ClosureFixtureSpec): Promise<string> {
  const packageDir = await fs.mkdtemp(path.join(os.tmpdir(), "wf-closure-"));
  createdDirs.push(packageDir);

  // Lay out `node_modules/@intx/workflow` the way the closure
  // machinery would, so the entry module's `@intx/workflow/definition`
  // import resolves.
  const scopeDir = path.join(packageDir, "node_modules", "@intx");
  await fs.mkdir(scopeDir, { recursive: true });
  await fs.symlink(
    WORKFLOW_PACKAGE_DIR,
    path.join(scopeDir, "workflow"),
    "dir",
  );

  const pkgJson: Record<string, unknown> = {
    name: "@fixture/workflow-package",
    version: "1.0.0",
  };
  if (spec.workflowEntry !== null) {
    pkgJson.interchange = { workflow: spec.workflowEntry };
  }
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify(pkgJson, null, 2),
  );

  if (spec.entrySource !== undefined) {
    const entryRelPath = spec.entryRelPath ?? "workflow.js";
    const entryAbs = path.join(packageDir, entryRelPath);
    await fs.mkdir(path.dirname(entryAbs), { recursive: true });
    await fs.writeFile(entryAbs, spec.entrySource);
  }
  return packageDir;
}

const DEFAULT_EXPORT_ENTRY = `
import { defineWorkflow } from "@intx/workflow/definition";
export default defineWorkflow({
  id: "fixture-workflow",
  steps: {
    wait: { kind: "sleep", id: "", durationMs: 5 },
  },
});
`;

describe("loadWorkflowDefinitionFromClosure", () => {
  test("loads a fixture workflow package entry to a validated definition", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });

    const definition = await loadWorkflowDefinitionFromClosure({ packageDir });

    expect(definition.id).toBe("fixture-workflow");
    expect(definition.stepOrder).toEqual(["wait"]);
    expect(Array.isArray(definition.triggers)).toBe(true);
    // A workflow with no declared trigger normalizes to a manual trigger.
    expect(definition.triggers).toEqual([{ type: "manual" }]);
    expect(definition.steps.wait?.kind).toBe("sleep");
  });

  test("accepts a named export of the definition", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./nested/entry.js",
      entryRelPath: "nested/entry.js",
      entrySource: `
import { defineWorkflow } from "@intx/workflow/definition";
export const workflow = defineWorkflow({
  id: "named-export-workflow",
  steps: { only: { kind: "sleep", id: "", durationMs: 1 } },
});
`,
    });

    const definition = await loadWorkflowDefinitionFromClosure({ packageDir });

    expect(definition.id).toBe("named-export-workflow");
  });

  test("busts the ESM module cache with importCacheKey", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });

    const definition = await loadWorkflowDefinitionFromClosure({
      packageDir,
      importCacheKey: "sha512-fixture",
    });

    expect(definition.id).toBe("fixture-workflow");
  });

  test("rejects a package.json without an interchange.workflow field", async () => {
    const packageDir = await createClosureFixture({ workflowEntry: null });

    await expect(
      loadWorkflowDefinitionFromClosure({ packageDir }),
    ).rejects.toThrow(/no "interchange\.workflow" field/);
  });

  test("rejects an entry path that escapes the package directory", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "../escape.js",
    });

    await expect(
      loadWorkflowDefinitionFromClosure({ packageDir }),
    ).rejects.toThrow(/escapes the workflow package directory/);
  });

  test("rejects an entry that exports no WorkflowDefinition", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: `export const notADefinition = { hello: "world" };`,
    });

    await expect(
      loadWorkflowDefinitionFromClosure({ packageDir }),
    ).rejects.toThrow(
      /exported no value that validates as a WorkflowDefinition/,
    );
  });

  test("rejects an entry that exports more than one WorkflowDefinition", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: `
import { defineWorkflow } from "@intx/workflow/definition";
export const first = defineWorkflow({
  id: "first",
  steps: { a: { kind: "sleep", id: "", durationMs: 1 } },
});
export const second = defineWorkflow({
  id: "second",
  steps: { b: { kind: "sleep", id: "", durationMs: 1 } },
});
`,
    });

    await expect(
      loadWorkflowDefinitionFromClosure({ packageDir }),
    ).rejects.toThrow(/exported 2 WorkflowDefinition values/);
  });

  test("surfaces an import failure from the entry module", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./missing.js",
    });

    await expect(
      loadWorkflowDefinitionFromClosure({ packageDir }),
    ).rejects.toThrow(/could not be resolved|failed to import/);
  });
});
