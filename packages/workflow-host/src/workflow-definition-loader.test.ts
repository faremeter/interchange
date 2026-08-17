import { afterEach, describe, expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  loadWorkflowDefinitionFromClosure,
  loadWorkflowDirectorRegistryFromClosure,
} from "./workflow-definition-loader";

// The workflow package the fixture entry modules import
// `@intx/workflow/definition` from. A materialized closure lays this out
// under the package's `node_modules/`; the fixtures below symlink it so
// the entry's bare-specifier import resolves the same way.
const WORKFLOW_PACKAGE_DIR = path.resolve(import.meta.dir, "../../workflow");
// The agent package a directors entry imports `defineDirector` from -- laid
// out the same way so the directors module's bare-specifier import resolves.
const AGENT_PACKAGE_DIR = path.resolve(import.meta.dir, "../../agent");

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
  /** Value written to `interchange.directors`; omitted when absent. */
  readonly directorsEntry?: string;
  /** Source of the directors module, written to `directorsEntry`'s path. */
  readonly directorsSource?: string;
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
  await fs.symlink(AGENT_PACKAGE_DIR, path.join(scopeDir, "agent"), "dir");

  const interchange: Record<string, unknown> = {};
  if (spec.workflowEntry !== null) {
    interchange.workflow = spec.workflowEntry;
  }
  if (spec.directorsEntry !== undefined) {
    interchange.directors = spec.directorsEntry;
  }
  const pkgJson: Record<string, unknown> = {
    name: "@fixture/workflow-package",
    version: "1.0.0",
  };
  if (Object.keys(interchange).length > 0) {
    pkgJson.interchange = interchange;
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
  if (spec.directorsSource !== undefined) {
    const rel = spec.directorsEntry ?? "./directors.js";
    const abs = path.join(packageDir, rel);
    await fs.mkdir(path.dirname(abs), { recursive: true });
    await fs.writeFile(abs, spec.directorsSource);
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

  test("rejects an absolute entry path at the string boundary", async () => {
    // An absolute entry must be rejected by the shared string-level
    // containment check before any realpath resolution -- the same rejection
    // the push-time asset validator makes -- so the two boundaries agree.
    const packageDir = await createClosureFixture({
      workflowEntry: "/pkg/index.js",
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

// A directors entry module exports the AnnotatedDirectorFactory (the
// defineDirector(...).factory), which is what the loader's structural check
// accepts and the registry stores -- not the { factory, build } wrapper.
const CUSTOM_DIRECTOR_ENTRY = `
import { defineDirector } from "@intx/agent";
export const custom = defineDirector({
  id: "@fixture/pkg/custom-director",
  configSchema: (config) => config,
  factory: () => ({
    async decide() {
      return { type: "wait" };
    },
  }),
}).factory;
`;

describe("loadWorkflowDirectorRegistryFromClosure", () => {
  test("composes the built-in default when the package ships no directors", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
    });

    // The built-in default resolves; a custom id the package did not ship
    // does not.
    expect(() => registry.resolve(registry.buildDefaultRef())).not.toThrow();
    expect(() =>
      registry.resolve({ id: "@fixture/pkg/custom-director", config: {} }),
    ).toThrow();
  });

  test("resolves a custom director the closure package ships", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      directorsEntry: "./directors.js",
      directorsSource: CUSTOM_DIRECTOR_ENTRY,
    });

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
    });

    const factory = registry.resolve({
      id: "@fixture/pkg/custom-director",
      config: {},
    });
    expect(factory.id).toBe("@fixture/pkg/custom-director");
    // The built-in default still resolves alongside the custom director.
    expect(() => registry.resolve(registry.buildDefaultRef())).not.toThrow();
  });

  test("throws when the directors module exports no director factory", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      directorsEntry: "./directors.js",
      directorsSource: `export const notADirector = { hello: "world" };`,
    });

    await expect(
      loadWorkflowDirectorRegistryFromClosure({ packageDir }),
    ).rejects.toThrow(/exported no AnnotatedDirectorFactory values/);
  });

  test("rejects a directors entry path that escapes the package", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      directorsEntry: "../escape-directors.js",
    });

    await expect(
      loadWorkflowDirectorRegistryFromClosure({ packageDir }),
    ).rejects.toThrow(/escapes the workflow package directory/);
  });
});
