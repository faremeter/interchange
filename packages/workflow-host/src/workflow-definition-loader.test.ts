import { afterEach, describe, expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";

import { loadVerifiedWorkflowDefinitionFromClosure } from "./child/verified-definition-loader";
import {
  loadWorkflowActionHandlersFromClosure,
  loadWorkflowDefinitionFromClosure,
  loadWorkflowDirectorRegistryFromClosure,
  loadWorkflowLoopFnsFromClosure,
  projectWorkflowDefinitionFromClosure,
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
  /** Value written to `interchange.loops`; omitted when absent. */
  readonly loopsEntry?: string;
  /** Source of the loops module, written to `loopsEntry`'s path. */
  readonly loopsSource?: string;
  /** Value written to `interchange.actions`; omitted when absent. */
  readonly actionsEntry?: string;
  /** Source of the actions module, written to `actionsEntry`'s path. */
  readonly actionsSource?: string;
  /** A plugin package laid out as a direct dependency of the workflow. */
  readonly plugin?: { readonly name: string; readonly toolsSource: string };
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
  if (spec.loopsEntry !== undefined) {
    interchange.loops = spec.loopsEntry;
  }
  if (spec.actionsEntry !== undefined) {
    interchange.actions = spec.actionsEntry;
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
  if (spec.loopsSource !== undefined) {
    const rel = spec.loopsEntry ?? "./loops.js";
    const abs = path.join(packageDir, rel);
    await fs.mkdir(path.dirname(abs), { recursive: true });
    await fs.writeFile(abs, spec.loopsSource);
  }
  if (spec.actionsSource !== undefined) {
    const rel = spec.actionsEntry ?? "./actions.js";
    const abs = path.join(packageDir, rel);
    await fs.mkdir(path.dirname(abs), { recursive: true });
    await fs.writeFile(abs, spec.actionsSource);
  }
  if (spec.plugin !== undefined) {
    const pluginDir = path.join(packageDir, "node_modules", spec.plugin.name);
    await fs.mkdir(pluginDir, { recursive: true });
    await fs.writeFile(
      path.join(pluginDir, "package.json"),
      JSON.stringify({
        name: spec.plugin.name,
        version: "1.0.0",
        interchange: { tools: "./tools.js" },
      }),
    );
    await fs.writeFile(
      path.join(pluginDir, "tools.js"),
      spec.plugin.toolsSource,
    );
  }
  return packageDir;
}

// An actions module exporting a handler plus a non-function export, for the
// resolve/fail-closed tests.
const ACTIONS_SOURCE = `
export async function echo(input, _ctx, _signal) {
  return input;
}
export const notAFunction = 7;
`;

// A loops module exporting two pure fns plus a non-function export, for the
// resolve/fail-closed tests.
const LOOPS_SOURCE = `
export function keepGoing(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) < 2;
}
export function nextCount(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) + 1;
}
export const notAFunction = 42;
`;

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

describe("loadWorkflowLoopFnsFromClosure", () => {
  test("resolves a loop fn by export name", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: null,
      loopsEntry: "./loops.js",
      loopsSource: LOOPS_SOURCE,
    });

    const registry = await loadWorkflowLoopFnsFromClosure({ packageDir });
    expect(registry("keepGoing")(null, 1)).toBe(true);
    expect(registry("keepGoing")(null, 2)).toBe(false);
    expect(registry("nextCount")(null, 4)).toBe(5);
  });

  test("returns an empty registry that throws when the package ships no loops module", async () => {
    const packageDir = await createClosureFixture({ workflowEntry: null });

    const registry = await loadWorkflowLoopFnsFromClosure({ packageDir });
    // No loop primitive would ever call this; a workflow that declares a loop
    // fails closed here when its ref is resolved.
    expect(() => registry("keepGoing")).toThrow(
      /declares no interchange\.loops module/,
    );
  });

  test("throws when the loops module exports no fn by that name", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: null,
      loopsEntry: "./loops.js",
      loopsSource: LOOPS_SOURCE,
    });

    const registry = await loadWorkflowLoopFnsFromClosure({ packageDir });
    expect(() => registry("missing")).toThrow(
      /exports no loop fn named "missing"/,
    );
  });

  test("throws when the named export is not a function", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: null,
      loopsEntry: "./loops.js",
      loopsSource: LOOPS_SOURCE,
    });

    const registry = await loadWorkflowLoopFnsFromClosure({ packageDir });
    expect(() => registry("notAFunction")).toThrow(
      /exports no loop fn named "notAFunction"/,
    );
  });

  test("rejects a loops entry path that escapes the package", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: null,
      loopsEntry: "../escape-loops.js",
    });

    await expect(
      loadWorkflowLoopFnsFromClosure({ packageDir }),
    ).rejects.toThrow(/escapes the workflow package directory/);
  });
});

describe("loadWorkflowActionHandlersFromClosure", () => {
  const dummyCtx = {
    perform: async () => undefined,
  };

  test("resolves an action handler by export name", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: null,
      actionsEntry: "./actions.js",
      actionsSource: ACTIONS_SOURCE,
    });

    const resolve = await loadWorkflowActionHandlersFromClosure({ packageDir });
    const handler = resolve("echo");
    expect(typeof handler).toBe("function");
    expect(await handler("hi", dummyCtx, new AbortController().signal)).toBe(
      "hi",
    );
  });

  test("returns a resolver that throws when the package ships no actions module", async () => {
    const packageDir = await createClosureFixture({ workflowEntry: null });

    const resolve = await loadWorkflowActionHandlersFromClosure({ packageDir });
    expect(() => resolve("echo")).toThrow(
      /declares no interchange\.actions module/,
    );
  });

  test("throws when the actions module exports no handler by that name", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: null,
      actionsEntry: "./actions.js",
      actionsSource: ACTIONS_SOURCE,
    });

    const resolve = await loadWorkflowActionHandlersFromClosure({ packageDir });
    expect(() => resolve("missing")).toThrow(
      /exports no action handler named "missing"/,
    );
  });

  test("throws when the named export is not a function", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: null,
      actionsEntry: "./actions.js",
      actionsSource: ACTIONS_SOURCE,
    });

    const resolve = await loadWorkflowActionHandlersFromClosure({ packageDir });
    expect(() => resolve("notAFunction")).toThrow(
      /exports no action handler named "notAFunction"/,
    );
  });

  test("rejects an actions entry path that escapes the package", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: null,
      actionsEntry: "../escape-actions.js",
    });

    await expect(
      loadWorkflowActionHandlersFromClosure({ packageDir }),
    ).rejects.toThrow(/escapes the workflow package directory/);
  });
});

const PLUGIN_NAME = "@fixture/lsp";

const PLUGIN_WORKFLOW_ENTRY = `
import { defineAgent } from "@intx/agent";
import { defineWorkflow, step } from "@intx/workflow/definition";
export default defineWorkflow({
  id: "plugin-workflow",
  steps: {
    work: step({
      agent: defineAgent({
        id: "agent_work",
        systemPrompt: "work",
        tools: [],
        plugins: [${JSON.stringify(PLUGIN_NAME)}],
        capabilities: [],
        inference: { sources: [{ provider: "openai", model: "gpt-4o" }] },
      }),
    }),
  },
});
`;

function pluginToolsSource(definitions: unknown[]): string {
  return `
import { definePlugin } from "@intx/agent";
export const lsp = definePlugin({
  id: "@fixture/lsp/plugin",
  definitions: ${JSON.stringify(definitions)},
  factory: () => ({}),
});
`;
}

async function createPluginClosure(definitions: unknown[]): Promise<string> {
  return createClosureFixture({
    workflowEntry: "./workflow.js",
    entrySource: PLUGIN_WORKFLOW_ENTRY,
    plugin: { name: PLUGIN_NAME, toolsSource: pluginToolsSource(definitions) },
  });
}

async function closureWireHash(packageDir: string): Promise<string> {
  const definition = await loadWorkflowDefinitionFromClosure({ packageDir });
  return computeWireDefinitionHash(
    await projectWorkflowDefinitionFromClosure({ packageDir, definition }),
  );
}

describe("projectWorkflowDefinitionFromClosure", () => {
  test("projects each declared plugin with the tools the closure's copy declares", async () => {
    const packageDir = await createPluginClosure([
      { name: "lsp" },
      { name: "lsp_fix", approval: "ask" },
    ]);
    const definition = await loadWorkflowDefinitionFromClosure({ packageDir });

    const projection = await projectWorkflowDefinitionFromClosure({
      packageDir,
      definition,
    });

    const work = projection.steps["work"];
    if (work === undefined || work.kind !== "step") {
      throw new Error("expected a projected `step` at key `work`");
    }
    expect(work.agent.plugins).toEqual([
      {
        name: PLUGIN_NAME,
        definitions: [{ name: "lsp" }, { name: "lsp_fix", approval: "ask" }],
      },
    ]);
  });

  test("a plugin whose tools changed moves the wire hash", async () => {
    const before = await createPluginClosure([{ name: "lsp" }]);
    const after = await createPluginClosure([{ name: "lsp", approval: "ask" }]);

    expect(await closureWireHash(after)).not.toBe(
      await closureWireHash(before),
    );
  });

  // Running deployments re-verify against the hash they were approved with,
  // so a change that moves this value fails every deployment that declares a
  // plugin. Update it only together with an upgrade note to redeploy them.
  test("keeps the wire hash of a plugin definition stable", async () => {
    const packageDir = await createPluginClosure([
      { name: "lsp" },
      { name: "lsp_fix", approval: "ask" },
    ]);

    expect(await closureWireHash(packageDir)).toBe(
      "dc4b77c23b1e330bcc74873dce01089f370e1c4ae08a91759ee995e6a93ee329",
    );
  });
});

describe("loadVerifiedWorkflowDefinitionFromClosure", () => {
  test("accepts a closure whose plugin tools match the approved hash", async () => {
    const packageDir = await createPluginClosure([{ name: "lsp" }]);

    const definition = await loadVerifiedWorkflowDefinitionFromClosure({
      packageDir,
      approvedHash: await closureWireHash(packageDir),
    });

    expect(definition.id).toBe("plugin-workflow");
  });

  test("routes the plugin tools import through importModule", async () => {
    const packageDir = await createPluginClosure([{ name: "lsp" }]);
    const imported: string[] = [];

    await loadVerifiedWorkflowDefinitionFromClosure({
      packageDir,
      approvedHash: await closureWireHash(packageDir),
      importModule: (url) => {
        imported.push(url);
        return import(url) as Promise<unknown>;
      },
    });

    expect(imported.some((url) => url.endsWith("/tools.js"))).toBe(true);
  });

  test("refuses a closure whose plugin tools drifted from the approved hash", async () => {
    const approved = await createPluginClosure([{ name: "lsp" }]);
    const drifted = await createPluginClosure([
      { name: "lsp" },
      { name: "rm" },
    ]);

    await expect(
      loadVerifiedWorkflowDefinitionFromClosure({
        packageDir: drifted,
        approvedHash: await closureWireHash(approved),
      }),
    ).rejects.toThrow(/does not match the approved hash/);
  });

  // Approvals made before plugin tools were hashed recorded each plugin by
  // name only. No fallback accepts them; those workflows must be redeployed.
  test("refuses a hash approved when plugins were hashed by name only", async () => {
    const packageDir = await createPluginClosure([{ name: "lsp" }]);
    const definition = await loadWorkflowDefinitionFromClosure({ packageDir });
    const projection = await projectWorkflowDefinitionFromClosure({
      packageDir,
      definition,
    });
    const work = projection.steps["work"];
    if (work === undefined || work.kind !== "step") {
      throw new Error("expected a projected `step` at key `work`");
    }
    const nameOnlyHash = await computeWireDefinitionHash({
      ...projection,
      steps: {
        ...projection.steps,
        work: { ...work, agent: { ...work.agent, plugins: [PLUGIN_NAME] } },
      },
    });

    await expect(
      loadVerifiedWorkflowDefinitionFromClosure({
        packageDir,
        approvedHash: nameOnlyHash,
      }),
    ).rejects.toThrow(/does not match the approved hash/);
  });
});
