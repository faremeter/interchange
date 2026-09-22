import { afterEach, describe, expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { defineAgent, UnknownDirectorIdError } from "@intx/agent";
import {
  defineWorkflow,
  step,
  type Primitive,
  type WorkflowDefinition,
} from "@intx/workflow/definition";

import {
  loadWorkflowActionHandlersFromClosure,
  loadWorkflowDefinitionFromClosure,
  loadWorkflowDirectorRegistryFromClosure,
  loadWorkflowLoopFnsFromClosure,
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

interface DependencyFixtureSpec {
  /** Package name, e.g. "@fixture/director-pkg" (scoped names lay out one dir deep). */
  readonly name: string;
  /** Value written to `interchange.directors`; omitted when absent. */
  readonly directorsEntry?: string;
  /** Source of the directors module, written to `directorsEntry`'s path. */
  readonly directorsSource?: string;
  /** Transitive deps, declared by and laid out under THIS dep. */
  readonly dependencies?: readonly DependencyFixtureSpec[];
}

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
  /** Dependency packages declared on and laid out under this package. */
  readonly dependencies?: readonly DependencyFixtureSpec[];
}

// The `dependencies` map written to the declaring package's package.json:
// the loader resolves a dep by id prefix through the requirer's
// `node_modules/`, so the declaration mirrors what a real manifest carries.
function depMap(
  specs: readonly DependencyFixtureSpec[],
): Record<string, string> {
  const dependencies: Record<string, string> = {};
  for (const dep of specs) {
    dependencies[dep.name] = "*";
  }
  return dependencies;
}

// Each materialization gets its own store slot so two copies of one package
// name stay distinct on disk.
let depStoreCounter = 0;

// Materialize one dependency package: a REAL directory under `storeDir`
// (mirroring the materializer, which extracts each package once and symlinks
// it into the requirer's `node_modules/`). Node resolves a module's
// bare-specifier imports from the realpath of the importing file, so the dep
// carries its own `node_modules/@intx/agent` link for `defineDirector`.
async function materializeDep(
  spec: DependencyFixtureSpec,
  storeDir: string,
): Promise<string> {
  const depDir = path.join(
    storeDir,
    String(depStoreCounter++),
    ...spec.name.split("/"),
  );
  await fs.mkdir(depDir, { recursive: true });

  const pkgJson: Record<string, unknown> = {
    name: spec.name,
    version: "1.0.0",
  };
  const interchange: Record<string, unknown> = {};
  if (spec.directorsEntry !== undefined) {
    interchange.directors = spec.directorsEntry;
  }
  if (Object.keys(interchange).length > 0) {
    pkgJson.interchange = interchange;
  }
  if (spec.dependencies !== undefined && spec.dependencies.length > 0) {
    pkgJson.dependencies = depMap(spec.dependencies);
  }
  await fs.writeFile(
    path.join(depDir, "package.json"),
    JSON.stringify(pkgJson, null, 2),
  );

  const intxScopeDir = path.join(depDir, "node_modules", "@intx");
  await fs.mkdir(intxScopeDir, { recursive: true });
  await fs.symlink(AGENT_PACKAGE_DIR, path.join(intxScopeDir, "agent"), "dir");

  if (spec.directorsSource !== undefined) {
    const rel = spec.directorsEntry ?? "./directors.js";
    const abs = path.join(depDir, rel);
    await fs.mkdir(path.dirname(abs), { recursive: true });
    await fs.writeFile(abs, spec.directorsSource);
  }

  for (const dep of spec.dependencies ?? []) {
    await linkDep(dep, depDir, storeDir);
  }
  return depDir;
}

// Declare nothing on the requirer -- the maps are written by the caller --
// just symlink the materialized dep into `requirerDir/node_modules/<name>`
// (scoped names sit one directory deep), as the store layout does.
async function linkDep(
  spec: DependencyFixtureSpec,
  requirerDir: string,
  storeDir: string,
): Promise<void> {
  const depDir = await materializeDep(spec, storeDir);
  const linkPath = path.join(requirerDir, "node_modules", spec.name);
  await fs.mkdir(path.dirname(linkPath), { recursive: true });
  await fs.symlink(depDir, linkPath, "dir");
}

async function createClosureFixture(spec: ClosureFixtureSpec): Promise<string> {
  const packageDir = await fs.mkdtemp(path.join(os.tmpdir(), "wf-closure-"));
  createdDirs.push(packageDir);
  const storeDir = await fs.mkdtemp(path.join(os.tmpdir(), "wf-store-"));
  createdDirs.push(storeDir);

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
  if (spec.dependencies !== undefined && spec.dependencies.length > 0) {
    pkgJson.dependencies = depMap(spec.dependencies);
  }
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify(pkgJson, null, 2),
  );

  for (const dep of spec.dependencies ?? []) {
    await linkDep(dep, packageDir, storeDir);
  }

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

// A definition whose steps name the given director ids (one agent step per
// id): the registry loader resolves only the ids the definition references.
function definitionNamingDirectors(
  ...directorIds: string[]
): WorkflowDefinition {
  const steps: Record<string, Primitive> = {};
  if (directorIds.length === 0) {
    steps.wait = { kind: "sleep", id: "", duration: 1 };
  }
  for (const [index, id] of directorIds.entries()) {
    steps[`s${String(index)}`] = step({
      agent: defineAgent({
        id: `agent-${String(index)}`,
        systemPrompt: "fixture agent",
        tools: [],
        capabilities: [],
        inference: {
          sources: [{ provider: "anthropic", model: "mock-model" }],
        },
        director: { id, config: {} },
      }),
    });
  }
  return defineWorkflow({
    id: "fixture-director-wf",
    steps,
  });
}

// A directors entry module exports the AnnotatedDirectorFactory (the
// defineDirector(...).factory), which is what the loader's structural check
// accepts and the registry stores -- not the { factory, build } wrapper.
const CUSTOM_DIRECTOR_ENTRY = `
import { defineDirector } from "@intx/agent";
export const custom = defineDirector({
  id: "@fixture/workflow-package/custom-director",
  configSchema: (config) => config,
  factory: () => ({
    async decide() {
      return { type: "wait" };
    },
  }),
}).factory;
`;

// The directors entry a dependency package ships: one `defineDirector`
// factory under the given id. Same shape as CUSTOM_DIRECTOR_ENTRY, keyed
// per dep so collision and transitive fixtures pick their own ids.
function dependencyDirectorEntry(id: string): string {
  return `
import { defineDirector } from "@intx/agent";
export const director = defineDirector({
  id: ${JSON.stringify(id)},
  configSchema: (config) => config,
  factory: () => ({
    async decide() {
      return { type: "wait" };
    },
  }),
}).factory;
`;
}

describe("loadWorkflowDirectorRegistryFromClosure", () => {
  test("composes the built-in default when the definition names no director", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      directorsEntry: "./directors.js",
      directorsSource: CUSTOM_DIRECTOR_ENTRY,
    });

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors(),
    });

    expect(() => registry.resolve(registry.buildDefaultRef())).not.toThrow();
    // The package's own director is NOT loaded: no step references it.
    expect(() =>
      registry.resolve({
        id: "@fixture/workflow-package/custom-director",
        config: {},
      }),
    ).toThrow(UnknownDirectorIdError);
  });

  test("resolves a director the workflow package ships when a step names it", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      directorsEntry: "./directors.js",
      directorsSource: CUSTOM_DIRECTOR_ENTRY,
    });

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors(
        "@fixture/workflow-package/custom-director",
      ),
    });

    const factory = registry.resolve({
      id: "@fixture/workflow-package/custom-director",
      config: {},
    });
    expect(factory.id).toBe("@fixture/workflow-package/custom-director");
    // The built-in default still resolves alongside the custom director.
    expect(() => registry.resolve(registry.buildDefaultRef())).not.toThrow();
  });

  test("resolves a director shipped by a direct dependency package", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      dependencies: [
        {
          name: "@fixture/director-pkg",
          directorsEntry: "./directors.js",
          directorsSource: dependencyDirectorEntry(
            "@fixture/director-pkg/coding",
          ),
        },
      ],
    });

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors("@fixture/director-pkg/coding"),
    });

    const factory = registry.resolve({
      id: "@fixture/director-pkg/coding",
      config: {},
    });
    expect(factory.id).toBe("@fixture/director-pkg/coding");
    expect(() => registry.resolve(registry.buildDefaultRef())).not.toThrow();
  });

  test("composes the workflow package's own directors with a dependency's", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      directorsEntry: "./directors.js",
      directorsSource: CUSTOM_DIRECTOR_ENTRY,
      dependencies: [
        {
          name: "@fixture/director-pkg",
          directorsEntry: "./directors.js",
          directorsSource: dependencyDirectorEntry(
            "@fixture/director-pkg/coding",
          ),
        },
      ],
    });

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors(
        "@fixture/workflow-package/custom-director",
        "@fixture/director-pkg/coding",
      ),
    });

    expect(
      registry.resolve({
        id: "@fixture/workflow-package/custom-director",
        config: {},
      }).id,
    ).toBe("@fixture/workflow-package/custom-director");
    expect(
      registry.resolve({ id: "@fixture/director-pkg/coding", config: {} }).id,
    ).toBe("@fixture/director-pkg/coding");
  });

  test("loads a package's directors module once when two ids name it", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      dependencies: [
        {
          name: "@fixture/director-pkg",
          directorsEntry: "./directors.js",
          directorsSource: `
import { defineDirector } from "@intx/agent";
const make = (id) =>
  defineDirector({
    id,
    configSchema: (config) => config,
    factory: () => ({ async decide() { return { type: "wait" }; } }),
  }).factory;
export const a = make("@fixture/director-pkg/a");
export const b = make("@fixture/director-pkg/b");
`,
        },
      ],
    });

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors(
        "@fixture/director-pkg/a",
        "@fixture/director-pkg/b",
      ),
    });

    expect(
      registry.resolve({ id: "@fixture/director-pkg/a", config: {} }).id,
    ).toBe("@fixture/director-pkg/a");
    expect(
      registry.resolve({ id: "@fixture/director-pkg/b", config: {} }).id,
    ).toBe("@fixture/director-pkg/b");
  });

  test("never imports an unreferenced dependency's directors module", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      dependencies: [
        {
          name: "@fixture/broken-pkg",
          directorsEntry: "./directors.js",
          // Evaluating this module throws; the loader must never import it
          // because no step names an id under this package's prefix.
          directorsSource: `throw new Error("must not be imported");`,
        },
      ],
    });

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors(),
    });

    expect(() => registry.resolve(registry.buildDefaultRef())).not.toThrow();
  });

  test("a director id naming a package the workflow does not depend on stays unresolved", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors("@fixture/ghost-pkg/coding"),
    });

    expect(() =>
      registry.resolve({ id: "@fixture/ghost-pkg/coding", config: {} }),
    ).toThrow(UnknownDirectorIdError);
  });

  test("a director reachable only transitively stays unresolved", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      dependencies: [
        {
          name: "@fixture/mid",
          dependencies: [
            {
              name: "@fixture/deep",
              directorsEntry: "./directors.js",
              directorsSource: dependencyDirectorEntry(
                "@fixture/deep/director",
              ),
            },
          ],
        },
      ],
    });

    // `@fixture/deep` is not a direct dependency of the workflow package, so
    // `node_modules/@fixture/deep` does not exist under it and the id stays
    // unregistered: the workflow must depend on the package it names.
    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors("@fixture/deep/director"),
    });

    expect(() =>
      registry.resolve({ id: "@fixture/deep/director", config: {} }),
    ).toThrow(UnknownDirectorIdError);
  });

  test("a director id with no package prefix stays unresolved", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      directorsEntry: "./directors.js",
      directorsSource: CUSTOM_DIRECTOR_ENTRY,
    });

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors("bare-director"),
    });

    expect(() => registry.resolve({ id: "bare-director", config: {} })).toThrow(
      UnknownDirectorIdError,
    );
  });

  test("throws when the directors module exports no director factory", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      directorsEntry: "./directors.js",
      directorsSource: `export const notADirector = { hello: "world" };`,
    });

    await expect(
      loadWorkflowDirectorRegistryFromClosure({
        packageDir,
        definition: definitionNamingDirectors(
          "@fixture/workflow-package/custom-director",
        ),
      }),
    ).rejects.toThrow(/exported no AnnotatedDirectorFactory values/);
  });

  test("rejects a directors entry path that escapes the package", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      directorsEntry: "../escape-directors.js",
    });

    await expect(
      loadWorkflowDirectorRegistryFromClosure({
        packageDir,
        definition: definitionNamingDirectors(
          "@fixture/workflow-package/custom-director",
        ),
      }),
    ).rejects.toThrow(/escapes the workflow package directory/);
  });

  test("rejects a director declared outside the shipping package's namespace", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      dependencies: [
        {
          name: "@fixture/director-pkg",
          directorsEntry: "./directors.js",
          directorsSource: dependencyDirectorEntry("@fixture/other-pkg/coding"),
        },
      ],
    });

    await expect(
      loadWorkflowDirectorRegistryFromClosure({
        packageDir,
        definition: definitionNamingDirectors("@fixture/director-pkg/coding"),
      }),
    ).rejects.toThrow(/outside the package's own namespace/);
  });

  test("throws when a referenced dependency's directors module exports no director factory", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      dependencies: [
        {
          name: "@fixture/director-pkg",
          directorsEntry: "./directors.js",
          directorsSource: `export const notADirector = 1;`,
        },
      ],
    });

    await expect(
      loadWorkflowDirectorRegistryFromClosure({
        packageDir,
        definition: definitionNamingDirectors("@fixture/director-pkg/coding"),
      }),
    ).rejects.toThrow(/exported no AnnotatedDirectorFactory values/);
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
  const dummyCtx = { perform: async () => undefined };

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
