import { afterEach, describe, expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { defineAgent, UnknownDirectorIdError } from "@intx/agent";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import {
  childWorkflow,
  defineWorkflow,
  loop,
  onTrigger,
  sleep,
  step,
  type Primitive,
  type WorkflowDefinition,
} from "@intx/workflow/definition";

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
  /** A plugin package laid out as a direct dependency of the workflow. */
  readonly plugin?: { readonly name: string; readonly toolsSource: string };
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

  const nestedBodies: Record<string, (body: WorkflowDefinition) => Primitive> =
    {
      loop: (body) =>
        loop({
          body,
          while: "continue",
          carry: "carry",
          maxIterations: 1,
          onExhausted: "done",
        }),
      childWorkflow: (body) => childWorkflow({ definition: body }),
      onTrigger: (body) =>
        onTrigger({ on: { type: "mail", to: "nested@example.com" }, body }),
    };

  for (const [kind, nest] of Object.entries(nestedBodies)) {
    test(`resolves a director referenced only inside a ${kind} body`, async () => {
      const id = "@fixture/director-pkg/nested";
      const packageDir = await createClosureFixture({
        workflowEntry: "./workflow.js",
        entrySource: DEFAULT_EXPORT_ENTRY,
        dependencies: [
          {
            name: "@fixture/director-pkg",
            directorsEntry: "./directors.js",
            directorsSource: dependencyDirectorEntry(id),
          },
        ],
      });
      const definition = defineWorkflow({
        id: `nested-${kind}`,
        steps: {
          nested: nest(definitionNamingDirectors(id)),
          done: sleep({ duration: 1, after: ["nested"] }),
        },
      });

      const registry = await loadWorkflowDirectorRegistryFromClosure({
        packageDir,
        definition,
      });

      expect(registry.resolve({ id, config: {} }).id).toBe(id);
    });
  }

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

  test("a file at node_modules/<name> stays unresolved instead of throwing ENOTDIR", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });
    await fs.writeFile(path.join(packageDir, "node_modules", "ghost-pkg"), "");

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors("ghost-pkg/coding"),
    });

    expect(() =>
      registry.resolve({ id: "ghost-pkg/coding", config: {} }),
    ).toThrow(UnknownDirectorIdError);
  });

  test("an unreadable dependency slot stays unresolved instead of throwing EACCES", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      dependencies: [
        {
          name: "ghost-pkg",
          directorsEntry: "./directors.js",
          directorsSource: dependencyDirectorEntry("ghost-pkg/coding"),
        },
      ],
    });
    // The slot is a symlink to the materialized store dir; the loader
    // realpaths `node_modules/<name>/package.json`. Make that target
    // directory unreadable so the realpath fails with EACCES, then restore
    // it so cleanup can remove the fixture.
    const depRealDir = await fs.realpath(
      path.join(packageDir, "node_modules", "ghost-pkg"),
    );
    await fs.chmod(depRealDir, 0o000);
    try {
      const registry = await loadWorkflowDirectorRegistryFromClosure({
        packageDir,
        definition: definitionNamingDirectors("ghost-pkg/coding"),
      });

      expect(() =>
        registry.resolve({ id: "ghost-pkg/coding", config: {} }),
      ).toThrow(UnknownDirectorIdError);
    } finally {
      await fs.chmod(depRealDir, 0o755);
    }
  });

  test("an unreadable manifest in a dependency slot stays unresolved instead of throwing EACCES", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      dependencies: [
        {
          name: "ghost-pkg",
          directorsEntry: "./directors.js",
          directorsSource: dependencyDirectorEntry("ghost-pkg/coding"),
        },
      ],
    });
    // The directory stays readable. On Linux the realpath of the manifest
    // succeeds and the read of the manifest fails with EACCES; Bun on macOS
    // already fails the realpath with EACCES.
    const manifest = await fs.realpath(
      path.join(packageDir, "node_modules", "ghost-pkg", "package.json"),
    );
    await fs.chmod(manifest, 0o000);
    try {
      const registry = await loadWorkflowDirectorRegistryFromClosure({
        packageDir,
        definition: definitionNamingDirectors("ghost-pkg/coding"),
      });

      expect(() =>
        registry.resolve({ id: "ghost-pkg/coding", config: {} }),
      ).toThrow(UnknownDirectorIdError);
    } finally {
      await fs.chmod(manifest, 0o644);
    }
  });

  test("a directory at a dependency slot's package.json stays unresolved instead of throwing EISDIR", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });
    await fs.mkdir(
      path.join(packageDir, "node_modules", "ghost-pkg", "package.json"),
      { recursive: true },
    );

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors("ghost-pkg/coding"),
    });

    expect(() =>
      registry.resolve({ id: "ghost-pkg/coding", config: {} }),
    ).toThrow(UnknownDirectorIdError);
  });

  test("throws when a dependency slot's manifest is malformed", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });
    const slotDir = path.join(packageDir, "node_modules", "broken-pkg");
    await fs.mkdir(slotDir, { recursive: true });
    await fs.writeFile(path.join(slotDir, "package.json"), "{ not json");

    await expect(
      loadWorkflowDirectorRegistryFromClosure({
        packageDir,
        definition: definitionNamingDirectors("broken-pkg/coding"),
      }),
    ).rejects.toThrow(/malformed package\.json/);
  });

  test("a scoped prefix with a .. segment stays unresolved even when the joined path holds a matching manifest", async () => {
    // `@fixture/../coding` passes `validateNamespacedId`, and joining its
    // prefix walks from `node_modules/@fixture/..` back to `node_modules`.
    // A manifest there named `@fixture/..` would pass the manifest-name
    // check, so only the package-name grammar keeps this module unimported.
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });
    const nodeModules = path.join(packageDir, "node_modules");
    await fs.writeFile(
      path.join(nodeModules, "package.json"),
      JSON.stringify({
        name: "@fixture/..",
        version: "1.0.0",
        interchange: { directors: "./directors.js" },
      }),
    );
    await fs.writeFile(
      path.join(nodeModules, "directors.js"),
      `throw new Error("dot-segment directors module must not be imported");\n`,
    );

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors("@fixture/../coding"),
    });

    expect(() =>
      registry.resolve({ id: "@fixture/../coding", config: {} }),
    ).toThrow(UnknownDirectorIdError);
  });

  test("a self-referential symlink dependency slot stays unresolved instead of throwing ELOOP", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });
    // A slot symlinked to itself: realpath of `node_modules/<name>/package.json`
    // recurses forever and fails with ELOOP.
    await fs.symlink(
      "ghost-pkg",
      path.join(packageDir, "node_modules", "ghost-pkg"),
      "dir",
    );

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors("ghost-pkg/coding"),
    });

    expect(() =>
      registry.resolve({ id: "ghost-pkg/coding", config: {} }),
    ).toThrow(UnknownDirectorIdError);
  });

  test("a package prefix over npm's 214-character limit stays unresolved instead of throwing ENAMETOOLONG", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });
    const id = `${"a".repeat(300)}/coding`;

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors(id),
    });

    expect(() => registry.resolve({ id, config: {} })).toThrow(
      UnknownDirectorIdError,
    );
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

  test("does not import a directors module reached by a .. prefix", async () => {
    // `../coding` is a legal namespaced id (`pkg=..`, `name=coding`). Joining
    // that prefix as a path walks out of node_modules onto the workflow
    // package itself. The workflow's directors module throws on evaluation;
    // containment must refuse the prefix so the module is never imported.
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
      directorsEntry: "./directors.js",
      directorsSource: `throw new Error("escaped directors module must not be imported");`,
    });

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors("../coding"),
    });

    expect(() => registry.resolve({ id: "../coding", config: {} })).toThrow(
      UnknownDirectorIdError,
    );
  });

  test("does not import a transitive directors module reached through nested node_modules in the id", async () => {
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
              directorsSource: `throw new Error("escaped directors module must not be imported");`,
            },
          ],
        },
      ],
    });

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors(
        "@fixture/mid/node_modules/@fixture/deep/director",
      ),
    });

    expect(() =>
      registry.resolve({
        id: "@fixture/mid/node_modules/@fixture/deep/director",
        config: {},
      }),
    ).toThrow(UnknownDirectorIdError);
  });

  test("rejects an absolute director package prefix", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });
    const directorId = "/absolute/director/coding";

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors(directorId),
    });

    expect(() => registry.resolve({ id: directorId, config: {} })).toThrow(
      UnknownDirectorIdError,
    );
  });

  test("does not import a package whose manifest name does not match the id prefix", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });
    const spoofDir = path.join(packageDir, "node_modules", "@fixture", "spoof");
    await fs.mkdir(spoofDir, { recursive: true });
    await fs.writeFile(
      path.join(spoofDir, "package.json"),
      JSON.stringify(
        {
          name: "@fixture/real",
          version: "1.0.0",
          interchange: { directors: "./directors.js" },
        },
        null,
        2,
      ),
    );
    await fs.writeFile(
      path.join(spoofDir, "directors.js"),
      `throw new Error("mismatched package name directors must not be imported");\n`,
    );

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors("@fixture/spoof/coding"),
    });

    expect(() =>
      registry.resolve({ id: "@fixture/spoof/coding", config: {} }),
    ).toThrow(UnknownDirectorIdError);
  });

  test("does not route a director whose package prefix is @intx/agent", async () => {
    const packageDir = await createClosureFixture({
      workflowEntry: "./workflow.js",
      entrySource: DEFAULT_EXPORT_ENTRY,
    });
    const agentDir = path.join(packageDir, "node_modules", "@intx", "agent");
    await fs.rm(agentDir, { recursive: true, force: true });
    await fs.mkdir(agentDir, { recursive: true });
    await fs.writeFile(
      path.join(agentDir, "package.json"),
      JSON.stringify(
        {
          name: "@intx/agent",
          version: "1.0.0",
          interchange: { directors: "./directors.js" },
        },
        null,
        2,
      ),
    );
    await fs.writeFile(
      path.join(agentDir, "directors.js"),
      `throw new Error("@intx/agent directors module must not be imported");\n`,
    );

    const registry = await loadWorkflowDirectorRegistryFromClosure({
      packageDir,
      definition: definitionNamingDirectors("@intx/agent/other"),
    });

    expect(() =>
      registry.resolve({ id: "@intx/agent/other", config: {} }),
    ).toThrow(UnknownDirectorIdError);
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
