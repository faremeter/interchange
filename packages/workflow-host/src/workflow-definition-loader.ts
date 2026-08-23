// Workflow-definition loader: the code-evaluation step the sidecar
// child performs during probe and deploy.
//
// The closure-materialization machinery in `@intx/tool-packaging`
// fetches, verifies, extracts, and lays out an installed workflow
// package (and its dependency closure) into a resolvable
// `node_modules/` tree. This module takes that materialized package
// directory, reads its `package.json`, imports the module named by the
// `interchange.workflow` field, and evaluates it: the module's
// `defineWorkflow(...)` call produces a `WorkflowDefinition`, which is
// validated at this boundary before being returned.
//
// Materialization is deliberately NOT done here. `@intx/workflow-host`
// stays free of a `@intx/tool-packaging` dependency (the sidecar owns
// that layer, see `apps/sidecar/src/tool-materialization.ts`), so the
// caller runs the closure machinery and hands the resulting package
// directory in. This module only performs the import + evaluate +
// validate step, which is the part that must run inside the child's
// address space because it evaluates author code.

import { promises as fs } from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { type } from "arktype";
import { getLogger } from "@intx/log";
import {
  createDefaultDirectorRegistry,
  createWorkflowDirectorRegistry,
  isAnnotatedDirectorFactory,
  isAnnotatedPluginFactory,
  type AnnotatedPluginFactory,
  type DirectorRegistry,
  type ToolDeclaration,
} from "@intx/agent";
import { PackageJSON, isContainedEntryPath } from "@intx/types/package-json";
import { workflowDefinitionEnvelopeSchema } from "@intx/hub-sessions/substrate";
import type { WorkflowDefinition } from "@intx/workflow/definition";
import type { LoopFn, LoopFnRegistry } from "@intx/workflow";

const logger = getLogger(["workflow-host", "definition-loader"]);

export interface LoadWorkflowDefinitionFromClosureArgs {
  /**
   * Directory of the materialized workflow package within the closure:
   * the directory holding the package's `package.json`, with its
   * `node_modules/` already laid out by the closure-materialization
   * machinery so the entry module's bare-specifier imports resolve.
   */
  readonly packageDir: string;
  /**
   * Optional token mixed into the import URL's query string to bust
   * Node's ESM module cache. Node keys the ESM cache by resolved
   * URL/path, not by content: a process that imports the same package
   * directory twice with different bytes underneath (a rare re-apply in
   * a reused child) would otherwise resolve to the first-imported module
   * instance. Passing a per-materialization token (the closure's
   * integrity SRI is the natural choice) makes each materialization a
   * distinct ESM cache entry. Omit it when the process imports a given
   * package directory at most once.
   */
  readonly importCacheKey?: string;
  /**
   * Test seam for dynamic import. Production omits this and the loader
   * uses the native dynamic-import expression. The argument is the
   * `file://` URL the loader resolves for the `interchange.workflow`
   * entry.
   */
  readonly importModule?: (importUrl: string) => Promise<unknown>;
}

/**
 * Import the `interchange.workflow` entry from a materialized workflow
 * package closure, evaluate it, and return the validated
 * `WorkflowDefinition` its `defineWorkflow(...)` call produced.
 *
 * @param args - the materialized package directory plus optional import
 *   seams
 * @returns the validated `WorkflowDefinition`
 * @throws if the package.json is missing/malformed, declares no
 *   `interchange.workflow` entry, the entry path escapes the package
 *   directory, the module cannot be imported, or its evaluation does not
 *   produce exactly one value that validates as a `WorkflowDefinition`
 */
export async function loadWorkflowDefinitionFromClosure(
  args: LoadWorkflowDefinitionFromClosureArgs,
): Promise<WorkflowDefinition> {
  const importModule =
    args.importModule ?? ((url: string) => import(url) as Promise<unknown>);

  const pkgJson = await readPackageJSON(args.packageDir);
  const entryRel = pkgJson.interchange?.workflow;
  if (entryRel === undefined) {
    throw new Error(
      `workflow package at ${args.packageDir} has no "interchange.workflow" field in package.json`,
    );
  }

  const entryAbs = await resolveContainedEntry(
    args.packageDir,
    entryRel,
    "interchange.workflow",
  );

  const importUrl =
    args.importCacheKey === undefined
      ? pathToFileURL(entryAbs).href
      : `${pathToFileURL(entryAbs).href}?importCacheKey=${encodeURIComponent(args.importCacheKey)}`;

  let mod: unknown;
  try {
    mod = await importModule(importUrl);
  } catch (cause) {
    throw new Error(
      `failed to import interchange.workflow entry ${JSON.stringify(entryRel)} for workflow package at ${args.packageDir}`,
      { cause },
    );
  }
  if (mod === null || typeof mod !== "object") {
    throw new Error(
      `interchange.workflow entry ${JSON.stringify(entryRel)} for workflow package at ${args.packageDir} did not evaluate to a module object`,
    );
  }

  const definition = selectWorkflowDefinition(mod, args.packageDir, entryRel);
  logger.debug`loaded workflow definition ${definition.id} from ${args.packageDir}`;
  return definition;
}

export interface LoadWorkflowDirectorRegistryFromClosureArgs {
  /**
   * Directory of the materialized workflow package within the closure --
   * the same directory `loadWorkflowDefinitionFromClosure` reads. Both the
   * approval-time probe and the run-child call this over the SAME frozen
   * closure, so the director set they compose cannot drift.
   */
  readonly packageDir: string;
  /** See `LoadWorkflowDefinitionFromClosureArgs.importCacheKey`. */
  readonly importCacheKey?: string;
  /** Test seam for dynamic import; see the definition loader's variant. */
  readonly importModule?: (importUrl: string) => Promise<unknown>;
}

/**
 * Compose the `DirectorRegistry` for a workflow closure from the closure
 * package's OWN `interchange.directors` module (if any), alongside the
 * built-in default director. A package with no `interchange.directors`
 * field composes to the built-ins-only registry -- absence is valid, a
 * workflow need not ship a director. A present-but-empty directors module
 * is malformed and throws, matching the tool-package loader.
 *
 * Only the workflow's OWN package directors are loaded here. Directors
 * shipped by PINNED dependency packages are deliberately not resolved on
 * the source-ref path yet: the airlocked probe does not materialize pinned
 * packages, so loading them here would let the runtime resolve a director
 * the probe never advertised for approval. A workflow referencing a
 * pinned-package director fails closed (the capability walk reports it as
 * unresolved).
 *
 * @throws if the directors entry path escapes the package, the module
 *   cannot be imported, or it exports no `AnnotatedDirectorFactory` value
 */
export async function loadWorkflowDirectorRegistryFromClosure(
  args: LoadWorkflowDirectorRegistryFromClosureArgs,
): Promise<DirectorRegistry> {
  const importModule =
    args.importModule ?? ((url: string) => import(url) as Promise<unknown>);

  const pkgJson = await readPackageJSON(args.packageDir);
  const entryRel = pkgJson.interchange?.directors;
  if (entryRel === undefined) {
    // No custom directors: built-ins only.
    return createDefaultDirectorRegistry();
  }

  const entryAbs = await resolveContainedEntry(
    args.packageDir,
    entryRel,
    "interchange.directors",
  );

  const importUrl =
    args.importCacheKey === undefined
      ? pathToFileURL(entryAbs).href
      : `${pathToFileURL(entryAbs).href}?importCacheKey=${encodeURIComponent(args.importCacheKey)}`;

  let mod: unknown;
  try {
    mod = await importModule(importUrl);
  } catch (cause) {
    throw new Error(
      `failed to import interchange.directors entry ${JSON.stringify(entryRel)} for workflow package at ${args.packageDir}`,
      { cause },
    );
  }
  if (mod === null || typeof mod !== "object") {
    throw new Error(
      `interchange.directors entry ${JSON.stringify(entryRel)} for workflow package at ${args.packageDir} did not evaluate to a module object`,
    );
  }

  const loaded = Object.values(mod).filter(isAnnotatedDirectorFactory);
  if (loaded.length === 0) {
    throw new Error(
      `interchange.directors entry ${JSON.stringify(entryRel)} for workflow package at ${args.packageDir} exported no AnnotatedDirectorFactory values`,
    );
  }
  logger.debug`loaded ${String(loaded.length)} custom director(s) from ${args.packageDir}`;
  return createWorkflowDirectorRegistry(loaded);
}

export interface LoadWorkflowLoopFnsFromClosureArgs {
  /**
   * Directory of the materialized workflow package within the closure --
   * the same directory `loadWorkflowDefinitionFromClosure` reads.
   */
  readonly packageDir: string;
  /** See `LoadWorkflowDefinitionFromClosureArgs.importCacheKey`. */
  readonly importCacheKey?: string;
  /** Test seam for dynamic import; see the definition loader's variant. */
  readonly importModule?: (importUrl: string) => Promise<unknown>;
}

/**
 * Compose the `LoopFnRegistry` for a workflow closure from the closure
 * package's OWN `interchange.loops` module. A `loop` primitive's `while` and
 * `carry` refs resolve by EXPORT NAME against that module's exports.
 *
 * Unlike directors there is NO built-in default: a package with no
 * `interchange.loops` field composes to an EMPTY registry that throws on any
 * ref lookup. A workflow that declares a `loop` but ships no loops module thus
 * fails closed when its refs are resolved (eagerly, at establish); a workflow
 * with no `loop` primitive never resolves a ref, so an absent field is valid
 * there. Loading OUTSIDE the definition-hash re-verify is safe: the approved
 * hash pins each ref string, and the closure's SRI pins the module bytes.
 *
 * @throws (from the returned registry) if a requested ref names no export, or
 *   names an export that is not a function.
 * @throws if the loops entry path escapes the package or cannot be imported.
 */
export async function loadWorkflowLoopFnsFromClosure(
  args: LoadWorkflowLoopFnsFromClosureArgs,
): Promise<LoopFnRegistry> {
  const importModule =
    args.importModule ?? ((url: string) => import(url) as Promise<unknown>);

  const pkgJson = await readPackageJSON(args.packageDir);
  const entryRel = pkgJson.interchange?.loops;
  if (entryRel === undefined) {
    // No loops module. A workflow with no loop primitive never calls this; one
    // that declares a loop fails closed here when its ref is resolved.
    return (ref: string): LoopFn => {
      throw new Error(
        `loop fn ${JSON.stringify(ref)} was requested, but the workflow package at ${args.packageDir} declares no interchange.loops module`,
      );
    };
  }

  const entryAbs = await resolveContainedEntry(
    args.packageDir,
    entryRel,
    "interchange.loops",
  );

  const importUrl =
    args.importCacheKey === undefined
      ? pathToFileURL(entryAbs).href
      : `${pathToFileURL(entryAbs).href}?importCacheKey=${encodeURIComponent(args.importCacheKey)}`;

  let mod: unknown;
  try {
    mod = await importModule(importUrl);
  } catch (cause) {
    throw new Error(
      `failed to import interchange.loops entry ${JSON.stringify(entryRel)} for workflow package at ${args.packageDir}`,
      { cause },
    );
  }
  if (mod === null || typeof mod !== "object") {
    throw new Error(
      `interchange.loops entry ${JSON.stringify(entryRel)} for workflow package at ${args.packageDir} did not evaluate to a module object`,
    );
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- module namespace object: loop fns resolve by export name
  const loopModule = mod as Record<string, unknown>;
  logger.debug`loaded interchange.loops module from ${args.packageDir}`;
  return (ref: string): LoopFn => {
    const fn = loopModule[ref];
    if (typeof fn !== "function") {
      throw new Error(
        `interchange.loops entry ${JSON.stringify(entryRel)} for workflow package at ${args.packageDir} exports no loop fn named ${JSON.stringify(ref)}`,
      );
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- resolved by export name; the loop runtime applies it as a pure (childOutput, carryState) fn
    return fn as LoopFn;
  };
}

export interface LoadWorkflowPluginsFromClosureArgs {
  /**
   * Directory of the materialized workflow package within the closure --
   * the same directory `loadWorkflowDefinitionFromClosure` reads. Each
   * declared plugin package is resolved from this package's laid-out
   * `node_modules/`, exactly as the workflow entry's own bare-specifier
   * imports resolve.
   */
  readonly packageDir: string;
  /**
   * Plugin-package names the workflow's agents declare via
   * `AgentDefinition.plugins` (`["@intx/tools-lsp"]`). Each MUST be a
   * direct dependency of the workflow package so it is laid out under the
   * workflow package's `node_modules/`. Empty is valid (no plugins).
   */
  readonly plugins: readonly string[];
  /** See `LoadWorkflowDefinitionFromClosureArgs.importCacheKey`. */
  readonly importCacheKey?: string;
  /** Test seam for dynamic import; see the definition loader's variant. */
  readonly importModule?: (importUrl: string) => Promise<unknown>;
}

/**
 * Import each declared plugin package's `interchange.tools` module from the
 * materialized workflow closure and collect the `AnnotatedPluginFactory`
 * values it exports. This is the run-child counterpart to the tool-package
 * loader's plugin channel: a source-ref workflow contributes no plugin factory
 * through its agent definition (a plugin has no agent slot), so the child
 * materializes the declared plugins straight from the already-laid-out closure
 * -- no re-download, no manifest -- and feeds them into the existing per-step
 * plugin chain. The closure bytes were SRI-verified when the deploy applied the
 * frozen closure, and resolution walks the same `node_modules/` graph the
 * workflow entry's imports use.
 *
 * @throws if a declared plugin package cannot be resolved, declares no
 *   `interchange.tools` entry, the entry escapes the package, cannot be
 *   imported, or exports no `AnnotatedPluginFactory` value
 */
export async function loadWorkflowPluginFactoriesFromClosure(
  args: LoadWorkflowPluginsFromClosureArgs,
): Promise<AnnotatedPluginFactory[]> {
  const importModule =
    args.importModule ?? ((url: string) => import(url) as Promise<unknown>);
  const out: AnnotatedPluginFactory[] = [];
  for (const pluginName of args.plugins) {
    const factories = await loadPluginPackageFactories({
      workflowPackageDir: args.packageDir,
      pluginName,
      importModule,
      ...(args.importCacheKey !== undefined
        ? { importCacheKey: args.importCacheKey }
        : {}),
    });
    out.push(...factories);
  }
  return out;
}

/**
 * Read the static tool `definitions` each declared plugin package
 * contributes, keyed by plugin-package name, WITHOUT retaining the plugin
 * factory (so the caller never instantiates a plugin, which for LSP would
 * start a subprocess). This is the probe/capability-walk counterpart to
 * `loadWorkflowPluginFactoriesFromClosure`: it loads the SAME plugin module
 * from the SAME frozen closure so the tool grant surface the walk approves
 * matches the plugin the run-child materializes.
 *
 * A plugin package that exports plugin factories but declares no tool
 * definitions (a middleware-only plugin) maps to an empty array -- valid,
 * it contributes no tool grant.
 *
 * @throws under the same conditions as `loadWorkflowPluginFactoriesFromClosure`
 */
export async function loadWorkflowPluginToolDefinitionsFromClosure(
  args: LoadWorkflowPluginsFromClosureArgs,
): Promise<Map<string, readonly ToolDeclaration[]>> {
  const importModule =
    args.importModule ?? ((url: string) => import(url) as Promise<unknown>);
  const byPackage = new Map<string, readonly ToolDeclaration[]>();
  for (const pluginName of args.plugins) {
    const factories = await loadPluginPackageFactories({
      workflowPackageDir: args.packageDir,
      pluginName,
      importModule,
      ...(args.importCacheKey !== undefined
        ? { importCacheKey: args.importCacheKey }
        : {}),
    });
    const definitions: ToolDeclaration[] = [];
    for (const factory of factories) {
      definitions.push(...factory.definitions);
    }
    byPackage.set(pluginName, definitions);
  }
  return byPackage;
}

async function loadPluginPackageFactories(args: {
  workflowPackageDir: string;
  pluginName: string;
  importCacheKey?: string;
  importModule: (importUrl: string) => Promise<unknown>;
}): Promise<AnnotatedPluginFactory[]> {
  // Resolve the plugin package from the workflow package's laid-out
  // `node_modules/`. The closure materializer symlinks each direct
  // dependency into the requirer's `node_modules/`, so a declared plugin
  // package (which must be a workflow dependency) sits here. Realpath it so
  // a plugin whose entry-path containment is checked below compares
  // realpath-vs-realpath.
  const linkedDir = path.join(
    args.workflowPackageDir,
    "node_modules",
    args.pluginName,
  );
  let pluginPkgDir: string;
  try {
    pluginPkgDir = await fs.realpath(linkedDir);
  } catch (cause) {
    throw new Error(
      `plugin package ${JSON.stringify(args.pluginName)} could not be resolved from the workflow closure at ${args.workflowPackageDir}; it must be a direct dependency of the workflow package`,
      { cause },
    );
  }

  const pkgJson = await readPackageJSON(pluginPkgDir);
  const entryRel = pkgJson.interchange?.tools;
  if (entryRel === undefined) {
    throw new Error(
      `plugin package ${JSON.stringify(args.pluginName)} at ${pluginPkgDir} declares no "interchange.tools" entry; it is not a tool package`,
    );
  }

  const entryAbs = await resolveContainedEntry(
    pluginPkgDir,
    entryRel,
    "interchange.tools",
  );

  const importUrl =
    args.importCacheKey === undefined
      ? pathToFileURL(entryAbs).href
      : `${pathToFileURL(entryAbs).href}?importCacheKey=${encodeURIComponent(args.importCacheKey)}`;

  let mod: unknown;
  try {
    mod = await args.importModule(importUrl);
  } catch (cause) {
    throw new Error(
      `failed to import interchange.tools entry ${JSON.stringify(entryRel)} for plugin package ${JSON.stringify(args.pluginName)} at ${pluginPkgDir}`,
      { cause },
    );
  }
  if (mod === null || typeof mod !== "object") {
    throw new Error(
      `interchange.tools entry ${JSON.stringify(entryRel)} for plugin package ${JSON.stringify(args.pluginName)} at ${pluginPkgDir} did not evaluate to a module object`,
    );
  }

  const factories = Object.values(mod).filter(isAnnotatedPluginFactory);
  if (factories.length === 0) {
    throw new Error(
      `interchange.tools entry ${JSON.stringify(entryRel)} for plugin package ${JSON.stringify(args.pluginName)} at ${pluginPkgDir} exported no AnnotatedPluginFactory values; a package named in an agent's plugins list must export a definePlugin factory`,
    );
  }
  logger.debug`loaded ${String(factories.length)} plugin factory(ies) from ${args.pluginName} at ${pluginPkgDir}`;
  return factories;
}

async function readPackageJSON(packageDir: string): Promise<PackageJSON> {
  const pkgJsonPath = path.join(packageDir, "package.json");
  let raw: string;
  try {
    raw = await fs.readFile(pkgJsonPath, "utf8");
  } catch (cause) {
    throw new Error(
      `cannot read package.json for workflow package at ${packageDir}`,
      { cause },
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new Error(
      `malformed package.json for workflow package at ${packageDir}`,
      { cause },
    );
  }
  const validated = PackageJSON(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `package.json for workflow package at ${packageDir} failed validation: ${validated.summary}`,
    );
  }
  return validated;
}

/**
 * Resolve `entryRel` against `packageDir` and confine the result to the
 * package's own directory. `entryRel` originates from the package's
 * `package.json` and crosses the trust boundary into the child process,
 * so a `..`-traversal, an absolute path, or a `node_modules` symlink
 * escape would let a malicious package import any file the child can
 * read. The string-level check rejects `..`/absolute paths; the
 * realpath check rejects an escape through a symlink in the closure's
 * `node_modules` layout. Both sides are realpath'd so the comparison
 * holds even when the closure lives under a symlinked temp root (macOS
 * resolves `/tmp` to `/private/tmp`).
 */
async function resolveContainedEntry(
  packageDir: string,
  entryRel: string,
  fieldLabel: string,
): Promise<string> {
  // String-level containment, shared with the push-time asset validator so the
  // two boundaries agree on what "contained" means.
  if (!isContainedEntryPath(entryRel)) {
    throw new Error(
      `${fieldLabel} entry path ${JSON.stringify(entryRel)} escapes the workflow package directory ${packageDir}`,
    );
  }
  const entryAbs = path.resolve(packageDir, entryRel);

  let realPackageDir: string;
  let realEntryAbs: string;
  try {
    realPackageDir = await fs.realpath(packageDir);
    realEntryAbs = await fs.realpath(entryAbs);
  } catch (cause) {
    throw new Error(
      `${fieldLabel} entry path ${JSON.stringify(entryRel)} for workflow package at ${packageDir} could not be resolved`,
      { cause },
    );
  }
  const realContainmentRoot = realPackageDir.endsWith(path.sep)
    ? realPackageDir
    : realPackageDir + path.sep;
  if (
    realEntryAbs !== realPackageDir &&
    !realEntryAbs.startsWith(realContainmentRoot)
  ) {
    throw new Error(
      `${fieldLabel} entry path ${JSON.stringify(entryRel)} for workflow package at ${packageDir} escapes the package directory via a symlink`,
    );
  }
  return entryAbs;
}

/**
 * Pick the single `WorkflowDefinition` the entry module produces. A
 * workflow package's entry evaluates one `defineWorkflow(...)` call and
 * exposes its result as an export (by convention `export default`, but a
 * named export is accepted too). Every export is validated against the
 * envelope schema; exactly one must pass. Zero or more than one is a
 * malformed workflow package and fails loudly rather than guessing.
 */
function selectWorkflowDefinition(
  mod: object,
  packageDir: string,
  entryRel: string,
): WorkflowDefinition {
  const matches: WorkflowDefinition[] = [];
  for (const value of Object.values(mod)) {
    const validated = workflowDefinitionEnvelopeSchema(value);
    if (validated instanceof type.errors) {
      continue;
    }
    // The envelope schema enforces the cross-cutting structural shape
    // (`id`, `triggers`, `steps`, `stepOrder`); the per-primitive narrow
    // lives downstream in the runtime that hydrates the definition. This
    // mirrors the boundary the repo's other `WorkflowDefinition` readers
    // use (see `run-child.ts`, `spawn-child.ts`).
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- envelope schema enforces structural shape; primitive narrows live downstream in the runtime body
    matches.push(validated as unknown as WorkflowDefinition);
  }

  if (matches.length === 0) {
    throw new Error(
      `interchange.workflow entry ${JSON.stringify(entryRel)} for workflow package at ${packageDir} exported no value that validates as a WorkflowDefinition`,
    );
  }
  if (matches.length > 1) {
    throw new Error(
      `interchange.workflow entry ${JSON.stringify(entryRel)} for workflow package at ${packageDir} exported ${String(matches.length)} WorkflowDefinition values; the entry must produce exactly one`,
    );
  }
  const [definition] = matches;
  if (definition === undefined) {
    throw new Error(
      `interchange.workflow entry ${JSON.stringify(entryRel)} for workflow package at ${packageDir} produced no WorkflowDefinition`,
    );
  }
  return definition;
}
