// Hub-side dependency-closure resolution for a SOURCE-format workflow
// definition -- a codebase living as a subtree of a hub git asset at a
// pinned commit.
//
// Unlike the registry and tarball arms (which resolve against npm packuments
// with SRI integrity), a source package has no packument: its content
// identity is the git tree oid of its subtree, and its dependency closure is
// a DISJOINT UNION of two origins built by construction --
//   - workspace-local members (other subtrees in the SAME asset) become
//     `format:"source"` entries and are walked recursively, and
//   - external npm dependencies are resolved through the pristine registry
//     walker (which keeps its SRI invariant intact) into tarball entries.
// A member name never appears in the external set, so the two origins cannot
// collide on a workspace member; the one residual collision (an external
// transitive dep whose `name@version` equals a member's) is detected and
// fails loud rather than silently picking an origin.

import {
  type PackumentFetcher,
  type RegistryConfig,
  HttpRegistrySource,
  createClosureResolver,
} from "@intx/tool-packaging";
import type {
  ToolPackageManifest,
  ToolPackageManifestEntry,
  ToolPackagePin,
} from "@intx/types/tool-packages";
import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";

import type { CommittedTreeEntry } from "./repo-store/types";

/** Git-tree reads pinned to the source's commit. */
export interface SourceTreeReads {
  readBlob(path: string): Promise<Uint8Array>;
  listDir(path: string): Promise<CommittedTreeEntry[]>;
  treeOid(path: string): Promise<string | null>;
}

export interface ResolveSourceWorkflowClosureArgs {
  /** A source whose `package.format` is `"source"`. */
  readonly source: WorkflowDefinitionAssetSource;
  readonly reads: SourceTreeReads;
  /**
   * The registry name external deps are stamped with in the frozen closure.
   * The sidecar resolves each entry's `source.registry` against its own
   * registry map at materialization, so this MUST be a name that map is keyed
   * by (the sidecar's npm registry, e.g. `"npmjs"`); an unknown name fails the
   * materialization loud. The caller owns the sidecar's registry configuration,
   * so it supplies the name here rather than the walk inventing one.
   */
  readonly registryName: string;
  /** URL and credentials for the npm registry external deps resolve against. */
  readonly registryConfig: RegistryConfig;
  /** Test seam for external packument fetches. */
  readonly fetchPackument?: PackumentFetcher;
}

interface WorkspaceMember {
  readonly name: string;
  readonly version: string;
  /** Repo-relative POSIX path; "." for a single-package repo root. */
  readonly packageDir: string;
  readonly dependencies: Record<string, string>;
}

/**
 * Resolve a source-format workflow definition's dependency closure to a frozen
 * `ToolPackageManifest`: a `format:"source"` entry for the workflow package and
 * each workspace-local dependency (identity = git tree oid), plus tarball
 * entries for the external npm closure.
 */
export async function resolveSourceWorkflowClosure(
  args: ResolveSourceWorkflowClosureArgs,
): Promise<ToolPackageManifest> {
  const { source, reads, registryName, registryConfig, fetchPackument } = args;
  if (source.package.format !== "source") {
    throw new Error(
      "resolveSourceWorkflowClosure: source.package.format must be 'source'",
    );
  }
  const { commitSha, packageName } = source.package;
  const assetId = source.assetId;

  const { members, rootName, catalog } = await enumerateMembers(reads);
  const selected = selectMember(members, packageName);

  // BFS over the reachable workspace members. A dependency classifies by NAME,
  // never by parsing its range: a name that is a member recurses (a
  // workspace-local edge); the workspace root's own name is an invalid target
  // (the root is not a member) and fails loud regardless of how the range is
  // spelled; anything else is external and resolves through the registry walker.
  //
  // External deps collect by name, and a second member contributing the same
  // name at a DIFFERENT resolved range fails loud rather than silently
  // collapsing to one pin -- the walker resolves a single range per name, so a
  // silent overwrite would drop a real constraint. This is a DELIBERATE v1
  // limitation: two members with drifting-but-compatible ranges (e.g. `^1.0.0`
  // and `^1.2.0`) that npm/bun would reconcile to one version are rejected here,
  // not intersected. Aligning the ranges across members, or declaring the dep
  // once in the root `catalog`, resolves it. (Semver intersection across
  // members is tracked separately as INTR-460.)
  const sourceEntries: ToolPackageManifestEntry[] = [];
  const externalPins = new Map<string, string>();
  const seen = new Set<string>();
  const queue: string[] = [selected.name];
  while (queue.length > 0) {
    const memberName = queue.shift();
    if (memberName === undefined || seen.has(memberName)) continue;
    seen.add(memberName);
    const member = members.get(memberName);
    if (member === undefined) {
      throw new Error(
        `resolveSourceWorkflowClosure: internal -- member ${JSON.stringify(memberName)} was queued but not enumerated`,
      );
    }
    const treeOid = await reads.treeOid(member.packageDir);
    if (treeOid === null) {
      throw new Error(
        `resolveSourceWorkflowClosure: subtree ${JSON.stringify(member.packageDir)} not found at commit ${commitSha}`,
      );
    }
    sourceEntries.push({
      name: member.name,
      version: member.version,
      source: {
        kind: "asset",
        assetId,
        package: {
          format: "source",
          commitSha,
          packageDir: member.packageDir,
          treeOid,
        },
      },
    });
    for (const [depName, depSpec] of Object.entries(member.dependencies)) {
      if (members.has(depName)) {
        queue.push(depName);
        continue;
      }
      if (rootName !== undefined && depName === rootName) {
        throw new Error(
          `resolveSourceWorkflowClosure: member ${JSON.stringify(member.name)} depends on the workspace root ${JSON.stringify(rootName)}, which is not a workspace member`,
        );
      }
      const resolvedSpec = resolveExternalSpec(depName, depSpec, catalog);
      const existing = externalPins.get(depName);
      if (existing !== undefined && existing !== resolvedSpec) {
        throw new Error(
          `resolveSourceWorkflowClosure: workspace members pin external ${JSON.stringify(depName)} at conflicting ranges ${JSON.stringify(existing)} and ${JSON.stringify(resolvedSpec)}; the closure resolves a single range per name`,
        );
      }
      externalPins.set(depName, resolvedSpec);
    }
  }

  const externalEntries =
    externalPins.size > 0
      ? await resolveExternalClosure(
          externalPins,
          registryName,
          registryConfig,
          fetchPackument,
        )
      : [];

  return {
    schemaVersion: "1",
    topLevel: [{ name: selected.name, version: selected.version }],
    entries: mergeDisjoint(sourceEntries, externalEntries),
  };
}

interface EnumeratedWorkspace {
  readonly members: Map<string, WorkspaceMember>;
  /**
   * The workspace root's package name, when it declares one. Used to reject a
   * member that depends on the root (which is not itself a member).
   */
  readonly rootName: string | undefined;
  /**
   * The root's `catalog` object. A bare `catalog:` dependency specifier
   * expands against it.
   */
  readonly catalog: Record<string, string> | undefined;
}

/**
 * Read the workspace members from the asset's tree. A repo with no
 * `workspaces` field is a single package rooted at the tree (the root IS the
 * one member). A `workspaces` monorepo enumerates its members from the globs;
 * the root is the workspace root, NOT a member -- its `name`/`catalog` inform
 * dependency classification but it contributes no closure entry.
 */
async function enumerateMembers(
  reads: SourceTreeReads,
): Promise<EnumeratedWorkspace> {
  const root = await readPackageJSON(reads, ".");

  if (root.workspaces === undefined) {
    // A pnpm monorepo declares its members in `pnpm-workspace.yaml`, not the
    // package.json `workspaces` field, so a pnpm root reaches here looking like
    // a single package. Detect that layout and fail loud rather than silently
    // misread the private root as the workflow. Full pnpm support is tracked in
    // INTR-461.
    if (await rootHasPnpmWorkspaceFile(reads)) {
      throw new Error(
        `resolveSourceWorkflowClosure: the asset root declares a pnpm-workspace.yaml; the pnpm workspace layout is not supported -- declare members via a package.json "workspaces" array`,
      );
    }
    // Single-package: the root is the workflow package itself, so it must
    // declare a name and version.
    const member = requireMember(root, ".");
    return {
      members: new Map([[member.name, member]]),
      rootName: root.name,
      catalog: root.catalog,
    };
  }

  const members = new Map<string, WorkspaceMember>();
  for (const glob of root.workspaces) {
    for (const packageDir of await expandWorkspaceGlob(reads, glob)) {
      // A `<dir>/*` glob matches every subdirectory, but not all are packages
      // (docs, fixtures, ...). Skip a directory with no package.json rather than
      // fail the whole resolution, matching how bun/yarn/pnpm treat a
      // non-package directory that a workspace glob happens to match. A
      // directory that HAS a package.json must still parse and declare a name
      // and version, so a malformed member fails loud.
      if (!(await dirHasPackageJSON(reads, packageDir))) continue;
      const parsed = await readPackageJSON(reads, packageDir);
      const member = requireMember(parsed, packageDir);
      if (members.has(member.name)) {
        throw new Error(
          `resolveSourceWorkflowClosure: workspace member ${JSON.stringify(member.name)} is declared by more than one package directory`,
        );
      }
      members.set(member.name, member);
    }
  }
  if (members.size === 0) {
    throw new Error(
      `resolveSourceWorkflowClosure: workspaces ${JSON.stringify(root.workspaces)} matched no member packages`,
    );
  }
  return { members, rootName: root.name, catalog: root.catalog };
}

/**
 * Turn a parsed package.json at `packageDir` into a workspace member, failing
 * loud if it does not declare both a string `name` and `version` (the closure
 * entry it produces is keyed on `name@version`).
 */
function requireMember(
  parsed: ParsedPackageJSON,
  packageDir: string,
): WorkspaceMember {
  if (parsed.name === undefined || parsed.version === undefined) {
    const blobPath =
      packageDir === "." ? "package.json" : `${packageDir}/package.json`;
    throw new Error(
      `resolveSourceWorkflowClosure: ${blobPath} must declare string "name" and "version"`,
    );
  }
  return {
    name: parsed.name,
    version: parsed.version,
    packageDir,
    dependencies: parsed.dependencies,
  };
}

/**
 * Expand one `workspaces` glob to the member directories it names. Supports
 * `<base>/*` (each subtree directly under `<base>`) and an exact path (no glob
 * character). Any richer shape (`**`, a mid-segment `*`, braces, negation)
 * fails loud rather than risk mis-enumerating the workspace.
 */
async function expandWorkspaceGlob(
  reads: SourceTreeReads,
  glob: string,
): Promise<string[]> {
  const trimmed = glob.replace(/\/+$/, "");
  if (!/[*{}!]/.test(trimmed)) {
    return [trimmed];
  }
  const suffix = "/*";
  const base = trimmed.slice(0, -suffix.length);
  if (trimmed.endsWith(suffix) && !/[*{}!]/.test(base)) {
    const children = await reads.listDir(base === "" ? "." : base);
    return children
      .filter((child) => child.type === "tree")
      .map((child) => (base === "" ? child.name : `${base}/${child.name}`));
  }
  throw new Error(
    `resolveSourceWorkflowClosure: unsupported workspaces glob ${JSON.stringify(glob)}; only "<dir>/*" and exact paths are supported`,
  );
}

/** Whether `dir` holds a `package.json` blob (i.e. is a package directory). */
async function dirHasPackageJSON(
  reads: SourceTreeReads,
  dir: string,
): Promise<boolean> {
  const entries = await reads.listDir(dir);
  return entries.some(
    (entry) => entry.name === "package.json" && entry.type === "blob",
  );
}

/** Whether the tree root holds a `pnpm-workspace.yaml` blob (the pnpm layout). */
async function rootHasPnpmWorkspaceFile(
  reads: SourceTreeReads,
): Promise<boolean> {
  const entries = await reads.listDir(".");
  return entries.some(
    (entry) => entry.name === "pnpm-workspace.yaml" && entry.type === "blob",
  );
}

function selectMember(
  members: Map<string, WorkspaceMember>,
  packageName: string | undefined,
): WorkspaceMember {
  if (packageName !== undefined) {
    const member = members.get(packageName);
    if (member === undefined) {
      throw new Error(
        `resolveSourceWorkflowClosure: workflow member ${JSON.stringify(packageName)} is not a workspace member`,
      );
    }
    return member;
  }
  if (members.size !== 1) {
    throw new Error(
      "resolveSourceWorkflowClosure: a monorepo source requires a packageName selector",
    );
  }
  const [only] = members.values();
  if (only === undefined) {
    throw new Error("resolveSourceWorkflowClosure: internal -- no members");
  }
  return only;
}

interface ParsedPackageJSON {
  /** Absent when the package.json declares no string `name` (e.g. a private workspace root). */
  readonly name: string | undefined;
  /** Absent when the package.json declares no string `version`. */
  readonly version: string | undefined;
  readonly dependencies: Record<string, string>;
  readonly workspaces: readonly string[] | undefined;
  /** The `catalog` object (`Record<name, range>`), when present. */
  readonly catalog: Record<string, string> | undefined;
}

async function readPackageJSON(
  reads: SourceTreeReads,
  packageDir: string,
): Promise<ParsedPackageJSON> {
  const blobPath =
    packageDir === "." ? "package.json" : `${packageDir}/package.json`;
  let bytes: Uint8Array;
  try {
    bytes = await reads.readBlob(blobPath);
  } catch (err) {
    throw new Error(
      `resolveSourceWorkflowClosure: could not read ${blobPath}: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  let raw: unknown;
  try {
    raw = JSON.parse(new TextDecoder().decode(bytes));
  } catch (err) {
    throw new Error(
      `resolveSourceWorkflowClosure: ${blobPath} is not valid JSON: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  if (!isRecord(raw)) {
    throw new Error(
      `resolveSourceWorkflowClosure: ${blobPath} is not an object`,
    );
  }
  // `name`/`version` are optional here: a private workspace root routinely
  // omits them. A member that omits either fails loud where it is turned into a
  // WorkspaceMember (`requireMember`), not here.
  const nameRaw = raw["name"];
  const versionRaw = raw["version"];
  const name = typeof nameRaw === "string" ? nameRaw : undefined;
  const version = typeof versionRaw === "string" ? versionRaw : undefined;
  const depsRaw = raw["dependencies"];
  const dependencies: Record<string, string> = {};
  if (isRecord(depsRaw)) {
    for (const [k, v] of Object.entries(depsRaw)) {
      if (typeof v === "string") dependencies[k] = v;
    }
  }
  const workspacesRaw = raw["workspaces"];
  let workspaces: readonly string[] | undefined;
  if (workspacesRaw === undefined) {
    workspaces = undefined;
  } else if (
    Array.isArray(workspacesRaw) &&
    workspacesRaw.every((w): w is string => typeof w === "string")
  ) {
    workspaces = workspacesRaw;
  } else {
    throw new Error(
      `resolveSourceWorkflowClosure: ${blobPath} "workspaces" must be an array of glob strings; the object form ({ packages, catalog, catalogs }) is not supported`,
    );
  }
  const catalogRaw = raw["catalog"];
  let catalog: Record<string, string> | undefined;
  if (isRecord(catalogRaw)) {
    catalog = {};
    for (const [k, v] of Object.entries(catalogRaw)) {
      if (typeof v === "string") catalog[k] = v;
    }
  }
  return { name, version, dependencies, workspaces, catalog };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Translate an EXTERNAL dependency specifier (one whose name is not a workspace
 * member) to a concrete npm range the registry walker can pick against:
 *   - `workspace:` protocol -- the name is not a member, so this is an invalid
 *     workspace reference; fail loud.
 *   - bare `catalog:` -- expand against the root `catalog` object; fail loud if
 *     the catalog declares no entry for this name.
 *   - named `catalog:<name>` -- not supported; fail loud.
 *   - plain range -- pass through.
 */
function resolveExternalSpec(
  depName: string,
  spec: string,
  catalog: Record<string, string> | undefined,
): string {
  if (spec.startsWith("workspace:")) {
    throw new Error(
      `resolveSourceWorkflowClosure: ${depName} uses ${JSON.stringify(spec)} but is not a workspace member`,
    );
  }
  if (spec === "catalog:") {
    const range = catalog?.[depName];
    if (range === undefined) {
      throw new Error(
        `resolveSourceWorkflowClosure: ${depName} uses the default catalog but the workspace root declares no "catalog" entry for it`,
      );
    }
    return range;
  }
  if (spec.startsWith("catalog:")) {
    throw new Error(
      `resolveSourceWorkflowClosure: ${depName} uses named catalog ${JSON.stringify(spec)}, which is not supported`,
    );
  }
  return spec;
}

async function resolveExternalClosure(
  pins: Map<string, string>,
  registryName: string,
  registryConfig: RegistryConfig,
  fetchPackument: PackumentFetcher | undefined,
): Promise<ToolPackageManifestEntry[]> {
  const registrySource = new HttpRegistrySource({
    name: registryName,
    config: registryConfig,
    ...(fetchPackument !== undefined ? { fetchPackument } : {}),
  });
  const resolver = createClosureResolver({
    registries: new Map([[registryName, registrySource]]),
    defaultRegistry: registryName,
  });
  const rootPins: ToolPackagePin[] = [...pins].map(([name, version]) => ({
    name,
    version,
  }));
  const manifest = await resolver.resolveClosure(rootPins);
  return [...manifest.entries];
}

/**
 * Union the source and external entries, deduped on `name@version`. A key that
 * appears in BOTH origins is an unrepresentable collision (the manifest keys on
 * `name@version` with neither treeOid nor SRI participating), so fail loud
 * rather than silently picking one origin.
 */
function mergeDisjoint(
  sourceEntries: readonly ToolPackageManifestEntry[],
  externalEntries: readonly ToolPackageManifestEntry[],
): ToolPackageManifestEntry[] {
  const byKey = new Map<string, ToolPackageManifestEntry>();
  for (const entry of sourceEntries) {
    byKey.set(`${entry.name}@${entry.version}`, entry);
  }
  for (const entry of externalEntries) {
    const key = `${entry.name}@${entry.version}`;
    if (byKey.has(key)) {
      throw new Error(
        `resolveSourceWorkflowClosure: ${key} resolved from both a workspace member and an external registry; the closure cannot represent both origins`,
      );
    }
    byKey.set(key, entry);
  }
  return [...byKey.values()];
}
