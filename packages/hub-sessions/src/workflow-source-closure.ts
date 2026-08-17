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

  const members = await enumerateMembers(reads);
  const selected = selectMember(members, packageName);

  // BFS over workspace-local deps; collect external deps by name. Enumeration
  // rejects a `workspaces` monorepo, so the walk visits exactly one member and
  // each external `depName` is seen once -- there is no second write to
  // reconcile. The `.set` below is last-writer-wins if that ever changes, which
  // only becomes meaningful once multi-member support lands.
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
      } else {
        externalPins.set(depName, resolveExternalSpec(depName, depSpec));
      }
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

/**
 * Read the workspace members from the asset's tree. A repo with no
 * `workspaces` field is a single package rooted at the tree; a `workspaces`
 * monorepo is not yet supported here.
 */
async function enumerateMembers(
  reads: SourceTreeReads,
): Promise<Map<string, WorkspaceMember>> {
  const root = await readPackageJSON(reads, ".");
  if (root.workspaces !== undefined) {
    throw new Error(
      "resolveSourceWorkflowClosure: monorepo workspaces are not yet supported",
    );
  }
  return new Map([
    [
      root.name,
      {
        name: root.name,
        version: root.version,
        packageDir: ".",
        dependencies: root.dependencies,
      },
    ],
  ]);
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
  readonly name: string;
  readonly version: string;
  readonly dependencies: Record<string, string>;
  readonly workspaces: readonly string[] | undefined;
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
  const name = raw["name"];
  const version = raw["version"];
  if (typeof name !== "string" || typeof version !== "string") {
    throw new Error(
      `resolveSourceWorkflowClosure: ${blobPath} must declare string "name" and "version"`,
    );
  }
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
      `resolveSourceWorkflowClosure: ${blobPath} "workspaces" must be an array of globs`,
    );
  }
  return { name, version, dependencies, workspaces };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Translate an external dependency specifier to a concrete npm range the
 * registry walker can pick against. Plain ranges pass through; the
 * `workspace:`/`catalog:` protocols require a workspace and are handled where
 * a member map / catalog is in scope, so they fail loud here.
 */
function resolveExternalSpec(depName: string, spec: string): string {
  if (spec.startsWith("workspace:")) {
    throw new Error(
      `resolveSourceWorkflowClosure: ${depName} uses ${JSON.stringify(spec)} but is not a workspace member`,
    );
  }
  if (spec.startsWith("catalog:")) {
    throw new Error(
      `resolveSourceWorkflowClosure: ${depName} uses catalog protocol, which is not yet supported`,
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
