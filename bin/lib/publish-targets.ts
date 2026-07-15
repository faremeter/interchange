// The testable core of the guarded publish path: the target model and the
// pure pre-flight checks (version sync, dependency expressions, leaf-first
// order, the load matrix, and the packed-manifest rewrite), separate from
// the effectful orchestration that builds, packs, and uploads.
//
// This lives in `bin/lib` rather than in the entry point because the entry
// point shares a basename with the `bin/publish` launcher wrapper; a test
// importing `./publish` would resolve to the extensionless bash wrapper
// instead of the `.ts`. A `bin/lib` module has no such twin, so both the
// entry point and its test import these from here.

import { type } from "arktype";

import { INTX_FIELDS, manifestSchema, readWorkspacePackages } from "./packages";

/** A non-private package that `bin/publish` targets — the fields the publish
 *  guards project from the shared `WorkspacePackage` record. */
export interface Target {
  name: string;
  dir: string;
  version: string | undefined;
  internalDeps: string[];
  intxSpecs: { field: string; name: string; spec: string }[];
  importSpecifiers: string[];
  optionalPeers: { name: string; range: string }[];
}

/** Read every non-private package under `packages/`. */
export function readTargets(repoRoot: string): Target[] {
  return readWorkspacePackages(repoRoot).map((p) => ({
    name: p.name,
    dir: p.dir,
    version: p.version,
    internalDeps: p.internalDeps,
    intxSpecs: p.intxSpecs,
    importSpecifiers: p.importSpecifiers,
    optionalPeers: p.optionalPeers,
  }));
}

/** Every target whose `version` is not the release version. */
export function checkVersionSync(
  targets: Target[],
  tagVersion: string,
): string[] {
  return targets
    .filter((t) => t.version !== tagVersion)
    .map(
      (t) =>
        `${t.name} is at ${t.version}, not the release version ${tagVersion}`,
    );
}

/** Every internal dependency pinned to a literal instead of `workspace:`/
 *  `catalog:`. A literal can lag the release version — the 0.1.x failure. */
export function checkInternalDepExpressions(targets: Target[]): string[] {
  const violations: string[] = [];
  for (const t of targets) {
    for (const { field, name, spec } of t.intxSpecs) {
      if (!spec.startsWith("workspace:") && !spec.startsWith("catalog:")) {
        violations.push(
          `${t.name}: ${field}.${name} is pinned to "${spec}"; internal ` +
            `dependencies must use workspace: or catalog: so pack rewrites ` +
            `them to the release version`,
        );
      }
    }
  }
  return violations;
}

/** Order targets leaf-first: a package appears after every internal
 *  dependency it imports, so publishing in order never references an
 *  unpublished sibling. Throws on a dependency cycle. */
export function topoSortLeafFirst(targets: Target[]): Target[] {
  const byName = new Map(targets.map((t) => [t.name, t]));
  const state = new Map<string, "visiting" | "done">();
  const ordered: Target[] = [];
  function visit(t: Target, path: string[]): void {
    const s = state.get(t.name);
    if (s === "done") return;
    if (s === "visiting") {
      throw new Error(
        `publish: dependency cycle detected: ${[...path, t.name].join(" -> ")}`,
      );
    }
    state.set(t.name, "visiting");
    for (const dep of t.internalDeps) {
      const d = byName.get(dep);
      if (d !== undefined) visit(d, [...path, t.name]);
    }
    state.set(t.name, "done");
    ordered.push(t);
  }
  for (const t of targets) visit(t, []);
  return ordered;
}

export const RUNTIMES = ["node", "bun", "deno"] as const;
export type Runtime = (typeof RUNTIMES)[number];

/** Runtimes whose results gate the publish — all three the distribution
 *  targets. A dry run tolerates an asserted runtime that is not on PATH
 *  (with a loud warning); `--execute` requires every one. */
export const ASSERTED_RUNTIMES: ReadonlySet<Runtime> = new Set([
  "node",
  "bun",
  "deno",
]);

export type LoadResult = "load" | "fail";
export type ObservedMatrix = Record<
  string,
  Partial<Record<Runtime, LoadResult>>
>;

/** Every import specifier must load under every asserted runtime. A `fail` —
 *  whether from an import error or the `dist/`-resolution check in LOAD_CHECK —
 *  is a violation. A runtime absent from `observed[specifier]` (not on PATH) is
 *  skipped, not failed. */
export function assertMatrix(
  observed: ObservedMatrix,
  asserted: ReadonlySet<Runtime> = ASSERTED_RUNTIMES,
): string[] {
  const violations: string[] = [];
  for (const [specifier, byRuntime] of Object.entries(observed)) {
    for (const runtime of asserted) {
      const result = byRuntime[runtime];
      if (result === undefined) continue; // runtime unavailable
      if (result === "fail") {
        violations.push(`${specifier} failed to load under ${runtime}`);
      }
    }
  }
  return violations;
}

/** Every internal `@intx/*` dependency in a packed manifest whose specifier
 *  is not the release version. `bun pm pack` rewrites `workspace:`/`catalog:`
 *  to a concrete version using `bun.lock`; a specifier that lands on anything
 *  else means the lockfile is stale. A plain `bun install` after a version
 *  bump does not refresh those records — `bin/release` regenerates them. */
export function checkPackedManifest(
  name: string,
  manifest: unknown,
  version: string,
): string[] {
  const parsed = manifestSchema(manifest);
  if (parsed instanceof type.errors) {
    throw new Error(
      `publish: packed manifest for ${name} malformed: ${parsed.summary}`,
    );
  }
  const violations: string[] = [];
  for (const field of INTX_FIELDS) {
    for (const [dep, spec] of Object.entries(parsed[field] ?? {})) {
      if (!dep.startsWith("@intx/")) continue;
      // `workspace:*` packs to a bare version; `workspace:^`/`~` pack to a
      // caret/tilde range. Both must resolve to the release version.
      const bare = spec.replace(/^[\^~]/, "");
      if (bare !== version) {
        violations.push(
          `${name}: packed ${field}.${dep} resolved to "${spec}", not the ` +
            `release version ${version}`,
        );
      }
    }
  }
  return violations;
}
