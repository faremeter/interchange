// The one place that answers "which are the non-private workspace packages,
// and what are their internal @intx/* dependencies?". Several bin scripts
// need this list — the publish gate, the dist emitter, the exports-shape and
// publish-metadata checks, and the tool-load smoke — and every copy of the
// question must agree, or a guard can silently stop covering its target.
// Each caller projects the fields it needs from the returned record.

import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { type } from "arktype";

// The dependency fields that can name an internal @intx/* package. All three
// affect what a consumer installs, so all three define the internal graph;
// walking only `dependencies` would drop a peer/optional sibling.
export const INTX_FIELDS = [
  "dependencies",
  "peerDependencies",
  "optionalDependencies",
] as const;

export const manifestSchema = type({
  name: "string",
  // Optional here because not every consumer needs it (exports-shape and
  // publish-metadata do not); the publish gate's version-sync guard is what
  // requires and enforces a version on publishable packages.
  "version?": "string",
  "private?": "boolean",
  "dependencies?": { "[string]": "string" },
  "peerDependencies?": { "[string]": "string" },
  "optionalDependencies?": { "[string]": "string" },
  // Only the subpath keys are read here (to enumerate importable specifiers);
  // exports-shape owns the per-condition-target validation.
  "exports?": { "[string]": "unknown" },
  "peerDependenciesMeta?": { "[string]": { "optional?": "boolean" } },
});

export type PackageManifest = typeof manifestSchema.infer;

/** A non-private workspace package under `packages/`. */
export interface WorkspacePackage {
  name: string;
  /** Absolute path to the package directory. */
  dir: string;
  /** Absolute path to the package's `package.json`. */
  manifestPath: string;
  version: string | undefined;
  /** The parsed manifest (name/version/private/dependency fields). */
  manifest: PackageManifest;
  /** Every internal `@intx/*` dependency spec across the resolution-relevant
   *  fields, for the expression guard. */
  intxSpecs: { field: string; name: string; spec: string }[];
  /** Distinct internal `@intx/*` dependency names across those fields, for
   *  ordering and closure walks. */
  internalDeps: string[];
  /** Every importable specifier the package exports: the bare package name
   *  for the `.` entry, plus `<name>/<subpath>` for each other `exports` key.
   *  The load smoke imports each one. */
  importSpecifiers: string[];
  /** Peer dependencies flagged optional in `peerDependenciesMeta`. A consumer
   *  that uses the gated subpath must install these; the load smoke does so
   *  before importing such a subpath. */
  optionalPeers: { name: string; range: string }[];
}

/** Read every non-private package under `packages/`, sorted by name. A
 *  directory without a `package.json` is skipped; any other read error
 *  surfaces rather than silently dropping a package. */
export function readWorkspacePackages(repoRoot: string): WorkspacePackage[] {
  const pkgsDir = join(repoRoot, "packages");
  const packages: WorkspacePackage[] = [];
  for (const entry of readdirSync(pkgsDir)) {
    const dir = join(pkgsDir, entry);
    const manifestPath = join(dir, "package.json");
    let text: string;
    try {
      text = readFileSync(manifestPath, "utf8");
    } catch (err) {
      if (err instanceof Error && "code" in err && err.code === "ENOENT") {
        continue; // directory without a package.json is not a package
      }
      throw err;
    }
    const parsed = manifestSchema(JSON.parse(text));
    if (parsed instanceof type.errors) {
      throw new Error(
        `packages: ${manifestPath} is not a well-formed manifest: ${parsed.summary}`,
      );
    }
    if (parsed.private === true) continue;
    const intxSpecs: WorkspacePackage["intxSpecs"] = [];
    for (const field of INTX_FIELDS) {
      for (const [name, spec] of Object.entries(parsed[field] ?? {})) {
        if (name.startsWith("@intx/")) intxSpecs.push({ field, name, spec });
      }
    }
    const importSpecifiers = [parsed.name];
    for (const key of Object.keys(parsed.exports ?? {})) {
      // Skip the root (already added) and wildcard subpaths (`./*`), which
      // are patterns, not a single importable specifier.
      if (key === "." || key.includes("*")) continue;
      importSpecifiers.push(parsed.name + key.slice(1));
    }
    const peerMeta = parsed.peerDependenciesMeta ?? {};
    const optionalPeers: WorkspacePackage["optionalPeers"] = [];
    for (const [name, range] of Object.entries(parsed.peerDependencies ?? {})) {
      if (peerMeta[name]?.optional === true)
        optionalPeers.push({ name, range });
    }
    packages.push({
      name: parsed.name,
      dir,
      manifestPath,
      version: parsed.version,
      manifest: parsed,
      intxSpecs,
      internalDeps: [...new Set(intxSpecs.map((s) => s.name))],
      importSpecifiers,
      optionalPeers,
    });
  }
  return packages.sort((a, b) => a.name.localeCompare(b.name));
}
