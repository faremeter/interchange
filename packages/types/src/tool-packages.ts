// Schemas for the tool-package distribution path.
//
// An agent pins one or more tool packages via `ToolPackagePin[]`. At
// deploy-assembly time, the hub walks the pinned set, resolves the full
// dependency closure, and writes a `ToolPackageManifest` into the deploy
// pack; the sidecar reads the manifest at apply time and materializes
// every entry. Only entries listed in `topLevel` contribute tools to the
// agent; transitive entries exist to satisfy `require()`/`import`
// resolution inside the top-level packages.

import { type } from "arktype";
import semver from "semver";

import {
  ToolCredentialDeclarationArray,
  isContainedEntryPath,
} from "./package-json";

/**
 * npm's documented package-name rules as an arktype regex literal:
 * lowercase, optional `@scope/` prefix, URL-safe segment characters,
 * no leading dot or underscore. The npm registry rejects anything
 * else; mirroring the rule at the REST boundary keeps mixed-case or
 * malformed pins from reaching the resolver, which would otherwise
 * self-resolve them and fail at the sidecar loader.
 *
 * A regex literal (not a `narrow`) lets the JSON-Schema generator
 * surface the rule as a `pattern` in the OpenAPI spec.
 */
export const ToolPackagePinName = type(
  /^(?:@[a-z0-9][a-z0-9._-]*\/)?[a-z0-9][a-z0-9._-]*$/,
);

/**
 * A pin in an agent definition: `name` (matching `ToolPackagePinName`)
 * plus an npm-style `version` range ("^1.2.3", "~1.2", "1.2.3",
 * "*"). The hub resolves this against configured registries at
 * deploy-assembly time via `npm-pick-manifest`.
 *
 * Semver-range validation and per-name dedup live on
 * `ToolPackagePinArray` (the REST boundary for pins), not here, so the
 * JSON-Schema generator sees a plain string.
 */
export const ToolPackagePin = type({
  name: ToolPackagePinName,
  version: "string",
});
export type ToolPackagePin = typeof ToolPackagePin.infer;

/**
 * Array of pins with no-duplicate-name and parseable-version
 * invariants at parse time. The resolver keys its top-level map by
 * name, so duplicates would silently collapse to the first arrival;
 * an unparseable range would fail mid-walk. Rejecting both at the
 * REST boundary surfaces the bug to the caller. `*` is the
 * documented any-version range; anything else must satisfy
 * `semver.validRange`.
 *
 * NOTE: the same `*` special-case lives in `parsePin` in the
 * tool-packaging resolver. New magic ranges must be carved at both
 * sites — the wire-type and resolver packages cannot import each
 * other.
 */
export const ToolPackagePinArray = ToolPackagePin.array().narrow(
  (pins, ctx) => {
    const seen = new Set<string>();
    for (const pin of pins) {
      if (seen.has(pin.name)) {
        return ctx.mustBe(
          `an array with no duplicate package names; "${pin.name}" appears more than once`,
        );
      }
      seen.add(pin.name);
      if (pin.version !== "*" && semver.validRange(pin.version) === null) {
        return ctx.mustBe(
          `every pin to carry a parseable semver range; "${pin.name}" has version ${JSON.stringify(pin.version)}`,
        );
      }
    }
    return true;
  },
);
export type ToolPackagePinArray = typeof ToolPackagePinArray.infer;

/**
 * A top-level manifest entry: a pinned package at its concrete resolved
 * version, carrying the credential declarations harvested from its
 * `interchange.credentials`. Only top-level pins contribute
 * declarations, which is why this shape hangs off `topLevel` rather
 * than `entries`.
 */
export const ToolPackageTopLevelEntry = type({
  name: ToolPackagePinName,
  version: "string",
  "credentials?": ToolCredentialDeclarationArray,
});
export type ToolPackageTopLevelEntry = typeof ToolPackageTopLevelEntry.infer;

/**
 * The manifest's top-level entries, preserving the no-duplicate-name
 * invariant `ToolPackagePinArray` gives agent-side pins. Versions here
 * are concrete (already picked by the resolver), so no semver check
 * applies.
 */
export const ToolPackageTopLevelArray = ToolPackageTopLevelEntry.array().narrow(
  (entries, ctx) => {
    const seen = new Set<string>();
    for (const entry of entries) {
      if (seen.has(entry.name)) {
        return ctx.mustBe(
          `an array with no duplicate package names; "${entry.name}" appears more than once`,
        );
      }
      seen.add(entry.name);
    }
    return true;
  },
);
export type ToolPackageTopLevelArray = typeof ToolPackageTopLevelArray.infer;

/**
 * The entry's bytes are fetched from an EXTERNAL npm registry at apply
 * time; the sidecar's registry config maps `registry` to a URL and
 * credentials. `integrity` is the SRI string ("sha512-...") the
 * registry served; the loader verifies fetched bytes against it before
 * unpacking and uses it as the content-addressed cache key.
 */
export const ToolPackageRegistrySource = type({
  kind: "'registry'",
  registry: "string",
  integrity: "string",
});
export type ToolPackageRegistrySource = typeof ToolPackageRegistrySource.infer;

/**
 * The entry's bytes are a prepackaged npm tarball at `path` inside the
 * asset's checkout (the package-registry kind stores them under
 * `tarballs/<filename>.tgz`). `integrity` is the SRI of the tarball
 * bytes: a reclassified tarball keeps the SRI an external registry
 * would serve, so a byte-identical artifact has one identity regardless
 * of transport, and the loader verifies read bytes against it.
 */
export const ToolPackageAssetTarball = type({
  format: "'tarball'",
  path: "string",
  integrity: "string",
});
export type ToolPackageAssetTarball = typeof ToolPackageAssetTarball.infer;

/**
 * The entry's bytes are a source package: the subtree at `packageDir`
 * of the asset's checkout at `commitSha`, used in place (not packed).
 *
 * `packageDir` is the resolved POSIX subtree path within the repo ("."
 * for a single-package root, "packages/foo" for a monorepo member) — a
 * resolved directory, not a package name, so a frozen materialization
 * coordinate needs no re-resolution at apply time. The narrow rejects
 * absolute paths and `..` traversal.
 *
 * `treeOid` is the git tree object id of the subtree at `commitSha` —
 * the content identity the loader verifies against. It is a git tree
 * oid, not an SRI, because a source subtree has no tarball bytes to
 * hash.
 */
export const ToolPackageAssetSourceTree = type({
  format: "'source'",
  commitSha: "string",
  packageDir: type("string").narrow((dir, ctx) =>
    isContainedEntryPath(dir)
      ? true
      : ctx.mustBe("a repo-relative path with no '..' traversal"),
  ),
  treeOid: "string",
});
export type ToolPackageAssetSourceTree =
  typeof ToolPackageAssetSourceTree.infer;

/**
 * The entry's bytes come from a hub `asset` -- a checked-out git repo
 * attached to the agent at session time. `assetId` is the hub-side asset
 * row id; the sidecar resolves it against the deploy pack's mount map to
 * reach the checkout. The package is either a prepackaged `tarball` or a
 * `source` subtree, discriminated by `package.format`.
 */
export const ToolPackageAssetSource = type({
  kind: "'asset'",
  assetId: "string",
  package: ToolPackageAssetTarball.or(ToolPackageAssetSourceTree),
});
export type ToolPackageAssetSource = typeof ToolPackageAssetSource.infer;

/**
 * Discriminated union over where a manifest entry's bytes come from: an
 * external npm `registry`, or a hub `asset` (a git checkout holding a
 * tarball or a source package).
 */
export const ToolPackageSource = ToolPackageRegistrySource.or(
  ToolPackageAssetSource,
);
export type ToolPackageSource = typeof ToolPackageSource.infer;

/**
 * A closure entry's content identity whatever its source: the tarball
 * SRI, or the git tree oid of an `asset` source subtree. Cache-bust keys
 * read this rather than reaching into a shape-specific field.
 */
export function getToolPackageSourceContentIdentity(
  source: ToolPackageSource,
): string {
  if (source.kind === "registry") {
    return source.integrity;
  }
  return source.package.format === "tarball"
    ? source.package.integrity
    : source.package.treeOid;
}

/**
 * A single pinned package in the closure. Content identity lives on the
 * `source` arm because how it is derived and verified depends on where
 * the bytes come from.
 *
 * `os`/`cpu` appear on entries from an `optionalDependencies`
 * declaration with platform constraints; the sidecar filters by its own
 * host before fetching. `tarballUrl` is preserved for registry-sourced
 * entries so the sidecar can fetch without re-resolving the packument;
 * the hub recorded the exact URL the registry served.
 */
export const ToolPackageManifestEntry = type({
  name: "string",
  version: "string",
  source: ToolPackageSource,
  "os?": "string[]",
  "cpu?": "string[]",
  "tarballUrl?": "string",
});
export type ToolPackageManifestEntry = typeof ToolPackageManifestEntry.infer;

/**
 * The manifest written into the deploy pack at
 * `deploy/tool-packages-manifest.json`.
 *
 * `schemaVersion` is literal "1"; future schema changes bump it and the
 * loader refuses unknown versions with `manifest.invalid`.
 *
 * `topLevel` lists the packages the agent explicitly pinned; the loader
 * scans only these for `interchange.tools`. `entries` carries the full
 * pinned closure (top-level plus transitive dependencies, deduped by
 * `(name, version)`); entries absent from `topLevel` are transitive
 * dependencies materialized for runtime `require()`/`import`.
 *
 * `topLevel` extends the agent-side pin shape with harvested
 * `credentials`. Its `version` is always concrete: the resolver walks
 * each pin's range through `npm-pick-manifest` and writes the picked
 * version, and the loader pairs `topLevel[i]` to `entries[j]` by
 * `${name}@${version}` equality — a range-form version would match no
 * entry and silently contribute no tool factories.
 */
export const ToolPackageManifest = type({
  schemaVersion: "'1'",
  // Array-level narrow so the wire validator catches duplicate top-level
  // names even in a manifest the resolver did not author; the resolver
  // enforces uniqueness when building, the validator is the second line
  // of defense.
  topLevel: ToolPackageTopLevelArray,
  entries: ToolPackageManifestEntry.array(),
});
export type ToolPackageManifest = typeof ToolPackageManifest.infer;
