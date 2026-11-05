// Schemas for the tool-package distribution path.
//
// At deploy-assembly time, the hub resolves a workflow's source to its
// full dependency closure and freezes it as a `ToolPackageManifest`. The
// sidecar materializes every entry of that manifest at apply time.
//
// `topLevel` names the packages the closure was resolved from; transitive
// entries exist to satisfy `require()` / `import` resolution inside them.

import { type } from "arktype";

import { isContainedEntryPath } from "./package-json";

/**
 * npm's documented package-name rules expressed as an arktype regex
 * literal: lowercase, may begin with a scope (`@scope/`), the rest of
 * each segment is URL-safe (letters, digits, `_`, `-`, `.`), no
 * leading dot or underscore, scoped names require a `/`. The npm
 * registry rejects anything else; mirroring the rule here keeps a
 * mixed-case or malformed name out of a manifest, where it would
 * self-resolve and then fail at the sidecar.
 *
 * Using a regex literal (rather than a `narrow` predicate) lets the
 * JSON-Schema generator surface the rule as a `pattern` field in the
 * OpenAPI spec without a fallback hook.
 */
export const ToolPackagePinName = type(
  /^(?:@[a-z0-9][a-z0-9._-]*\/)?[a-z0-9][a-z0-9._-]*$/,
);

/**
 * A package to resolve a closure from: name + version range. The hub
 * resolves this against configured registries at deploy-assembly time.
 *
 * `version` is an npm-style spec ("^1.2.3", "~1.2", "1.2.3", "*").
 * Resolution is performed by `npm-pick-manifest` against the registry
 * packument.
 *
 * `name` must match npm's documented package-name rules — lowercase,
 * optional scope prefix, URL-safe characters only. npm itself rejects
 * uppercase names; packuments arrive lowercased, so a mixed-case pin
 * would self-resolve and then silently fail the sidecar's
 * `${name}@${version}` lookup against the lowercase entry the
 * packument produced.
 *
 * A `ToolPackagePin[]` must contain at most one entry per `name`; the
 * resolver rejects duplicates.
 */
export const ToolPackagePin = type({
  name: ToolPackagePinName,
  version: "string",
});
export type ToolPackagePin = typeof ToolPackagePin.infer;

/**
 * A top-level manifest entry: a root package at its concrete resolved version.
 */
export const ToolPackageTopLevelEntry = type({
  name: ToolPackagePinName,
  version: "string",
});
export type ToolPackageTopLevelEntry = typeof ToolPackageTopLevelEntry.infer;

/**
 * The manifest's top-level entries with the no-duplicate-name invariant
 * preserved. Versions here are concrete, already picked by the resolver.
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
 * A pinned entry's bytes are fetched from an EXTERNAL npm registry at
 * apply time. The sidecar's registry config maps `registry` to a URL and
 * credentials.
 *
 * `integrity` is the SRI string ("sha512-...") the registry served for
 * the picked version. The sidecar verifies the fetched bytes against it
 * before unpacking and uses it as the content-addressed cache key.
 */
export const ToolPackageRegistrySource = type({
  kind: "'registry'",
  registry: "string",
  integrity: "string",
});
export type ToolPackageRegistrySource = typeof ToolPackageRegistrySource.infer;

/**
 * The entry's bytes are a prepackaged npm tarball living at `path` inside
 * the asset's checkout (the package-registry kind stores them under
 * `tarballs/<filename>.tgz`). The sidecar reads the blob and extracts it.
 *
 * `integrity` is the SRI string ("sha512-...") of the tarball bytes. A
 * reclassified npm tarball keeps its SRI: it is the same artifact an
 * external registry would serve, so the sidecar verifies the read bytes
 * against it and uses it as the content-addressed cache key, and a
 * byte-identical tarball has one identity regardless of transport.
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
 * The sidecar checks the tree out and copies the subtree into the store.
 *
 * `packageDir` is the resolved POSIX subtree path of this package within
 * the repo ("." for a single-package repo root, "packages/foo" for a
 * monorepo member). It is a resolved directory, not a package name: a
 * frozen materialization coordinate must not require re-resolving a
 * `package.json` name against the tree at apply time. The narrow rejects
 * absolute paths and `..` traversal at the boundary.
 *
 * `treeOid` is the git tree object id of the subtree at `commitSha` --
 * the content identity the sidecar verifies the checked-out subtree
 * against. Unlike a tarball's `integrity`, it is a git tree oid, not an
 * SRI, because a source subtree has no tarball bytes to hash.
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
 * A pinned entry's bytes come from a hub `asset` -- a checked-out git
 * repo attached to the agent at session time. `assetId` is the hub-side
 * asset row id; the sidecar resolves it against its mount
 * map to reach the asset's checkout. The package lives at a location
 * within the checkout, either a prepackaged `tarball` or a `source`
 * subtree, discriminated by `package.format`.
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
 * A single pinned package in the closure.
 *
 * The entry's content identity lives on its `source` arm, because how it
 * is derived and verified depends on where the bytes come from: an SRI
 * over tarball bytes for the `asset` and `registry` arms.
 *
 * `os` / `cpu` are present when the entry comes from an
 * `optionalDependencies` declaration with platform constraints. The
 * sidecar filters entries by its own host before fetching; entries
 * whose `os` or `cpu` does not include the host's value are skipped
 * with a `platform.mismatch.skipped` debug log.
 *
 * `tarballUrl` is preserved for registry-sourced entries so the sidecar
 * can fetch without re-resolving against the registry's packument; the
 * hub recorded the exact URL the registry served at resolution time.
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
 * The manifest of a resolved tool-package closure.
 *
 * `schemaVersion` is a literal "1" for now. Future schema changes bump
 * this, and this validator refuses unknown versions.
 *
 * `topLevel` enumerates the packages the closure was resolved from;
 * entries present in `entries` but absent from `topLevel` are transitive
 * dependencies materialized for runtime `require()` / `import` resolution.
 *
 * The `version` field of a `topLevel` entry is always a concrete version
 * (e.g. `"1.2.3"`), not a range. The resolver walks each pin's range through `npm-pick-manifest` and writes the
 * picked version. The sidecar finds a top-level package in the store by
 * this `${name}@${version}`, so a range-form `version` here would name a
 * directory that does not exist.
 *
 * `entries` carries the full pinned closure: every top-level pin plus
 * every transitive dependency, deduped by `(name, version)`. The
 * sidecar materializes every entry whose `os`/`cpu` matches its host.
 */
export const ToolPackageManifest = type({
  schemaVersion: "'1'",
  // Use the array-level narrow so the wire validator catches duplicate
  // top-level names directly, even when the manifest is produced by a
  // hub the resolver did not author. The resolver enforces uniqueness
  // when building the manifest; the validator is the second line of
  // defense for any third-party hub or hand-edited file that slips a
  // duplicate through.
  topLevel: ToolPackageTopLevelArray,
  entries: ToolPackageManifestEntry.array(),
});
export type ToolPackageManifest = typeof ToolPackageManifest.infer;
