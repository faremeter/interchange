// Schemas for the tool-package distribution path.
//
// An agent pins one or more tool packages via `ToolPackagePin[]`. At
// deploy-assembly time, the hub walks the pinned set, resolves the full
// dependency closure, and writes a `ToolPackageManifest` into the deploy
// pack. The sidecar reads the manifest at apply time and materializes
// every entry.
//
// Only entries listed in `topLevel` contribute tools to the agent;
// transitive entries exist to satisfy `require()` / `import` resolution
// inside the top-level packages.

import { type } from "arktype";
import semver from "semver";

import {
  ToolCredentialDeclarationArray,
  isContainedEntryPath,
} from "./package-json";

/**
 * npm's documented package-name rules expressed as an arktype regex
 * literal: lowercase, may begin with a scope (`@scope/`), the rest of
 * each segment is URL-safe (letters, digits, `_`, `-`, `.`), no
 * leading dot or underscore, scoped names require a `/`. The npm
 * registry rejects anything else; mirroring the rule at the REST
 * boundary keeps mixed-case or malformed pins from threading past
 * the API into the resolver, which would otherwise self-resolve
 * them and then fail at the sidecar loader.
 *
 * Using a regex literal (rather than a `narrow` predicate) lets the
 * JSON-Schema generator surface the rule as a `pattern` field in the
 * OpenAPI spec without a fallback hook.
 */
export const ToolPackagePinName = type(
  /^(?:@[a-z0-9][a-z0-9._-]*\/)?[a-z0-9][a-z0-9._-]*$/,
);

/**
 * A pin in an agent definition: name + version range. The hub resolves
 * this against configured registries at deploy-assembly time.
 *
 * `version` is an npm-style spec ("^1.2.3", "~1.2", "1.2.3", "*").
 * Resolution is performed by `npm-pick-manifest` against the registry
 * packument. Semver-range validation lives on `ToolPackagePinArray`
 * (below) so the JSON-Schema generator sees a plain string here; the
 * array narrow is the actual REST boundary for pins and runs before
 * any value reaches the resolver.
 *
 * `name` must match npm's documented package-name rules — lowercase,
 * optional scope prefix, URL-safe characters only. npm itself rejects
 * uppercase names; packuments arrive lowercased, so a mixed-case pin
 * would self-resolve and then silently fail the sidecar loader's
 * `${name}@${version}` lookup against the lowercase entry the
 * packument produced.
 *
 * A `ToolPackagePin[]` must contain at most one entry per `name`. Use
 * `ToolPackagePinArray` (below) at REST boundaries to enforce dedup
 * before the resolver runs; the resolver still rejects duplicates at
 * its own boundary as belt-and-suspenders.
 */
export const ToolPackagePin = type({
  name: ToolPackagePinName,
  version: "string",
});
export type ToolPackagePin = typeof ToolPackagePin.infer;

/**
 * Array of pins with the no-duplicate-name and parseable-version
 * invariants enforced at parse time. The downstream resolver keys
 * its top-level resolution map by name; two pins of the same name
 * would silently collapse to the first arrival's resolved version,
 * and an unparseable semver range would fail mid-walk. Rejecting
 * both at the REST boundary surfaces the bug to the caller instead
 * of leaving it to misbehave at launch time.
 *
 * `*` is accepted as the documented any-version range; anything
 * else must satisfy `semver.validRange`.
 *
 * NOTE: the same `*` special-case lives in `parsePin` inside the
 * tool-packaging resolver. Any new magic-range additions need to be
 * carved at both sites — the packages are separated by the wire-type
 * vs. resolver boundary and cannot import each other.
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
 * version, carrying the credential declarations harvested from the package's
 * `interchange.credentials` (absent when it declares none). Only top-level
 * pins contribute declarations; transitive dependencies never do, which is why
 * this shape hangs off `topLevel` rather than `entries`.
 */
export const ToolPackageTopLevelEntry = type({
  name: ToolPackagePinName,
  version: "string",
  "credentials?": ToolCredentialDeclarationArray,
});
export type ToolPackageTopLevelEntry = typeof ToolPackageTopLevelEntry.infer;

/**
 * The manifest's top-level entries with the no-duplicate-name invariant
 * preserved -- the same guarantee `ToolPackagePinArray` gives agent-side pins.
 * Versions here are concrete (already picked by the resolver), so the
 * semver-range check that guards agent-side pins is unnecessary.
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
 * the picked version. The loader verifies the fetched bytes against it
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
 * `tarballs/<filename>.tgz`). The loader reads the blob and extracts it.
 *
 * `integrity` is the SRI string ("sha512-...") of the tarball bytes. A
 * reclassified npm tarball keeps its SRI: it is the same artifact an
 * external registry would serve, so the loader verifies the read bytes
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
 * The loader checks the tree out and copies the subtree into the store.
 *
 * `packageDir` is the resolved POSIX subtree path of this package within
 * the repo ("." for a single-package repo root, "packages/foo" for a
 * monorepo member). It is a resolved directory, not a package name: a
 * frozen materialization coordinate must not require re-resolving a
 * `package.json` name against the tree at apply time. The narrow rejects
 * absolute paths and `..` traversal at the boundary.
 *
 * `treeOid` is the git tree object id of the subtree at `commitSha` --
 * the content identity the loader verifies the checked-out subtree
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
 * asset row id; the sidecar resolves it against the deploy pack's mount
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
 * A closure entry's content identity, whatever its source: the tarball
 * SRI for a `registry` entry or an `asset` tarball, the subtree git tree
 * oid for an `asset` source package. Cache-bust keys read this rather
 * than reaching into a shape-specific field.
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
 * The manifest written into the deploy pack at
 * `deploy/tool-packages-manifest.json`.
 *
 * `schemaVersion` is a literal "1" for now. Future schema changes bump
 * this and the loader refuses unknown versions with `manifest.invalid`.
 *
 * `topLevel` enumerates the packages the agent definition explicitly
 * pinned. The loader only scans these for `interchange.tools`; entries
 * present in `entries` but absent from `topLevel` are transitive
 * dependencies materialized for runtime `require()` / `import`
 * resolution.
 *
 * `topLevel` extends the agent-side `ToolPackagePin` shape with the
 * package's harvested `credentials` declarations. The `version` field
 * here is always a concrete version (e.g. `"1.2.3"`), not a range. The resolver walks each
 * agent-side pin's range through `npm-pick-manifest` and writes the
 * picked version. The sidecar loader pairs `topLevel[i]` against
 * `entries[j]` by `${name}@${version}` equality, so a range-form
 * `version` here would never match any entry and the package would
 * silently contribute no tool factories at apply time.
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
