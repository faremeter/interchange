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
 * A pinned entry's tarball lives inside an asset attached to the
 * agent at session time. `assetId` is the hub-side asset row id; the
 * sidecar resolves it against the deploy pack's `deploy/asset-mounts.json`
 * map to get a workspace-relative mount path, then opens the tarball at
 * `<workspaceRoot>/<mount>/<path>`. `path` is the asset-root-relative
 * POSIX path of the tarball blob (the package-registry kind handler
 * stores them under `tarballs/<filename>.tgz`).
 *
 * Pre-INTR-108 manifests carried `path` without `assetId` because the
 * single attached asset was implicit; the substrate now supports
 * multiple attached package-registry assets, so the entry must name
 * which asset to read from.
 */
export const ToolPackageAssetSource = type({
  kind: "'asset'",
  assetId: "string",
  path: "string",
});
export type ToolPackageAssetSource = typeof ToolPackageAssetSource.infer;

/**
 * A pinned entry's tarball is fetched from the named registry at apply
 * time. The sidecar's registry config maps `registry` to a URL and
 * credentials.
 */
export const ToolPackageRegistrySource = type({
  kind: "'registry'",
  registry: "string",
});
export type ToolPackageRegistrySource = typeof ToolPackageRegistrySource.infer;

/**
 * Discriminated union over where a manifest entry's bytes come from.
 */
export const ToolPackageSource = ToolPackageAssetSource.or(
  ToolPackageRegistrySource,
);
export type ToolPackageSource = typeof ToolPackageSource.infer;

/**
 * A single pinned package in the closure.
 *
 * `integrity` is an SRI string ("sha512-..."). The loader verifies
 * fetched bytes against it before unpacking.
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
  integrity: "string",
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
 * Although `topLevel` shares the `ToolPackagePin` shape used at agent
 * definition time, the `version` field here is always a concrete
 * version (e.g. `"1.2.3"`), not a range. The resolver walks each
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
  topLevel: ToolPackagePinArray,
  entries: ToolPackageManifestEntry.array(),
});
export type ToolPackageManifest = typeof ToolPackageManifest.infer;
