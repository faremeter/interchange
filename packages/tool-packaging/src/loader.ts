// eslint-disable-next-line @typescript-eslint/triple-slash-reference -- npm-team packages ship no types; declarations.d.ts must be visible to downstream typecheckers that import from this package's source.
/// <reference path="./declarations.d.ts" />
// Sidecar-side tool-package loader.
//
// Given a resolved `ToolPackageManifest`, the loader builds an
// npm-compatible nested `node_modules/` layout under the per-instance
// scratch directory so each top-level package and each transitive
// dependency can satisfy its own `require()` / `import` calls without
// help from the sidecar host.
//
//   1. Filters by host os/cpu metadata; mismatches are skipped with a
//      debug log (`platform.mismatch.skipped`).
//   2. Materializes every remaining entry into the content-addressable
//      cache: bytes are pulled from the entry's source on a miss and
//      verified through `cache.put`; the bytes are then unpacked via
//      `cache.extractTarball` so a single sha512 has a single extraction
//      shared across instances.
//   3. Lays out each entry under `<scratch>/store/<name>/<version>/` by
//      copying the file tree from the cache extraction. Each layout
//      directory gets its own `node_modules/<dep>` symlink to the
//      sibling `store/<dep>/<depVersion>/` chosen for that requirer.
//      Diamond dependencies share a single store entry; version
//      conflicts coexist as separate store entries and Node's standard
//      ancestor-walk resolves each requirer's deps to the version that
//      satisfies its own range.
//   4. Reads each top-level package's unpacked `package.json`, resolves
//      the `interchange.tools` entry path, and dynamic-import()s it.
//   5. Validates each named export is an `AnnotatedToolFactory` (a
//      callable with `id: string` and `requires: readonly string[]`).
//
// Only entries listed in `manifest.topLevel` contribute tools; the
// loader still materializes every other entry (modulo platform
// filtering) because top-level packages reach them through Node's
// `node_modules/` resolution at apply time.
//
// Errors are surfaced as `ToolLoaderError` with a `category` matching
// one of the `DeployApplyErrorCategory` values. The atomic-apply layer
// catches these and translates them into wire-level frames.

import { promises as fs } from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import npmRegistryFetch from "npm-registry-fetch";
import { type } from "arktype";

import type {
  AnnotatedDirectorFactory,
  AnnotatedPluginFactory,
  AnnotatedToolFactory,
  BaseEnv,
} from "@intx/agent";
import {
  isAnnotatedDirectorFactory,
  isAnnotatedPluginFactory,
} from "@intx/agent";
import type { ToolCredentialDeclaration } from "@intx/types/package-json";
import { ToolCredentialDeclarationArray } from "@intx/types/package-json";
import type { ToolCall, ToolResult } from "@intx/types/runtime";
import { getLogger } from "@intx/log";
import type {
  ToolPackageManifest,
  ToolPackageManifestEntry,
} from "@intx/types/tool-packages";
import { getToolPackageSourceContentIdentity } from "@intx/types/tool-packages";

import type { TarballCache } from "./cache";
import {
  DEFAULT_MAX_REGISTRY_TARBALL_BYTES,
  DEFAULT_REGISTRY_FETCH_TIMEOUT_MS,
  ToolLoaderError,
  describeError,
} from "./loader-internal";
import {
  buildRegistryFetchOpts,
  defaultTarballUrl,
  readResponseWithLimit,
} from "./registry-fetch";
import type { RegistryConfig } from "./resolver";
import { materializeClosure, storeEntryDir } from "./store-layout";

// Re-export the members that moved to focused modules so existing `./loader`
// and package-root consumers keep resolving them here: the public failure type
// and fetch caps (now in `loader-internal`), the registry fetch helpers, and
// the closure materialization + store-layout entry points.
export {
  ToolLoaderError,
  DEFAULT_MAX_REGISTRY_TARBALL_BYTES,
  DEFAULT_REGISTRY_FETCH_TIMEOUT_MS,
  buildRegistryFetchOpts,
  readResponseWithLimit,
  materializeClosure,
  storeEntryDir,
};

const logger = getLogger(["sidecar", "tool-packaging", "loader"]);

/**
 * Loaded factory shape. We re-export `AnnotatedToolFactory<BaseEnv>` so
 * callers receive the canonical agent type without re-deriving it; the
 * loader still performs the structural check at import time so a
 * package emitting something that does not satisfy this shape is
 * rejected with `package.entry.invalid`.
 */
export type LoadedToolFactory = AnnotatedToolFactory<BaseEnv>;

/**
 * Loaded director factory shape. Same erased-Config storage as the
 * registry uses: the loader walks `interchange.directors` and surfaces
 * every export the structural check accepts. Downstream consumers feed
 * these into `createDirectorRegistry` alongside the built-in defaults.
 */
export type LoadedDirectorFactory = AnnotatedDirectorFactory<unknown, BaseEnv>;

/** One pinned package after materialization and entry-module import. */
export interface LoadedToolPackage {
  readonly name: string;
  readonly version: string;
  readonly factories: readonly LoadedToolFactory[];
  /**
   * Plugin factories the package's `interchange.tools` module exported
   * (via `definePlugin`). Plugins are instantiated by the loader before
   * tool factories are run; their results are passed via `env.plugins`
   * to the tool factories that read them.
   */
  readonly plugins: readonly AnnotatedPluginFactory[];
  /**
   * Director factories the package's `interchange.directors` module
   * exported (via `defineDirector`). The capability walk resolves
   * `DirectorRef.id` against these at deploy time; the agent runtime
   * resolves them again at instantiation. Empty when the package's
   * `package.json` omits the `interchange.directors` field — the
   * walker treats absence as a no-op so a tools-only package stays
   * valid.
   */
  readonly directors: readonly LoadedDirectorFactory[];
  /**
   * The provider-backed credentials the package's tools statically
   * declare via `interchange.credentials`. A declaration is advisory: it
   * names a handle the agent definition binds to a concrete credential and
   * the launch-time grant gate authorizes; it consents to nothing on its
   * own. Surfaced here -- parsed from the SAME `package.json` the code
   * loaded from -- so the declared set is the authoritative one (the loaded
   * package's own) rather than a hub manifest that could drift. Empty when
   * the package omits the field; a tools-only package stays valid.
   */
  readonly credentials: readonly ToolCredentialDeclaration[];
}

export interface HostPlatform {
  readonly os: string;
  readonly cpu: string;
}

export interface LoaderConfig {
  readonly cache: TarballCache;
  /**
   * Registry identifier → registry config. The key is the same
   * `registry` identifier manifest entries point at and the resolver
   * keyed its own registries map under. The loader resolves each
   * `kind: "registry"` entry by looking up this map.
   */
  readonly registries: ReadonlyMap<string, RegistryConfig>;
  readonly host: HostPlatform;
  /**
   * Hard cap on the byte length of a tarball fetched from an HTTP
   * registry. The default fetcher honors both the upstream
   * `Content-Length` header (rejecting up front when the header value
   * exceeds the cap) and the realized body byte count (aborting the
   * read once the running total crosses the cap). Asset-sourced
   * tarballs do not go through this path; their containment is the
   * substrate's upload-time cap on the hub side.
   *
   * The default mirrors the hub's `HUB_MAX_TARBALL_BYTES` cap so a
   * tarball legitimately accepted by the hub-side upload route is
   * also legitimately fetchable from a registry mirror seeded from
   * that hub. An operator pointing the sidecar at a third-party
   * registry whose curated tarballs run larger should raise the cap
   * explicitly rather than relying on the runtime to grow.
   */
  readonly maxRegistryTarballBytes?: number;
  /**
   * Deadline in milliseconds for a single HTTP-registry tarball fetch,
   * spanning the request and the streamed body read. A stalled registry
   * cannot block the fetch -- and the deploy's tool materialization
   * awaiting it -- past this bound. Defaults to
   * `DEFAULT_REGISTRY_FETCH_TIMEOUT_MS`. Asset-sourced tarballs read from
   * the local filesystem and are not subject to it.
   */
  readonly registryFetchTimeoutMs?: number;
  /**
   * Test seam for tarball fetching. Production omits this and the
   * loader uses npm-registry-fetch + filesystem reads.
   */
  readonly fetchTarball?: TarballFetcher;
  /**
   * Test seam for dynamic import. Production omits this and the loader
   * uses the native dynamic-import expression. The argument is the URL
   * the loader hands to `import()`: a `file://` URL with an
   * `integrity=<sri>` query string the loader appends to bust Node's
   * ESM module cache across applies that swap bytes under the same
   * `(name, version)` pair.
   */
  readonly importModule?: (importUrl: string) => Promise<unknown>;
}

export type TarballFetcher = (
  entry: ToolPackageManifestEntry,
  ctx: {
    registries: ReadonlyMap<string, RegistryConfig>;
    assetRoot: string;
    assetMounts: ReadonlyMap<string, string>;
  },
) => Promise<Uint8Array>;

export interface LoadManifestArgs {
  readonly manifest: ToolPackageManifest;
  readonly instanceScratchDir: string;
  /**
   * Filesystem root that `assetMounts` paths are joined against. Mount
   * paths from the deploy pack are workspace-relative; the loader
   * resolves them against `assetRoot` to get the absolute tarball
   * location for `kind: "asset"` entries.
   */
  readonly assetRoot: string;
  /**
   * Maps a `source.assetId` from a manifest entry to a
   * workspace-relative mount path. The session service emits this map
   * into the deploy pack as `deploy/asset-mounts.json`; the sidecar
   * threads it through to here. Empty map is valid when no entry
   * sources from an asset.
   */
  readonly assetMounts: ReadonlyMap<string, string>;
  /**
   * Maps a source-format asset entry's `source.assetId` to an absolute path
   * to an indexed git directory whose object database holds the pinned
   * commit and its trees. The caller checks the delivered pack out into
   * this directory once per asset before applying. Empty map is valid
   * when no entry sources from a git subtree.
   */
  readonly gitDirs: ReadonlyMap<string, string>;
}

export interface ToolLoader {
  loadManifest(args: LoadManifestArgs): Promise<LoadedToolPackage[]>;
}

/**
 * Arguments for `materializeClosure`, the eval-free materialization
 * primitive `loadManifest` wraps. Everything phases 1-2 need is passed
 * explicitly so the function stands alone without a `ToolLoader`
 * instance: the closure's `cache`, `registries`, `host`, and resolved
 * `fetchTarball` are threaded in directly. There is no import seam here
 * because `materializeClosure` never imports author code — that first
 * `import()` belongs to `loadManifest`'s phase-3 loop.
 */
export interface MaterializeClosureArgs {
  readonly manifest: ToolPackageManifest;
  readonly instanceScratchDir: string;
  readonly assetRoot: string;
  readonly assetMounts: ReadonlyMap<string, string>;
  /**
   * Maps a source-format asset entry's `source.assetId` to an absolute
   * indexed git directory the pinned subtree is read from. Empty map is valid
   * when no entry sources from a git subtree.
   */
  readonly gitDirs: ReadonlyMap<string, string>;
  readonly host: HostPlatform;
  readonly cache: TarballCache;
  /**
   * Registry identifier → registry config, the same map
   * `createToolLoader` keys its registries under. Used both to gate
   * `kind: "registry"` entries against the sidecar config and as the
   * `registries` context handed to `fetchTarball`.
   */
  readonly registries: ReadonlyMap<string, RegistryConfig>;
  /**
   * Resolved tarball fetcher. `createToolLoader` builds the default
   * (npm-registry-fetch + filesystem) fetcher or honors a test seam and
   * threads the result here; `materializeClosure` does not construct one
   * of its own.
   */
  readonly fetchTarball: TarballFetcher;
}

/**
 * Result of `materializeClosure`: the per-instance store directory the
 * closure was laid out under (`<instanceScratchDir>/store`), plus the exact
 * host-filtered entries that were laid out under it. A caller computes a
 * specific package's directory with `storeEntryDir`, and iterates `entries` to
 * load exactly what was materialized -- rather than re-deriving the host filter
 * with a second predicate and risking the two drifting apart.
 */
export interface MaterializeClosureResult {
  readonly storeDir: string;
  readonly entries: readonly ToolPackageManifestEntry[];
}

export function createToolLoader(config: LoaderConfig): ToolLoader {
  const registriesByName = config.registries;
  const maxRegistryTarballBytes =
    config.maxRegistryTarballBytes ?? DEFAULT_MAX_REGISTRY_TARBALL_BYTES;
  if (
    !Number.isFinite(maxRegistryTarballBytes) ||
    maxRegistryTarballBytes <= 0
  ) {
    throw new Error(
      `createToolLoader: maxRegistryTarballBytes must be a positive finite number; got ${String(maxRegistryTarballBytes)}`,
    );
  }
  const registryFetchTimeoutMs =
    config.registryFetchTimeoutMs ?? DEFAULT_REGISTRY_FETCH_TIMEOUT_MS;
  if (!Number.isFinite(registryFetchTimeoutMs) || registryFetchTimeoutMs <= 0) {
    throw new Error(
      `createToolLoader: registryFetchTimeoutMs must be a positive finite number; got ${String(registryFetchTimeoutMs)}`,
    );
  }
  const fetchTarball = config.fetchTarball ?? makeDefaultTarballFetcher();
  const importModule =
    config.importModule ?? ((u: string) => import(u) as Promise<unknown>);

  async function loadTopLevel(
    entry: ToolPackageManifestEntry,
    pkgDir: string,
  ): Promise<LoadedToolPackage> {
    const pkgJsonPath = path.join(pkgDir, "package.json");
    let pkgJsonRaw: string;
    try {
      pkgJsonRaw = await fs.readFile(pkgJsonPath, "utf8");
    } catch (err) {
      throw new ToolLoaderError({
        category: "package.entry.invalid",
        message: `package.json missing for ${entry.name}@${entry.version}: ${describeError(err)}`,
        package: { name: entry.name, version: entry.version },
      });
    }
    let pkgJson: unknown;
    try {
      pkgJson = JSON.parse(pkgJsonRaw);
    } catch (err) {
      throw new ToolLoaderError({
        category: "package.entry.invalid",
        message: `malformed package.json in ${entry.name}@${entry.version}: ${describeError(err)}`,
        package: { name: entry.name, version: entry.version },
      });
    }
    const toolsRel = readInterchangeEntry(pkgJson, "tools");
    if (toolsRel === null) {
      throw new ToolLoaderError({
        category: "package.entry.missing",
        message: `${entry.name}@${entry.version} package.json has no "interchange.tools" field`,
        package: { name: entry.name, version: entry.version },
      });
    }
    const toolsMod = await importInterchangeEntry({
      entry,
      pkgDir,
      entryRel: toolsRel,
      field: "tools",
    });
    const factories: LoadedToolFactory[] = [];
    const plugins: AnnotatedPluginFactory[] = [];
    for (const value of Object.values(toolsMod)) {
      if (isAnnotatedPluginFactory(value)) {
        plugins.push(value);
      } else if (isAnnotatedToolFactory(value)) {
        factories.push(
          applyNamespacePrefix(value, {
            name: entry.name,
            version: entry.version,
          }),
        );
      }
    }
    if (factories.length === 0 && plugins.length === 0) {
      throw new ToolLoaderError({
        category: "package.entry.invalid",
        message: `${entry.name}@${entry.version} interchange.tools entry exported no AnnotatedToolFactory or AnnotatedPluginFactory values`,
        package: { name: entry.name, version: entry.version },
      });
    }

    // Director walk: separate `package.json` field, separate dynamic
    // import, separate structural validation. Absence is a no-op so a
    // tools-only package stays valid; a directors-only package is not
    // supported because the tools field's absence is already a hard
    // error above. A package whose director-entry module exports
    // nothing director-shaped is rejected the same way the tool entry
    // would be.
    const directors: LoadedDirectorFactory[] = [];
    const directorsRel = readInterchangeEntry(pkgJson, "directors");
    if (directorsRel !== null) {
      const directorsMod = await importInterchangeEntry({
        entry,
        pkgDir,
        entryRel: directorsRel,
        field: "directors",
      });
      for (const value of Object.values(directorsMod)) {
        if (isAnnotatedDirectorFactory(value)) {
          directors.push(value);
        }
      }
      if (directors.length === 0) {
        throw new ToolLoaderError({
          category: "package.entry.invalid",
          message: `${entry.name}@${entry.version} interchange.directors entry exported no AnnotatedDirectorFactory values`,
          package: { name: entry.name, version: entry.version },
        });
      }
    }

    // Inline credential declarations: read + validate from the same
    // package.json above. Runs after the tools/directors walks so a
    // malformed `interchange.credentials` surfaces the same
    // package.entry.invalid class as a bad tool/director entry.
    const credentials = readInterchangeCredentials(pkgJson, entry);

    return {
      name: entry.name,
      version: entry.version,
      factories,
      plugins,
      directors,
      credentials,
    };
  }

  /**
   * Resolve `entryRel` against `pkgDir`, enforce path-safety
   * (`..`-traversal, absolute-path, and node_modules symlink-graph
   * escapes), and dynamic-import the result. Centralized so the
   * `interchange.tools` and `interchange.directors` walkers share one
   * containment surface.
   */
  async function importInterchangeEntry(args: {
    entry: ToolPackageManifestEntry;
    pkgDir: string;
    entryRel: string;
    field: "tools" | "directors";
  }): Promise<object> {
    const { entry, pkgDir, entryRel, field } = args;
    const entryAbs = path.resolve(pkgDir, entryRel);
    // `entryRel` originates from the tarball's `package.json` and
    // crosses the trust boundary into the sidecar process. `..` or an
    // absolute path inside `entryRel` would let a malicious tarball
    // import any file the sidecar process can read. Confine the
    // resolved import target to the package's own extraction
    // directory and reject anything that escapes.
    //
    // The string-level check rejects `..` and absolute paths inside
    // `entryRel`. It is not enough on its own: the per-instance
    // scratch tree contains a `node_modules/` symlink graph the
    // loader builds to satisfy nested resolution, and an
    // `interchange.*` entry that traverses that graph would
    // string-contain inside `pkgDir` but resolve via realpath to
    // another package's code (or anywhere else the symlink target
    // points). Re-check containment against the realpath so a
    // tarball cannot reach another package's bytes through its own
    // package directory's symlinks.
    //
    // The containment check assumes POSIX-shaped path separators on
    // disk — the sidecar runs on Linux and macOS only; Windows
    // path-separator handling (drive letters, mixed `/` and `\\`,
    // case-insensitive comparison) is out of scope.
    const containmentRoot = pkgDir.endsWith(path.sep)
      ? pkgDir
      : pkgDir + path.sep;
    if (entryAbs !== pkgDir && !entryAbs.startsWith(containmentRoot)) {
      throw new ToolLoaderError({
        category: "package.entry.invalid",
        message: `${entry.name}@${entry.version} interchange.${field} entry path ${JSON.stringify(entryRel)} escapes the package directory`,
        package: { name: entry.name, version: entry.version },
      });
    }
    // Realpath the entry so a `node_modules/` symlink traversal
    // inside `entryRel` does not let a tarball point at another
    // package's bytes. The package directory itself is resolved the
    // same way so the comparison is realpath-vs-realpath rather than
    // realpath-vs-as-declared (the per-instance scratch tree may
    // itself live under a symlinked tmpdir, notably on macOS where
    // `/tmp` is a symlink to `/private/tmp`).
    let realPkgDir: string;
    let realEntryAbs: string;
    try {
      realPkgDir = await fs.realpath(pkgDir);
      realEntryAbs = await fs.realpath(entryAbs);
    } catch (err) {
      throw new ToolLoaderError({
        category: "package.entry.invalid",
        message: `${entry.name}@${entry.version} interchange.${field} entry path ${JSON.stringify(entryRel)} could not be resolved: ${describeError(err)}`,
        package: { name: entry.name, version: entry.version },
      });
    }
    const realContainmentRoot = realPkgDir.endsWith(path.sep)
      ? realPkgDir
      : realPkgDir + path.sep;
    if (
      realEntryAbs !== realPkgDir &&
      !realEntryAbs.startsWith(realContainmentRoot)
    ) {
      throw new ToolLoaderError({
        category: "package.entry.invalid",
        message: `${entry.name}@${entry.version} interchange.${field} entry path ${JSON.stringify(entryRel)} escapes the package extraction directory via a symlink`,
        package: { name: entry.name, version: entry.version },
      });
    }

    // Cache-bust the ESM module cache by appending the entry's content
    // identity as a query string. Node keys the ESM cache by resolved
    // URL/path, not by content: a `(name, version)` pair whose bytes
    // change across applies (an operator-recompiled built-in, a
    // hot-fixed tarball republished under the same version) would
    // otherwise resolve to the previously-imported module instance until
    // the sidecar restarts. Same path with a different query is a
    // distinct ESM cache entry, so the import reflects the bytes actually
    // extracted for this apply.
    const importUrl = `${pathToFileURL(entryAbs).href}?integrity=${encodeURIComponent(getToolPackageSourceContentIdentity(entry.source))}`;
    let mod: unknown;
    try {
      mod = await importModule(importUrl);
    } catch (err) {
      throw new ToolLoaderError({
        category: "package.entry.invalid",
        message: `dynamic import of ${entry.name}@${entry.version} interchange.${field} failed: ${describeError(err)}`,
        package: { name: entry.name, version: entry.version },
      });
    }
    if (mod === null || typeof mod !== "object") {
      throw new ToolLoaderError({
        category: "package.entry.invalid",
        message: `${entry.name}@${entry.version} interchange.${field} entry did not return an object`,
        package: { name: entry.name, version: entry.version },
      });
    }
    return mod;
  }

  return {
    async loadManifest(args) {
      // Phases 1-2 (fetch + SRI-verify + extract into the cache, then
      // resolve the closure ranges and lay out the per-instance store)
      // are the eval-free materialization primitive. They import no
      // author code; that first `import()` happens only in phase 3
      // below. `materializeClosure` returns the exact host-filtered
      // entries it laid out, so phase 3 loads precisely those -- there is
      // no second platform-filter pass that could drift from the first.
      const { storeDir, entries } = await materializeClosure({
        manifest: args.manifest,
        instanceScratchDir: args.instanceScratchDir,
        assetRoot: args.assetRoot,
        assetMounts: args.assetMounts,
        gitDirs: args.gitDirs,
        host: config.host,
        cache: config.cache,
        registries: config.registries,
        fetchTarball,
      });

      const topLevelKeys = new Set(
        args.manifest.topLevel.map((p) => `${p.name}@${p.version}`),
      );

      // 3. Load only the top-level packages; transitive entries exist
      //    for `node_modules/` satisfaction but do not contribute
      //    factories of their own. This is the first point at which
      //    author code is imported.
      const loaded: LoadedToolPackage[] = [];
      const coveredTopLevelKeys = new Set<string>();
      for (const entry of entries) {
        const key = `${entry.name}@${entry.version}`;
        if (!topLevelKeys.has(key)) continue;
        const pkgDir = storeEntryDir(storeDir, entry.name, entry.version);
        loaded.push(await loadTopLevel(entry, pkgDir));
        coveredTopLevelKeys.add(key);
      }
      // Top-level pins the platform filter dropped contribute zero
      // factories, which is a legitimate operator choice (e.g. an
      // optionalDependencies-shaped opt-in for a single-platform
      // helper). Surface it as a warn so an apply that produces no
      // tools at all because every pin was platform-filtered out is
      // diagnosable from the logs without re-reading the manifest.
      const droppedTopLevelKeys: string[] = [];
      for (const key of topLevelKeys) {
        if (!coveredTopLevelKeys.has(key)) droppedTopLevelKeys.push(key);
      }
      if (droppedTopLevelKeys.length > 0) {
        logger.warn`tool-package apply dropped top-level pins via platform filter on host os=${config.host.os} cpu=${config.host.cpu}: ${droppedTopLevelKeys.join(", ")}`;
      }
      return loaded;
    },
  };

  function makeDefaultTarballFetcher(): TarballFetcher {
    return async (entry, ctx) => {
      if (entry.source.kind === "asset") {
        // A source-format asset is a git subtree materialized by checkout
        // in the store layout, never fetched as a tarball. Reaching the
        // tarball fetcher with one is a loader bug; fail loud.
        if (entry.source.package.format !== "tarball") {
          throw new ToolLoaderError({
            category: "git.materialization.failed",
            message: `source-format asset entry ${entry.name}@${entry.version} reached the tarball fetcher`,
            package: { name: entry.name, version: entry.version },
          });
        }
        const tarballPath = entry.source.package.path;
        // The mount lookup is guaranteed by `materialize`'s
        // pre-fetch gate, but reassert here so the narrowing is
        // visible to readers — the caller of fetchTarball has no
        // structural guarantee it ran through that gate.
        const mount = ctx.assetMounts.get(entry.source.assetId);
        if (mount === undefined) {
          throw new ToolLoaderError({
            category: "asset.mount.missing",
            message: `default fetcher reached without a mount for assetId "${entry.source.assetId}"`,
            package: { name: entry.name, version: entry.version },
          });
        }
        // Both `mount` and `entry.source.package.path` originate from the
        // hub and cross the trust boundary into the sidecar process. A `..`
        // segment in either would let a malicious manifest read any
        // file the sidecar can open. Resolve the join and assert the
        // result still sits under `assetRoot` so a traversal attempt
        // surfaces as a structured manifest rejection rather than a
        // silent arbitrary read.
        //
        // Reject absolute mount paths up front: `path.resolve` would
        // discard the assetRoot prefix when handed an absolute segment,
        // letting an absolute mount escape the containment check that
        // follows. Defense-in-depth for the (today-trusted) hub-side
        // mount producer.
        if (path.isAbsolute(mount)) {
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `assetMounts entry for ${entry.name}@${entry.version} is absolute (${JSON.stringify(mount)}); mounts must be assetRoot-relative`,
            package: { name: entry.name, version: entry.version },
          });
        }
        const mountAbs = path.resolve(ctx.assetRoot, mount);
        const absPath = path.resolve(mountAbs, tarballPath);
        const mountContainmentRoot = mountAbs.endsWith(path.sep)
          ? mountAbs
          : mountAbs + path.sep;
        if (absPath !== mountAbs && !absPath.startsWith(mountContainmentRoot)) {
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `source.package.path for ${entry.name}@${entry.version} resolves to ${JSON.stringify(absPath)} which escapes the declared mount ${JSON.stringify(mountAbs)} (cross-mount traversal)`,
            package: { name: entry.name, version: entry.version },
          });
        }
        try {
          return await fs.readFile(absPath);
        } catch (err) {
          throw new ToolLoaderError({
            category: "tarball.missing",
            message: `asset-stored tarball for ${entry.name}@${entry.version} not present at ${absPath}: ${describeError(err)}`,
            package: { name: entry.name, version: entry.version },
          });
        }
      }
      const registry = registriesByName.get(entry.source.registry);
      if (registry === undefined) {
        throw new ToolLoaderError({
          category: "registry.unknown",
          message: `manifest references registry "${entry.source.registry}" which is not in the sidecar config`,
          package: { name: entry.name, version: entry.version },
        });
      }
      const tarballUrl =
        entry.tarballUrl ??
        defaultTarballUrl(registry.url, entry.name, entry.version);
      // Bound the whole fetch -- request and streamed body read -- so a
      // stalled registry cannot block the awaiting deploy forever.
      // npm-registry-fetch honors the signal for the request phase;
      // readResponseWithLimit honors it for the manual body read. The
      // timer spans both phases and is cleared only once the read settles.
      const controller = new AbortController();
      const timer = setTimeout(() => {
        controller.abort();
      }, registryFetchTimeoutMs);
      try {
        const res = await npmRegistryFetch(tarballUrl, {
          ...buildRegistryFetchOpts(registry),
          signal: controller.signal,
        });
        if (res.status === 401 || res.status === 403) {
          throw new ToolLoaderError({
            category: "registry.auth.failed",
            message: `registry "${entry.source.registry}" rejected credentials for ${entry.name}@${entry.version} (HTTP ${String(res.status)})`,
            package: { name: entry.name, version: entry.version },
          });
        }
        if (!res.ok) {
          throw new ToolLoaderError({
            category: "registry.fetch.failed",
            message: `registry "${entry.source.registry}" returned HTTP ${String(res.status)} fetching ${entry.name}@${entry.version}`,
            package: { name: entry.name, version: entry.version },
          });
        }
        return await readResponseWithLimit(
          res,
          maxRegistryTarballBytes,
          {
            registry: entry.source.registry,
            name: entry.name,
            version: entry.version,
          },
          controller.signal,
        );
      } catch (err) {
        if (err instanceof ToolLoaderError) throw err;
        if (controller.signal.aborted) {
          throw new ToolLoaderError({
            category: "registry.fetch.failed",
            message: `registry "${entry.source.registry}" fetch for ${entry.name}@${entry.version} exceeded the ${String(registryFetchTimeoutMs)}ms timeout`,
            package: { name: entry.name, version: entry.version },
          });
        }
        throw new ToolLoaderError({
          category: "registry.fetch.failed",
          message: `registry "${entry.source.registry}" fetch failed for ${entry.name}@${entry.version}: ${describeError(err)}`,
          package: { name: entry.name, version: entry.version },
        });
      } finally {
        clearTimeout(timer);
      }
    };
  }
}

function readInterchangeEntry(
  pkgJson: unknown,
  field: "tools" | "directors",
): string | null {
  if (pkgJson === null || typeof pkgJson !== "object") return null;
  if (!("interchange" in pkgJson)) return null;
  const interchange = (pkgJson as { interchange: unknown }).interchange;
  if (interchange === null || typeof interchange !== "object") return null;
  // Branch on the field rather than dynamic index-access so each path
  // narrows through a single-property shape — matches the pattern the
  // surrounding helpers use to inspect package.json without widening
  // through a `Record<string, unknown>` assertion.
  let value: unknown;
  if (field === "tools") {
    if (!("tools" in interchange)) return null;
    value = (interchange as { tools: unknown }).tools;
  } else {
    if (!("directors" in interchange)) return null;
    value = (interchange as { directors: unknown }).directors;
  }
  if (typeof value !== "string") return null;
  return value;
}

/**
 * Read a package's inline `interchange.credentials` declarations from its
 * parsed `package.json`. Unlike `tools`/`directors` (module-path fields
 * `readInterchangeEntry` resolves and imports), `credentials` is inline
 * data, so it is validated here against `ToolCredentialDeclarationArray` --
 * the same arktype the upload boundary enforces. A duplicate handle or a
 * malformed entry is therefore rejected at load with parity to the push
 * gate rather than collapsing silently downstream. Absence is a no-op: a
 * tools-only package that declares no credentials returns an empty array
 * and stays valid.
 */
function readInterchangeCredentials(
  pkgJson: unknown,
  entry: ToolPackageManifestEntry,
): readonly ToolCredentialDeclaration[] {
  if (pkgJson === null || typeof pkgJson !== "object") return [];
  if (!("interchange" in pkgJson)) return [];
  const interchange = (pkgJson as { interchange: unknown }).interchange;
  if (interchange === null || typeof interchange !== "object") return [];
  if (!("credentials" in interchange)) return [];
  const raw = (interchange as { credentials: unknown }).credentials;
  const validated = ToolCredentialDeclarationArray(raw);
  if (validated instanceof type.errors) {
    throw new ToolLoaderError({
      category: "package.entry.invalid",
      message: `${entry.name}@${entry.version} interchange.credentials failed validation: ${validated.summary}`,
      package: { name: entry.name, version: entry.version },
    });
  }
  return validated;
}

/**
 * Wrap a factory so the bundle it returns has its tool definitions
 * prefixed by the bundle's `id`. Package authors write bare tool
 * names; the model and the grant evaluator see
 * `<bundle.id>:<def.name>`. Audit provenance recorded against the
 * bundle id stays correct because the prefix is the bundle id.
 */
function applyNamespacePrefix(
  factory: LoadedToolFactory,
  pkg: { name: string; version: string },
): LoadedToolFactory {
  const prefix = `${factory.id}:`;
  // Freeze the wrapper AND the requires array it points at so
  // downstream consumers cannot mutate the `id`/`requires` metadata
  // the namespacing depends on. A mutated `id` would skew audit-trail
  // provenance away from the bundle the loader actually constructed;
  // a mutated `requires` would let a wrapper accumulate unintended
  // capability requests over its lifetime. Freezing the wrapper alone
  // blocks reassigning `wrapped.requires`; freezing the array (after
  // copying so the source factory's own `requires` is not also frozen
  // as a side-effect) blocks the `push` / `splice` mutations that
  // would otherwise grow the surface in place.
  const frozenRequires = Object.freeze([...factory.requires]);
  // The wrapped factory contributes prefixed tool names at runtime, so
  // its static declaration must carry the same prefixed names to stay
  // truthful for callers that enumerate `definitions` without invoking
  // the factory.
  const frozenDefinitions = Object.freeze(
    factory.definitions.map((def) => ({
      ...def,
      name: `${prefix}${def.name}`,
    })),
  );
  const wrapped: LoadedToolFactory = Object.freeze(
    Object.assign(
      (env: BaseEnv) => {
        const bundle = factory(env);
        // A definition whose raw name already starts with the bundle's
        // prefix indicates the package author either double-prefixed or
        // happened to choose a name that collides with the prefix shape.
        // Either way silently passing it through would yield surprising
        // results in the audit trail and grant evaluator — surface it.
        // Build the prefixed-definition list and the prefixed→raw name
        // map in a single pass so the name map is provably aligned with
        // the array TypeScript already proved was the same length.
        // Shape-check `bundle.definitions` before iterating: a factory
        // that returns `definitions: null` (or omits the field) would
        // otherwise yield a bare TypeError that the apply pipeline
        // surfaces as `factory.construct.failed` instead of the more
        // accurate `package.entry.invalid` (the bundle's shape is
        // wrong, not its construction).
        if (!Array.isArray(bundle.definitions)) {
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `bundle ${factory.id} returned a non-array \`definitions\` field; AnnotatedToolFactory bundles must produce an array of tool definitions`,
          });
        }
        // Surface intra-bundle name collisions BEFORE prefixing so two
        // definitions named `search` would not silently collapse to a
        // single `<id>:search` entry in the name map.
        //
        // Timing note: this check runs at first factory invocation
        // (agent construction at sidecar boot), NOT at apply time. The
        // loader cannot read `bundle.definitions` without invoking the
        // factory, and the `BaseEnv` the factory needs is constructed
        // by the sidecar harness only after the apply commits. As a
        // consequence, an intra-bundle duplicate surfaces on the
        // runtime construct-failure channel rather than as an
        // apply-error frame. The cross-bundle case (in atomic-apply.ts)
        // catches the same category at apply time because it operates
        // on `factory.id` metadata, which is available without invoking
        // the factory. See the `tool.name.duplicate` category docstring
        // on `DeployApplyErrorCategory` for the operator-facing
        // contract this split honors.
        const rawSeen = new Set<string>();
        for (const def of bundle.definitions) {
          if (rawSeen.has(def.name)) {
            throw new ToolLoaderError({
              category: "tool.name.duplicate",
              message: `bundle ${factory.id} exports two tool definitions named ${JSON.stringify(def.name)}; tool names must be unique within a bundle`,
              package: pkg,
            });
          }
          rawSeen.add(def.name);
        }
        const nameMap = new Map<string, string>();
        const prefixed = bundle.definitions.map((def) => {
          if (def.name.startsWith(prefix)) {
            throw new ToolLoaderError({
              category: "package.entry.invalid",
              message: `tool definition name ${JSON.stringify(def.name)} already begins with bundle prefix ${JSON.stringify(prefix)}; raw definition names must not include the bundle id`,
              package: pkg,
            });
          }
          const prefixedName = `${prefix}${def.name}`;
          nameMap.set(prefixedName, def.name);
          return { ...def, name: prefixedName };
        });
        return {
          definitions: prefixed,
          run: (call: ToolCall, signal: AbortSignal): Promise<ToolResult> => {
            const original = nameMap.get(call.name);
            // nameMap holds every prefixed form this bundle minted; a
            // miss means `call.name` is not one of those prefixed
            // names. Forwarding the unprefixed name into the inner
            // bundle would bypass the namespacing the wrapper exists
            // to enforce — an unprefixed name that happened to match
            // the bundle's raw tool name would run the tool — so
            // return a structured unknown-tool error directly.
            if (original === undefined) {
              return Promise.resolve({
                callId: call.id,
                content: `unknown tool: ${call.name}`,
                isError: true,
              });
            }
            const inner: ToolCall = { ...call, name: original };
            return bundle.run(inner, signal);
          },
          ...(bundle.dispose !== undefined ? { dispose: bundle.dispose } : {}),
        };
      },
      {
        id: factory.id,
        requires: frozenRequires,
        definitions: frozenDefinitions,
      },
    ),
  );
  return wrapped;
}

function isAnnotatedToolFactory(value: unknown): value is LoadedToolFactory {
  if (typeof value !== "function") return false;
  // Plugin factories carry the same id/requires duck-shape — explicitly
  // reject anything bearing the plugin marker so the predicate stands
  // alone instead of relying on the loader's ordering at the call site.
  if (isAnnotatedPluginFactory(value)) return false;
  if (!("id" in value) || !("requires" in value)) return false;
  // Director factories carry id/requires plus a callable `configSchema`;
  // without this guard a director placed in `interchange.tools` would be
  // silently classified as a tool and namespace-prefixed. Mirrors the
  // discriminator `isAnnotatedDirectorFactory` uses against tool shapes.
  if ("configSchema" in value) {
    const configSchema = (value as { configSchema: unknown }).configSchema;
    if (typeof configSchema === "function") return false;
  }
  const id = (value as { id: unknown }).id;
  const requires = (value as { requires: unknown }).requires;
  if (typeof id !== "string") return false;
  if (!Array.isArray(requires)) return false;
  return requires.every((r) => typeof r === "string");
}
