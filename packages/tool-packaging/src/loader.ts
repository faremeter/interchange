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
//      hardlinking the file tree from the cache extraction. Each layout
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
import semver from "semver";
import npmRegistryFetch from "npm-registry-fetch";

import type {
  AnnotatedDirectorFactory,
  AnnotatedPluginFactory,
  AnnotatedToolFactory,
  BaseEnv,
} from "@intx/agent";
import { isAnnotatedPluginFactory } from "@intx/agent";
import type { ToolCall, ToolResult } from "@intx/types/runtime";
import { getLogger } from "@intx/log";
import type { DeployApplyErrorCategory } from "@intx/types/sidecar";
import type {
  ToolPackageManifest,
  ToolPackageManifestEntry,
} from "@intx/types/tool-packages";

import type { TarballCache } from "./cache";
import { TarballIntegrityMismatchError } from "./cache";
import type { RegistryConfig } from "./resolver";

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

/**
 * Default cap on a single HTTP-registry tarball fetch. Matches the
 * hub's `DEFAULT_HUB_MAX_TARBALL_BYTES` so a tarball the hub accepted
 * on upload is one the sidecar can also fetch back when a registry
 * mirror replays it.
 */
export const DEFAULT_MAX_REGISTRY_TARBALL_BYTES = 10 * 1024 * 1024;

/**
 * Default deadline for a single HTTP-registry tarball fetch, covering
 * both the request and the streamed body read. `readResponseWithLimit`
 * consumes the body through a manual reader loop, so the byte cap bounds
 * size but nothing bounds time: a registry that accepts the connection
 * and then stalls mid-stream would block the fetch -- and the deploy's
 * tool materialization awaiting it -- indefinitely.
 * The deadline is generous so a legitimately large tarball on a slow
 * link still completes within it. Callers that need a different bound
 * pass `registryFetchTimeoutMs` to `createToolLoader`.
 */
export const DEFAULT_REGISTRY_FETCH_TIMEOUT_MS = 120 * 1000;

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
}

export interface ToolLoader {
  loadManifest(args: LoadManifestArgs): Promise<LoadedToolPackage[]>;
}

export class ToolLoaderError extends Error {
  readonly category: DeployApplyErrorCategory;
  readonly package:
    | { readonly name: string; readonly version: string }
    | undefined;

  constructor(opts: {
    category: DeployApplyErrorCategory;
    message: string;
    package?: { name: string; version: string };
  }) {
    super(opts.message);
    this.name = "ToolLoaderError";
    this.category = opts.category;
    this.package = opts.package;
  }
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

  async function materialize(
    entry: ToolPackageManifestEntry,
    assetRoot: string,
    assetMounts: ReadonlyMap<string, string>,
  ): Promise<{ dir: string; release: () => void }> {
    // Resolve registry-sourced entries against the sidecar config
    // before doing any I/O. If the manifest references an unknown
    // registry name the apply fails loudly here, regardless of whether
    // the bytes are already cached, so the failure surfaces even on
    // cache hits that would otherwise hide the misconfiguration.
    if (entry.source.kind === "registry") {
      if (!registriesByName.has(entry.source.registry)) {
        throw new ToolLoaderError({
          category: "registry.unknown",
          message: `manifest references registry "${entry.source.registry}" which is not in the sidecar config`,
          package: { name: entry.name, version: entry.version },
        });
      }
    } else if (entry.source.kind === "asset") {
      // Reject up front (parallel to the registry.unknown gate) so a
      // cache hit cannot hide a missing mount from the manifest fan-out.
      if (!assetMounts.has(entry.source.assetId)) {
        throw new ToolLoaderError({
          category: "asset.mount.missing",
          message: `manifest entry references assetId "${entry.source.assetId}" which is not in the deploy pack's asset-mounts map`,
          package: { name: entry.name, version: entry.version },
        });
      }
    }

    // Probe cache presence with `has` rather than `get`: the bytes are
    // only needed when they have to be fetched-then-stored, and
    // `extractTarball` below re-reads them from disk on the way to the
    // per-integrity unpack directory. `has` checks file existence
    // without reading or atime-touching the bytes, so a cache-hit
    // apply avoids the wasted read of a tarball that immediately gets
    // discarded.
    if (!(await config.cache.has(entry.integrity))) {
      const bytes = await fetchTarball(entry, {
        registries: config.registries,
        assetRoot,
        assetMounts,
      });
      try {
        await config.cache.put(entry.integrity, bytes);
      } catch (err) {
        if (err instanceof TarballIntegrityMismatchError) {
          throw new ToolLoaderError({
            category: "integrity.mismatch",
            message: `bytes for ${entry.name}@${entry.version} did not match pinned integrity`,
            package: { name: entry.name, version: entry.version },
          });
        }
        throw err;
      }
    }

    try {
      return await config.cache.extractTarball(entry.integrity);
    } catch (err) {
      // Eviction is reserved for the integrity-mismatch path: the bytes
      // on disk no longer match the pinned hash, so the entry is poison
      // and must be re-fetched. Other failures — tar parse errors, FS
      // transients (EIO, ENOSPC) — leave the cached bytes intact. The
      // cache's `evict` defers physical reclaim of the extraction tree
      // until every outstanding `release` from a concurrent
      // `extractTarball` has fired, so a parallel agent's in-flight
      // `hardlinkTree` walk against the same extraction will not
      // ENOENT mid-readdir.
      if (err instanceof TarballIntegrityMismatchError) {
        await config.cache.evict(entry.integrity);
      }
      throw new ToolLoaderError({
        category: "tarball.extract.failed",
        message: `tar extraction failed for ${entry.name}@${entry.version}: ${describeError(err)}`,
        package: { name: entry.name, version: entry.version },
      });
    }
  }

  function passesPlatformFilter(entry: ToolPackageManifestEntry): boolean {
    if (
      entry.os !== undefined &&
      !platformListMatches(entry.os, config.host.os)
    ) {
      logger.debug`platform.mismatch.skipped: ${entry.name}@${entry.version} requires os ${entry.os.join(",")} (host is ${config.host.os})`;
      return false;
    }
    if (
      entry.cpu !== undefined &&
      !platformListMatches(entry.cpu, config.host.cpu)
    ) {
      logger.debug`platform.mismatch.skipped: ${entry.name}@${entry.version} requires cpu ${entry.cpu.join(",")} (host is ${config.host.cpu})`;
      return false;
    }
    return true;
  }

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

    return {
      name: entry.name,
      version: entry.version,
      factories,
      plugins,
      directors,
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

    // Cache-bust the ESM module cache by appending the entry integrity
    // as a query string. Node keys the ESM cache by resolved URL/path,
    // not by content: a `(name, version)` pair whose bytes change
    // across applies (an operator-recompiled built-in, a hot-fixed
    // tarball republished under the same version) would otherwise
    // resolve to the previously-imported module instance until the
    // sidecar restarts. Same path with a different query is a distinct
    // ESM cache entry, so the import reflects the bytes actually
    // extracted for this apply.
    const importUrl = `${pathToFileURL(entryAbs).href}?integrity=${encodeURIComponent(entry.integrity)}`;
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
      const filtered = args.manifest.entries.filter(passesPlatformFilter);
      const storeDir = path.join(args.instanceScratchDir, "store");
      const topLevelKeys = new Set(
        args.manifest.topLevel.map((p) => `${p.name}@${p.version}`),
      );

      // 1. Materialize every filtered entry into the cache and capture
      //    its extraction directory. This validates the manifest is
      //    registry-chain-consistent (each entry resolves end-to-end
      //    against its declared source) and primes the cache so the
      //    layout step can hardlink without re-fetching.
      //
      //    Each materialize() returns an `{ dir, release }` pair: the
      //    cache treats the returned `dir` as held until `release` is
      //    called, so a concurrent eviction of the same integrity
      //    defers its physical reclaim of the extraction tree until
      //    after the buildStoreLayout pass below has finished walking
      //    every dir to hardlink files out. Releases are aggregated and
      //    drained in a `finally` so an error mid-layout still hands
      //    the cache its references back.
      const extractionByEntry = new Map<string, string>();
      const entriesByNameVersion = new Map<string, ToolPackageManifestEntry>();
      const releases: (() => void)[] = [];
      try {
        for (const entry of filtered) {
          const handle = await materialize(
            entry,
            args.assetRoot,
            args.assetMounts,
          );
          const key = `${entry.name}@${entry.version}`;
          extractionByEntry.set(key, handle.dir);
          entriesByNameVersion.set(key, entry);
          releases.push(handle.release);
        }

        // 2. Build the per-instance store layout. Each filtered entry
        //    gets a real directory at `<store>/<name>/<version>/`
        //    populated by hardlinks from its cache extraction; the
        //    direct-dependency walk then symlinks `node_modules/<dep>`
        //    into each layout dir so Node's standard ancestor walk
        //    resolves bare-specifier imports from inside the package's
        //    body against the closure's pinned versions.
        const rangeResolution = await resolveRangesByFirstArrival({
          topLevel: args.manifest.topLevel,
          filtered,
          extractionByEntry,
          entriesByNameVersion,
        });
        await buildStoreLayout({
          filtered,
          storeDir,
          extractionByEntry,
          rangeResolution,
        });

        // 3. Then load only the top-level packages; transitive entries
        //    exist for `node_modules/` satisfaction but do not contribute
        //    factories of their own.
        const loaded: LoadedToolPackage[] = [];
        const coveredTopLevelKeys = new Set<string>();
        for (const entry of filtered) {
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
      } finally {
        for (const release of releases) {
          release();
        }
      }
    },
  };

  function makeDefaultTarballFetcher(): TarballFetcher {
    return async (entry, ctx) => {
      if (entry.source.kind === "asset") {
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
        // Both `mount` and `entry.source.path` originate from the hub
        // and cross the trust boundary into the sidecar process. A `..`
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
        const absPath = path.resolve(mountAbs, entry.source.path);
        const mountContainmentRoot = mountAbs.endsWith(path.sep)
          ? mountAbs
          : mountAbs + path.sep;
        if (absPath !== mountAbs && !absPath.startsWith(mountContainmentRoot)) {
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `source.path for ${entry.name}@${entry.version} resolves to ${JSON.stringify(absPath)} which escapes the declared mount ${JSON.stringify(mountAbs)} (cross-mount traversal)`,
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

export function buildRegistryFetchOpts(
  registry: RegistryConfig,
): Record<string, unknown> {
  const opts: Record<string, unknown> = { registry: registry.url };
  if (registry.auth?.token !== undefined) {
    opts.token = registry.auth.token;
  }
  if (registry.auth?.basic !== undefined) {
    const { user, pass } = registry.auth.basic;
    // `npm-registry-fetch` builds the `Authorization: Basic` header by
    // base64-encoding `<username>:<password>` itself. Pre-encoding
    // `pass` would double-encode the password component (the registry
    // would see `base64(plaintext)` as the password, not `plaintext`).
    opts.forceAuth = { username: user, password: pass };
  }
  return opts;
}

function defaultTarballUrl(
  registryUrl: string,
  name: string,
  version: string,
): string {
  const base = registryUrl.endsWith("/") ? registryUrl : `${registryUrl}/`;
  // Match npm's canonical tarball URL: {registry}/{name}/-/{basename}-{version}.tgz
  const basename = name.startsWith("@") ? name.split("/")[1] : name;
  if (basename === undefined) {
    throw new Error(`internal: cannot derive tarball basename for ${name}`);
  }
  return `${base}${name}/-/${basename}-${version}.tgz`;
}

/**
 * Read an HTTP-registry tarball response into a Uint8Array while enforcing
 * `maxBytes`. Two guards:
 *
 *   1. If the upstream sent a `Content-Length` header, parse it (digit-
 *      only, per RFC 9110 §8.6) and reject up front when the declared
 *      length exceeds the cap. A header that fails the digit shape is
 *      also rejected so a header like `1e9` cannot read as 1e9 against
 *      `Number()` while a digit-only cap check would pass.
 *   2. Stream the body chunk-by-chunk, tallying byte length, and abort
 *      the read when the running total crosses the cap. This catches
 *      the missing-or-lying header case.
 *
 * An optional `signal` adds a time guard: when it aborts (the caller's
 * fetch deadline), the in-flight read is cancelled and the call rejects,
 * so a registry that streams the body slowly or stalls mid-stream cannot
 * outlast the deadline while staying under the byte cap.
 *
 * All rejections surface as `registry.fetch.failed` so the apply layer
 * routes them the same as any other registry-side fetch defect.
 *
 * Exported for direct unit testing.
 */
export async function readResponseWithLimit(
  res: Response,
  maxBytes: number,
  ctx: {
    readonly registry: string;
    readonly name: string;
    readonly version: string;
  },
  signal?: AbortSignal,
): Promise<Uint8Array> {
  const declaredLengthRaw = res.headers.get("content-length");
  if (declaredLengthRaw !== null) {
    if (!/^\d+$/.test(declaredLengthRaw)) {
      throw new ToolLoaderError({
        category: "registry.fetch.failed",
        message: `registry "${ctx.registry}" returned non-digit Content-Length ${JSON.stringify(declaredLengthRaw)} for ${ctx.name}@${ctx.version}`,
        package: { name: ctx.name, version: ctx.version },
      });
    }
    const declaredLength = Number(declaredLengthRaw);
    if (!Number.isFinite(declaredLength) || declaredLength > maxBytes) {
      throw new ToolLoaderError({
        category: "registry.fetch.failed",
        message: `tarball for ${ctx.name}@${ctx.version} declares Content-Length ${declaredLengthRaw} which exceeds the ${String(maxBytes)}-byte cap`,
        package: { name: ctx.name, version: ctx.version },
      });
    }
  }

  const body = res.body;
  if (body === null) {
    // No body and the upstream returned 2xx: treat as a zero-byte
    // tarball. The cache and tar-extract layers will reject the
    // resulting bytes as non-tar content, but the fetch itself didn't
    // fail — keep this path simple rather than over-rejecting.
    return new Uint8Array(0);
  }
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  // Cancelling the reader settles any pending read() as done, so the
  // post-read check below surfaces the timeout even when the underlying
  // body stream does not itself observe the abort signal.
  let timedOut = false;
  const onAbort = () => {
    timedOut = true;
    void reader.cancel();
  };
  signal?.addEventListener("abort", onAbort, { once: true });
  if (signal?.aborted === true) onAbort();
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (timedOut) {
        throw new ToolLoaderError({
          category: "registry.fetch.failed",
          message: `tarball read for ${ctx.name}@${ctx.version} exceeded the registry fetch timeout`,
          package: { name: ctx.name, version: ctx.version },
        });
      }
      if (done) break;
      if (value === undefined) continue;
      total += value.byteLength;
      if (total > maxBytes) {
        // Stop reading; we already have enough evidence the upstream
        // is over the cap. The reader.cancel() call requests
        // cancellation upstream; the runtime decides whether to drop
        // the in-flight TCP frames or just unsubscribe our reader.
        await reader.cancel();
        throw new ToolLoaderError({
          category: "registry.fetch.failed",
          message: `tarball for ${ctx.name}@${ctx.version} streamed past the ${String(maxBytes)}-byte cap`,
          package: { name: ctx.name, version: ctx.version },
        });
      }
      chunks.push(value);
    }
  } finally {
    signal?.removeEventListener("abort", onAbort);
    reader.releaseLock();
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

function storeEntryDir(
  storeDir: string,
  name: string,
  version: string,
): string {
  // `@scope/name` carries a slash that, taken naively, would push the
  // package's contents one directory deeper than `loadTopLevel`
  // expects. Mirror npm's on-disk shape: `node_modules/@scope/name/`,
  // so a scoped entry's dir is `<store>/@scope/name/<version>/`.
  return path.join(storeDir, name, version);
}

interface BuildStoreLayoutArgs {
  readonly filtered: readonly ToolPackageManifestEntry[];
  readonly storeDir: string;
  readonly extractionByEntry: ReadonlyMap<string, string>;
  readonly rangeResolution: RangeResolution;
}

/**
 * Build the per-instance `<store>/<name>/<version>/` tree for every
 * filtered manifest entry: hardlink each entry's source files in from
 * the cache extraction, then symlink each direct dep into the entry's
 * `node_modules/`. Hardlinks keep byte usage to one copy per integrity
 * per filesystem; symlinks at the `node_modules/` boundary let Node's
 * realpath-based resolver walk to the dep's own layout dir (with its
 * own `node_modules/`) so transitive resolution composes recursively.
 */
async function buildStoreLayout(args: BuildStoreLayoutArgs): Promise<void> {
  // First materialize every layout dir with its hardlinked contents.
  // node_modules symlinks come after, so a dep's layout dir is already
  // populated when its parent's symlink starts pointing at it.
  for (const entry of args.filtered) {
    const key = `${entry.name}@${entry.version}`;
    const extraction = args.extractionByEntry.get(key);
    if (extraction === undefined) {
      throw new Error(
        `internal: layout build for ${key} found no cache extraction`,
      );
    }
    const layoutDir = storeEntryDir(args.storeDir, entry.name, entry.version);
    await fs.mkdir(path.dirname(layoutDir), { recursive: true });
    await hardlinkTree(extraction, layoutDir);
  }

  for (const entry of args.filtered) {
    const key = `${entry.name}@${entry.version}`;
    const extraction = args.extractionByEntry.get(key);
    if (extraction === undefined) {
      throw new Error(
        `internal: layout link pass for ${key} found no cache extraction`,
      );
    }
    const layoutDir = storeEntryDir(args.storeDir, entry.name, entry.version);
    const deps = await readDirectDependencies(extraction, entry);

    if (deps.length === 0) continue;
    const modulesDir = path.join(layoutDir, "node_modules");
    await fs.mkdir(modulesDir, { recursive: true });

    for (const dep of deps) {
      const pickedVersion = args.rangeResolution.lookup(dep.name, dep.range);
      if (pickedVersion === null) {
        if (dep.optional) {
          logger.debug`optional.dropped.skipped: ${entry.name}@${entry.version} optional dep ${dep.name}@${dep.range} has no satisfying version in the closure (likely platform-filtered out)`;
          continue;
        }
        throw new ToolLoaderError({
          category: "package.entry.invalid",
          message: `${entry.name}@${entry.version} depends on ${dep.name}@${dep.range} but the manifest closure has no satisfying version; the resolver was expected to include it`,
          package: { name: entry.name, version: entry.version },
        });
      }
      const target = storeEntryDir(args.storeDir, dep.name, pickedVersion);
      const symlinkPath = path.join(modulesDir, dep.name);
      // Scoped deps live one directory deep under `node_modules/`;
      // ensure the scope dir exists before linking.
      await fs.mkdir(path.dirname(symlinkPath), { recursive: true });
      const relativeTarget = path.relative(path.dirname(symlinkPath), target);
      try {
        await fs.symlink(relativeTarget, symlinkPath, "dir");
      } catch (err) {
        if (!isEEXIST(err)) throw err;
        const existing = await fs.readlink(symlinkPath);
        if (existing !== relativeTarget) {
          // A symlink collision inside the loader's per-package
          // layout pass is a loader-layer invariant violation, not an
          // unknown error shape — route it through the same structured
          // envelope every other loader failure uses so atomic-apply
          // surfaces it as `package.entry.invalid` instead of falling
          // back to the unknown-shape catch-all (`factory.construct.
          // failed`).
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `symlink collision at ${symlinkPath}: existing target ${existing} differs from ${relativeTarget}`,
          });
        }
      }
    }
  }
}

interface ResolveRangesArgs {
  readonly topLevel: readonly {
    readonly name: string;
    readonly version: string;
  }[];
  readonly filtered: readonly ToolPackageManifestEntry[];
  readonly extractionByEntry: ReadonlyMap<string, string>;
  readonly entriesByNameVersion: ReadonlyMap<string, ToolPackageManifestEntry>;
}

interface RangeResolution {
  lookup(name: string, range: string): string | null;
}

/**
 * Walk the closure in BFS order from the top-level pins (in their
 * input order) and record, for each `(name, range)` first encountered,
 * the version chosen out of the closure. Subsequent edges with the
 * same `(name, range)` reuse the recorded pick instead of re-running
 * `semver.maxSatisfying` against the current closure shape.
 *
 * Mirrors the resolver's first-arrival-per-`(name, range)` semantics
 * on the loader side. Without this, two requirers with overlapping
 * ranges of the same dep could each pick a different version of that
 * dep — `maxSatisfying` is deterministic given its candidate set, but
 * the candidate set is the full closure for the name and a transitive
 * addition since the first arrival can shift the answer. Recording
 * the first arrival per range freezes the pick so every requirer in
 * the same equivalence class lands on the same version of the dep.
 *
 * Returns null for a `(name, range)` that has no satisfying entry in
 * the filtered closure; callers decide whether that is fatal (hard
 * dep) or skippable (optional dep).
 */
async function resolveRangesByFirstArrival(
  args: ResolveRangesArgs,
): Promise<RangeResolution> {
  const recorded = new Map<string, string | null>();
  const visited = new Set<string>();
  const filteredKeys = new Set(
    args.filtered.map((e) => `${e.name}@${e.version}`),
  );

  function rangeKey(name: string, range: string): string {
    return `${name}@${range}`;
  }

  function pickFromClosure(name: string, range: string): string | null {
    const candidates: string[] = [];
    for (const entry of args.entriesByNameVersion.values()) {
      if (entry.name !== name) continue;
      if (!filteredKeys.has(`${entry.name}@${entry.version}`)) continue;
      candidates.push(entry.version);
    }
    if (candidates.length === 0) return null;
    const valid = candidates.filter((v) => semver.valid(v) !== null);
    if (valid.length > 0) {
      const picked = semver.maxSatisfying(valid, range, {
        includePrerelease: true,
      });
      if (picked !== null) return picked;
    }
    // Literal-version fallback: when a transitive dep's range is
    // itself a concrete version string (e.g. `'1.0.0'` not
    // `'^1.0.0'`), `maxSatisfying` rejects on prerelease semantics but
    // the literal match is valid.
    if (candidates.includes(range)) return range;
    return null;
  }

  // BFS frontier carries the entry whose direct deps we are about to
  // fan out on next. Seed with the top-level pins in pin order, mapped
  // through the filtered closure so platform-filtered tops are skipped
  // (their deps would not have layout dirs to link into).
  const queue: ToolPackageManifestEntry[] = [];
  for (const pin of args.topLevel) {
    const key = `${pin.name}@${pin.version}`;
    const entry = args.entriesByNameVersion.get(key);
    if (entry === undefined) continue;
    if (!filteredKeys.has(key)) continue;
    if (visited.has(key)) continue;
    visited.add(key);
    queue.push(entry);
  }

  while (queue.length > 0) {
    const entry = queue.shift();
    if (entry === undefined) break;
    const extraction = args.extractionByEntry.get(
      `${entry.name}@${entry.version}`,
    );
    if (extraction === undefined) continue;
    const deps = await readDirectDependencies(extraction, entry);
    for (const dep of deps) {
      const key = rangeKey(dep.name, dep.range);
      // `recorded.get(key)` returning `null` is the "we picked this
      // range against the closure and got nothing" cached answer.
      // Caching the null is safe only because the closure is static
      // across this loader pass — `entriesByNameVersion` does not
      // grow underneath us. If a future change starts adding entries
      // mid-walk (e.g. lazy fetches during BFS), the cached null
      // would shadow the new candidates and produce a phantom miss;
      // the cache key would need to be invalidated alongside the
      // closure additions.
      let picked = recorded.get(key);
      if (picked === undefined) {
        picked = pickFromClosure(dep.name, dep.range);
        recorded.set(key, picked);
      }
      if (picked === null) continue;
      const depKey = `${dep.name}@${picked}`;
      if (visited.has(depKey)) continue;
      visited.add(depKey);
      const depEntry = args.entriesByNameVersion.get(depKey);
      if (depEntry === undefined) continue;
      queue.push(depEntry);
    }
  }

  return {
    lookup(name, range) {
      const key = rangeKey(name, range);
      if (recorded.has(key)) {
        const picked = recorded.get(key);
        return picked === undefined ? null : picked;
      }
      // The BFS only walks entries reachable from the top-level pins.
      // A dep declared by an entry the BFS did not reach (e.g. a
      // closure entry that no top-level chain ever required) is not
      // pre-recorded; fall through to a fresh pick from the closure
      // so the layout for such entries still resolves deterministically.
      const fallback = pickFromClosure(name, range);
      recorded.set(key, fallback);
      return fallback;
    },
  };
}

async function hardlinkTree(
  srcDir: string,
  destDir: string,
  extractionRoot: string = srcDir,
): Promise<void> {
  await fs.mkdir(destDir, { recursive: true });
  const entries = await fs.readdir(srcDir, { withFileTypes: true });
  for (const entry of entries) {
    const src = path.join(srcDir, entry.name);
    const dest = path.join(destDir, entry.name);
    if (entry.isDirectory()) {
      await hardlinkTree(src, dest, extractionRoot);
    } else if (entry.isFile()) {
      try {
        await fs.link(src, dest);
      } catch (err) {
        if (!isEEXIST(err)) throw err;
      }
    } else if (entry.isSymbolicLink()) {
      // Preserve symlinks from the tarball verbatim; npm packages
      // occasionally ship them and clobbering with a hardlink would
      // change the file's identity.
      //
      // ISOMORPHIC-LAYOUT ASSUMPTION: writing the source-side
      // relative target verbatim into the destination only works
      // because the source extraction tree and the per-instance
      // store tree mirror each other entry-for-entry — the symlink
      // copies into the same shape, so the relative target still
      // resolves to the same sibling in the destination. A future
      // change that flattens, reshapes, or partially copies the
      // extraction tree would invalidate every symlink it touched
      // and would need to rewrite the targets instead of preserving
      // them.
      //
      // Symlink targets originate from the tarball and cross the trust
      // boundary into the sidecar. Resolve each target against the
      // symlink's own directory and verify it lands inside the
      // extraction root; a target that escapes would let a malicious
      // tarball point at arbitrary sidecar-readable files via the
      // layout dir's `node_modules` walk.
      //
      // The `tar` package version we use rejects absolute symlink
      // targets during extraction, so by the time we observe a
      // symlink here it is necessarily relative.
      //
      // The immediate target of `src` may itself be a directory whose
      // own contents include another symlink. Resolving only the
      // first hop with `path.resolve(path.dirname(src), target)`
      // checks containment of the link's literal target — a chain
      // whose first hop lands inside the extraction root but whose
      // realpath ultimately escapes (target is a directory that
      // itself contains an escaping symlink) would slip past.
      // `fs.realpath` walks the full chain and returns the canonical
      // absolute path; verify containment against that.
      const target = await fs.readlink(src);
      // Compare against the realpath of the extraction root so a chain
      // whose canonical path lands under the same logical root, but
      // via a symlinked tmpdir prefix (notably macOS where `/tmp`
      // resolves to `/private/tmp`), is not incorrectly flagged as
      // an escape.
      let realExtractionRoot: string;
      try {
        realExtractionRoot = await fs.realpath(extractionRoot);
      } catch (err) {
        throw new ToolLoaderError({
          category: "package.entry.invalid",
          message: `tarball symlink ${src} → ${target}: extraction-root realpath failed: ${describeError(err)}`,
        });
      }
      // `path.resolve` produces the absolute path the symlink would
      // dereference to without following any links itself; realpath
      // walks the chain. A dangling symlink — one whose target chain
      // ENOENTs before the final inode — is harmless on disk (it
      // points at a name that does not exist), so the containment
      // check falls back to the literal resolved path in that case.
      // Any other realpath error is fatal; we cannot prove containment
      // and the package is rejected.
      //
      // The fallback anchors the literal resolution at `realpath(src
      // dirname)` rather than the as-declared `dirname(src)`. The
      // dirname already exists on disk (extraction wrote it); realpath
      // walks any symlinks in the prefix so the comparison against
      // `realExtractionRoot` is realpath-vs-realpath on both sides.
      // Without this, platforms whose extraction-root prefix contains
      // symlinks (notably macOS, where `/var/folders/...` resolves to
      // `/private/var/folders/...`) would reject a properly-contained
      // dangling link because the literal path keeps the as-declared
      // prefix while the extraction root has been realpath'd.
      let targetAbs: string;
      try {
        targetAbs = await fs.realpath(path.resolve(path.dirname(src), target));
      } catch (err) {
        if (!isENOENT(err)) {
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `tarball contains symlink ${src} → ${target} whose target could not be resolved: ${describeError(err)}`,
          });
        }
        let srcDirReal: string;
        try {
          srcDirReal = await fs.realpath(path.dirname(src));
        } catch (dirErr) {
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `tarball symlink ${src} → ${target}: dirname realpath failed during dangling-link fallback: ${describeError(dirErr)}`,
          });
        }
        targetAbs = path.resolve(srcDirReal, target);
      }
      const realContainmentRoot = realExtractionRoot.endsWith(path.sep)
        ? realExtractionRoot
        : realExtractionRoot + path.sep;
      if (
        targetAbs !== realExtractionRoot &&
        !targetAbs.startsWith(realContainmentRoot)
      ) {
        throw new ToolLoaderError({
          category: "package.entry.invalid",
          message: `tarball contains symlink ${src} → ${target} that escapes the package extraction directory`,
        });
      }
      try {
        await fs.symlink(target, dest);
      } catch (err) {
        if (!isEEXIST(err)) throw err;
      }
    }
  }
}

interface DirectDep {
  readonly name: string;
  readonly range: string;
  readonly optional: boolean;
}

/**
 * Read the package.json at `extractionDir/package.json` and return the
 * union of `dependencies` and `optionalDependencies`. Each entry is
 * tagged with whether it came from the optional field so the layout
 * pass can decide whether a missing closure entry is fatal (hard dep)
 * or skippable (the resolver's platform filter excluded it from the
 * closure for this host).
 *
 * `dependencies` shadows `optionalDependencies` when the same name
 * appears in both — npm treats the dep as required in that case.
 */
async function readDirectDependencies(
  extractionDir: string,
  entry: { readonly name: string; readonly version: string },
): Promise<DirectDep[]> {
  const pkgJsonRaw = await fs.readFile(
    path.join(extractionDir, "package.json"),
    "utf8",
  );
  let pkg: unknown;
  try {
    pkg = JSON.parse(pkgJsonRaw);
  } catch (err) {
    throw new ToolLoaderError({
      category: "package.entry.invalid",
      message: `malformed package.json in ${entry.name}@${entry.version}: ${describeError(err)}`,
      package: { name: entry.name, version: entry.version },
    });
  }
  const byName = new Map<string, DirectDep>();
  if (pkg === null || typeof pkg !== "object") return [];
  const record: Record<string, unknown> = { ...pkg };
  // A non-string range value (number, null, nested object, array) is
  // a malformed package.json the npm CLI would also reject. Silently
  // dropping it would let the closure resolver later reject the apply
  // with a misleading `package.entry.invalid` for the wrong layer —
  // the malformation is here, not in the closure walk. Surface it as
  // `package.entry.invalid` directly so the operator-facing message
  // points at the bad package.
  //
  // Iteration order matters: write optionalDependencies FIRST, then
  // dependencies. The `dependencies` write overwrites the same key on
  // collision, which is the npm-shadowing rule documented above.
  // Reversing these two blocks would silently make the optional
  // declaration win and demote a hard dependency to optional.
  const optionalDeps = record["optionalDependencies"];
  if (optionalDeps !== undefined) {
    assertDepMapShape(optionalDeps, "optionalDependencies", entry);
    if (optionalDeps !== null && typeof optionalDeps === "object") {
      for (const [name, range] of Object.entries(optionalDeps)) {
        if (typeof range !== "string") {
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `package.json field optionalDependencies["${name}"] in ${entry.name}@${entry.version} is ${typeof range}, expected a string range`,
            package: { name: entry.name, version: entry.version },
          });
        }
        byName.set(name, { name, range, optional: true });
      }
    }
  }
  const deps = record["dependencies"];
  if (deps !== undefined) {
    assertDepMapShape(deps, "dependencies", entry);
    if (deps !== null && typeof deps === "object") {
      for (const [name, range] of Object.entries(deps)) {
        if (typeof range !== "string") {
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `package.json field dependencies["${name}"] in ${entry.name}@${entry.version} is ${typeof range}, expected a string range`,
            package: { name: entry.name, version: entry.version },
          });
        }
        byName.set(name, { name, range, optional: false });
      }
    }
  }
  return Array.from(byName.values());
}

/**
 * Reject array-shaped `dependencies` / `optionalDependencies`. The
 * surrounding code narrows with `typeof X === "object"`, which is true
 * for arrays — and `Object.entries(["foo"])` produces `[["0", "foo"]]`,
 * feeding nonsense package names into the closure resolver. Failure
 * downstream is loud but the message points at the wrong layer. Reject
 * at the package-json read with a clear, structured failure instead.
 */
function assertDepMapShape(
  value: unknown,
  field: "dependencies" | "optionalDependencies",
  entry: { readonly name: string; readonly version: string },
): void {
  if (Array.isArray(value)) {
    throw new ToolLoaderError({
      category: "package.entry.invalid",
      message: `package.json#${field} for ${entry.name}@${entry.version} must be an object map of name→range, not an array`,
      package: { name: entry.name, version: entry.version },
    });
  }
}

/**
 * npm's `os`/`cpu` filter language. Each list entry is either a bare
 * platform string (allow-list) or a `!`-prefixed string (block-list).
 *
 *   - Any `!`-prefixed entry switches the list into block-list mode:
 *     the entry matches the host iff no `!host` token appears. Bare
 *     entries in the same list are ignored (this matches npm's own
 *     `npm-install-checks` semantics, which keys "blocked" off the
 *     presence of any `!` token).
 *   - With no `!` token the list is an allow-list: the entry matches
 *     iff the host string appears verbatim.
 *
 * The plain `entries.includes(host)` check the loader used previously
 * treated `!win32` as a literal token, so `os: ["!win32"]` on linux
 * read as a never-matching allow-list and the package was incorrectly
 * filtered out.
 */
function platformListMatches(
  entries: readonly string[],
  host: string,
): boolean {
  const hasNegation = entries.some((e) => e.startsWith("!"));
  if (hasNegation) {
    return !entries.includes(`!${host}`);
  }
  return entries.includes(host);
}

function isEEXIST(err: unknown): boolean {
  if (err === null || typeof err !== "object") return false;
  if (!("code" in err)) return false;
  return (err as { code: unknown }).code === "EEXIST";
}

function isENOENT(err: unknown): boolean {
  if (err === null || typeof err !== "object") return false;
  if (!("code" in err)) return false;
  return (err as { code: unknown }).code === "ENOENT";
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
      { id: factory.id, requires: frozenRequires },
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

/**
 * Structural check for an `AnnotatedDirectorFactory` export. The shape
 * is callable + `{ id: string, requires: string[], configSchema:
 * function }`. The `configSchema` field is the discriminator against
 * tool factories (which carry only `id` and `requires`); without it,
 * any tool-factory export from a directors-entry module would be
 * accepted as a director.
 */
function isAnnotatedDirectorFactory(
  value: unknown,
): value is LoadedDirectorFactory {
  if (typeof value !== "function") return false;
  if (isAnnotatedPluginFactory(value)) return false;
  if (!("id" in value) || !("requires" in value)) return false;
  if (!("configSchema" in value)) return false;
  const id = (value as { id: unknown }).id;
  const requires = (value as { requires: unknown }).requires;
  const configSchema = (value as { configSchema: unknown }).configSchema;
  if (typeof id !== "string") return false;
  if (!Array.isArray(requires)) return false;
  if (!requires.every((r) => typeof r === "string")) return false;
  // `defineDirector` requires a callable arktype validator. A non-
  // callable schema would crash later inside `validateDirectorConfig`;
  // reject here so the failure surfaces as `package.entry.invalid` at
  // load time rather than at first config-validation call.
  if (typeof configSchema !== "function") return false;
  return true;
}

function describeError(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
