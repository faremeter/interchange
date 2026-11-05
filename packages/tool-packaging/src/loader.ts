// eslint-disable-next-line @typescript-eslint/triple-slash-reference -- npm-team packages ship no types; declarations.d.ts must be visible to downstream typecheckers that import from this package's source.
/// <reference path="./declarations.d.ts" />
// The production tarball fetcher for closure materialization, and the
// argument types of `materializeClosure`.
//
// Errors are surfaced as `ToolLoaderError` with a `category` matching
// one of the `DeployApplyErrorCategory` values.

import { promises as fs } from "node:fs";
import path from "node:path";
import npmRegistryFetch from "npm-registry-fetch";

import type {
  ToolPackageManifest,
  ToolPackageManifestEntry,
} from "@intx/types/tool-packages";

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

export interface HostPlatform {
  readonly os: string;
  readonly cpu: string;
}

export interface TarballFetcherConfig {
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
   * cannot block the fetch -- and the closure materialization
   * awaiting it -- past this bound. Defaults to
   * `DEFAULT_REGISTRY_FETCH_TIMEOUT_MS`. Asset-sourced tarballs read from
   * the local filesystem and are not subject to it.
   */
  readonly registryFetchTimeoutMs?: number;
}

export type TarballFetcher = (
  entry: ToolPackageManifestEntry,
  ctx: {
    registries: ReadonlyMap<string, RegistryConfig>;
    assetRoot: string;
    assetMounts: ReadonlyMap<string, string>;
  },
) => Promise<Uint8Array>;

/**
 * Arguments for `materializeClosure`. Everything it needs is passed
 * explicitly: the closure's `cache`, `registries`, `host`, and resolved
 * `fetchTarball` are threaded in directly.
 */
export interface MaterializeClosureArgs {
  readonly manifest: ToolPackageManifest;
  readonly instanceScratchDir: string;
  /**
   * Filesystem root that `assetMounts` paths are joined against. Mount
   * paths are workspace-relative; the fetcher resolves them against
   * `assetRoot` to get the absolute tarball location for `kind: "asset"`
   * entries.
   */
  readonly assetRoot: string;
  /**
   * Maps a `source.assetId` from a manifest entry to a
   * workspace-relative mount path. Empty map is valid when no entry
   * sources from an asset.
   */
  readonly assetMounts: ReadonlyMap<string, string>;
  /**
   * Maps a source-format asset entry's `source.assetId` to an absolute
   * indexed git directory whose object database holds the pinned commit and
   * its trees. Empty map is valid when no entry sources from a git subtree.
   */
  readonly gitDirs: ReadonlyMap<string, string>;
  readonly host: HostPlatform;
  readonly cache: TarballCache;
  /**
   * Registry identifier → registry config. The key is the `registry`
   * identifier manifest entries point at. Used both to gate
   * `kind: "registry"` entries against the sidecar config and as the
   * `registries` context handed to `fetchTarball`.
   */
  readonly registries: ReadonlyMap<string, RegistryConfig>;
  /**
   * Resolved tarball fetcher, normally from `createTarballFetcher`.
   * `materializeClosure` does not construct one of its own.
   */
  readonly fetchTarball: TarballFetcher;
}

/**
 * Result of `materializeClosure`: the per-instance store directory the
 * closure was laid out under (`<instanceScratchDir>/store`), plus the exact
 * host-filtered entries that were laid out under it. A caller computes a
 * specific package's directory with `storeEntryDir`.
 */
export interface MaterializeClosureResult {
  readonly storeDir: string;
  readonly entries: readonly ToolPackageManifestEntry[];
}

/**
 * Build the production tarball fetcher: asset entries read from their mount
 * under `assetRoot`, registry entries over HTTP through `npm-registry-fetch`.
 */
export function createTarballFetcher(
  config: TarballFetcherConfig = {},
): TarballFetcher {
  const maxRegistryTarballBytes =
    config.maxRegistryTarballBytes ?? DEFAULT_MAX_REGISTRY_TARBALL_BYTES;
  if (
    !Number.isFinite(maxRegistryTarballBytes) ||
    maxRegistryTarballBytes <= 0
  ) {
    throw new Error(
      `createTarballFetcher: maxRegistryTarballBytes must be a positive finite number; got ${String(maxRegistryTarballBytes)}`,
    );
  }
  const registryFetchTimeoutMs =
    config.registryFetchTimeoutMs ?? DEFAULT_REGISTRY_FETCH_TIMEOUT_MS;
  if (!Number.isFinite(registryFetchTimeoutMs) || registryFetchTimeoutMs <= 0) {
    throw new Error(
      `createTarballFetcher: registryFetchTimeoutMs must be a positive finite number; got ${String(registryFetchTimeoutMs)}`,
    );
  }
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
    const registry = ctx.registries.get(entry.source.registry);
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
