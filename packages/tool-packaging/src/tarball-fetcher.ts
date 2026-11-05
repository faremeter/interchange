// eslint-disable-next-line @typescript-eslint/triple-slash-reference -- npm-team packages ship no types; declarations.d.ts must be visible to downstream typecheckers that import from this package's source.
/// <reference path="./declarations.d.ts" />
// The production tarball fetcher for closure materialization. Failures
// surface as `ClosureMaterializationError` with a `category` matching one of
// the `ClosureMaterializationErrorCategory` values.

import { promises as fs } from "node:fs";
import path from "node:path";
import npmRegistryFetch from "npm-registry-fetch";

import type { ToolPackageManifestEntry } from "@intx/types/tool-packages";

import {
  DEFAULT_MAX_REGISTRY_TARBALL_BYTES,
  DEFAULT_REGISTRY_FETCH_TIMEOUT_MS,
  ClosureMaterializationError,
  describeError,
} from "./materialization-internal";
import {
  buildRegistryFetchOpts,
  defaultTarballUrl,
  readResponseWithLimit,
} from "./registry-fetch";
import type { RegistryConfig } from "./resolver";

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
      // tarball fetcher with one is a bug; fail loud.
      if (entry.source.package.format !== "tarball") {
        throw new ClosureMaterializationError({
          category: "git.materialization.failed",
          message: `source-format asset entry ${entry.name}@${entry.version} reached the tarball fetcher`,
        });
      }
      const tarballPath = entry.source.package.path;
      // The mount lookup is guaranteed by `materializeClosure`'s
      // pre-fetch gate, but reassert here so the narrowing is
      // visible to readers — the caller of fetchTarball has no
      // structural guarantee it ran through that gate.
      const mount = ctx.assetMounts.get(entry.source.assetId);
      if (mount === undefined) {
        throw new ClosureMaterializationError({
          category: "asset.mount.missing",
          message: `default fetcher reached for ${entry.name}@${entry.version} without a mount for assetId "${entry.source.assetId}"`,
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
        throw new ClosureMaterializationError({
          category: "package.entry.invalid",
          message: `assetMounts entry for ${entry.name}@${entry.version} is absolute (${JSON.stringify(mount)}); mounts must be assetRoot-relative`,
        });
      }
      const mountAbs = path.resolve(ctx.assetRoot, mount);
      const absPath = path.resolve(mountAbs, tarballPath);
      const mountContainmentRoot = mountAbs.endsWith(path.sep)
        ? mountAbs
        : mountAbs + path.sep;
      if (absPath !== mountAbs && !absPath.startsWith(mountContainmentRoot)) {
        throw new ClosureMaterializationError({
          category: "package.entry.invalid",
          message: `source.package.path for ${entry.name}@${entry.version} resolves to ${JSON.stringify(absPath)} which escapes the declared mount ${JSON.stringify(mountAbs)} (cross-mount traversal)`,
        });
      }
      try {
        return await fs.readFile(absPath);
      } catch (err) {
        throw new ClosureMaterializationError({
          category: "tarball.missing",
          message: `asset-stored tarball for ${entry.name}@${entry.version} not present at ${absPath}: ${describeError(err)}`,
        });
      }
    }
    const registry = ctx.registries.get(entry.source.registry);
    if (registry === undefined) {
      throw new ClosureMaterializationError({
        category: "registry.unknown",
        message: `manifest entry ${entry.name}@${entry.version} references registry "${entry.source.registry}" which is not in the sidecar config`,
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
        throw new ClosureMaterializationError({
          category: "registry.auth.failed",
          message: `registry "${entry.source.registry}" rejected credentials for ${entry.name}@${entry.version} (HTTP ${String(res.status)})`,
        });
      }
      if (!res.ok) {
        throw new ClosureMaterializationError({
          category: "registry.fetch.failed",
          message: `registry "${entry.source.registry}" returned HTTP ${String(res.status)} fetching ${entry.name}@${entry.version}`,
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
      if (err instanceof ClosureMaterializationError) throw err;
      if (controller.signal.aborted) {
        throw new ClosureMaterializationError({
          category: "registry.fetch.failed",
          message: `registry "${entry.source.registry}" fetch for ${entry.name}@${entry.version} exceeded the ${String(registryFetchTimeoutMs)}ms timeout`,
        });
      }
      throw new ClosureMaterializationError({
        category: "registry.fetch.failed",
        message: `registry "${entry.source.registry}" fetch failed for ${entry.name}@${entry.version}: ${describeError(err)}`,
      });
    } finally {
      clearTimeout(timer);
    }
  };
}
