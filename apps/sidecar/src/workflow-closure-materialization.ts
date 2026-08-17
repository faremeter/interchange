// Host-side materializer for a workflow-probe frame's frozen closure.
//
// The airlocked probe child (`workflow-probe-handler.ts`) evaluates a
// code-sourced workflow's `interchange.workflow` entry, but the frozen
// dependency closure it evaluates against is materialized on the sidecar
// HOST first -- fetch + SRI-verify + extract + `node_modules` layout is
// I/O, not author-code evaluation, so it stays out of the child. This
// module builds the production `MaterializeWorkflowClosure` the probe
// executor injects: it lays out the frame's frozen closure and returns
// the workflow package directory the child loads from, without importing
// any author code on the host.
//
// Layering (greybeard): the concrete materializer lives here in
// `apps/sidecar` so `@intx/workflow-host` stays free of a
// `@intx/tool-packaging` dependency and `workflow-probe-handler.ts`
// stays free of one too -- the materializer is an injected seam. The
// portable packages only see the `MaterializeWorkflowClosure` callback
// this module produces.
//
// Phases 1-2 only, no `applyAtomic`: a probe is ephemeral and inert, so
// the durable-deploy lifecycle bookkeeping (`active-deploy-id`, the
// per-deploy-id retention ladder) is the wrong semantics. The closure is
// laid out under a per-probe scratch dir that `cleanup` removes once the
// child has been reaped. `createToolLoader(...).loadManifest(...)` is
// invoked with an EMPTIED `topLevel`: the loader's phase-3 import loop
// only imports packages named in `topLevel`, so an empty `topLevel`
// fetches + extracts + lays out every closure entry (the full `entries`
// set) while importing NONE of them. That runs exactly the eval-free
// `materializeClosure` phases the probe needs, using the loader's
// production registry fetcher -- the tarball fetcher `@intx/tool-packaging`
// owns is only reachable through `createToolLoader`, so the layout is
// driven through the loader rather than by calling `materializeClosure`
// with a fetcher this package would otherwise have to build (and thereby
// reach for the npm-registry machinery that package exists to contain).

import { promises as fs } from "node:fs";
import path from "node:path";

import { type } from "arktype";
import { base64Decode } from "@intx/types";
import { applyAssetPack } from "@intx/hub-agent";
import { getLogger } from "@intx/log";
import {
  type RegistryConfig,
  type TarballFetcher,
  createTarballCache,
  createToolLoader,
  storeEntryDir,
} from "@intx/tool-packaging";
import { PackageJSON } from "@intx/types/package-json";
import type {
  WorkflowSourceAssetMount,
  WorkflowProbeRequestFrame,
} from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

import { resolveHostPlatform } from "./sidecar-materialization-config";
import type {
  MaterializedWorkflowClosure,
  MaterializeWorkflowClosure,
} from "./workflow-probe-handler";

const logger = getLogger(["sidecar", "workflow-closure-materialization"]);

export interface WorkflowClosureMaterializerConfig {
  /** Content-addressable tarball cache root shared across materializations. */
  readonly cacheRoot: string;
  /** Byte cap for the tarball cache. */
  readonly cacheMaxBytes: number;
  /** Byte cap for a single HTTP-registry tarball fetch. */
  readonly registryMaxTarballBytes: number;
  /**
   * Byte cap for the total base64-encoded asset payload a probe frame may
   * deliver inline. Measured against the base64 wire length (a conservative
   * upper bound on the decoded bytes), it is enforced before any pack is
   * decoded so an oversized frame fails loud rather than being materialized.
   */
  readonly maxAssetPayloadBytes: number;
  /** Registry identifier -> URL + credentials the loader resolves entries against. */
  readonly registries: ReadonlyMap<string, RegistryConfig>;
  /**
   * Root directory under which each probe's ephemeral closure scratch dir
   * is created (one per probe, removed by the returned `cleanup`).
   */
  readonly scratchRoot: string;
  /**
   * Test seam for tarball fetching, forwarded to `createToolLoader`.
   * Production omits it and the loader fetches from the configured registry.
   */
  readonly fetchTarball?: TarballFetcher;
}

/**
 * Check out each inline-delivered asset pack as plain files under
 * `<assetRoot>/<mountPath>/` and return the `assetId -> mountPath` map the
 * loader resolves `kind:"asset"` entries against. `applyAssetPack` owns the
 * containment defenses (mount-path sanitization, `commitSha` integrity,
 * submodule and `.git`-injection rejection); this only enforces the total
 * payload cap and never evaluates any delivered content.
 */
export async function materializeSourceAssetMounts(
  assets: readonly WorkflowSourceAssetMount[],
  assetRoot: string,
  maxAssetPayloadBytes: number,
): Promise<ReadonlyMap<string, string>> {
  const mounts = new Map<string, string>();
  let totalPayloadBytes = 0;
  for (const asset of assets) {
    // Cap on the base64 wire length before decoding, so an oversized frame
    // fails loud rather than allocating the decoded pack.
    totalPayloadBytes += asset.pack.length;
    if (totalPayloadBytes > maxAssetPayloadBytes) {
      throw new Error(
        `workflow source-asset materialization: inline asset payload exceeds the ${String(maxAssetPayloadBytes)}-byte cap`,
      );
    }
    if (mounts.has(asset.assetId)) {
      throw new Error(
        `workflow source-asset materialization: asset ${JSON.stringify(asset.assetId)} is delivered more than once`,
      );
    }
    await applyAssetPack({
      workspaceRoot: assetRoot,
      mountPath: asset.mountPath,
      pack: base64Decode(asset.pack),
      ref: asset.ref,
      commitSha: asset.commitSha,
    });
    mounts.set(asset.assetId, asset.mountPath);
  }
  return mounts;
}

/**
 * Build the production `MaterializeWorkflowClosure` the workflow-probe
 * executor injects. The returned function lays out a probe frame's frozen
 * closure under a fresh scratch dir and returns the workflow package
 * directory plus a `cleanup` that removes the scratch dir.
 *
 * @throws (from the returned materializer) if the closure does not pin
 *   exactly one top-level package, the source registry is not configured,
 *   the layout fails (fetch / integrity / extract), or the frame's `entry`
 *   disagrees with the materialized package's `interchange.workflow`.
 */
export function createWorkflowClosureMaterializer(
  config: WorkflowClosureMaterializerConfig,
): MaterializeWorkflowClosure {
  const host = resolveHostPlatform();

  return async function materialize(
    frame: WorkflowProbeRequestFrame,
  ): Promise<MaterializedWorkflowClosure> {
    // Gap 1: the closure's single top-level pin IS the workflow definition
    // package (the hub resolved the closure for exactly that pin). Assert
    // the cardinality and fail loud rather than silently picking `[0]`; a 0-
    // or >1-pin closure is an incoherent request the materializer owns
    // rejecting at this boundary.
    const topLevel = frame.closure.topLevel;
    if (topLevel.length !== 1) {
      throw new Error(
        `workflow-probe closure materialization: the frozen closure must pin exactly one top-level package (the workflow definition package), got ${String(topLevel.length)}`,
      );
    }
    const workflowPin = topLevel[0];
    if (workflowPin === undefined) {
      throw new Error(
        "workflow-probe closure materialization: the frozen closure's single top-level pin is undefined",
      );
    }

    // Boundary check on the definition's source. The `registry` arm surfaces a
    // missing source registry loudly before any I/O (the per-entry registry
    // gates fire again inside the loader). A tarball-format `asset` closure is
    // materialized from the `assets` the frame delivers, mounted below. A
    // source-format asset closure has no sidecar delivery path yet, so that
    // arm fails closed. The `never` default makes a future source kind a
    // compile error rather than a silent fallthrough.
    switch (frame.source.kind) {
      case "registry":
        if (!config.registries.has(frame.source.registry)) {
          throw new Error(
            `workflow-probe closure materialization: source registry ${JSON.stringify(frame.source.registry)} is not in the sidecar registry config`,
          );
        }
        break;
      case "asset":
        if (frame.source.package.format === "source") {
          throw new Error(
            "workflow-probe closure materialization: source-format asset probe closures cannot be materialized on the sidecar yet",
          );
        }
        break;
      default: {
        const _exhaustive: never = frame.source;
        throw new Error(
          `workflow-probe closure materialization: unhandled workflow source kind ${String(_exhaustive)}`,
        );
      }
    }

    const scratchDir = path.join(config.scratchRoot, crypto.randomUUID());
    await fs.mkdir(scratchDir, { recursive: true });
    const cleanup = async (): Promise<void> => {
      await fs.rm(scratchDir, { recursive: true, force: true });
    };

    try {
      const cache = createTarballCache({
        rootDir: config.cacheRoot,
        maxBytes: config.cacheMaxBytes,
      });
      const loader = createToolLoader({
        cache,
        registries: config.registries,
        host,
        maxRegistryTarballBytes: config.registryMaxTarballBytes,
        ...(config.fetchTarball !== undefined
          ? { fetchTarball: config.fetchTarball }
          : {}),
      });

      // Materialize any inline-delivered assets as plain files under the
      // workspace root, so the loader can resolve `kind:"asset"` closure
      // entries against their mounts. Registry-sourced closures deliver none;
      // the map stays empty and the loader fetches over HTTP.
      const assetRoot = path.join(scratchDir, "workspace");
      const assetMounts = await materializeSourceAssetMounts(
        frame.assets ?? [],
        assetRoot,
        config.maxAssetPayloadBytes,
      );
      // A source-format asset entry is checked out from an indexed pack the
      // probe would deliver; the probe does not deliver git checkouts yet, so
      // the map stays empty and a source-format entry fails loud downstream.
      const gitDirs = new Map<string, string>();

      // Source-boundary check for the asset arm, the post-unpack analog of the
      // registry arm's config check: the asset the definition is sourced from
      // holds the workflow package, so it MUST be among the delivered assets.
      // Surface a missing delivery here rather than as a downstream
      // `asset.mount.missing` on the top-level entry.
      if (
        frame.source.kind === "asset" &&
        !assetMounts.has(frame.source.assetId)
      ) {
        throw new Error(
          `workflow-probe closure materialization: asset source ${JSON.stringify(frame.source.assetId)} was not among the delivered assets`,
        );
      }

      // Lay out phases 1-2 only. `topLevel` is emptied so the loader's
      // phase-3 loop imports nothing -- no author code is evaluated on the
      // host; the airlocked child owns the single import of the workflow
      // entry. The full `entries` set is still fetched, SRI-verified,
      // extracted, and laid out with its `node_modules` graph.
      const layoutManifest: ToolPackageManifest = {
        schemaVersion: frame.closure.schemaVersion,
        topLevel: [],
        entries: frame.closure.entries,
      };
      await loader.loadManifest({
        manifest: layoutManifest,
        instanceScratchDir: scratchDir,
        assetRoot,
        assetMounts,
        gitDirs,
      });

      const storeDir = path.join(scratchDir, "store");
      const packageDir = storeEntryDir(
        storeDir,
        workflowPin.name,
        workflowPin.version,
      );

      await assertFrameEntryMatchesPackage(packageDir, frame.entry);

      logger.debug`materialized workflow-probe closure for ${workflowPin.name}@${workflowPin.version} at ${packageDir}`;
      return { packageDir, cleanup };
    } catch (err) {
      // On any failure before a closure handle is handed back, the executor
      // never sees a `cleanup` to call, so the scratch dir is this function's
      // to reclaim.
      await cleanup();
      throw err;
    }
  };
}

/**
 * Gap 2: cross-check the probe frame's `entry` against the materialized
 * package's own `interchange.workflow`. The child loader reads the entry
 * path from the package's `package.json`, ignoring the frame's `entry`;
 * left unchecked the frame field is an input that travels but is never
 * validated. Comparing them host-side -- a `package.json` read, no author
 * code -- surfaces a tampered or incoherent request before the child is
 * ever spawned, and fails loud on mismatch.
 */
async function assertFrameEntryMatchesPackage(
  packageDir: string,
  frameEntry: string,
): Promise<void> {
  const pkgJsonPath = path.join(packageDir, "package.json");
  let raw: string;
  try {
    raw = await fs.readFile(pkgJsonPath, "utf8");
  } catch (cause) {
    throw new Error(
      `workflow-probe closure materialization: cannot read package.json at ${packageDir} to cross-check the frame entry`,
      { cause },
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new Error(
      `workflow-probe closure materialization: malformed package.json at ${packageDir}`,
      { cause },
    );
  }
  const pkg = PackageJSON(parsed);
  if (pkg instanceof type.errors) {
    throw new Error(
      `workflow-probe closure materialization: package.json at ${packageDir} failed validation: ${pkg.summary}`,
    );
  }
  const declaredEntry = pkg.interchange?.workflow;
  if (declaredEntry === undefined) {
    throw new Error(
      `workflow-probe closure materialization: workflow package at ${packageDir} declares no "interchange.workflow" entry`,
    );
  }
  if (declaredEntry !== frameEntry) {
    throw new Error(
      `workflow-probe closure materialization: probe frame entry ${JSON.stringify(frameEntry)} does not match the materialized package's interchange.workflow ${JSON.stringify(declaredEntry)}`,
    );
  }
}
