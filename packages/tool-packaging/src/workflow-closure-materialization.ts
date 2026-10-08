// Host-side materializer for a workflow-probe frame's frozen closure.
// The airlocked probe child evaluates the code-sourced workflow entry,
// but the frozen closure it evaluates against is materialized here on
// the host first — fetch + SRI-verify + extract + `node_modules`
// layout is I/O, not author-code evaluation. Phases 1-2 only, no
// `applyAtomic`: a probe is ephemeral and inert, so the durable-deploy
// lifecycle is the wrong semantics. `loadManifest` runs with an emptied
// `topLevel` so the loader imports no author code while still
// materializing the full `entries` set.

import { promises as fs } from "node:fs";
import path from "node:path";

import { type } from "arktype";
import { getLogger } from "@intx/log";
import { PackageJSON } from "@intx/types/package-json";
import type {
  WorkflowProbeRequestFrame,
  WorkflowSourceAssetMount,
} from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

import { createTarballCache } from "./cache";
import {
  createToolLoader,
  type HostPlatform,
  type TarballFetcher,
} from "./loader";
import type { RegistryConfig } from "./resolver";
import { storeEntryDir } from "./store-layout";

const logger = getLogger(["sidecar", "workflow-closure-materialization"]);

/**
 * Laid-out probe closure. The probe handler names this shape
 * `MaterializedWorkflowClosure`; this module keeps its own alias so it
 * does not import the handler.
 */
interface MaterializedProbeClosure {
  readonly packageDir: string;
  cleanup(): Promise<void>;
}

export interface WorkflowClosureMaterializerConfig {
  /** Content-addressable tarball cache root shared across materializations. */
  readonly cacheRoot: string;
  /** Byte cap for the tarball cache. */
  readonly cacheMaxBytes: number;
  /** Byte cap for a single HTTP-registry tarball fetch. */
  readonly registryMaxTarballBytes: number;
  /**
   * Byte cap for the total base64-encoded asset payload a probe frame may
   * deliver inline. Forwarded to `materializeAssets`, which enforces it
   * before any pack is decoded.
   */
  readonly maxAssetPayloadBytes: number;
  /** Registry identifier -> URL + credentials the loader resolves entries against. */
  readonly registries: ReadonlyMap<string, RegistryConfig>;
  /**
   * Root directory under which each probe's ephemeral closure scratch dir
   * is created (one per probe, removed by the returned `cleanup`).
   */
  readonly scratchRoot: string;
  /** npm `os`/`cpu` pair the loader selects optional dependencies with. */
  readonly host: HostPlatform;
  /**
   * Materialize the frame's inline-delivered assets into `assetRoot` and
   * `gitDirRoot`. The caller supplies the real delivery function; this
   * package does not import it.
   */
  readonly materializeAssets: (args: {
    readonly assets: readonly WorkflowSourceAssetMount[];
    readonly closure: ToolPackageManifest;
    readonly assetRoot: string;
    readonly gitDirRoot: string;
    readonly maxAssetPayloadBytes: number;
  }) => Promise<{
    readonly assetMounts: ReadonlyMap<string, string>;
    readonly gitDirs: ReadonlyMap<string, string>;
  }>;
  /**
   * Test seam for tarball fetching, forwarded to `createToolLoader`.
   * Production omits it and the loader fetches from the configured registry.
   */
  readonly fetchTarball?: TarballFetcher;
}

/**
 * Build the materializer the workflow-probe executor injects. The
 * returned function lays out a probe frame's frozen closure under a
 * fresh scratch dir and returns the workflow package directory plus a
 * `cleanup` that removes the scratch dir.
 */
export function createWorkflowClosureMaterializer(
  config: WorkflowClosureMaterializerConfig,
): (frame: WorkflowProbeRequestFrame) => Promise<MaterializedProbeClosure> {
  return async function materialize(
    frame: WorkflowProbeRequestFrame,
  ): Promise<MaterializedProbeClosure> {
    // The closure's single top-level pin IS the workflow definition
    // package (the hub resolved the closure for exactly that pin).
    // Assert the cardinality rather than silently picking `[0]`.
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

    // Boundary check on the definition's source. The `registry` arm
    // surfaces a missing source registry loudly before any I/O (the
    // per-entry gates fire again inside the loader); the `asset` arm's
    // delivery check happens below after materialization. The `never`
    // default makes a future source kind a compile error.
    switch (frame.source.kind) {
      case "registry":
        if (!config.registries.has(frame.source.registry)) {
          throw new Error(
            `workflow-probe closure materialization: source registry ${JSON.stringify(frame.source.registry)} is not in the sidecar registry config`,
          );
        }
        break;
      case "asset":
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
        host: config.host,
        maxRegistryTarballBytes: config.registryMaxTarballBytes,
        ...(config.fetchTarball !== undefined
          ? { fetchTarball: config.fetchTarball }
          : {}),
      });

      // Materialize inline-delivered assets under the probe scratch:
      // tarball entries as plain files under the workspace root, source
      // entries as an indexed gitDir. Registry-sourced closures deliver
      // none; both maps stay empty.
      const assetRoot = path.join(scratchDir, "workspace");
      const gitDirRoot = path.join(scratchDir, "gitdirs");
      const { assetMounts, gitDirs } = await config.materializeAssets({
        assets: frame.assets ?? [],
        closure: frame.closure,
        assetRoot,
        gitDirRoot,
        maxAssetPayloadBytes: config.maxAssetPayloadBytes,
      });

      // The asset the definition is sourced from must be among the
      // delivered assets — as a gitDir for a source definition, as a
      // mount for a tarball one. Surface a missing delivery here rather
      // than as a downstream failure on the top-level entry.
      if (frame.source.kind === "asset") {
        const delivered =
          frame.source.package.format === "source"
            ? gitDirs.has(frame.source.assetId)
            : assetMounts.has(frame.source.assetId);
        if (!delivered) {
          throw new Error(
            `workflow-probe closure materialization: asset source ${JSON.stringify(frame.source.assetId)} was not among the delivered assets`,
          );
        }
      }

      // Lay out phases 1-2 only: `topLevel` is emptied so the loader's
      // phase-3 loop imports nothing — no author code is evaluated on
      // the host; the airlocked child owns the workflow-entry import.
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
 * Cross-check the probe frame's `entry` against the materialized
 * package's own `interchange.workflow`. The child loader reads the
 * entry path from `package.json`, ignoring the frame's `entry`, so
 * left unchecked that field travels but is never validated. Comparing
 * them host-side (a `package.json` read, no author code) fails a
 * tampered or incoherent request before the child is spawned.
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
