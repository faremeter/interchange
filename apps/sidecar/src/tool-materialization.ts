// Tool-package materialization for the sidecar.
//
// The deploy-tree reader, the `@intx/tool-packaging` loader
// construction, `applyAtomic`, and the active-deploy-id persistence
// ladder live here so both the in-process harness builder
// (`default-harness.ts`) and the workflow-process child's substrate
// factory (`workflow-substrate-factory.ts`) reach the same
// implementation. Keeping this in `apps/sidecar` (and out of
// `@intx/workflow-host`) preserves the portable orchestration
// package's host-agnostic layering: the child IS the sidecar binary,
// so the sidecar's tool runtime is already present in the child's
// address space without the portable package depending on it.
//
// The module is deliberately free of any `@intx/harness` reactor
// dependency so the workflow-process child does not drag the
// in-process harness's transport/reactor ownership into its graph.

import fs from "node:fs";
import path from "node:path";
import { type } from "arktype";
import { type AnnotatedPluginFactory } from "@intx/agent";
import { type DeployApplyErrorEmitter } from "@intx/hub-agent/paths";
import { getLogger } from "@intx/log";
import {
  type LoadedToolFactory,
  type LoadedToolPackage,
  type RegistryConfig,
  applyAtomic,
  createTarballCache,
  createToolLoader,
} from "@intx/tool-packaging";
import { hasCode } from "@intx/types";
import { ToolPackageManifest } from "@intx/types/tool-packages";

// Boundary validator for the SIDECAR_TOOL_REGISTRIES env var. The
// env-wire shape carries `name` alongside the registry config so an
// operator can author the JSON as a flat array; the boundary collapses
// the array into a Map keyed by name before handing it to the loader.
const RegistryConfigEnvEntry = type({
  name: "string",
  url: "string",
  "auth?": type({
    "token?": "string",
    "basic?": type({ user: "string", pass: "string" }),
  }),
});
const RegistryConfigEnvArray = RegistryConfigEnvEntry.array();

function readRegistries(): ReadonlyMap<string, RegistryConfig> {
  const raw = process.env["SIDECAR_TOOL_REGISTRIES"];
  if (raw === undefined) {
    return new Map([["npmjs", { url: "https://registry.npmjs.org" }]]);
  }
  // Distinguish unset from empty. An operator setting the var to an
  // empty string almost always indicates misconfig (CI secret
  // expansion failed, a templater dropped the value). Falling through
  // to the npmjs default at that point silently routes tool packages
  // through public npm, which is precisely the misroute a custom
  // registry pin was meant to prevent. Surface the gap loudly; the
  // recovery is `unset SIDECAR_TOOL_REGISTRIES`, not `=""`.
  if (raw.trim() === "") {
    throw new Error(
      "SIDECAR_TOOL_REGISTRIES is set but empty — unset the variable to use the default npmjs registry",
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new Error(
      `SIDECAR_TOOL_REGISTRIES is not valid JSON: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  const validated = RegistryConfigEnvArray(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `SIDECAR_TOOL_REGISTRIES failed validation: ${validated.summary}`,
    );
  }
  const out = new Map<string, RegistryConfig>();
  for (const entry of validated) {
    if (out.has(entry.name)) {
      throw new Error(
        `SIDECAR_TOOL_REGISTRIES has duplicate registry name ${JSON.stringify(entry.name)}`,
      );
    }
    const config: RegistryConfig = {
      url: entry.url,
      ...(entry.auth !== undefined ? { auth: entry.auth } : {}),
    };
    out.set(entry.name, config);
  }
  return out;
}

const logger = getLogger(["sidecar", "harness-builder"]);

// npm's `os` token namespace, mirrored from Node's `process.platform`
// enum. Any value outside this set means the host is running a Node
// build the loader's platform filter would silently mis-route — a
// pinned package whose `os` list excludes the host would not be
// excluded if `process.platform` is a token npm has never heard of.
// Validate at the boundary so an unknown platform fails the boot
// instead of producing a quiet, host-shaped mis-resolution at apply
// time.
//
// UPGRADE TAX: Node periodically adds platforms (and Bun ships
// extensions of its own). A Node/Bun major bump that lands a new
// `process.platform` value will fail boot until this allowlist is
// refreshed against the upstream enum. Sidecar operators upgrading
// the runtime should expect this as part of the cutover, not as a
// surprise regression.
const KNOWN_PROCESS_PLATFORMS = new Set<NodeJS.Platform>([
  "aix",
  "android",
  "darwin",
  "freebsd",
  "haiku",
  "linux",
  "openbsd",
  "sunos",
  "win32",
  "cygwin",
  "netbsd",
]);

// npm's `cpu` token namespace, mirrored from Node's `process.arch`
// enum. Same rationale as KNOWN_PROCESS_PLATFORMS — an unknown arch
// would mis-route the loader's filter without surfacing the gap.
// Same upgrade tax applies: Node has added `loong64` and `riscv64`
// in recent releases, and future arch additions will need to be
// added here when the sidecar is rebuilt against them.
const KNOWN_PROCESS_ARCHS = new Set<NodeJS.Architecture>([
  "arm",
  "arm64",
  "ia32",
  "loong64",
  "mips",
  "mipsel",
  "ppc64",
  "riscv64",
  "s390x",
  "x64",
]);

function assertKnownHostPlatform(platform: NodeJS.Platform): void {
  if (!KNOWN_PROCESS_PLATFORMS.has(platform)) {
    throw new Error(
      `sidecar boot: process.platform ${JSON.stringify(platform)} is not a recognized npm \`os\` token; tool-package platform filtering would be unreliable`,
    );
  }
}

function assertKnownHostArch(arch: NodeJS.Architecture): void {
  if (!KNOWN_PROCESS_ARCHS.has(arch)) {
    throw new Error(
      `sidecar boot: process.arch ${JSON.stringify(arch)} is not a recognized npm \`cpu\` token; tool-package platform filtering would be unreliable`,
    );
  }
}

// Sentinel `previousDeployId` for an instance that has never applied
// a deploy successfully. Encoded as a literal string so the value
// travels through `applyAtomic`'s `ApplyAtomicFailure.previousDeployId`
// channel unchanged; the hub treats it as "no prior deploy" rather
// than as a real id.
const NO_PRIOR_DEPLOY_ID = "none";

interface MaterializedToolPackages {
  readonly factories: readonly LoadedToolFactory[];
  readonly pluginFactories: readonly AnnotatedPluginFactory[];
}

// Exported for direct unit testing of the manifest-invalid gate
// (JSON.parse failure + arktype schema failure). The in-process
// harness builder reaches it through `createDefaultHarnessBuilder`;
// the workflow-process child reaches it through the substrate
// factory's per-step agent builder.
export async function materializeToolPackages(args: {
  rawManifestBytes: string | undefined;
  /**
   * `assetId` → workspace-relative mount path, parsed from the deploy
   * pack's `deploy/asset-mounts.json`. The loader resolves
   * `kind: "asset"` manifest entries against this map.
   */
  assetMounts: ReadonlyMap<string, string>;
  storeDir: string;
  /**
   * Workspace root the loader resolves `kind: "asset"` tarball mounts
   * against (`<assetRoot>/<mountPath>/...`). Defaults to
   * `<storeDir>/workspace` -- the in-process harness builder's layout,
   * where the deploy flow stages asset packs into the same dir the
   * agent runs in.
   *
   * The workflow-process child overrides this: it stages assets in the
   * step's LEGACY agent dir workspace (where the hub's asset-pack push
   * lands them) but roots the per-step apply-state + agent `env.workdir`
   * under a distinct per-step store dir, so the loader's asset source
   * and the apply-state root are two different directories.
   */
  assetRoot?: string;
  agentAddress: string;
  cacheRoot: string;
  cacheMaxBytes: number;
  registryMaxTarballBytes: number;
  emitDeployApplyError: DeployApplyErrorEmitter | undefined;
}): Promise<MaterializedToolPackages> {
  if (args.rawManifestBytes === undefined) {
    return { factories: [], pluginFactories: [] };
  }
  // Capture into a local so closures below see a `string` (the property
  // narrowing on args.rawManifestBytes does not survive the nested
  // function boundary, even though the property is readonly).
  const rawManifestBytes = args.rawManifestBytes;

  // The apply-state tree (`tool-packages/`) shares the agent's
  // storeDir root with the workspace tree the tool factories run
  // against. Factories execute with `cwd: env.workdir`; the loader
  // and the agent runtime both rely on factories not writing outside
  // their workspace. A factory that walks `..` out of the workspace
  // could touch the apply-state tree. The substrate trusts factories
  // to honor that boundary — a misbehaving factory is treated as a
  // security regression against the package, not a sandboxing gap to
  // close at this layer.
  const instanceDir = path.join(args.storeDir, "tool-packages");
  await fs.promises.mkdir(instanceDir, { recursive: true });

  const activeIdFile = path.join(instanceDir, "active-deploy-id");
  const activeIdDirtyFile = `${activeIdFile}.dirty`;
  // The dirty marker is written by `persistActiveDeployId`'s catch
  // path when the commit persist (the normal write + fsync + dir
  // fsync) failed on the prior apply and even the no-fsync fallback
  // failed. Its presence means the staged deploy was committed but the
  // recorded `active-deploy-id` is stale, and the marker carries the id
  // that belongs there. Read it first so a degraded prior boot doesn't
  // surface as `previousDeployId="none"` and silently demote the
  // committed deploy to "fresh instance".
  let previousDeployId = NO_PRIOR_DEPLOY_ID;
  try {
    const dirtyRaw = (
      await fs.promises.readFile(activeIdDirtyFile, "utf-8")
    ).trim();
    const dirtyId = parseActiveDeployId(dirtyRaw, activeIdDirtyFile);
    logger.warn`active-deploy-id dirty marker present at ${activeIdDirtyFile}; the prior apply could not durably record the committed deploy id and the boot is reconciling from ${dirtyId}`;
    previousDeployId = dirtyId;
  } catch (err) {
    if (!(hasCode(err) && err.code === "ENOENT")) {
      throw err;
    }
    try {
      const raw = (await fs.promises.readFile(activeIdFile, "utf-8")).trim();
      previousDeployId = parseActiveDeployId(raw, activeIdFile);
    } catch (innerErr) {
      if (!(hasCode(innerErr) && innerErr.code === "ENOENT")) {
        throw innerErr;
      }
    }
  }

  const attemptId = crypto.randomUUID();
  const newDeployId = crypto.randomUUID();

  // Validate the manifest at the loader boundary. Both JSON parse
  // failures and arktype schema failures route through the same
  // `manifest.invalid` category so the hub sees a single failure
  // shape for malformed manifests regardless of the specific defect.
  // The helper persists the rejected bytes + the failure payload and
  // emits the WS frame, then returns the Error for the caller to throw.
  // Returning instead of throwing lets the caller use `throw await ...`,
  // which TypeScript narrows control flow against without any
  // dead-code suffix at the call site.
  const rejectManifestInvalid = async (reason: string): Promise<Error> => {
    const occurredAt = new Date().toISOString();
    logger.warn`tool-package apply rejected for ${args.agentAddress}: manifest.invalid — ${reason}`;
    const message = `deploy/tool-packages-manifest.json could not be loaded: ${reason}`;
    await writeRejectedApplyAudit({
      storeDir: args.storeDir,
      attemptId,
      manifestBytes: rawManifestBytes,
      failure: {
        attemptId,
        previousDeployId,
        category: "manifest.invalid",
        message,
        occurredAt,
      },
    });
    if (args.emitDeployApplyError !== undefined) {
      args.emitDeployApplyError({
        attemptId,
        previousDeployId,
        category: "manifest.invalid",
        message,
        occurredAt,
      });
    }
    return new Error(
      `tool-package apply rejected (manifest.invalid): ${reason}`,
    );
  };

  let parsedManifest: unknown;
  try {
    parsedManifest = JSON.parse(rawManifestBytes);
  } catch (err) {
    throw await rejectManifestInvalid(
      `JSON.parse failed: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  const validated = ToolPackageManifest(parsedManifest);
  if (validated instanceof type.errors) {
    throw await rejectManifestInvalid(
      `schema validation failed: ${validated.summary}`,
    );
  }

  const cache = createTarballCache({
    rootDir: args.cacheRoot,
    maxBytes: args.cacheMaxBytes,
  });
  assertKnownHostPlatform(process.platform);
  assertKnownHostArch(process.arch);
  const loader = createToolLoader({
    cache,
    registries: readRegistries(),
    host: { os: process.platform, cpu: process.arch },
    maxRegistryTarballBytes: args.registryMaxTarballBytes,
  });
  const result = await applyAtomic({
    manifest: validated,
    loader,
    instanceDir,
    assetRoot: args.assetRoot ?? path.join(args.storeDir, "workspace"),
    assetMounts: args.assetMounts,
    attemptId,
    previousDeployId,
    newDeployId,
  });
  if (result.status === "failed") {
    logger.warn`tool-package apply rejected for ${args.agentAddress}: ${result.category} — ${result.message}`;
    // A failed apply never wrote `active-deploy-id`: `applyAtomic`
    // stages into a per-deploy-id directory and the commit is the
    // persist below, which only runs on success. So the prior deploy is
    // trivially still live and `result.previousDeployId` carries the
    // unchanged prior id. There is no committed-but-failed swap to
    // reconcile here — that case existed only under the old rename
    // protocol.
    await writeRejectedApplyAudit({
      storeDir: args.storeDir,
      attemptId: result.attemptId,
      // The raw bytes from disk, not the arktype-narrowed object.
      // Re-serializing `validated` would drop unknown-tolerated
      // fields, normalize key order, and lose whitespace — exactly
      // the original-input evidence a future investigator needs to
      // reproduce the failure against a newer validator.
      manifestBytes: rawManifestBytes,
      failure: result,
    });
    if (args.emitDeployApplyError !== undefined) {
      args.emitDeployApplyError({
        attemptId: result.attemptId,
        previousDeployId: result.previousDeployId,
        category: result.category,
        message: result.message,
        ...(result.package !== undefined ? { package: result.package } : {}),
        occurredAt: result.occurredAt,
      });
    }
    throw new Error(
      `tool-package apply rejected (${result.category}): ${result.message}`,
    );
  }

  // Apply staged: the loader built the new deploy at
  // `packages/<newDeployId>/`. Persisting the new active id is the
  // commit — the single write that advances the live deploy from the
  // prior id to this one. If the write or its fsync fails (disk full,
  // EIO, EROFS), the on-disk state has diverged from the recorded id:
  // `persistActiveDeployIdWithFallback` has already written the new id
  // through its no-fsync / dirty-marker degradation ladder, so the new
  // deploy is logically committed, but the id was not durably flushed.
  // Route this through `apply.previous-rotation.failed` — the wire
  // contract for that category says `previousDeployId` carries the
  // **new** deploy id (the one now live) so the hub records the on-disk
  // truth. Emit the failure frame and the audit entry, then throw so
  // the harness tears down: the durability gap means the next apply
  // cannot trust `previousDeployId` until the next boot reconciles via
  // the dirty marker.
  const persistOutcome = await persistActiveDeployIdWithFallback(
    instanceDir,
    activeIdFile,
    result.activeDeployId,
  );
  if (persistOutcome.degraded) {
    const err = persistOutcome.error;
    const occurredAt = new Date().toISOString();
    const message = `active-deploy-id persist failed after staging deploy: ${err instanceof Error ? err.message : String(err)}`;
    logger.error`tool-package apply: ${message}; active deploy ${result.activeDeployId} is live on disk but the recorded id was not durably written`;
    await writeRejectedApplyAudit({
      storeDir: args.storeDir,
      attemptId,
      manifestBytes: rawManifestBytes,
      failure: {
        attemptId,
        previousDeployId: result.activeDeployId,
        category: "apply.previous-rotation.failed",
        message,
        occurredAt,
      },
    });
    if (args.emitDeployApplyError !== undefined) {
      args.emitDeployApplyError({
        attemptId,
        previousDeployId: result.activeDeployId,
        category: "apply.previous-rotation.failed",
        message,
        occurredAt,
      });
    }
    throw new Error(
      `tool-package apply rejected (apply.previous-rotation.failed): ${message}`,
      { cause: err },
    );
  }
  return {
    factories: collectFactories(result.loaded),
    pluginFactories: collectPluginFactories(result.loaded),
  };
}

/**
 * Write the active deploy id to `activeIdFile` and fsync both the
 * file's own data/metadata and the parent directory entry. POSIX does
 * not guarantee a parent-directory entry is durably linked from a
 * file's own fsync alone, so the dir handle is opened and synced
 * separately. Without that, a crash between a deploy's commit and the
 * next boot could leave the staged deploy directory present while
 * active-deploy-id is not yet visible — the next apply would then read
 * previousDeployId="none" and treat the committed deploy as belonging
 * to a fresh, deploy-less instance.
 *
 * Dir-fsync is best-effort durability hardening, not part of the
 * apply's structural success contract. Some filesystems (notably
 * FAT/exFAT, certain network mounts) do not support fsync on a
 * directory handle and will surface EINVAL / ENOTSUP. The deploy is
 * already staged on disk and the deploy-id file's own fsync has
 * landed; tearing the harness down at this point would force a
 * restart for a degraded-durability condition the operator has no
 * way to act on. Log it and continue.
 */
// Version prefix for the active-deploy-id file. A future format
// change (e.g. carrying additional fields alongside the id) bumps
// this and the reader rejects unknown prefixes loudly instead of
// silently treating an unrecognized payload as a deploy id.
const ACTIVE_DEPLOY_ID_VERSION = "v1";
const ACTIVE_DEPLOY_ID_PREFIX = `${ACTIVE_DEPLOY_ID_VERSION}:`;

// Exported for unit testing of the version-prefix gate.
export function parseActiveDeployId(raw: string, sourcePath: string): string {
  if (raw.length === 0) {
    throw new Error(
      `active-deploy-id file ${sourcePath} is empty; expected ${ACTIVE_DEPLOY_ID_PREFIX}<deploy-id>`,
    );
  }
  // Pre-versioning files carried just the raw deploy id. Accept those
  // for backward compatibility so an upgrade does not require an
  // operator-driven state rewrite, but require the prefix on any
  // file the new code writes.
  //
  // Deprecation horizon: this branch exists to ease the v1: cutover
  // from pre-versioning files. Once every deployed sidecar has been
  // restarted at least once on a version that writes the prefix, the
  // un-prefixed branch can be deleted and an un-prefixed file should
  // be rejected as garbage. The horizon is "remove after a few
  // sidecar minor versions have shipped that write the prefix"; the
  // exact cutover is an operational decision, not a code-encoded
  // one.
  if (raw.startsWith(ACTIVE_DEPLOY_ID_PREFIX)) {
    const id = raw.slice(ACTIVE_DEPLOY_ID_PREFIX.length);
    if (id.length === 0) {
      throw new Error(
        `active-deploy-id file ${sourcePath} carries the ${ACTIVE_DEPLOY_ID_VERSION} prefix but no id`,
      );
    }
    return id;
  }
  if (raw.includes(":")) {
    // A leading token shaped like a version prefix that we do not
    // recognize. Surface loudly rather than treating the whole string
    // as the deploy id.
    const prefix = raw.slice(0, raw.indexOf(":") + 1);
    throw new Error(
      `active-deploy-id file ${sourcePath} carries unknown version prefix ${JSON.stringify(prefix)}; this sidecar understands ${JSON.stringify(ACTIVE_DEPLOY_ID_PREFIX)}`,
    );
  }
  return raw;
}

function formatActiveDeployId(deployId: string): string {
  return `${ACTIVE_DEPLOY_ID_PREFIX}${deployId}`;
}

async function persistActiveDeployId(
  instanceDir: string,
  activeIdFile: string,
  deployId: string,
): Promise<void> {
  const handle = await fs.promises.open(activeIdFile, "w");
  try {
    await handle.writeFile(formatActiveDeployId(deployId));
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    const dirHandle = await fs.promises.open(instanceDir, "r");
    try {
      await dirHandle.sync();
    } finally {
      await dirHandle.close();
    }
  } catch (err) {
    logger.warn`parent-dir fsync failed for ${instanceDir} after deploy-id persist; deploy-id durability is degraded but the committed deploy is staged on disk — ${err instanceof Error ? err.message : String(err)}`;
  }
  await clearDirtyMarker(activeIdFile, "successful persist");
}

/**
 * Remove a stale `.dirty` sibling of the active-deploy-id file. Called
 * after either the fsync'd primary persist or the no-fsync fallback
 * persist successfully writes a fresh active-deploy-id; in both cases a
 * pre-existing marker carries a now-stale id that would otherwise
 * shadow the recorded id on the next boot (the boot reader prefers the
 * marker when present).
 *
 * Best-effort: a failure to remove the marker leaves the next boot
 * reading the now-redundant marker (which carries a stale id), so the
 * divergence is bounded to a noisier log path. ENOENT is the normal
 * case when no prior failure had written a marker.
 *
 * Exported for direct unit testing of the cleanup contract.
 */
export async function clearDirtyMarker(
  activeIdFile: string,
  reason: string,
): Promise<void> {
  try {
    await fs.promises.unlink(`${activeIdFile}.dirty`);
  } catch (err) {
    if (!(hasCode(err) && err.code === "ENOENT")) {
      logger.warn`failed to clear active-deploy-id dirty marker after ${reason}: ${err instanceof Error ? err.message : String(err)}`;
    }
  }
}

/**
 * Wrap `persistActiveDeployId` with a degradation ladder: try the
 * normal fsync'd write first, and on failure try a no-fsync write +
 * write of a sibling `.dirty` marker so the next boot can reconcile.
 *
 * Returns `{ degraded: false }` on full success. Returns
 * `{ degraded: true, error }` when the primary persist failed; the
 * caller routes the existing `apply.previous-rotation.failed` frame on
 * the back of it.
 *
 * If the fallback persist also fails, on-disk state will diverge from
 * the failure frame for one boot cycle. Boot-time reconciliation reads
 * the dirty marker and prefers it.
 *
 * Exported for direct unit testing of the dirty-marker contract.
 */
export async function persistActiveDeployIdWithFallback(
  instanceDir: string,
  activeIdFile: string,
  deployId: string,
): Promise<{ readonly degraded: boolean; readonly error?: unknown }> {
  try {
    await persistActiveDeployId(instanceDir, activeIdFile, deployId);
    return { degraded: false };
  } catch (primary) {
    logger.warn`active-deploy-id primary persist failed for ${activeIdFile}: ${primary instanceof Error ? primary.message : String(primary)}; attempting degraded write`;
    try {
      await fs.promises.writeFile(activeIdFile, formatActiveDeployId(deployId));
      logger.warn`active-deploy-id degraded write succeeded (no fsync); next boot reads the recorded id from disk but a crash before flush may reveal the prior id`;
      await clearDirtyMarker(activeIdFile, "degraded persist");
      return { degraded: true, error: primary };
    } catch (fallback) {
      logger.error`active-deploy-id degraded write also failed for ${activeIdFile}: ${fallback instanceof Error ? fallback.message : String(fallback)}; writing dirty marker so the next boot can reconcile`;
      try {
        const dirtyPath = `${activeIdFile}.dirty`;
        await fs.promises.writeFile(dirtyPath, formatActiveDeployId(deployId));
        logger.warn`active-deploy-id dirty marker written to ${dirtyPath}; next boot will prefer it over the stale recorded id`;
        return { degraded: true, error: primary };
      } catch (marker) {
        logger.error`active-deploy-id dirty marker write also failed for ${activeIdFile}.dirty: ${marker instanceof Error ? marker.message : String(marker)}; on-disk state will diverge from the failure frame for one boot cycle`;
        return { degraded: true, error: primary };
      }
    }
  }
}

/**
 * Persist the rejected manifest and the failure payload to the agent's
 * on-disk audit trail. The destination is
 * `<storeDir>/audit/rejected-applies/<attemptId>/`, two files:
 *
 *   - `manifest.json`  — the manifest bytes that were rejected, written
 *                        verbatim — for every failure category, these
 *                        are the original on-disk bytes the sidecar
 *                        read from `deploy/tool-packages-manifest.json`
 *                        (corrupt JSON, wrong-shape JSON, or a
 *                        validator-accepted manifest the loader later
 *                        rejected). Persisting the raw bytes lets a
 *                        future investigator replay the same input
 *                        against a newer validator without losing
 *                        whitespace, key order, or tolerated fields
 *                        the validator narrowed away.
 *   - `error.json`     — `{ attemptId, previousDeployId, category,
 *                          message, package?, occurredAt }`
 *
 * The files live in the agent's storeDir so a future
 * git-commit-of-audit-entries pass can pick them up without rerouting
 * the data. v1 writes them directly to disk; the formal git-commit
 * step (turning these into a signed audit-trail commit on the
 * agent-state repo) is a separate plumbing piece.
 */
async function writeRejectedApplyAudit(args: {
  storeDir: string;
  attemptId: string;
  manifestBytes: string;
  failure: {
    attemptId: string;
    previousDeployId: string;
    category: string;
    message: string;
    package?: { name: string; version: string };
    occurredAt: string;
  };
}): Promise<void> {
  const dir = path.join(
    args.storeDir,
    "audit",
    "rejected-applies",
    args.attemptId,
  );
  await fs.promises.mkdir(dir, { recursive: true });
  // fsync the files and their parent directory before returning so the
  // WS frame emitted by the caller is the second event in the durable
  // sequence, not the first. Otherwise, a crash after the frame leaves
  // the wire but before the audit bytes hit disk would have the hub
  // know about a rejection the sidecar cannot prove the manifest for
  // on replay. fsync failures are downgraded to warnings: the bytes
  // are written either way, and a refusing fsync (rare networked-FS
  // failure mode) should not turn into a second cascading failure on
  // an already-failing apply.
  await fsyncWriteFile(path.join(dir, "manifest.json"), args.manifestBytes);
  await fsyncWriteFile(
    path.join(dir, "error.json"),
    JSON.stringify(args.failure, null, 2),
  );
  try {
    const dirHandle = await fs.promises.open(dir, "r");
    try {
      await dirHandle.sync();
    } finally {
      await dirHandle.close();
    }
  } catch (err) {
    logger.warn`audit-dir fsync failed for ${dir}; rejected-apply durability is degraded but the files are written — ${err instanceof Error ? err.message : String(err)}`;
  }
}

async function fsyncWriteFile(
  filePath: string,
  contents: string,
): Promise<void> {
  const handle = await fs.promises.open(filePath, "w");
  try {
    await handle.writeFile(contents);
    try {
      await handle.sync();
    } catch (err) {
      logger.warn`fsync failed for ${filePath}; durability is degraded but the bytes are written — ${err instanceof Error ? err.message : String(err)}`;
    }
  } finally {
    await handle.close();
  }
}

function collectFactories(
  loaded: readonly LoadedToolPackage[],
): readonly LoadedToolFactory[] {
  const out: LoadedToolFactory[] = [];
  for (const pkg of loaded) {
    for (const f of pkg.factories) out.push(f);
  }
  return out;
}

function collectPluginFactories(
  loaded: readonly LoadedToolPackage[],
): readonly AnnotatedPluginFactory[] {
  const out: AnnotatedPluginFactory[] = [];
  for (const pkg of loaded) {
    for (const p of pkg.plugins) out.push(p);
  }
  return out;
}
