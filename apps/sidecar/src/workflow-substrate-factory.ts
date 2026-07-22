// Substrate factory the sidecar's `bin/workflow-child` hands to
// `runWorkflowChildFromProcessEnv`. The factory closes over the
// production substrate (`createAgentRepoStore`-backed `RepoStore`),
// the host-process scheduler singleton (adapted to the runtime's
// `Scheduler` shape), and the sidecar's grant-rule evaluator.
//
// The factory consumes the workflow-host's typed `SubstrateFactoryEnv`
// -- the parsed `SpawnTimeEnv` plus a narrow `substrateConfig`
// record carrying only the keys the binary listed in
// `RunWorkflowChildFromProcessEnvOpts.substrateConfigKeys`. The
// factory does not read `process.env` itself; the binary owns the
// only crossing of that boundary.
//
// Single-writer architecture: the workflow-run repo's ref has exactly
// one writer at a time -- the supervisor. The child opens a bare
// `createAgentRepoStore` against the shared on-disk data dir for
// read-only operations (`getRepoDir`, `subscribe`, `resolveRef`,
// etc.) and exposes a proxy `RepoStore` whose
// `writeTreePreservingPrefix` forwards every write over the control
// IPC into the supervisor's substrate. The supervisor's substrate is
// wrapped with the boot-edge pack-push facade, so the hub push fires
// as part of the supervisor's normal write path -- the child does
// not open its own pack-push pipeline.

import fs from "node:fs";
import path from "node:path";

import { type } from "arktype";

import { InferenceSource } from "@intx/types/runtime";
import type {
  ApprovalSnapshot,
  AuditStore,
  ContextStore,
  MessageTransport,
  PendingOperation,
} from "@intx/types/runtime";
import { evaluateGrants } from "@intx/authz";
import type { GrantRule } from "@intx/authz";
import {
  AdapterManifest,
  createDependencies,
  type AdapterRegistry,
} from "@intx/inference";
import { loadAdapterRegistry } from "@intx/inference/providers";
import type { DirectorRegistry } from "@intx/agent";
import { createDefaultDirectorRegistry } from "@intx/agent";
import { createSSHSignature } from "@intx/crypto";
import {
  createAgentRepoStore,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  type Principal,
  type RepoId,
  type RepoStore,
  type WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions/substrate";
import { createIsogitStore } from "@intx/storage-isogit";
import {
  adaptHostScheduler,
  createCredentialsBackedAuthorize,
  createProxyWorkflowRunRepoStore,
  createSupervisorBackedTransport,
  createWorkflowHostScheduler,
  createWorkflowRunBlobSubstrate,
  createWorkflowRunRepoStore,
  createWorkflowHostSignalChannel,
  createWorkflowSpawnChild,
  createWorkflowStepInvoker,
  hashGrants,
  type ChildOutboundMailBridge,
  type CredentialsSnapshot,
  type CredentialsSnapshotRef,
  type GrantEvaluator,
  type LoadParkedApproval,
  type RunChildWorkflow,
  type RunWorkflowChildBindings,
  type SourcesSnapshotRef,
  type StepEnvBase,
  type SubstrateFactory,
  type SubstrateFactoryEnv,
} from "@intx/workflow-host";
import {
  baseStepId,
  createNoopDrainController,
  emptyState,
  runtimeRun,
  type ParkedApprovalOp,
  type ReadParkedApprovalOps,
  type Scheduler,
  type StepInvokeRequest,
  type StepInvokeResult,
  type WorkflowAuthorizeFn,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

import {
  attachStepTools,
  createToolBearingAgentFactory,
  deriveToolMarkFloorGrants,
  materializeStepTools,
  type StepToolCacheConfig,
} from "./step-agent-tools";
import { readRunGrants, runGrantsPath } from "./run-grants";
import {
  createDurableConversationRegistry,
  isErrnoNotFound,
  reconstructDurableConversation,
  type DurableConversationRegistry,
} from "./conversation-state";

// The child does not construct a workflow-run pack-push pipeline of
// its own. The supervisor owns the workflow-run repo's write
// contract; the supervisor's substrate is wrapped at the sidecar's
// boot edge with the pack-pushing facade so any successful workflow-
// run write fires the hub push automatically. The child's proxy
// `RepoStore` forwards `writeTreePreservingPrefix` over IPC into the
// supervisor's wrapped substrate.

/**
 * Thrown by the child-runtime step invoker when a `childWorkflow` (or a
 * `map` nested inside one) reaches a per-step agent invocation. Real
 * per-step child execution -- threading the child
 * `WorkflowDefinition`-derived inference sources, tools, and grants into
 * a real agent, backed by deploy-side child asset staging and capability
 * approval -- is not built; that work is tracked in INTR-310.
 *
 * Failing here is deliberate. Returning a fabricated success output would
 * report a child run `completed` whose agent never ran, a silent
 * correctness trap; a loud, structured failure is the honest behavior for
 * an unbuilt seam.
 */
export class ChildStepNotImplementedError extends Error {
  constructor(agentId: string, stepId: string | undefined) {
    super(
      `childWorkflow per-step execution is not implemented (tracked as INTR-310); ` +
        `the child runtime cannot run a real per-step agent for step ${JSON.stringify(stepId)} (agent ${JSON.stringify(agentId)})`,
    );
    this.name = "ChildStepNotImplementedError";
  }
}

/**
 * Required substrate-config keys the sidecar's binary forwards into
 * the factory's `substrateConfig` slot. Listed here so the binary
 * passes the same names to the helper; the helper enforces
 * presence-and-non-empty against this allowlist before the factory
 * runs.
 *
 * `HUB_WS_URL`, `SIDECAR_ID`, and `SIDECAR_TOKEN` carry the
 * hub-connection trust anchors the child needs to ship workflow-run
 * pack pushes back to the hub. The sidecar's deploy router populates
 * these via the supervisor's `substrateEnv` plumbing
 * (`multistepSubstrateEnv` on `createSidecarDeployRouter`), threaded
 * from the boot edge's own env reads.
 */
export const SIDECAR_SUBSTRATE_CONFIG_KEYS = [
  "SIDECAR_DATA_DIR",
  "WORKFLOW_DEFINITION_REPO_ID",
  "WORKFLOW_DEFINITION_REF",
  "WORKFLOW_RUN_REPO_ID",
  "WORKFLOW_RUN_REF",
  "SIDECAR_SIGNING_PUBLIC_KEY",
  "SIDECAR_SIGNING_PRIVATE_KEY",
  "HUB_WS_URL",
  "SIDECAR_ID",
  "SIDECAR_TOKEN",
  "STEP_INFERENCE_SOURCES",
  "SIDECAR_CACHE_MAX_BYTES",
  "SIDECAR_REGISTRY_MAX_TARBALL_BYTES",
  "SIDECAR_ADAPTER_MANIFEST",
] as const;

const SubstrateConfig = type({
  SIDECAR_DATA_DIR: "string > 0",
  WORKFLOW_DEFINITION_REPO_ID: "string > 0",
  WORKFLOW_DEFINITION_REF: "string > 0",
  WORKFLOW_RUN_REPO_ID: "string > 0",
  WORKFLOW_RUN_REF: "string > 0",
  SIDECAR_SIGNING_PUBLIC_KEY: "string > 0",
  SIDECAR_SIGNING_PRIVATE_KEY: "string > 0",
  HUB_WS_URL: "string > 0",
  SIDECAR_ID: "string > 0",
  SIDECAR_TOKEN: "string > 0",
  STEP_INFERENCE_SOURCES: "string > 0",
  // Per-step tool-loader caps. The supervisor threads the boot edge's
  // resolved `SIDECAR_CACHE_MAX_BYTES` / `SIDECAR_REGISTRY_MAX_TARBALL_BYTES`
  // through `substrateEnv` so the child's per-step tool materialization is
  // bounded by the sidecar's boot-edge-resolved caps. Validated as
  // positive-finite-number strings at this boundary.
  SIDECAR_CACHE_MAX_BYTES: "string > 0",
  SIDECAR_REGISTRY_MAX_TARBALL_BYTES: "string > 0",
  // JSON-encoded custom inference adapter manifest. Required: the boot
  // edge always serializes it into `substrateEnv` (defaulting to "[]"
  // when no custom adapters are configured), so a missing key child-side
  // is a serialization bug and must fail loud here, exactly like the
  // byte-cap fields. Validated as a non-empty string at this boundary;
  // its JSON shape is re-validated against `AdapterManifest` in
  // `parseAdapterManifest` before any module is imported.
  SIDECAR_ADAPTER_MANIFEST: "string > 0",
}).onUndeclaredKey("ignore");

/**
 * Parse a substrate-config cap entry (`SIDECAR_CACHE_MAX_BYTES` /
 * `SIDECAR_REGISTRY_MAX_TARBALL_BYTES`) into a positive finite number.
 * The boot edge already validated these via the `config.ts` readers
 * before serializing them into `substrateEnv`; this re-parse at the
 * child boundary keeps the typed-config contract honest rather than
 * trusting the wire blindly.
 */
function parseByteCap(raw: string, name: string): number {
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(
      `sidecar workflow-child substrate config: ${name} must be a positive finite number; got ${JSON.stringify(raw)}`,
    );
  }
  return n;
}

/**
 * Per-step inference-source table parsed from the spawn-time
 * `STEP_INFERENCE_SOURCES` env entry. The deploy router serializes
 * `frame.workflow.sources` (a `Record<stepId, InferenceSource[]>`) as
 * JSON and threads it through the supervisor's `substrateEnv`; the
 * factory parses and validates the table once at construction time and
 * seeds it into the run loop's mutable sources reference, which each
 * `buildEnv` reads. Each value is the step's ordered
 * failover chain -- element 0 is the active source, the tail are the
 * reactor's forward-only failover targets -- so the list is non-empty.
 */
const StepInferenceSourceTable = type({
  "[string]": InferenceSource.array().atLeastLength(1),
});
type StepInferenceSourceTable = typeof StepInferenceSourceTable.infer;

/**
 * Parse and validate the JSON-encoded `STEP_INFERENCE_SOURCES` entry
 * the supervisor threaded through `substrateEnv`. A malformed JSON
 * payload, a non-object root, or a value that does not match
 * `Record<string, InferenceSource>` is rejected at the boundary with
 * a structured error rather than being deferred to a deep-stack
 * `buildEnv` failure.
 */
function parseStepInferenceSources(raw: string): StepInferenceSourceTable {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    throw new Error(
      `sidecar workflow-child substrate config: STEP_INFERENCE_SOURCES is not valid JSON: ${reason}`,
    );
  }
  const validated = StepInferenceSourceTable(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `sidecar workflow-child substrate config: STEP_INFERENCE_SOURCES failed validation: ${validated.summary}`,
    );
  }
  return validated;
}

/**
 * Parse and validate the JSON-encoded `SIDECAR_ADAPTER_MANIFEST` entry
 * the supervisor threaded through `substrateEnv` from the boot edge's
 * `readAdapterManifest`.
 *
 * Trust boundary: the child's substrate config is operator-supplied
 * (the supervisor's `Bun.spawn` env), so this re-validation is
 * defense-in-depth at the deserialization boundary, NOT a trust
 * upgrade. The manifest was already trusted operator config on the
 * parent side; the same channel already carries the sidecar's signing
 * private key, so it is not a lower-trust surface. Re-asserting the
 * shape here keeps the typed-config contract honest rather than
 * importing modules off an unvalidated wire value.
 *
 * Host contract for custom adapters: a manifest `specifier` must
 * resolve from BOTH the sidecar's and this child's module-resolution
 * roots (the child is a separate `bun` process spawned by the
 * supervisor), and an adapter module MUST be import-side-effect-free —
 * it is imported once per process by `loadAdapterRegistry`, and any
 * top-level side effect would run independently in the parent and in
 * every child.
 */
export function parseAdapterManifest(raw: string): AdapterManifest {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new Error(
      "sidecar workflow-child substrate config: SIDECAR_ADAPTER_MANIFEST is not valid JSON",
      { cause },
    );
  }
  const validated = AdapterManifest(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `sidecar workflow-child substrate config: SIDECAR_ADAPTER_MANIFEST failed validation: ${validated.summary}`,
    );
  }
  return validated;
}

/**
 * Resolve the per-step inference-source failover chain from the table a
 * build reads. The supervisor's multi-step branch only invokes
 * a step whose `stepId` appears in `frame.workflow.sources`; a lookup
 * miss here is a programmer error in the supervisor, not a wire-side
 * failure, and the resolver surfaces it with the missing base step id
 * named (plus the scoped invocation id, for a map iteration). The
 * returned list is the step's ordered chain (element 0 the active
 * source, the tail the reactor's failover targets); the table's arktype
 * guarantees it is non-empty.
 *
 * A `map` iteration runs under a scoped step id `<base>[<index>]`, but
 * deploy pins one source per base step, so the scoped id is resolved to
 * its base before the lookup -- every iteration shares the base step's
 * pinned source. `baseStepId` is the identity on an unscoped id, so a
 * plain step is unaffected.
 */
function createStepInferenceSourceResolver(
  table: StepInferenceSourceTable,
): (stepId: string) => InferenceSource[] {
  return (stepId: string): InferenceSource[] => {
    const base = baseStepId(stepId);
    const sources = table[base];
    if (sources === undefined) {
      const scopedNote =
        base === stepId
          ? ""
          : ` (normalized from scoped invocation id ${JSON.stringify(stepId)})`;
      throw new Error(
        `sidecar workflow-child step invoker buildEnv: no InferenceSource pinned for stepId ${JSON.stringify(base)}${scopedNote}; the supervisor must populate frame.workflow.sources for every stepOrder entry`,
      );
    }
    return sources;
  };
}

function hexDecode(hex: string, name: string): Uint8Array {
  if (hex.length % 2 !== 0) {
    throw new Error(
      `${name} must be even-length hex; got ${String(hex.length)} chars`,
    );
  }
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i += 1) {
    const byte = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    if (Number.isNaN(byte)) {
      throw new Error(`${name} contains non-hex characters`);
    }
    out[i] = byte;
  }
  return out;
}

/**
 * Dependency overrides accepted by `createSidecarSubstrateFactory`.
 * Production callers omit these to get the default-disk-backed bare
 * store and the IPC-bridge-backed substrate proxy; tests inject an
 * in-memory bare store and/or an explicit substrate-write bridge.
 */
interface SidecarSubstrateFactoryDeps {
  /**
   * Override the bare-store constructor. Production callers omit this
   * to get the `createAgentRepoStore`-backed `RepoStore` against
   * `SIDECAR_DATA_DIR`; tests inject an in-memory recording stub.
   *
   * The bare store backs the child's read-only operations
   * (`getRepoDir`, `subscribe`, `resolveRef`, `listRefs`,
   * `resolveHead`, `createPack`). The child's workflow-run writes do
   * NOT flow through this store; the proxy `RepoStore` forwards them
   * over IPC into the supervisor's substrate.
   */
  createBareRepoStore?: (config: {
    dataDir: string;
    signingKey: { publicKey: Uint8Array; privateKey: Uint8Array };
  }) => RepoStore;
}

/**
 * `CommitSigner` the per-step isogit stores use to sign every commit.
 * The factory's Ed25519 signing keypair (the same key the child's bare
 * `RepoStore` carries) is bound into an `sshsig`-shaped signer so the
 * per-step agent-state commits are attributable to the sidecar's
 * substrate identity, matching the signing surface the production
 * `RepoStore` uses for workflow-run writes.
 */
function createStepStorageSigner(signingKey: {
  publicKey: Uint8Array;
  privateKey: Uint8Array;
}): (payload: string) => Promise<string> {
  return (payload: string) =>
    Promise.resolve(
      createSSHSignature(payload, signingKey.privateKey, signingKey.publicKey),
    );
}

/**
 * Root directory for a single step invocation's agent-state storage and
 * workspace, derived from the sidecar data dir and the run/step/attempt
 * coordinates the workflow runtime owns.
 *
 * The per-step agent storage is a distinct isogit repo, deliberately
 * rooted OUTSIDE the workflow-run repo's working tree. The workflow-run
 * repo's single writer is the supervisor, and its working tree carries
 * the run-event log under `runs/<runId>/events/...`; nesting a second
 * git repo inside that tree would collide with the event subtree and
 * with the supervisor's write contract. Rooting the per-step store under
 * a dedicated `workflow-step-state/` sibling subtree keyed by the
 * workflow-run repo id keeps every step's storage isolated per run and
 * per step while never touching the run-event tree.
 *
 * Resume-attempt invariant: a suspended agent step commits its pending-op
 * + turns under the `attempt-N` directory where N is the step's attempt
 * at suspend time. On crash-resume the runtime (`runStep` in
 * `packages/workflow`) recovers that attempt from the reduced state's
 * `currentAttempt` and re-invokes with it, so this function reopens the
 * SAME `attempt-N` store and the reactor's `rehydrateGates` finds the
 * pending-op the delivered decision correlates against. This is why the
 * resume path recovers `currentAttempt` rather than assuming attempt 1:
 * attempt 1 is correct only for a step that never retried before
 * suspending; a retried-then-suspended step lives under `attempt-2`+, and
 * reopening `attempt-1` would rehydrate an empty store and hang on a
 * decision that correlates nothing. `createSidecarStepBuildEnv` asserts
 * this invariant loudly on the cold path (a resume opening a store that
 * lacks the resumed correlationId's pending-op throws).
 */
export function stepStorageRoot(args: {
  dataDir: string;
  workflowRunRepoId: RepoId;
  runId: string;
  stepId: string;
  attempt: number;
}): string {
  return path.join(
    args.dataDir,
    "workflow-step-state",
    args.workflowRunRepoId.id,
    "runs",
    args.runId,
    "steps",
    args.stepId,
    `attempt-${String(args.attempt)}`,
  );
}

/**
 * Root directory for a single workflow-run subtree's per-step scratch:
 * `<dataDir>/workflow-step-state/<repoId>/runs/<runId>/`. The cold
 * (multi-step) path's per-step `stepStorageRoot` nests under this, so
 * reclaiming this subtree on run completion drops every step/attempt the
 * run produced in one `rm -rf`. Kept distinct from `stepStorageRoot` so
 * the deletion granularity (a whole run, not a single step/attempt) is
 * expressed at the call site that owns run-completion cleanup.
 */
function runStepStorageRoot(args: {
  dataDir: string;
  workflowRunRepoId: RepoId;
  runId: string;
}): string {
  return path.join(
    args.dataDir,
    "workflow-step-state",
    args.workflowRunRepoId.id,
    "runs",
    args.runId,
  );
}

/**
 * Stable per-agent scratch root for the WARM single-step agent's
 * workspace + tool materialization (tarball-cache + apply-state). Keyed
 * by the step identity exactly like the durable conversation store's
 * `agent-conversation-state/<repoId>/<agentKey>/` (conversation-state.ts),
 * NOT by the arbitrary first-message runId. Keying it stably is what
 * bounds the warm case: the cached agent reuses ONE workspace across
 * every message in the child's lifetime, and that same workspace is
 * re-derived (and so survives) across a child respawn, instead of
 * stranding a fresh per-runId subtree each time. The whole subtree is
 * reclaimed on undeploy, when the deployment's supervisor + child are
 * already torn down. Rooted under a `warm/` sibling of the cold `runs/`
 * subtree so the undeploy sweep of `workflow-step-state/<repoId>/`
 * reclaims both with one removal and the two keyings never collide.
 */
function warmStepStorageRoot(args: {
  dataDir: string;
  workflowRunRepoId: RepoId;
  stepId: string;
}): string {
  return path.join(
    args.dataDir,
    "workflow-step-state",
    args.workflowRunRepoId.id,
    "warm",
    encodeURIComponent(args.stepId),
  );
}

async function directoryExists(dir: string): Promise<boolean> {
  try {
    return (await fs.promises.stat(dir)).isDirectory();
  } catch (cause) {
    if (isErrnoNotFound(cause)) return false;
    throw cause;
  }
}

function findApprovalSnapshot(
  pendingOperations: readonly PendingOperation[],
  correlationId: string,
): ApprovalSnapshot | undefined {
  return pendingOperations.find((op) => op.correlationId === correlationId)
    ?.approvalSnapshot;
}

/**
 * Read a cold (multi-step) parked step's durable pending operations from its
 * on-disk per-attempt isogit store. The store is written at suspend and
 * survives while the run is non-terminal -- a parked step keeps the run
 * in-flight, so the run-completion reclamation (`cleanupRunStorage`) never
 * fires against it.
 *
 * Returns an empty list when the store directory is absent rather than
 * manufacturing an empty repo on the read path: `createIsogitStore` calls
 * `initAgentRepo`, which would `mkdir` and init a fresh repo for a
 * non-existent dir. The `directoryExists` guard keeps the read a read -- on an
 * existing store `initAgentRepo` finds a repo and commits nothing, so no
 * signer is needed (`load()` never signs).
 */
export async function readColdParkedPendingOperations(args: {
  dataDir: string;
  workflowRunRepoId: RepoId;
  runId: string;
  stepId: string;
  attempt: number;
}): Promise<PendingOperation[]> {
  const storeDir = stepStorageRoot({
    dataDir: args.dataDir,
    workflowRunRepoId: args.workflowRunRepoId,
    runId: args.runId,
    stepId: args.stepId,
    attempt: args.attempt,
  });
  if (!(await directoryExists(storeDir))) return [];
  const store = await createIsogitStore(storeDir);
  const { pendingOperations } = await store.load();
  return pendingOperations;
}

export async function readColdParkedApprovalSnapshot(args: {
  dataDir: string;
  workflowRunRepoId: RepoId;
  runId: string;
  stepId: string;
  attempt: number;
  correlationId: string;
}): Promise<ApprovalSnapshot | undefined> {
  return findApprovalSnapshot(
    await readColdParkedPendingOperations(args),
    args.correlationId,
  );
}

/**
 * Read a warm (single-step) parked agent's durable pending operations from
 * substrate state. A warm agent's pending operations live in its durable
 * conversation store, mirrored to the workflow-run substrate under
 * `agent-state/<stepId>/`.
 *
 * Reconstructs that state read-only -- deliberately NOT through
 * `DurableConversationRegistry.acquire`, whose first acquire writes and
 * commits a substrate restore into the live store and would front-run the warm
 * agent's own restore ordering. A respawned child has not rebuilt the live
 * store when re-registration runs (resume re-parks without re-invoking the
 * step), so the substrate is the only place the pending operations live at that
 * moment. Returns an empty list when no durable state exists for the agent.
 */
export async function readWarmParkedPendingOperations(args: {
  substrate: RepoStore;
  workflowRunRepoId: RepoId;
  stepId: string;
}): Promise<PendingOperation[]> {
  const agentStateDir = path.join(
    args.substrate.getRepoDir(args.workflowRunRepoId),
    WORKFLOW_RUN_AGENT_STATE_PREFIX,
    encodeURIComponent(args.stepId),
  );
  const reconstructed = await reconstructDurableConversation(
    agentStateDir,
    args.stepId,
  );
  if (reconstructed === null) return [];
  return reconstructed.pendingOperations;
}

export async function readWarmParkedApprovalSnapshot(args: {
  substrate: RepoStore;
  workflowRunRepoId: RepoId;
  stepId: string;
  correlationId: string;
}): Promise<ApprovalSnapshot | undefined> {
  return findApprovalSnapshot(
    await readWarmParkedPendingOperations(args),
    args.correlationId,
  );
}

/**
 * Project a parked step's durable pending operations down to the minimal
 * approval records the resume classifier needs. Filters to `approval` (the only
 * control-plane kind today) and keeps only the correlationId and the optional
 * epoch-ms deadline; the runtime reconstructs the lost `SignalAwaited` from
 * those alone, and must not see the reactor's pending-operation internals.
 */
export function toParkedApprovalOps(
  pendingOperations: PendingOperation[],
): ParkedApprovalOp[] {
  return pendingOperations
    .filter((op) => op.kind === "approval")
    .map((op) => ({
      correlationId: op.correlationId,
      ...(op.timeoutAt !== undefined ? { timeoutAtMs: op.timeoutAt } : {}),
    }));
}

export interface SidecarStepBuildEnvDeps {
  dataDir: string;
  workflowRunRepoId: RepoId;
  signer: (payload: string) => Promise<string>;
  /**
   * Deployment mailbox address the supervisor threaded into the child
   * (`MAILBOX_ADDRESS`). Used to locate each step's on-disk deploy tree
   * for tool materialization (see `stepDeployTreeDir`) AND as the step
   * agent's outbound mail `address`: the supervisor signs the agent's
   * outbound mail as this address through the host transport (OUTBOUND
   * half of mailbox ownership, §3a). For the single-step launched-agent
   * deploy this is the legacy `ins_<hex>` identity the host registered
   * the agent's `CryptoProvider` against.
   */
  mailboxAddress: string;
  /**
   * Step count of the deployed `WorkflowDefinition` (`stepOrder.length`),
   * threaded from the host through the spawn-time env. Selects the
   * head/step collapse when locating a step's deploy tree
   * (`stepDeployTreeDir` -> `resolveStepAddress`): a single-step
   * deployment reads at the head, a multi-step deployment at the per-step
   * address, matching the host's producer push.
   */
  stepCount: number;
  /**
   * Child-side outbound-mail bridge over the upstream control channel
   * (OUTBOUND half of mailbox ownership, §3a). The per-step env builder
   * wraps it in a supervisor-backed `MessageTransport` it supplies as
   * the step agent's `env.transport`; the agent's mail tools call
   * `transport.send`, which routes through the bridge to the supervisor
   * for the actual signed send. The step agent never holds the signing
   * key.
   */
  outboundMailBridge: ChildOutboundMailBridge;
  /** Per-step tool-loader caps (cache + registry tarball size). */
  cache: StepToolCacheConfig;
  /**
   * Adapter registry the step agent resolves inference adapters through.
   * The child builds this eagerly at boot from the validated
   * `SIDECAR_ADAPTER_MANIFEST` (built-ins merged with operator custom
   * adapters) and the env builder sets it on `env.deps`, so a step whose
   * source names a custom provider resolves in the child exactly as it
   * does on the sidecar main path. Without it the step agent would fall
   * back to `createAgent`'s built-ins-only default and a custom-provider
   * source would fail to resolve at run time.
   */
  adapters: AdapterRegistry;
  /**
   * Durable-conversation registry for the warm single-step agent
   * (design §3c). When present, the env builder swaps the per-run isogit
   * `ContextStore` for a per-agent durable store whose conversation is
   * mirrored to the workflow-run substrate, and restores the prior
   * conversation from the substrate before returning the env (so the
   * agent's reactor `load()` and the warm cache's lazy build see the
   * restored turns -- including the respawn-rebuild path). Absent for a
   * multi-step deploy, whose per-step agents are not warm/long-lived and
   * need no cross-run conversation durability.
   */
  durableConversation?: DurableConversationRegistry;
  /**
   * Record the tool-mark floor grants derived from the step's
   * materialized tool factories, keyed by the step's base id. The env
   * builder is the only place the child holds the loaded factories'
   * static `definitions` (name + approval mark), so it derives the floor
   * here; the grant evaluator (a sibling closure in the substrate
   * factory) reads the recorded floor by base step id and merges it under
   * the credentials snapshot's grants at authorization time. Keyed by
   * base id (matching the credentials snapshot and inference-source
   * table) so a `map` iteration's scoped id shares its base step's floor.
   * The recording persists across a warm agent's messages: the env
   * builder runs once (first build) but the floor it records must be
   * available for every later tool call the warm agent makes.
   */
  recordToolMarkFloor: (baseStepId: string, grants: GrantRule[]) => void;
}

/**
 * Build the step-invoker `buildEnv` callback the workflow-host's
 * adapter consumes. Pulled out of `createSidecarSubstrateFactory` so
 * the per-step env construction is observable without standing up the
 * full substrate.
 *
 * The closure reads the per-step source table from the mutable
 * reference passed per build, derives the `stepId` / `runId` / `attempt`
 * from the runtime's `AuthorizeContext`, resolves the per-step
 * `InferenceSource` from that table, and stands up
 * a per-step isogit `ContextStore` (also serving as the audit store)
 * plus a per-step workspace directory rooted under the run. A
 * construction failure (mkdir, isogit init) surfaces here rather than
 * being papered over with a stub: the single-step path now always runs
 * a real agent against real storage.
 */
export function createSidecarStepBuildEnv(
  deps: SidecarStepBuildEnvDeps,
): (
  req: StepInvokeRequest,
  sourcesRef: SourcesSnapshotRef,
) => Promise<StepEnvBase> {
  return async (
    req: StepInvokeRequest,
    sourcesRef: SourcesSnapshotRef,
  ): Promise<StepEnvBase> => {
    // Resolve against the live table each build so a source rotation that
    // wrote `sourcesRef.current` before this build is reflected in the
    // agent this build constructs. A warm agent that is already built does
    // not pass through here again, so a rotation does not reach it through
    // this path -- this ref covers only a build that has not happened yet.
    const resolveStepInferenceSource = createStepInferenceSourceResolver(
      sourcesRef.current,
    );
    const { stepId, runId, attempt } = req.authzContext;
    if (stepId === undefined) {
      throw new Error(
        "sidecar workflow-child step invoker buildEnv: AuthorizeContext.stepId is required for per-step InferenceSource resolution; the workflow runtime must populate stepId on every step-originated invocation",
      );
    }
    if (runId === undefined) {
      throw new Error(
        "sidecar workflow-child step invoker buildEnv: AuthorizeContext.runId is required to root per-step storage under the run; the workflow runtime must populate runId on every step-originated invocation",
      );
    }
    if (attempt === undefined) {
      throw new Error(
        "sidecar workflow-child step invoker buildEnv: AuthorizeContext.attempt is required to root per-step storage per attempt; the workflow runtime must populate attempt on every step-originated invocation",
      );
    }
    const sources = resolveStepInferenceSource(stepId);
    // The resolver's arktype guarantees a non-empty chain; assert it here so
    // the reactor's initial-source pin (element 0) is a checked fact rather
    // than an unchecked index.
    const activeSource = sources[0];
    if (activeSource === undefined) {
      throw new Error(
        `sidecar workflow-child step invoker buildEnv: empty InferenceSource chain pinned for stepId ${JSON.stringify(stepId)}`,
      );
    }

    // Root the per-step scratch (workspace + tool tarball-cache +
    // apply-state). The cold (multi-step) path keys it per
    // run/step/attempt: each run rebuilds the agent and its scratch, and
    // the run's whole `runs/<runId>/` subtree is reclaimed on run
    // completion. The warm single-step path (`durableConversation`
    // present) keys it STABLY per agent so the cached agent reuses one
    // workspace across every message -- bounding the warm case to one
    // dir per agent and letting that workspace survive child respawn --
    // and the subtree is reclaimed on undeploy. The two keyings live
    // under disjoint `runs/` and `warm/` sub-roots so neither sweep
    // touches the other's tree, and the durable conversation under
    // `agent-conversation-state/` is a different root that neither
    // sweep touches.
    const storeDir =
      deps.durableConversation !== undefined
        ? warmStepStorageRoot({
            dataDir: deps.dataDir,
            workflowRunRepoId: deps.workflowRunRepoId,
            stepId,
          })
        : stepStorageRoot({
            dataDir: deps.dataDir,
            workflowRunRepoId: deps.workflowRunRepoId,
            runId,
            stepId,
            attempt,
          });
    // Conversation storage. For the warm single-step agent the
    // conversation must survive child respawn, so it is backed by a
    // per-agent durable store whose content is mirrored to the
    // workflow-run substrate (design §3c); building it here restores the
    // prior conversation before the agent's reactor loads. A multi-step
    // deploy (no durable registry) keeps the per-run isogit store: its
    // per-step agents are not warm/long-lived and have no cross-run
    // conversation to carry. The workdir + tools stay per-run in both
    // cases -- only the conversation context is durable across runs.
    const storage: ContextStore & AuditStore =
      deps.durableConversation !== undefined
        ? (await deps.durableConversation.acquire(stepId)).storage
        : await createIsogitStore(storeDir, deps.signer);

    // Cold-path resume keying assertion (correct-by-construction guard for
    // the resume-attempt invariant documented on `stepStorageRoot`). A
    // resume re-invocation (`req.resume` present) delivers the correlated
    // decision to the reactor, which rehydrates its gate from THIS store's
    // pending operations. The store the runtime reopened is keyed by
    // `attempt` (`stepStorageRoot` above); if that attempt does not match
    // the attempt the step suspended on, the store carries no pending-op
    // for the resumed correlationId, the reactor comes up gateless, and the
    // delivered decision correlates against nothing -- a silent forever-hang.
    // Make that keying violation loud here, at the single seam that both
    // opened the store AND knows a resume must find its gate, rather than
    // letting it surface as a hang. The warm path keys its durable store per
    // agent (not per attempt) and rehydrates from a different lifecycle, so
    // this assertion is cold-path only.
    if (deps.durableConversation === undefined && req.resume !== undefined) {
      const resumeCorrelationId = req.resume.correlationId;
      const loaded = await storage.load();
      const hasPendingOp = loaded.pendingOperations.some(
        (op) => op.correlationId === resumeCorrelationId,
      );
      if (!hasPendingOp) {
        throw new Error(
          `sidecar workflow-child step invoker buildEnv: resume of step ${JSON.stringify(stepId)} (run ${JSON.stringify(runId)}, attempt ${String(attempt)}) reopened a ContextStore with no pending operation for correlationId ${JSON.stringify(resumeCorrelationId)}. The cold-path store is keyed by attempt (${storeDir}); a resume that finds no gate here means it reopened the wrong attempt's store -- the reactor would rehydrate no gate and the delivered decision would correlate against nothing. This is a keying violation, not a recoverable state.`,
        );
      }
    }

    const workdir = path.join(storeDir, "workspace");
    await fs.promises.mkdir(workdir, { recursive: true });

    // Materialize the step's pinned tool-package closure (posix, LSP,
    // mail, ...) from its on-disk deploy tree, rooted per step under
    // `storeDir` so concurrent steps in one child never collide on the
    // tarball cache or the apply-state tree. A deploy with no manifest
    // yields empty tools (the legitimate `rawManifestBytes === undefined`
    // case); a present-but-broken manifest surfaces loudly through
    // `materializeStepTools` rather than degrading to empty tools.
    const materialization = await materializeStepTools({
      dataDir: deps.dataDir,
      mailboxAddress: deps.mailboxAddress,
      stepId,
      stepCount: deps.stepCount,
      storeDir,
      cache: deps.cache,
    });

    // Derive and record the step's tool-mark floor from the just-loaded
    // factories' static definitions. A pinned tool loads here in the
    // child and never reached the hub's capability walk, so its
    // `tool:<name>` grant is absent from the credentials snapshot; the
    // recorded floor is what lets the grant evaluator authorize a pinned
    // tool against its own static mark. Keyed by base step id so the
    // evaluator's `baseStepId(stepId)` lookup resolves for both a plain
    // step and a `map` iteration's scoped id.
    deps.recordToolMarkFloor(
      baseStepId(stepId),
      deriveToolMarkFloorGrants(materialization.factories),
    );

    // Supervisor-backed transport for the step agent's mail tools
    // (OUTBOUND half of mailbox ownership, §3a). Inbound is inert -- the
    // supervisor delivers the agent's input as the step input, not
    // through the agent's own mailbox -- and outbound (`send`) routes
    // over the control IPC to the supervisor, which performs the actual
    // signed send through the host transport as `address`. `address` is
    // the deployment mailbox address: the same identity the host
    // registered the agent's `CryptoProvider` against, so the outbound
    // mail carries the agent's signature with parity to the in-process
    // path. Both `transport` and `address` are the env keys
    // `@intx/tools-mail`'s sidecar bundle declares in its `requires`.
    const transport = createSupervisorBackedTransport(
      deps.outboundMailBridge,
      deps.mailboxAddress,
    );

    // The step env carries `transport` + `address` beyond `BaseEnv` so
    // the mail-tool bundle (`@intx/tools-mail`, `requires: ["transport",
    // "address"]`) resolves its handles. The two keys are extra env
    // surface the tool factory reads at handler-init; they widen the
    // returned `StepEnvBase` structurally, which the buildEnv return
    // type (`StepEnvBase`) accepts (a wider object is assignable to the
    // narrower type).
    const env: StepEnvBase & { transport: MessageTransport; address: string } =
      {
        // Feed the reactor the step's full ordered failover chain and pin
        // its initial source to element 0. The reactor resolves the initial
        // source by id and fails over forward through `sources`, so this
        // restores cross-source failover inside the workflow-child.
        sources,
        defaultSource: activeSource.id,
        storage,
        workdir,
        audit: storage,
        directors: createDefaultDirectorRegistry(),
        // Resolve inference adapters through the child's boot-built
        // registry (built-ins + operator custom adapters), so a
        // custom-provider step source resolves in the child the same way
        // it does on the sidecar main path rather than hitting
        // `createAgent`'s built-ins-only default.
        deps: createDependencies(deps.adapters),
        transport,
        address: deps.mailboxAddress,
      };
    // Carry the materialized tool runtime to the tool-bearing
    // `agentFactory` via the env's symbol-keyed slot. The step-invoker
    // adapter spreads this env (`{ ...envBase, authorize }`) before
    // handing it to `agentFactory`; object spread preserves own
    // symbol-keyed properties, so the slot survives the spread.
    attachStepTools(env, materialization);
    return env;
  };
}

/**
 * Step invoker the child runtime's `env.invokeStep` wraps. It widens the
 * workflow-runtime `StepInvoker` with the child's credentials-backed
 * `authorize`: the runtime body calls `env.invokeStep` with the request
 * alone, so the invoker is the seam that resolves each tool call against
 * the run's grants. Mirrors the workflow-process child's `ChildStepInvoker`
 * authorize slot without carrying the event/warm-cache/sources slots the
 * in-process child does not use.
 */
export type SidecarChildStepInvoker = (
  req: StepInvokeRequest,
  authorize: WorkflowAuthorizeFn,
) => Promise<StepInvokeResult>;

/**
 * Inputs required to construct the sidecar's in-process child runtime.
 * Lifted out of `createSidecarSubstrateFactory` so the implementation
 * is exercisable in isolation (the co-located test wires a hand-built
 * substrate/principal/scheduler/invokeStep against this surface).
 *
 * Sub-namespace scoping: the child runtime is invoked with
 * `runId: childRunId`. The runtime body threads that id through every
 * `repoStore.read/append/subscribe` call, every `blobs.recordOutput`
 * call, and every `signalChannel.deliver/awaitNext` call. The host-
 * adapter implementations (`createWorkflowRunRepoStore`,
 * `createWorkflowRunBlobSubstrate`, `createWorkflowHostSignalChannel`)
 * each compute their on-disk path as `runs/<runId>/...` against the
 * supplied workflow-run repo. The net effect is that the child's
 * events land under `runs/<childRunId>/events/<seq>.json` of the
 * parent's workflow-run repo, sibling to the parent's own
 * `runs/<parentRunId>/...` subtree.
 *
 * Substrate identity: the child reuses the parent's wrapped `RepoStore`
 * (the workflow-run pack-pushing wrap installed by the factory) so a
 * successful child write fires the same hub pack push the parent's
 * writes do. The substrate's signing principal (a workflow-process
 * principal scoped to the parent's deploymentId) is reused verbatim
 * because the child runs under the same supervisor authority.
 */
interface SidecarRunChildDeps {
  /** Wrapped workflow-run substrate (the factory's `substrate`). */
  substrate: RepoStore;
  /** Workflow-run repo identifying the parent's deployment. */
  workflowRunRepoId: RepoId;
  /** Workflow-run ref the child reads/writes against. */
  workflowRunRef: string;
  /**
   * Deploy ref the child env's recursive `spawnChild` resolves
   * grandchild `definitionRef`s against. The runtime body's
   * `runChildWorkflow` was designed for arbitrary depth; the child's
   * env's `spawnChild` slot must itself be a `createWorkflowSpawnChild`
   * adapter against this deploy ref so a grandchild spawn resolves
   * the grandchild's `workflow.json` from the workflow asset substrate
   * the same way the parent's spawn does. The sub-namespace scoping
   * (`runs/<runId>/...`) continues to work because each rung's
   * runtime env routes through `runId`-keyed substrate adapters.
   */
  workflowDefinitionRef: string;
  /** Principal the child presents on every substrate operation. */
  principal: Principal;
  /** Host-process scheduler singleton; shared with the parent. */
  scheduler: Scheduler;
  /**
   * Step invoker the child runtime delegates per-step invocations to.
   *
   * The in-process child runs a WorkflowDefinition whose stepIds are
   * disjoint from the parent's. The parent's
   * `STEP_INFERENCE_SOURCES`-pinned `buildStepEnv` knows only the
   * parent's stepIds and throws on any other id; routing the child's
   * step invocations through that closure surfaces a misleading
   * "no InferenceSource pinned" error for every child step. Callers
   * therefore supply a SEPARATE invokeStep for the child. The substrate
   * factory's default rejects with `ChildStepNotImplementedError`
   * because real per-step child execution is not built (see
   * `childInvokeStep` in `createSidecarSubstrateFactory`); a test may
   * inject a functional invoker.
   *
   * The invoker receives the child's credentials-backed `authorize`
   * alongside the request, mirroring the workflow-process child's
   * `ChildStepInvoker`: the runtime body calls `env.invokeStep` with the
   * request only, so the invoker -- not the runtime -- is the seam that
   * gates each tool call against the run's grants.
   */
  invokeStep: SidecarChildStepInvoker;
  /**
   * Grant evaluator the child's credentials-backed `authorize` delegates
   * each `(resource, action)` decision to. The parent factory owns the
   * sidecar's grant-rule grammar and supplies its adapter here so the
   * child resolves authorization against the same evaluator the parent's
   * steps use.
   */
  evaluateGrants: GrantEvaluator;
  /** Director registry the child runtime uses; defaults to the canonical built-ins. */
  directors?: DirectorRegistry;
  /** Clock for timestamp generation; defaults to `() => new Date()`. */
  clock?: () => Date;
  /**
   * Random id generator for run ids, signal ids, timer ids; defaults to
   * a monotonic counter combined with a random suffix.
   */
  newId?: (prefix: string) => string;
}

/**
 * Write a spawned child's inherited grants to its own
 * `runs/<childRunId>/grants.json` in the deployment's workflow-run repo.
 *
 * The write goes through the child proxy substrate's
 * `writeTreePreservingPrefix` -- the only write path the proxy forwards
 * to the supervisor -- preserving the child's own `runs/<childRunId>/`
 * subtree (empty at spawn time, before the runtime writes any events)
 * and adding the single `grants.json` file. A later event append under
 * `runs/<childRunId>/events/` preserves a nested prefix, so it leaves
 * this sibling file untouched.
 */
async function writeChildRunGrants(args: {
  substrate: RepoStore;
  workflowRunRepoId: RepoId;
  principal: Principal;
  ref: string;
  childRunId: string;
  grants: readonly unknown[];
}): Promise<void> {
  const prefix = `runs/${args.childRunId}/`;
  const grantsFile = runGrantsPath(args.childRunId);
  const serialized = JSON.stringify({ grants: args.grants }, null, 2);
  await args.substrate.writeTreePreservingPrefix(
    args.principal,
    args.workflowRunRepoId,
    args.ref,
    {
      preservePrefix: prefix,
      merge: async (existing) => {
        const files: Record<string, string | Uint8Array> = {};
        for (const [k, v] of existing) files[k] = v;
        files[grantsFile] = serialized;
        return files;
      },
      message: `Write inherited run grants for ${args.childRunId}`,
    },
  );
}

/**
 * Construct the `RunChildWorkflow` callback the spawn-child adapter
 * delegates to. The returned callback, when invoked with the parent
 * runtime's attribution + the parent-allocated `childRunId` + the
 * resolved `WorkflowDefinition`, builds a fresh `WorkflowRuntimeEnv`
 * scoped to `childRunId`, invokes `runtimeRun`, and returns the
 * child's terminal status.
 *
 * Abort propagation: the parent-supplied `signal` is observed at every
 * runtime observation point. If the signal aborts mid-flight the
 * runtime body's cancel cascade fires and the returned promise
 * resolves with `terminalStatus: "cancelled"`. A pre-aborted signal is
 * handled by the spawn-child adapter's entry-time short-circuit; the
 * runChild callback itself does not see the pre-abort case.
 *
 * Resource lifecycle: the child's per-run signal channel handle is
 * `stop()`ped in a finally block so any background `subscribeKind`
 * loop tied to the child's runId tears down before the callback
 * returns. The blob substrate, repo store, and scheduler entries are
 * either per-call (no handle to dispose) or shared with the parent
 * (the scheduler).
 */
export function createSidecarRunChild(
  deps: SidecarRunChildDeps,
): RunChildWorkflow {
  const directors = deps.directors ?? createDefaultDirectorRegistry();
  const clock = deps.clock ?? defaultClock;
  const newId = deps.newId ?? defaultNewId;
  const repoStore = createWorkflowRunRepoStore({
    substrate: deps.substrate,
    repoId: deps.workflowRunRepoId,
    principal: deps.principal,
    ref: deps.workflowRunRef,
  });
  // Self-referential `RunChildWorkflow` so a child env's recursive
  // `spawnChild` (built via `createWorkflowSpawnChild` below) can route
  // grandchild spawns back through the same adapter. Each invocation
  // builds a per-runId env that itself wires a `spawnChild` slot whose
  // `runChild` is this same `runChild` constant -- the recursion bottoms
  // out when a rung's `WorkflowDefinition` has no `childWorkflow`
  // primitive. Sub-namespace scoping continues to hold at every depth
  // because `childRunId` flows verbatim into the per-rung
  // `blobs`/`signalChannel`/`runtimeRun` calls, keeping every rung's
  // events under `runs/<runId>/...` of the parent's workflow-run repo.
  const runChild: RunChildWorkflow = async ({
    definition,
    childRunId,
    input,
    parentRunId,
    signal,
  }) => {
    // Inherit the parent run's grants. A spawned child runs under the
    // authority of the run that spawned it, so its authorize resolves
    // against the parent's per-run grant set -- the same flat set read
    // back at `runs/<parentRunId>/grants.json` in the deployment's
    // workflow-run repo. Fail closed if the parent's file is absent: a
    // run that reached the spawn point carries a grants file (every birth
    // path materializes one), so its absence is a defect, not a run that
    // legitimately holds no grants.
    const parentGrants = await readRunGrants({
      repoStore: deps.substrate,
      deploymentId: deps.workflowRunRepoId.id,
      runId: parentRunId,
    });
    if (parentGrants === undefined) {
      throw new Error(
        `sidecar runChild: parent run ${parentRunId} has no grants file at ${runGrantsPath(parentRunId)}; refusing to spawn child ${childRunId} under-authorized`,
      );
    }
    // Persist the inherited grants as the child's OWN per-run file so a
    // grandchild spawned by this child reads them from
    // `runs/<childRunId>/grants.json`, exactly as this child read the
    // parent's. The multi-hop chain never prunes these files, so each
    // rung's grants stay resolvable for the rung below it.
    //
    // Ordering is LOAD-BEARING: this write happens BEFORE `runtimeRun`
    // dispatches the child, so `runs/<childRunId>/` holds no event blobs
    // yet and the grants write only adds `grants.json`. Reordering it
    // AFTER the runtime starts appending events would delete the child's
    // event log -- `writeChildRunGrants` rebuilds the preserved subtree
    // from the `merge` callback's inputs, so any run event committed under
    // `runs/<childRunId>/` before this write is not carried forward.
    await writeChildRunGrants({
      substrate: deps.substrate,
      workflowRunRepoId: deps.workflowRunRepoId,
      principal: deps.principal,
      ref: deps.workflowRunRef,
      childRunId,
      grants: parentGrants,
    });
    // The child's credentials snapshot applies the inherited flat grant
    // set uniformly across every step the child definition declares,
    // keyed on each step's id (the same shape the deploy-time and per-run
    // snapshot assemblies produce). The in-process child has no per-step
    // mail address, so the snapshot's `address` mirrors the step id --
    // `createCredentialsBackedAuthorize` reads only `grants`.
    const contentHash = await hashGrants(parentGrants);
    const credentialsSnapshot: CredentialsSnapshot = {
      steps: definition.stepOrder.map((stepId) => ({
        stepId,
        address: stepId,
        grants: parentGrants,
        contentHash,
      })),
    };
    const blobs = createWorkflowRunBlobSubstrate({
      substrate: deps.substrate,
      repoId: deps.workflowRunRepoId,
      principal: deps.principal,
      runId: childRunId,
      ref: deps.workflowRunRef,
    });
    const signalChannel = createWorkflowHostSignalChannel({
      repoStore: deps.substrate,
      principal: deps.principal,
      repoId: deps.workflowRunRepoId,
      ref: deps.workflowRunRef,
      runId: childRunId,
      readState: () => emptyState(childRunId),
      newId: () => newId("sig"),
      clock,
    });
    // The child's `env.authorize` binds to the inherited credentials
    // snapshot: each `(resource, action)` decision looks up the step's
    // grants and delegates to the parent factory's grant evaluator. The
    // runtime body stores this on the env; the child's `invokeStep`
    // wrapper below is the seam that consults it per tool call, and an
    // action step's `EffectContext` calls it directly for each effect.
    const credentialsRef: CredentialsSnapshotRef = {
      current: credentialsSnapshot,
    };
    const authorize = createCredentialsBackedAuthorize(
      credentialsRef,
      deps.evaluateGrants,
    );
    const drain = createNoopDrainController(definition);
    // Recursive `spawnChild`: a grandchild's `definitionRef` is resolved
    // against the workflow-asset substrate the parent's spawn used, and
    // the resolved `WorkflowDefinition` flows back into this same
    // `runChild` callback. The runtime body's `runChildWorkflow`
    // contract is depth-agnostic; the wiring here makes the sidecar's
    // adapter depth-agnostic too.
    const spawnChild = createWorkflowSpawnChild({
      substrate: deps.substrate,
      principal: deps.principal,
      deployRef: deps.workflowDefinitionRef,
      runChild,
    });
    const env: WorkflowRuntimeEnv = {
      repoStore,
      scheduler: deps.scheduler,
      signalChannel,
      blobs,
      directors,
      authorize,
      // The runtime body invokes `env.invokeStep` with the request alone;
      // forward the child's credentials-backed authorize so the invoker
      // gates each tool call against the inherited grants.
      invokeStep: (req) => deps.invokeStep(req, authorize),
      spawnChild,
      clock,
      newId,
      drain,
    };
    try {
      const handle = runtimeRun(definition, env, {
        runId: childRunId,
        triggerPayload: input,
      });
      const cancelOnAbort = (): void => {
        void handle.cancel("supervisor-operator", "parent cancelled");
      };
      if (signal.aborted) {
        cancelOnAbort();
      } else {
        signal.addEventListener("abort", cancelOnAbort, { once: true });
      }
      try {
        const result = await handle.complete;
        return { terminalStatus: result.terminalStatus };
      } finally {
        signal.removeEventListener("abort", cancelOnAbort);
      }
    } finally {
      await signalChannel.stop();
    }
  };
  return runChild;
}

function defaultClock(): Date {
  return new Date();
}

let runChildIdCounter = 0;
function defaultNewId(prefix: string): string {
  runChildIdCounter += 1;
  return `${prefix}-${String(runChildIdCounter)}-${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Build a `SubstrateFactory` closed over the supplied dependency
 * overrides. The production export `createSubstrate` is the
 * default-deps call.
 *
 * Construction order:
 *   1. Narrow the `substrateConfig` record against the typed schema.
 *      A missing or empty key already threw inside the helper; this
 *      pass enforces the exact shape the factory consumes.
 *   2. Open a bare `RepoStore` via `createAgentRepoStore` against the
 *      sidecar's data dir and Ed25519 keypair. This store backs the
 *      child's read-only operations against the workflow-run repo;
 *      the on-disk repo is shared with the supervisor's substrate so
 *      reads see whatever the supervisor has committed.
 *   3. Construct a proxy `RepoStore` whose
 *      `writeTreePreservingPrefix` forwards over the upstream control
 *      channel via the substrate-write bridge. The supervisor's
 *      handler runs its own substrate's `writeTreePreservingPrefix`
 *      (wrapped at the boot edge with the pack-push facade) under the
 *      per-repo lock and replies with the resulting `commitSha`.
 *   4. Start the host-process scheduler singleton against the proxy
 *      substrate, then adapt it to the runtime's `Scheduler` shape.
 *   5. Construct the production `invokeStep` and `spawnChild`
 *      adapters.
 *   6. Return the `RunWorkflowChildBindings` the runtime body
 *      consumes, with the proxy store in the `substrate` slot.
 */
export function createSidecarSubstrateFactory(
  deps: SidecarSubstrateFactoryDeps = {},
): SubstrateFactory {
  const createBareRepoStore =
    deps.createBareRepoStore ??
    (({ dataDir, signingKey }) =>
      createAgentRepoStore({ dataDir, signingKey }).repoStore);

  return async (env: SubstrateFactoryEnv) => {
    const validated = SubstrateConfig(env.substrateConfig);
    if (validated instanceof type.errors) {
      throw new Error(
        `sidecar workflow-child substrate config failed validation: ${validated.summary}`,
      );
    }

    const stepInferenceSources = parseStepInferenceSources(
      validated.STEP_INFERENCE_SOURCES,
    );

    // Build the child's adapter registry eagerly at boot from the
    // operator-supplied manifest. `loadAdapterRegistry` imports every
    // custom module now, so a bad specifier crashes the child loudly at
    // construction rather than silently degrading to built-ins-only at
    // first resolve. The closure registry the sidecar built at its own
    // boot edge cannot cross the fork; the child rebuilds an equivalent
    // one from the serialized-and-revalidated manifest.
    const childAdapterRegistry = await loadAdapterRegistry(
      parseAdapterManifest(validated.SIDECAR_ADAPTER_MANIFEST),
    );

    const signingKey = {
      publicKey: hexDecode(
        validated.SIDECAR_SIGNING_PUBLIC_KEY,
        "SIDECAR_SIGNING_PUBLIC_KEY",
      ),
      privateKey: hexDecode(
        validated.SIDECAR_SIGNING_PRIVATE_KEY,
        "SIDECAR_SIGNING_PRIVATE_KEY",
      ),
    };

    const bareStore: RepoStore = createBareRepoStore({
      dataDir: validated.SIDECAR_DATA_DIR,
      signingKey,
    });

    const workflowRunRepoId = {
      kind: "workflow-run" as const,
      id: validated.WORKFLOW_RUN_REPO_ID,
    };
    const workflowDefinitionRepoId = {
      kind: "workflow" as const,
      id: validated.WORKFLOW_DEFINITION_REPO_ID,
    };
    const principal: WorkflowRunWorkflowProcessPrincipal = {
      kind: "workflow-process",
      deploymentId: env.spawn.deploymentId,
    };

    // Proxy substrate: writes are forwarded over IPC into the
    // supervisor's substrate; reads consult the bare on-disk store.
    // The supervisor is the sole writer of the workflow-run ref so
    // the child's writes never race against the supervisor's
    // claim-check writes (inbox / processing / consumed).
    const substrate: RepoStore = createProxyWorkflowRunRepoStore({
      bareStore,
      bridge: env.substrateWriteBridge,
      workflowRunRepoId,
    });

    const hostScheduler = createWorkflowHostScheduler({
      repoStore: substrate,
      principal,
      listActiveDeployments: () => [workflowRunRepoId],
      ref: validated.WORKFLOW_RUN_REF,
      clock: () => new Date(),
    });
    await hostScheduler.start();
    const scheduler = adaptHostScheduler(hostScheduler);

    // The single-step / top-level path runs a real agent. The per-step
    // env builder stands up real per-step storage/workdir/audit/directors
    // rooted under the run (see `createSidecarStepBuildEnv`), resolving
    // the per-step `InferenceSource` from the pinned table; the real
    // step-invoker instantiates the step's agent via `createAgent`,
    // delivers the resolved input as a synthesized inbound message, and
    // captures the agent's reply as the step output.
    const stepToolCache: StepToolCacheConfig = {
      cacheMaxBytes: parseByteCap(
        validated.SIDECAR_CACHE_MAX_BYTES,
        "SIDECAR_CACHE_MAX_BYTES",
      ),
      registryMaxTarballBytes: parseByteCap(
        validated.SIDECAR_REGISTRY_MAX_TARBALL_BYTES,
        "SIDECAR_REGISTRY_MAX_TARBALL_BYTES",
      ),
    };

    // The single-step / top-level path runs a real agent with REAL
    // tools materialized in-child. The per-step env builder stands up
    // real per-step storage/workdir/audit/directors rooted under the
    // run (see `createSidecarStepBuildEnv`), resolves the per-step
    // `InferenceSource`, and materializes the step's pinned
    // tool-package closure (posix, LSP, mail, ...) from its on-disk
    // deploy tree -- rooted per step so concurrent steps in one child
    // never collide on the tarball cache or apply-state. The
    // tool-bearing `agentFactory` below attaches those factories to the
    // step's `AgentDefinition` and builds the plugin chain.
    // Durable-conversation registry for the warm single-step agent
    // (design §3c). Built only when the deployment is warm-kept: the
    // sole long-lived agent's conversation must survive child respawn,
    // so it is mirrored to the workflow-run substrate at a per-agent
    // path. A multi-step deploy leaves this `undefined` -- its per-step
    // agents are not warm/long-lived (§3b), so they carry no cross-run
    // conversation and keep the per-run isogit store. The registry lives
    // for the child's lifetime; on respawn the child rebuilds it empty
    // and each store restores its prior snapshot from the substrate on
    // first acquire.
    const conversationSigner = createStepStorageSigner(signingKey);
    const durableConversation: DurableConversationRegistry | undefined = env
      .spawn.warmKeep
      ? createDurableConversationRegistry({
          dataDir: validated.SIDECAR_DATA_DIR,
          workflowRunRepoId,
          workflowRunRef: validated.WORKFLOW_RUN_REF,
          substrate,
          principal,
          signer: conversationSigner,
        })
      : undefined;

    // Per-step tool-mark floor grants, keyed by base step id. The step
    // env builder derives and records each step's floor from its
    // materialized (pinned) tool factories; the grant evaluator reads it
    // by base step id and merges it under the credentials snapshot's
    // grants so a pinned tool authorizes against its own static mark. The
    // map lives for the factory's (child's) lifetime, so a warm agent's
    // floor -- recorded on its single first build -- remains available
    // for every later tool call it makes.
    const toolMarkFloorByStep = new Map<string, GrantRule[]>();

    const buildStepEnv = createSidecarStepBuildEnv({
      dataDir: validated.SIDECAR_DATA_DIR,
      workflowRunRepoId,
      signer: conversationSigner,
      mailboxAddress: env.spawn.mailboxAddress,
      stepCount: env.spawn.stepCount,
      outboundMailBridge: env.outboundMailBridge,
      cache: stepToolCache,
      adapters: childAdapterRegistry,
      recordToolMarkFloor: (stepId, grants) => {
        toolMarkFloorByStep.set(stepId, grants);
      },
      ...(durableConversation !== undefined ? { durableConversation } : {}),
    });

    // The tool-bearing agent factory reads the materialized tool
    // runtime off the per-step env (set by `buildStepEnv` via
    // `attachStepTools`), attaches the loaded tool factories to the
    // step's `AgentDefinition`, builds the plugin chain on
    // `env.plugins`, and wraps `agent.close()` so every plugin (the LSP
    // subprocess included) and tool bundle is torn down with the agent
    // on every exit path. The factory is stateless across steps, so it
    // is pinned once here and shared by every per-step invoker built
    // below.
    const stepAgentFactory = createToolBearingAgentFactory();

    // Child-runtime step invoker. The in-process `runChild` (see
    // `createSidecarRunChild` below) runs a separate WorkflowDefinition
    // whose stepIds are disjoint from the parent's, and deploy does not
    // stage the child definition's per-step assets (inference sources,
    // tool trees) or walk its capabilities. Running a real per-step agent
    // for a `childWorkflow` / `map` fan-out step is therefore not
    // implemented; that work is tracked in INTR-310. The `authorize`
    // argument -- the child's credentials-backed authorize -- is unused
    // here for the same reason: no agent runs to gate.
    //
    // This is a deliberate hard stop, not a fabricated result. A fake
    // success output (the shape this once returned) reported a child run
    // `completed` whose agent never ran -- a silent correctness trap.
    // Failing loudly surfaces the child step as `StepFailed` with a
    // structured, INTR-310-named error instead. The `spawnChild` /
    // `runChild` recursion and the sub-namespace scoping around it are
    // real and exercised right up to this seam.
    const childInvokeStep: SidecarChildStepInvoker = (req) =>
      Promise.reject(
        new ChildStepNotImplementedError(req.agent.id, req.authzContext.stepId),
      );

    // Adapt the workflow-runtime `StepInvoker` shape onto the host's
    // `ChildStepInvoker` shape. The host's `onEvent` is the child's
    // per-run event-channel sink: the runtime body passes it per step,
    // and the chain from here is `onEvent -> child event-channel sender
    // -> supervisor -> publishWorkflowInferenceEvent -> hub timeline`.
    //
    // The `authorize` argument is the child's credentials-backed
    // authorize closure (`createCredentialsBackedAuthorize`), threaded
    // in from `run-child.ts`'s runtime env. The step agent's runtime
    // gates EVERY tool call through `env.authorize` with
    // `resource = tool:<name>`, `action = "invoke"` (the inference
    // layer's authz before-tool extension); using the credentials-backed
    // authorize here means each tool call resolves against the per-step
    // grant snapshot the supervisor assembled from the agent's
    // `state/grants.json` and pushed over the control IPC. A tool the
    // agent's grants do not allow is blocked; a granted tool runs. The
    // operator gate at deploy time (the capability walk's `tool:<name>`
    // approval) and this runtime grant check are complementary: the walk
    // bounds the toolset the deploy may carry, the grant snapshot decides
    // which of those the agent may invoke at run time.
    //
    // A fresh `createWorkflowStepInvoker` is built per invocation so the
    // adapter subscribes the step agent's event stream to THIS step's
    // `onEvent`. The per-step env builder and the tool-bearing agent
    // factory are pinned (closed over above); the event sink and the
    // authorize closure vary per step.
    //
    // The `warmCache` (design §3b) is the run-loop's per-deployment
    // warm-agent cache, present only for the single-step long-lived
    // deployment the deploy projection marked a warm candidate. When
    // supplied, the adapter builds the agent once and reuses it across
    // messages; when absent, it keeps instantiate-send-teardown per
    // step. Forwarding it here is the only warm-keep wiring this binding
    // needs -- the adapter and the run-loop own the rest of the
    // lifecycle.
    // Run-boundary durability flush (design §3c). When the deployment is
    // warm-kept, mirror the warm agent's conversation snapshot to the
    // workflow-run substrate after each message's send settles. The key
    // is the step identity, the same key the env builder filed the
    // durable store under, so the hook resolves the right per-agent
    // store. Absent for a multi-step deploy (no durable registry).
    const onRunBoundary: ((key: string) => Promise<void>) | undefined =
      durableConversation !== undefined
        ? async (key: string) => {
            await durableConversation.get(key).mirrorToSubstrate();
          }
        : undefined;

    const invokeStep: RunWorkflowChildBindings["invokeStep"] = async (
      req,
      onEvent,
      authorize,
      warmCache,
      sourcesRef,
    ) =>
      createWorkflowStepInvoker({
        workflowAuthorize: authorize,
        buildEnv: (buildReq) => buildStepEnv(buildReq, sourcesRef),
        agentFactory: stepAgentFactory,
        onEvent,
        sourcesRef,
        ...(warmCache !== undefined ? { warmCache } : {}),
        ...(onRunBoundary !== undefined ? { onRunBoundary } : {}),
      })(req);

    const evaluateGrantsAdapter: GrantEvaluator = async ({
      resource,
      action,
      stepId,
      grants,
    }) => {
      // Merge the step's pinned-tool floor grants (derived and recorded
      // by the env builder from the step's materialized factories) under
      // the credentials snapshot's grants. The floor supplies the
      // `tool:<name>` authority a pinned tool never got from the hub's
      // capability walk. It is ADDITIVE: `evaluateGrants` ranks by
      // specificity then effect, so a declared `deny` (priority 2) still
      // beats the derived `ask`/`allow` and an explicit denial is
      // honored -- the floor only raises the minimum authority to the
      // tool's static mark.
      //
      // A missing floor entry (`?? []`) contributes no rows: this can
      // only ever fail MORE closed (a pinned tool the hub also did not
      // grant stays denied, the pre-#68 behavior), never open a hole, so
      // it is safe as an additive default. The floor is keyed by base
      // step id, so a `map` iteration's scoped id resolves to its base
      // step's floor.
      const floor = toolMarkFloorByStep.get(baseStepId(stepId)) ?? [];
      const result = await evaluateGrants(
        // The credentialsSnapshot's grants are typed as
        // `readonly unknown[]` so the workflow-host package does not
        // depend on the sidecar's grant-rule grammar. The sidecar owns
        // that grammar; the cast surfaces here at the boundary where
        // the typed grant shape is known.
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- credentialsSnapshot.steps[*].grants is typed unknown[] at the workflow-host boundary; the sidecar owns the GrantRule grammar
        [...(grants as readonly GrantRule[]), ...floor],
        resource,
        action,
      );
      return {
        effect: result.effect,
        matchingGrants: [],
        resolvedBy: null,
      };
    };

    const runChild = createSidecarRunChild({
      substrate,
      workflowRunRepoId,
      workflowRunRef: validated.WORKFLOW_RUN_REF,
      workflowDefinitionRef: validated.WORKFLOW_DEFINITION_REF,
      principal,
      scheduler,
      invokeStep: childInvokeStep,
      evaluateGrants: evaluateGrantsAdapter,
    });

    const spawnChild = createWorkflowSpawnChild({
      substrate,
      principal,
      deployRef: validated.WORKFLOW_DEFINITION_REF,
      runChild,
    });

    // Per-run scratch reclamation for the cold (multi-step) path. The
    // run-loop fires this once each run reaches its terminal status; it
    // drops the run's whole `workflow-step-state/<repoId>/runs/<runId>/`
    // subtree (every step/attempt the run produced), which nothing
    // reopens after terminal (resume reads the substrate run log, not
    // local step state).
    //
    // Parked-step safety: reclamation keys on the RUN's terminal status,
    // and a step parked on a signal (`awaiting-signal`) keeps the run
    // non-terminal, so this never fires while a suspended step's
    // `attempt-N` store still holds a live pending-op the resume path must
    // reopen. Any future per-STEP reclamation must preserve that invariant
    // -- it MUST exclude an `awaiting-signal` step, whose `attempt-N` store
    // is the exact store a later crash-resume reopens to rehydrate the
    // gate; dropping it would reproduce the empty-store hang the
    // resume-attempt recovery closes.
    //
    // Built only for the cold path: a warm deploy
    // roots its single agent's scratch per agent under the disjoint
    // `warm/` sub-root (reclaimed on undeploy), and the run-loop's own
    // `warmKeep` gate already suppresses the per-run call there, so
    // leaving this undefined for warm deploys keeps the path-owning
    // module's intent explicit. `rm -rf` semantics via `recursive +
    // force` so a run that never wrote scratch (no buildEnv reached) is
    // a no-op rather than an ENOENT throw.
    const cleanupRunStorage: ((runId: string) => Promise<void>) | undefined =
      env.spawn.warmKeep
        ? undefined
        : (runId: string) =>
            fs.promises.rm(
              runStepStorageRoot({
                dataDir: validated.SIDECAR_DATA_DIR,
                workflowRunRepoId,
                runId,
              }),
              { recursive: true, force: true },
            );

    // Recover a parked correlation's approval snapshot for the child's
    // re-registration enumeration. Wired unconditionally (unlike
    // `cleanupRunStorage`, which is cold-only): a warm agent parks on approval
    // just as a cold one does, and the branch on `warmKeep` selects the durable
    // read -- cold reads the per-attempt isogit store, warm reconstructs the
    // agent's durable conversation state from the substrate.
    const loadParkedApproval: LoadParkedApproval = ({
      runId,
      stepId,
      attempt,
      correlationId,
    }) =>
      env.spawn.warmKeep
        ? readWarmParkedApprovalSnapshot({
            substrate,
            workflowRunRepoId,
            stepId,
            correlationId,
          })
        : readColdParkedApprovalSnapshot({
            dataDir: validated.SIDECAR_DATA_DIR,
            workflowRunRepoId,
            runId,
            stepId,
            attempt,
            correlationId,
          });

    // Enumerate a crashed step's durable pending approval operations for the
    // resume classifier, off the same cold/warm durable read as
    // `loadParkedApproval`. Where that binding is a lookup by a known
    // correlationId (answering the supervisor's re-registration), this is the
    // enumeration the classifier needs when the correlationId never reached the
    // log -- the crash-across-park case: read the pending operations, project
    // to the minimal approval records the runtime reconstructs `SignalAwaited`
    // from.
    const readParkedApprovalOps: ReadParkedApprovalOps = async ({
      runId,
      stepId,
      attempt,
    }) =>
      toParkedApprovalOps(
        env.spawn.warmKeep
          ? await readWarmParkedPendingOperations({
              substrate,
              workflowRunRepoId,
              stepId,
            })
          : await readColdParkedPendingOperations({
              dataDir: validated.SIDECAR_DATA_DIR,
              workflowRunRepoId,
              runId,
              stepId,
              attempt,
            }),
      );

    const bindings: RunWorkflowChildBindings = {
      substrate,
      workflowRunRepoId,
      workflowRunRef: validated.WORKFLOW_RUN_REF,
      principal,
      workflowDefinitionRepoId,
      workflowDefinitionRef: validated.WORKFLOW_DEFINITION_REF,
      invokeStep,
      initialSources: stepInferenceSources,
      spawnChild,
      scheduler,
      evaluateGrants: evaluateGrantsAdapter,
      loadParkedApproval,
      readParkedApprovalOps,
      ...(cleanupRunStorage !== undefined ? { cleanupRunStorage } : {}),
    };
    return bindings;
  };
}

/**
 * Production substrate factory. The sidecar's
 * `bin/workflow-child` binary calls
 * `runWorkflowChildFromProcessEnv(createSubstrate, { substrateConfigKeys: SIDECAR_SUBSTRATE_CONFIG_KEYS })`
 * and the helper invokes this factory with the parsed env. The
 * factory is the default-deps variant of
 * `createSidecarSubstrateFactory`; deployments that need a recording
 * hub sink (tests, alternate hosts) construct their own via
 * `createSidecarSubstrateFactory`.
 */
export const createSubstrate: SubstrateFactory =
  createSidecarSubstrateFactory();
