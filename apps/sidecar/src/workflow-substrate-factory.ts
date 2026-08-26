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

import { signalName } from "@intx/types";
import { InferenceSource } from "@intx/types/runtime";
import type {
  ApprovalSnapshot,
  AuditStore,
  ContextStore,
  InboundMessage,
  InferenceEvent,
  MessageTransport,
  PendingOperation,
} from "@intx/types/runtime";
import type { RuntimeCapabilities } from "@intx/types/runtime-capabilities";
import { evaluateGrants } from "@intx/authz";
import type { GrantRule } from "@intx/authz";
import {
  AdapterManifest,
  createDependencies,
  type AdapterRegistry,
} from "@intx/inference";
import { loadAdapterRegistry } from "@intx/inference/providers";
import type { AnnotatedPluginFactory, DirectorRegistry } from "@intx/agent";
import { createDefaultDirectorRegistry } from "@intx/agent";
import {
  builtinCredentialProviders,
  createCredentialProviderRegistry,
  createHarnessRuntimeCapabilities,
  driveConnectorReplies,
  type AgentEventStream,
  type CredentialProviderRegistry,
} from "@intx/harness";
import { createSSHSignature } from "@intx/crypto";
import {
  createAgentRepoStore,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  type Principal,
  type RepoId,
  type RepoStore,
  type WorkflowRunSupervisorPrincipal,
  type WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions/substrate";
import { createIsogitStore } from "@intx/storage-isogit/node";
import {
  adaptHostScheduler,
  createCredentialsBackedAuthorize,
  createProxyWorkflowRunRepoStore,
  createSupervisorBackedTransport,
  createWorkflowHostScheduler,
  createWorkflowRunBlobSubstrate,
  createWorkflowRunRepoStore,
  createWorkflowHostSignalChannel,
  createInMemorySpawnChild,
  createWorkflowStepInvoker,
  hashGrants,
  loadWorkflowPluginFactoriesFromClosure,
  type ChildOutboundMailBridge,
  type CredentialsSnapshot,
  type CredentialsSnapshotRef,
  type GrantEvaluator,
  type LoadParkedApproval,
  type RunChildWorkflow,
  type RunSuspendableChild,
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
  rewriteInlineChildWorkflowBodies,
  runtimeRun,
  type ParkedApprovalOp,
  type ReadParkedApprovalOps,
  type Scheduler,
  type StepInvokeRequest,
  type StepInvokeResult,
  type SuspendableChildPark,
  type WorkflowAuthorizeFn,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

import {
  attachStepCredentialWiring,
  attachStepTools,
  createToolBearingAgentFactory,
  deriveToolMarkFloorGrants,
  materializeStepTools,
  type StepToolCacheConfig,
  type StepToolMaterialization,
} from "./step-agent-tools";
import type { CredentialMaterialCell } from "./step-credential-capabilities";
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
   * half of mailbox ownership, §3a). For the single-agent deploy this is
   * the run address the host registered the agent's `CryptoProvider`
   * against.
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
  /**
   * Feed the step agent's OWN evaluated tool factories into the
   * materialization slot, instead of reading a pinned tool-package manifest
   * off the deploy tree. Set for the source-ref lineage, whose child holds
   * the live `WorkflowDefinition` (re-verified against the approved wire hash
   * at run start) and threads each step's live `agent.toolFactories` in as
   * `req.agent.toolFactories`. Those are the same `AnnotatedToolFactory`
   * callables `materializeStepTools` would produce, bare-named (no
   * namespacing loader runs), so the existing tool-bearing agent factory
   * consumes them unchanged. Mutually exclusive with `toolless`.
   *
   * The source-ref lineage stages no `tool-packages-manifest.json`, so
   * `materializeStepTools` would find nothing and return empty tools; this
   * arm is what actually runs a source workflow's tools. No tool-mark floor
   * is recorded on this lineage: a source tool's runtime name is the bare
   * `definition.name` the probe's capability walk already emitted a
   * `tool:<name>` grant for into the frozen credentials snapshot, so the
   * snapshot authorizes it directly (unlike a pinned tool, whose namespaced
   * name the walk never saw and which the floor exists to compensate for).
   */
  sourceTools: boolean;
  /**
   * Sidecar-local directory of the materialized workflow-definition closure
   * (`env.spawn.closurePackageDir`), present only on the source-ref lineage.
   * The source arm materializes each step agent's declared plugin packages
   * (`req.agent.plugins`) from this already-laid-out closure -- resolving them
   * through the workflow package's `node_modules/` graph, no re-download -- and
   * feeds the resulting plugin factories into the per-step plugin chain. A
   * plugin package contributes no agent-visible tool factory, so this closure
   * load is the only channel that reaches an `env.plugins` a source workflow's
   * posix bundle consumes. Undefined on a toolless deploy, which never
   * materializes plugins from a closure.
   */
  closurePackageDir?: string;
  /**
   * Build a TOOLLESS env: skip tool materialization entirely and attach an
   * empty tool runtime. Set for an onTrigger body step. A body agent is
   * guaranteed toolless by the deploy-time guard (a tool-bearing body agent is
   * rejected at deploy, INTR-310), and -- critically -- the body child runs
   * under the PARENT deployment's `mailboxAddress`/`stepCount`, so resolving a
   * body step's deploy tree through `stepDeployTreeDir` would read the PARENT
   * step's tools for a body stepId that happens to collide with a parent step
   * id. Skipping materialization makes the toolless-body invariant structural
   * rather than incidental on non-collision. `recordToolMarkFloor` is not
   * called in this mode (there is no floor to record).
   */
  toolless: boolean;
}

/**
 * Materialize the plugin factories a source-ref step agent declares
 * (`req.agent.plugins`) from the workflow's frozen closure. Returns an
 * empty list when the agent declares no plugins. Fails closed when plugins
 * are declared but the source-ref closure dir is absent -- that pairing is a
 * wiring bug (the source arm always carries `closurePackageDir`), and
 * silently dropping the plugins would run the workflow without the tools it
 * declared and leave the reactor to fail closed on the un-loaded tool.
 *
 * The closure is already laid out on disk from the deploy-side apply, so
 * this is a load-only pass -- no re-download -- resolving each plugin
 * package through the workflow package's `node_modules/` graph and
 * collecting its `definePlugin` factories.
 */
async function materializeSourcePluginFactories(
  deps: SidecarStepBuildEnvDeps,
  req: StepInvokeRequest,
): Promise<readonly AnnotatedPluginFactory[]> {
  const plugins = req.agent.plugins ?? [];
  if (plugins.length === 0) {
    return [];
  }
  if (deps.closurePackageDir === undefined) {
    throw new Error(
      `sidecar workflow-child step invoker: step agent ${JSON.stringify(req.agent.id)} declares plugins ${JSON.stringify(plugins)} but the source-ref closure package dir is absent; a source workflow's plugin factories can only be materialized from its frozen closure`,
    );
  }
  return loadWorkflowPluginFactoriesFromClosure({
    packageDir: deps.closurePackageDir,
    plugins,
  });
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
/**
 * The per-run credential inputs a tool-bearing build resolves the step's
 * `credentials` wiring from. The live material cell and the grants resolver
 * ride in from the run child (per-run state); the provider registry is
 * sidecar-static and combined in at the invoke-step boundary. Absent for a
 * toolless build (an onTrigger body), which assembles no credentials.
 */
interface SidecarStepCredentialContext {
  readonly materialCell: CredentialMaterialCell;
  /**
   * The step's grants, resolved live by base step id. Typed `unknown[]` at
   * this boundary (the run child owns no grant grammar); the sidecar casts to
   * `GrantRule[]` here where the grammar is known, exactly as
   * `evaluateGrantsAdapter` does.
   */
  readonly resolveStepGrants: (stepId: string) => readonly unknown[];
  readonly providers: CredentialProviderRegistry;
}

export function createSidecarStepBuildEnv(
  deps: SidecarStepBuildEnvDeps,
): (
  req: StepInvokeRequest,
  sourcesRef: SourcesSnapshotRef,
  credentialContext?: SidecarStepCredentialContext,
) => Promise<StepEnvBase> {
  return async (
    req: StepInvokeRequest,
    sourcesRef: SourcesSnapshotRef,
    credentialContext?: SidecarStepCredentialContext,
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
    // the resume-attempt invariant documented on `stepStorageRoot`). An
    // APPROVAL resume re-invocation delivers the correlated decision to the
    // reactor, which rehydrates its gate from THIS store's pending
    // operations. A re-dispatchable ask-rail approval gate carries a
    // `suspendedCall` (the call the approval re-runs); an async-tool pending
    // marker shares `kind: "approval"` but has no `suspendedCall`, and on
    // resume the reactor clears its gate WITHOUT re-running the approved
    // call. So the resume must find a `suspendedCall`-bearing op for its
    // correlationId, mirroring the reactor's own discriminator. Two failure
    // modes make that false: the runtime reopened the wrong `attempt`'s
    // store (`stepStorageRoot` above), so no pending op matches at all and
    // the reactor comes up gateless -- a silent forever-hang; or the only
    // match is an async marker, so the gate clears and the approved call is
    // silently skipped. Make either loud here, at the single seam that both
    // opened the store AND knows an approval resume must find its gate,
    // rather than letting it surface as a hang or a skipped call. The
    // warm path keys its durable store per agent (not per attempt) and
    // rehydrates from a different lifecycle, so
    // this assertion is cold-path only. An `"input"` resume is exempt by
    // construction: its correlationId names the runtime-minted re-arm channel
    // awaiting the step's next trigger, not a reactor gate, so no pending
    // operation ever exists for it and the delivered decision is sent as a
    // plain next turn (no correlation to rehydrate).
    if (
      deps.durableConversation === undefined &&
      req.resume !== undefined &&
      req.resume.kind === "approval"
    ) {
      const resumeCorrelationId = req.resume.correlationId;
      const loaded = await storage.load();
      const hasPendingGate = loaded.pendingOperations.some(
        (op) =>
          op.correlationId === resumeCorrelationId &&
          op.suspendedCall !== undefined,
      );
      if (!hasPendingGate) {
        throw new Error(
          `sidecar workflow-child step invoker buildEnv: resume of step ${JSON.stringify(stepId)} (run ${JSON.stringify(runId)}, attempt ${String(attempt)}) reopened a ContextStore with no re-dispatchable approval gate for correlationId ${JSON.stringify(resumeCorrelationId)}. The cold-path store is keyed by attempt (${storeDir}); a resume that finds no suspendedCall-bearing pending operation here means it reopened the wrong attempt's store (the reactor would come up gateless and the decision would correlate against nothing) or matched only an async pending marker (the reactor would clear the gate without re-running the approved call). This is a keying violation, not a recoverable state.`,
        );
      }
    }

    const workdir = path.join(storeDir, "workspace");
    await fs.promises.mkdir(workdir, { recursive: true });

    // Assemble the step's tool runtime. Three arms:
    //
    //   - Toolless body step: empty tools, no manifest read. A body stepId
    //     that collides with a parent step id can never read the parent's
    //     deploy tree; the body agent is guaranteed toolless by the deploy
    //     guard, so there is nothing to materialize and no floor to record.
    //
    //   - Source-ref lineage (`sourceTools`): run the step agent's OWN
    //     evaluated tool factories. The child holds the live re-verified
    //     `WorkflowDefinition`, and the runtime threads each step's live agent
    //     in as `req.agent`, so `req.agent.toolFactories` are the real callable
    //     `AnnotatedToolFactory`s -- the same shape `materializeStepTools`
    //     produces (`LoadedToolFactory === AnnotatedToolFactory<BaseEnv>`),
    //     bare-named because no namespacing loader runs. The source deploy
    //     stages no `tool-packages-manifest.json`, so `materializeStepTools`
    //     would find nothing here; feeding the evaluated factories straight
    //     into the slot is what runs a source workflow's tools. `packageName`
    //     is the factory's bundle `id` (the credential Gate-2 key; a mail/plain
    //     tool declares no credentials, so it stays inert). Plugin factories --
    //     which have no agent slot, so Option A cannot carry them -- are
    //     materialized separately from the frozen closure the child already
    //     holds (`materializeSourcePluginFactories`) and fed into the SAME
    //     `pluginFactories` slot `materializeStepTools` fills for pinned
    //     packages, so the existing per-step plugin chain wires them onto
    //     `env.plugins`.
    //
    //   - Pinned tool packages (`materializeStepTools`): materialize the pinned
    //     tool-package closure (posix, LSP, mail, ...) from its on-disk deploy
    //     tree, rooted per step under `storeDir` so concurrent steps in one
    //     child never collide on the tarball cache or the apply-state tree. A
    //     step with no manifest yields empty tools (the legitimate
    //     `rawManifestBytes === undefined` case); a present-but-broken manifest
    //     surfaces loudly through `materializeStepTools` rather than degrading to
    //     empty tools.
    const materialization: StepToolMaterialization =
      deps.toolless === true
        ? { factories: [], pluginFactories: [] }
        : deps.sourceTools === true
          ? {
              factories: req.agent.toolFactories.map((factory) => ({
                packageName: factory.id,
                declaredCredentials: [],
                factory,
              })),
              pluginFactories: await materializeSourcePluginFactories(
                deps,
                req,
              ),
            }
          : await materializeStepTools({
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
    //
    // Skipped in the toolless mode: there are no factories, so recording an
    // empty floor would only risk clobbering a colliding parent step id's
    // real floor in a shared map. Skipped on the source-ref lineage too: a
    // source tool's runtime name is the bare `definition.name` the capability
    // walk already emitted a `tool:<name>` grant for, so the credentials
    // snapshot authorizes it directly with the correct effect -- no floor is
    // needed, and recording one would blur that invariant.
    if (deps.toolless !== true && deps.sourceTools !== true) {
      deps.recordToolMarkFloor(
        baseStepId(stepId),
        deriveToolMarkFloorGrants(
          materialization.factories.map((f) => f.factory),
        ),
      );
    }

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

    // The host owns capability assembly: it builds the RuntimeCapabilities
    // bag here (currently `mail.transport`) and puts it on `env.capabilities`,
    // so a bundle consumes the assembled bag rather than re-wrapping a raw
    // env key of its own. `@intx/tools-mail`'s sidecar bundle (`requires:
    // ["capabilities", "address"]`) resolves `mail.transport` from it.
    const capabilities = createHarnessRuntimeCapabilities({ transport });

    // The step env carries `transport`, `address`, and the assembled
    // `capabilities` beyond `BaseEnv`. `capabilities` is what the mail bundle
    // reads; `transport` remains a raw env surface for tool packages that
    // read it directly. `address` is observability-only. These widen the
    // returned `StepEnvBase` structurally, which the buildEnv return type
    // (`StepEnvBase`) accepts (a wider object is assignable to the narrower
    // type).
    const env: StepEnvBase & {
      transport: MessageTransport;
      address: string;
      capabilities: RuntimeCapabilities;
    } = {
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
      capabilities,
    };
    // Carry the materialized tool runtime to the tool-bearing
    // `agentFactory` via the env's symbol-keyed slot. The step-invoker
    // adapter spreads this env (`{ ...envBase, authorize }`) before
    // handing it to `agentFactory`; object spread preserves own
    // symbol-keyed properties, so the slot survives the spread.
    attachStepTools(env, materialization);
    // Attach the credential wiring for a tool-bearing build so the
    // `agentFactory` can assemble each bundle's consumer-scoped `credentials`
    // capability. Grants are wired as a THUNK, resolved (and cast to the
    // sidecar's `GrantRule` grammar, the same cast `evaluateGrantsAdapter`
    // makes) only when a package actually needs a capability -- a step with no
    // credential-consuming package never reads them, so a self-discovery
    // resume that precedes the grants barrier does not fault on a missing
    // snapshot. Omitted for a toolless build, which carries no
    // `credentialContext` and assembles no credentials.
    if (credentialContext !== undefined) {
      attachStepCredentialWiring(env, {
        materialCell: credentialContext.materialCell,
        resolveGrants: () =>
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- resolveStepGrants returns unknown[] at the run-child boundary; the sidecar owns the GrantRule grammar
          credentialContext.resolveStepGrants(stepId) as readonly GrantRule[],
        providers: credentialContext.providers,
      });
    }
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
 * Real per-step invoker for an onTrigger BODY child, distinct from the
 * `SidecarChildStepInvoker` stub the shared `childRunDeps` carries for
 * childWorkflow spawns (which stay `ChildStepNotImplementedError` until
 * childWorkflow agent execution is built). The body invoker runs a real
 * agent through `createWorkflowStepInvoker` (INTR-310), so it widens the stub
 * with the body's own per-step inference `sourcesRef` -- built fresh per body
 * spawn from the body's on-disk `sources.json`, disjoint from the top-level's
 * mutable source table so a top-level source rotation never leaks into a body.
 * It also carries an `onEvent` funnel that attributes the body child's live
 * inference events to the body run id on the hub timeline, giving a body the
 * same per-run observability the top level has.
 */
export type SidecarBodyStepInvoker = (
  req: StepInvokeRequest,
  authorize: WorkflowAuthorizeFn,
  sourcesRef: SourcesSnapshotRef,
  onEvent: (event: InferenceEvent) => void,
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
 * principal scoped to the parent's anchorRunId) is reused verbatim
 * because the child runs under the same supervisor authority.
 */
interface SidecarRunChildDeps {
  /** Wrapped workflow-run substrate (the factory's `substrate`). */
  substrate: RepoStore;
  /** Workflow-run repo identifying the parent's deployment. */
  workflowRunRepoId: RepoId;
  /** Workflow-run ref the child reads/writes against. */
  workflowRunRef: string;
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
   * Real per-step invoker used ONLY for an onTrigger body child's own steps
   * (INTR-310). `createSidecarSpawnSuspendableChild` wires it onto the body
   * env; the shared `invokeStep` stub above stays the seam for childWorkflow
   * spawns and for the body's own childWorkflow grandchildren. Absent leaves
   * the body on the stub (the pre-INTR-310 behavior a test may exercise).
   */
  bodyInvokeStep?: SidecarBodyStepInvoker;
  /**
   * Sidecar data dir, used ONLY on the body path to read a body's on-disk
   * `assets/workflow/<bodyRef>/sources.json` and build the body's per-step
   * inference `sourcesRef`. Required whenever `bodyInvokeStep` is wired.
   */
  dataDir?: string;
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
  // The in-process child runs under real supervisor authority; its
  // control-plane cancel (`CancelRequested`) must be signed by a supervisor
  // principal, which the kind handler requires and the substrate authorizes for
  // this deployment. Run-body events keep their workflow-process attribution.
  const supervisorPrincipal: WorkflowRunSupervisorPrincipal = {
    kind: "supervisor",
    anchorRunId: deps.workflowRunRepoId.id,
  };
  // Created once and shared across every child this factory spawns (the
  // runtime scopes reads/subscribes by runId), so sibling and grandchild
  // spawns route through one repo-store handle rather than a fresh one each.
  const repoStore = createWorkflowRunRepoStore({
    substrate: deps.substrate,
    repoId: deps.workflowRunRepoId,
    principal: deps.principal,
    controlPlanePrincipal: supervisorPrincipal,
    ref: deps.workflowRunRef,
  });
  // Self-referential `RunChildWorkflow` so a child env's recursive
  // `spawnChild` (built via `createInMemorySpawnChild` in `buildChildRunEnv`)
  // can route grandchild spawns back through the same adapter. Each invocation
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
    const {
      env,
      signalChannel,
      definition: rewrittenDefinition,
    } = await buildChildRunEnv({
      deps,
      directors,
      clock,
      newId,
      repoStore,
      runChild,
      definition,
      childRunId,
      parentRunId,
    });
    try {
      const handle = runtimeRun(rewrittenDefinition, env, {
        runId: childRunId,
        triggerPayload: input,
      });
      // The resulting `CancelRequested` is written under the supervisor
      // principal wired into this run's repo store (see `controlPlanePrincipal`
      // above): the kind handler requires a supervisor signer for any cancel
      // origin, so a workflow-process-signed cancel would be refused and a
      // parent abort would surface as a failed rather than cancelled child.
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

/**
 * Construct the `RunSuspendableChild` callback the suspendable-spawn
 * adapter delegates to. The park-aware analog of
 * {@link createSidecarRunChild}: instead of awaiting the child's terminal,
 * it returns a live {@link SuspendableChildHandle} the caller (`runOnTrigger`)
 * drives across the body's approval parks.
 *
 * Park surfacing: the built env's `onPark` sink translates the child body's
 * control-plane parks into the handle's `next()` stream. An `"approval"`
 * park is queued for the caller to proxy up on the same correlation; the
 * caller's granted decision returns through `resume`, which delivers it on
 * the child's own signal channel so the parked step unblocks. A body that
 * parks on a control-plane `"input"` channel is a nested onTrigger re-arm
 * the suspendable seam does not service -- the caller proxies approvals
 * only, so nothing would ever deliver that input and the child would park
 * forever. Rather than drop the park and hang, `onPark` surfaces it as a
 * hard error on `next()` and cancels the child so the section run ends
 * loudly.
 *
 * Signal-channel lifecycle: unlike `createSidecarRunChild`, which stops the
 * channel in a `finally` around a single awaited terminal, this keeps the
 * channel alive across every park (so `resume` can deliver) and ties its
 * teardown to the run's terminal -- the one lifecycle moment every path
 * funnels through (normal completion, cancel-on-abort, an illegal-input-park
 * cancel, or a runtime failure). Tearing down per-`next()` would leak the
 * channel when a parent abort makes `runOnTrigger` stop calling `next()`
 * mid-park.
 *
 * Abort propagation: the parent-supplied `signal` is threaded via
 * `handle.cancel` exactly as `createSidecarRunChild` does; the child runtime
 * takes no abort signal of its own. On abort the cancel cascade fires, the
 * run settles `cancelled`, and the terminal-tied teardown runs.
 */
export function createSidecarSpawnSuspendableChild(
  deps: SidecarRunChildDeps,
): RunSuspendableChild {
  const directors = deps.directors ?? createDefaultDirectorRegistry();
  const clock = deps.clock ?? defaultClock;
  const newId = deps.newId ?? defaultNewId;
  // The in-process body child runs under real supervisor authority; its
  // control-plane cancel (`CancelRequested`) must be signed by a supervisor
  // principal, which the kind handler requires and the substrate authorizes for
  // this deployment. Run-body events keep their workflow-process attribution.
  const supervisorPrincipal: WorkflowRunSupervisorPrincipal = {
    kind: "supervisor",
    anchorRunId: deps.workflowRunRepoId.id,
  };
  const repoStore = createWorkflowRunRepoStore({
    substrate: deps.substrate,
    repoId: deps.workflowRunRepoId,
    principal: deps.principal,
    controlPlanePrincipal: supervisorPrincipal,
    ref: deps.workflowRunRef,
  });
  // A body's own `childWorkflow` grandchildren spawn terminal-only: the
  // suspendable seam is exercised only by onTrigger sections, and
  // `buildChildRunEnv` wires the body env's `spawnChild` (not
  // `spawnSuspendableChild`), so a nested onTrigger inside a body fails loud
  // rather than silently spawning.
  const runChild = createSidecarRunChild(deps);

  return async (
    { definition, childRunId, input, parentRunId, signal, resumeFromEvents },
    onEvent,
  ) => {
    const {
      env: baseEnv,
      signalChannel,
      definition: rewrittenDefinition,
    } = await buildChildRunEnv({
      deps,
      directors,
      clock,
      newId,
      repoStore,
      runChild,
      definition,
      childRunId,
      parentRunId,
      // The BODY env runs real agent steps (INTR-310) when the factory wired a
      // body invoker; the body's own childWorkflow grandchildren, built via the
      // internal `createSidecarRunChild(deps)` above, do NOT get it and stay on
      // the stub. The live event sink is paired with the body invoker: it feeds
      // the body's inference to the parent run's event channel, and is omitted
      // for the stub path (which runs no agent).
      ...(deps.bodyInvokeStep !== undefined
        ? { bodyStepInvoker: deps.bodyInvokeStep, onEvent }
        : {}),
    });

    // FIFO the caller drains via `next()`: each entry is either an approval
    // park to proxy up or a fatal illegal-park error. A single waiter slot
    // suffices because `next()` has exactly one consumer (`runOnTrigger`)
    // driving it sequentially, mirroring the signal channel's single-consumer
    // shape.
    type BodyEvent =
      | { kind: "park"; park: SuspendableChildPark }
      | { kind: "signal-park"; name: string }
      | { kind: "error"; error: Error };
    const events: BodyEvent[] = [];
    let wake: (() => void) | null = null;
    const notify = (): void => {
      if (wake !== null) {
        const resolve = wake;
        wake = null;
        resolve();
      }
    };

    const env: WorkflowRuntimeEnv = {
      ...baseEnv,
      onPark: (park) => {
        if (park.parkKind === "approval") {
          events.push({
            kind: "park",
            park: {
              correlationId: park.correlationId,
              ...(park.approvalSnapshot !== undefined
                ? { approvalSnapshot: park.approvalSnapshot }
                : {}),
            },
          });
        } else {
          events.push({
            kind: "error",
            error: new Error(
              `onTrigger body ${childRunId} parked on a control-plane input ` +
                `channel (${park.correlationId}); a suspendable body may not ` +
                `re-arm an input park -- the section proxies approvals only, ` +
                `so this park has no resolver`,
            ),
          });
        }
        notify();
      },
      // A body `awaitSignal` gate on an author name: surface it so the section
      // proxies it up as a signal-relay await and relays the resolved signal
      // back via `deliverSignal`. Without this the body would park on the
      // signal channel with nothing upstream to route a delivery to it.
      onSignalPark: (park) => {
        events.push({ kind: "signal-park", name: park.name });
        notify();
      },
    };

    // On resume, drive the run from its durable log; the body step re-parks
    // silently (a re-park does not re-fire onPark), and the caller relays the
    // grant via resume on the correlation it recovered from its own log. On a
    // fresh spawn, seed the run with the event's trigger payload.
    const handle = runtimeRun(
      rewrittenDefinition,
      env,
      resumeFromEvents !== undefined
        ? { runId: childRunId, resumeFromEvents }
        : { runId: childRunId, triggerPayload: input },
    );

    // The resulting `CancelRequested` is written under the supervisor principal
    // wired into this run's repo store (see `controlPlanePrincipal` above): the
    // kind handler requires a supervisor signer for any cancel origin, so a
    // workflow-process-signed cancel would be refused and a parent abort would
    // surface as a failed rather than cancelled child.
    const cancelOnAbort = (): void => {
      void handle.cancel("supervisor-operator", "parent cancelled");
    };
    if (signal.aborted) {
      cancelOnAbort();
    } else {
      signal.addEventListener("abort", cancelOnAbort, { once: true });
    }

    let settled: {
      terminalStatus: "completed" | "failed" | "cancelled";
    } | null = null;
    let failure: Error | null = null;
    void handle.complete
      .then((result) => {
        settled = { terminalStatus: result.terminalStatus };
      })
      .catch((cause) => {
        failure = cause instanceof Error ? cause : new Error(String(cause));
      })
      .finally(() => {
        signal.removeEventListener("abort", cancelOnAbort);
        void signalChannel.stop();
        notify();
      });

    return {
      next: async () => {
        for (;;) {
          const event = events.shift();
          if (event !== undefined) {
            if (event.kind === "error") {
              // The body re-armed an input park nothing will resolve. Cancel
              // the child so its terminal (and the channel teardown tied to
              // it) fires, then surface the error: the throw lands the section
              // run's terminal via `runOnTrigger`'s `runPrimitiveSafe`.
              void handle.cancel(
                "supervisor-operator",
                "onTrigger body re-armed an unsupported input park",
              );
              throw event.error;
            }
            if (event.kind === "signal-park") {
              return { kind: "signal-park", name: event.name };
            }
            return { kind: "park", park: event.park };
          }
          if (failure !== null) throw failure;
          if (settled !== null) {
            return {
              kind: "terminal",
              terminalStatus: settled.terminalStatus,
            };
          }
          await new Promise<void>((resolve) => {
            wake = resolve;
          });
        }
      },
      resume: async (correlationId, decision) => {
        await signalChannel.deliver(signalName(correlationId), decision);
      },
      deliverSignal: async (name, payload, signalId) => {
        await signalChannel.deliver(name, payload, signalId);
      },
    };
  };
}

/**
 * Build the per-childRunId `WorkflowRuntimeEnv` a spawned child runs
 * against: inherit the parent's grants, assemble the child's credentials
 * snapshot, and wire the per-run repo store / blob substrate / signal
 * channel plus a recursive `spawnChild`. Returned alongside the child's
 * signal channel so the caller can `stop()` it once the child settles.
 * Shared by the child-drive callers so the env construction lives in one
 * place.
 */
async function buildChildRunEnv(args: {
  deps: SidecarRunChildDeps;
  directors: ReturnType<typeof createDefaultDirectorRegistry>;
  clock: () => Date;
  newId: (prefix: string) => string;
  repoStore: ReturnType<typeof createWorkflowRunRepoStore>;
  runChild: RunChildWorkflow;
  definition: WorkflowDefinition;
  childRunId: string;
  parentRunId: string;
  /**
   * Real body-step invoker (INTR-310). Present ONLY when this env hosts an
   * onTrigger body: `createSidecarSpawnSuspendableChild` passes it so the
   * body's agent steps run for real, resolving inference against the body's
   * own `sources.json`. Absent for a childWorkflow env (and a body's own
   * childWorkflow grandchildren), which stay on `deps.invokeStep` (the
   * `ChildStepNotImplementedError` stub).
   */
  bodyStepInvoker?: SidecarBodyStepInvoker;
  /**
   * Per-run live inference-event sink, threaded from the parent run's event
   * channel. Required WHENEVER `bodyStepInvoker` is present (a real body agent
   * emits inference the hub stream must see); a missing sink there is a wiring
   * defect, not a silent drop. Absent for the childWorkflow stub path, which
   * runs no agent.
   */
  onEvent?: (event: InferenceEvent) => void;
}): Promise<{
  env: WorkflowRuntimeEnv;
  signalChannel: ReturnType<typeof createWorkflowHostSignalChannel>;
  definition: WorkflowDefinition;
}> {
  const {
    deps,
    directors,
    clock,
    newId,
    repoStore,
    runChild,
    definition,
    childRunId,
    parentRunId,
    bodyStepInvoker,
    onEvent,
  } = args;
  // A rung may itself embed a child (grandchild recursion) as an inline
  // `childWorkflow`. Lift each to an internal `{ ref }` and run the rewritten
  // definition whose children are refs -- the shape the runtime dispatches --
  // and keep the lifted definitions in an in-memory map the rung's own
  // terminal resolver serves from, so a grandchild spawns with no on-disk read
  // at any depth. Returned to the caller so `runtimeRun` drives the rewritten
  // form.
  const { workflow: rewrittenDefinition, bodies: grandchildBodies } =
    rewriteInlineChildWorkflowBodies(definition);
  const grandchildMap = new Map(
    grandchildBodies.map((b) => [b.ref, b.definition]),
  );
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
    anchorRunId: deps.workflowRunRepoId.id,
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
    steps: rewrittenDefinition.stepOrder.map((stepId) => ({
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
  const drain = createNoopDrainController(rewrittenDefinition);
  // Recursive `spawnChild`: a grandchild embedded inline in this rung is
  // resolved from the in-memory map lifted above and flows back into this same
  // `runChild` callback. The runtime body's `runChildWorkflow` contract is
  // depth-agnostic; the in-memory resolver makes the sidecar's adapter
  // depth-agnostic too, with no on-disk read at any rung.
  const spawnChild = createInMemorySpawnChild({
    bodies: grandchildMap,
    runChild,
  });
  // Per-step invocation seam. The runtime body invokes `env.invokeStep` with
  // the request alone; the wrapper forwards the child's credentials-backed
  // authorize so the invoker gates each tool call against the inherited grants.
  //
  // Two shapes. The childWorkflow path keeps `deps.invokeStep` -- the
  // `ChildStepNotImplementedError` stub, which fails a childWorkflow agent step
  // loud (that feature is unbuilt). The onTrigger BODY path (INTR-310) runs a
  // real agent: `bodyStepInvoker` resolves inference against the body's OWN
  // per-step source pins, read fresh per spawn from the body's on-disk
  // `sources.json` (staged beside the body definition at deploy) into a
  // `sourcesRef` disjoint from the top-level's mutable table, so a top-level
  // source rotation never leaks into a body. The file is guaranteed present for
  // a body (deploy materializes it), so a missing/broken read fails loud rather
  // than silently degrading inference.
  let invokeStep: WorkflowRuntimeEnv["invokeStep"] = (req) =>
    deps.invokeStep(req, authorize);
  if (bodyStepInvoker !== undefined) {
    if (deps.dataDir === undefined) {
      throw new Error(
        "sidecar body child: bodyStepInvoker is wired but deps.dataDir is missing; the body's sources.json cannot be resolved",
      );
    }
    if (onEvent === undefined) {
      throw new Error(
        "sidecar body child: bodyStepInvoker is wired but onEvent is missing; body inference events would be silently dropped from the hub stream",
      );
    }
    const bodySourcesRef: SourcesSnapshotRef = {
      current: await readBodyStepInferenceSources(
        deps.dataDir,
        rewrittenDefinition.id,
      ),
    };
    const bodyOnEvent = onEvent;
    invokeStep = (req) =>
      bodyStepInvoker(req, authorize, bodySourcesRef, bodyOnEvent);
  }
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: deps.scheduler,
    signalChannel,
    blobs,
    directors,
    authorize,
    invokeStep,
    spawnChild,
    clock,
    newId,
    drain,
  };
  return { env, signalChannel, definition: rewrittenDefinition };
}

/**
 * Read an onTrigger body's per-step inference-source pins from
 * `${dataDir}/assets/workflow/<bodyRef>/sources.json`, staged beside the body
 * definition at deploy time. Parsed and validated through the same
 * `parseStepInferenceSources` boundary the top-level `STEP_INFERENCE_SOURCES`
 * env entry uses. A body's sources file is guaranteed present (the deploy
 * router materializes it for every referenced body), so a missing or malformed
 * file is a defect and surfaces loudly rather than degrading to empty pins.
 */
async function readBodyStepInferenceSources(
  dataDir: string,
  bodyRef: string,
): Promise<StepInferenceSourceTable> {
  const sourcesPath = path.join(
    dataDir,
    "assets",
    "workflow",
    bodyRef,
    "sources.json",
  );
  let raw: string;
  try {
    raw = await fs.promises.readFile(sourcesPath, "utf8");
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    throw new Error(
      `sidecar body child: failed to read body inference sources at ${sourcesPath}: ${reason}`,
      { cause },
    );
  }
  return parseStepInferenceSources(raw);
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
    const principal: WorkflowRunWorkflowProcessPrincipal = {
      kind: "workflow-process",
      anchorRunId: env.spawn.anchorRunId,
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
      toolless: false,
      // Source-ref is the only deploy lineage: the child runs each step agent's
      // own evaluated tool factories (fed from `req.agent.toolFactories`) from
      // the materialized closure, never a pinned tool-package manifest off a
      // deploy tree.
      sourceTools: true,
      // The materialized closure dir the source arm materializes each step
      // agent's declared plugin packages from. Always present (source-ref only).
      closurePackageDir: env.spawn.closurePackageDir,
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

    // The credential provider registry that shapes a delivered credential into
    // a mediated handle. Built once here from the sidecar-static built-ins (the
    // origin-pinned http provider) and shared by every per-step build; the
    // per-run material and grants ride in separately at each invoke.
    const credentialProviders = createCredentialProviderRegistry(
      builtinCredentialProviders(),
    );

    // childWorkflow step invoker (the STUB). A `childWorkflow` / `map` fan-out
    // spawns a separate WorkflowDefinition whose stepIds are disjoint from the
    // parent's, and deploy stages neither its per-step assets nor its tool
    // trees. Running a real per-step agent for a childWorkflow step is not
    // built; that is a distinct feature from onTrigger body execution below.
    //
    // This is a deliberate hard stop, not a fabricated result. A fake success
    // output (the shape this once returned) reported a child run `completed`
    // whose agent never ran -- a silent correctness trap. Failing loudly
    // surfaces the child step as `StepFailed` with a structured, INTR-310-named
    // error instead. The `spawnChild` / `runChild` recursion and the
    // sub-namespace scoping around it are real and exercised right up to this
    // seam. Wired as `childRunDeps.invokeStep`, so it also covers a body's own
    // childWorkflow grandchildren.
    const childInvokeStep: SidecarChildStepInvoker = (req) =>
      Promise.reject(
        new ChildStepNotImplementedError(req.agent.id, req.authzContext.stepId),
      );

    // onTrigger BODY step invoker (INTR-310). Unlike a childWorkflow child, an
    // onTrigger section body IS staged: its definition and per-step inference
    // sources land on disk beside each other at deploy, and its agents are
    // guaranteed toolless (a tool-bearing body agent is rejected at deploy). So
    // a body agent step runs for real through the same `createWorkflowStepInvoker`
    // the top level uses -- built COLD per invocation (no warm registry: a body
    // is a fresh run per section event, so no durableConversation, warmCache, or
    // run-boundary mirror) and TOOLLESS (the build-env skips tool
    // materialization, so a body stepId colliding with a parent step id can
    // never read the parent's tools). The per-body `sourcesRef` is threaded in
    // by `buildChildRunEnv`, disjoint from the top level's. `onEvent` is the
    // per-run event funnel `buildChildRunEnv` threads in from the parent run's
    // event channel, so a body agent's live inference events reach the hub
    // stream at the deployment-level granularity the top level already has
    // (per-run attribution stays durable via runs/<childRunId>/events/).
    const coldBodyBuildStepEnv = createSidecarStepBuildEnv({
      dataDir: validated.SIDECAR_DATA_DIR,
      workflowRunRepoId,
      signer: conversationSigner,
      mailboxAddress: env.spawn.mailboxAddress,
      stepCount: env.spawn.stepCount,
      outboundMailBridge: env.outboundMailBridge,
      cache: stepToolCache,
      adapters: childAdapterRegistry,
      // The toolless build-env never records a floor (it skips tool
      // materialization). Assert that invariant rather than silently no-op: a
      // call here would mean the toolless gate regressed.
      recordToolMarkFloor: () => {
        throw new Error(
          "toolless body build-env must not record a tool-mark floor",
        );
      },
      toolless: true,
      // A body agent is guaranteed toolless (the deploy guard rejects a
      // tool-bearing body), so the source-ref tool arm never applies here even
      // on a source-ref parent; the toolless arm wins regardless.
      sourceTools: false,
    });
    const bodyInvokeStep: SidecarBodyStepInvoker = (
      req,
      authorize,
      sourcesRef,
      onEvent,
    ) =>
      createWorkflowStepInvoker({
        workflowAuthorize: authorize,
        buildEnv: (buildReq) => coldBodyBuildStepEnv(buildReq, sourcesRef),
        agentFactory: stepAgentFactory,
        sourcesRef,
        onEvent,
      })(req);

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

    // Connector-thread seed (design §3c). When the deployment is warm-kept,
    // route each mail-derived inbound message onto the warm agent's
    // connector thread before its send, so the reply path has thread state.
    // The key is the step identity, the same key the durable store is filed
    // under. Absent for a multi-step deploy (no durable registry).
    const seedInbound:
      | ((key: string, message: InboundMessage) => Promise<void>)
      | undefined =
      durableConversation !== undefined
        ? async (key: string, message: InboundMessage) => {
            await durableConversation.get(key).seedInbound(message);
          }
        : undefined;

    // Connector reply drain (design §3c). When the deployment is
    // warm-kept, drive the warm agent's outbound replies through the shared
    // connector reply drain: on each `connector.reply` the agent emits,
    // compose a threaded reply from the durable store's connector thread,
    // send it through the outbound bridge (`bridge.submit`, the same signed-
    // send path the agent's own supervisor-backed transport uses), then
    // advance the thread from the receipt. Established once per warm agent
    // over its lifetime stream; the returned `done` promise is what the
    // step-invoker folds into the warm entry so eviction drains it. The key
    // is the step identity, the same key the durable store is filed under.
    // Absent for a multi-step deploy (no durable registry).
    const driveReplies:
      | ((key: string, stream: AgentEventStream) => Promise<void>)
      | undefined =
      durableConversation !== undefined
        ? (key: string, stream: AgentEventStream) =>
            driveConnectorReplies({
              stream,
              composeReply: () => durableConversation.get(key).composeReply(),
              send: (message) =>
                env.outboundMailBridge.submit(
                  env.spawn.mailboxAddress,
                  message,
                ),
              onReplySent: (receipt) =>
                durableConversation.get(key).onReplySent(receipt),
            }).done
        : undefined;

    const invokeStep: RunWorkflowChildBindings["invokeStep"] = async (
      req,
      onEvent,
      authorize,
      warmCache,
      sourcesRef,
      credentialWiring,
      mailPartReader,
    ) =>
      createWorkflowStepInvoker({
        workflowAuthorize: authorize,
        // Combine the per-run credential wiring (the live material cell and
        // the step-grants resolver, ridden in from the run child) with the
        // sidecar-static provider registry, so `buildStepEnv` attaches a
        // complete credential context and the agentFactory can assemble each
        // bundle's consumer-scoped `credentials` capability.
        buildEnv: (buildReq) =>
          buildStepEnv(buildReq, sourcesRef, {
            materialCell: credentialWiring.materialRef,
            resolveStepGrants: credentialWiring.resolveStepGrants,
            providers: credentialProviders,
          }),
        agentFactory: stepAgentFactory,
        onEvent,
        sourcesRef,
        mailPartReader,
        ...(warmCache !== undefined ? { warmCache } : {}),
        ...(onRunBoundary !== undefined ? { onRunBoundary } : {}),
        ...(seedInbound !== undefined ? { seedInbound } : {}),
        ...(driveReplies !== undefined ? { driveReplies } : {}),
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

    const childRunDeps: SidecarRunChildDeps = {
      substrate,
      workflowRunRepoId,
      workflowRunRef: validated.WORKFLOW_RUN_REF,
      principal,
      scheduler,
      invokeStep: childInvokeStep,
      // The onTrigger body path runs real agent steps; the childWorkflow path
      // (and a body's childWorkflow grandchildren) stay on `invokeStep`.
      bodyInvokeStep,
      dataDir: validated.SIDECAR_DATA_DIR,
      evaluateGrants: evaluateGrantsAdapter,
    };
    // Terminal childWorkflow executor. `run-child` builds the in-memory
    // resolver from this plus the lifted-body map it extracts after loading
    // the parent's re-verified definition, so an owned inline child spawns
    // with no on-disk asset read.
    const runChild = createSidecarRunChild(childRunDeps);

    // An onTrigger section runs each event's body as a suspendable child.
    // Source-ref is the only deploy lineage: `run-child` builds the in-memory
    // body resolver from this raw executor plus the lifted-body map it extracts
    // after re-evaluating the parent's closure, so a body resolves in-process
    // with no on-disk read and no separate per-body re-verify (the parent's
    // re-verify already covers every inline body).
    const runSuspendableChild =
      createSidecarSpawnSuspendableChild(childRunDeps);

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
      invokeStep,
      initialSources: stepInferenceSources,
      runChild,
      runSuspendableChild,
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
