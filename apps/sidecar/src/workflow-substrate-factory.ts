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
  type ConnectorReplyDrain,
  type CredentialProviderRegistry,
} from "@intx/harness";
import { createSSHSignature } from "@intx/crypto";
import {
  createAgentRepoStore,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  type Principal,
  type RepoId,
  type RepoStore,
  type WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions/substrate";
import { createIsogitStore } from "@intx/storage-isogit/node";
import {
  adaptHostScheduler,
  createChildMailboxReader,
  createCredentialsBackedAuthorize,
  createMailboxWatchRegistry,
  createProxyWorkflowRunRepoStore,
  createSupervisorBackedTransport,
  createWorkflowHostScheduler,
  createWorkflowRunBlobSubstrate,
  createWorkflowRunRepoStore,
  createWorkflowHostSignalChannel,
  createInMemorySpawnChild,
  createInMemorySpawnSuspendableChild,
  createWorkflowStepInvoker,
  hashGrants,
  loadWorkflowLoopFnsFromClosure,
  loadWorkflowPluginFactoriesFromClosure,
  loadWorkflowPluginToolDefinitionsFromClosure,
  type ChildMailboxReader,
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
  type SupervisorBackedTransportInbound,
} from "@intx/workflow-host";
import {
  baseStepId,
  collectDeclaredPluginNames,
  createLoopIterationHandle,
  createNoopDrainController,
  createSuspendableChildHandle,
  eagerlyResolveLoopFns,
  emptyState,
  enumerateInlineLoopBodies,
  rewriteInlineChildWorkflowBodies,
  runtimeRun,
  type LoopFnRegistry,
  type ParkedApprovalOp,
  type ReadParkedApprovalOps,
  type Scheduler,
  type StepInvokeRequest,
  type StepInvokeResult,
  type WorkflowAuthorizeFn,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { type PluginToolDefinitions } from "@intx/workflow-deploy";

import {
  attachStepCredentialWiring,
  attachStepTools,
  createToolBearingAgentFactory,
  deriveToolMarkFloorGrants,
  materializeStepTools,
  type StepToolCacheConfig,
  type StepToolMaterialization,
} from "./step-agent-tools";
import {
  createInferenceCredentialResolver,
  type CredentialMaterialCell,
} from "./step-credential-capabilities";
import { readRunGrants, runGrantsPath } from "./run-grants";
import {
  collectDeclaredResources,
  filterGrantsToDeclaredResources,
} from "./child-grant-filter";
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
  "WORKFLOW_BODY_SOURCES",
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
  // JSON `{ [definitionId]: { [stepId]: InferenceSource[] } }` of every spawned
  // body's plaintext inference sources, decrypted sidecar-side from the run
  // record. Always serialized by the deploy router (at least "{}" when the
  // deployment spawns no bodies), so a missing key child-side is a serialization
  // bug and must fail loud here.
  WORKFLOW_BODY_SOURCES: "string > 0",
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
 * Every spawned body's per-step inference-source table, keyed by the body's
 * definition id. Parsed from the `WORKFLOW_BODY_SOURCES` env entry the deploy
 * router serializes from the run record's decrypted body sources. Empty when the
 * deployment spawns no bodies.
 */
const BodyInferenceSources = type({
  "[string]": StepInferenceSourceTable,
});
type BodyInferenceSources = typeof BodyInferenceSources.infer;

/**
 * Parse and validate the JSON-encoded `WORKFLOW_BODY_SOURCES` entry. Mirrors
 * `parseStepInferenceSources`: a malformed payload is rejected at the boundary
 * rather than deep in a body spawn.
 */
function parseBodyInferenceSources(raw: string): BodyInferenceSources {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    throw new Error(
      `sidecar workflow-child substrate config: WORKFLOW_BODY_SOURCES is not valid JSON: ${reason}`,
    );
  }
  const validated = BodyInferenceSources(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `sidecar workflow-child substrate config: WORKFLOW_BODY_SOURCES failed validation: ${validated.summary}`,
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
 * Resolve the full RFC 5322 References chain for a reply whose parent is
 * `inReplyTo`, from the deployment's committed substrate mailbox. Opens a
 * fresh committed snapshot, locates the message whose Message-Id equals
 * `inReplyTo`, and returns that message's own References followed by its
 * Message-Id -- the complete ancestry a reply threaded onto it must carry.
 *
 * Returns `undefined` when the parent is not in the mailbox (the first reply
 * on a fresh thread, or an id that never arrived). The message identifiers are
 * carried through verbatim; the transport's send path (`buildReferences` in
 * `@intx/mail-memory`) owns filtering non-RFC identifiers out of the emitted
 * `References` header, so this reader does not pre-validate them.
 */
async function resolveMailboxReferences(
  reader: ChildMailboxReader,
  inReplyTo: string,
): Promise<string[] | undefined> {
  const store = await reader.open();
  const parent = store.messages.find((m) => m.envelope.messageId === inReplyTo);
  if (parent === undefined) return undefined;
  return [...parent.envelope.references, parent.envelope.messageId];
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
  /**
   * Inbound local IMAP surface for the step agent's supervisor-backed
   * transport (INBOUND half of mailbox ownership, §3b). Bundles the child
   * mailbox reader (a fresh committed snapshot of the deployment's substrate
   * `INBOX` per read), the shared mailbox watch registry (the same instance the
   * child's control loop fires `mailbox.notify` into), and the sender-key
   * resolver `fetchFull` verifies signatures against. When present, the warm
   * agent's `mail_read` / `mail_search` / `mail_wait` resolve against the
   * committed mailbox rather than throwing "not wired". Absent for a build that
   * owns no inbound mailbox (the toolless onTrigger body), whose transport
   * inbound stays inert.
   */
  inbound?: SupervisorBackedTransportInbound;
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

    // Supervisor-backed transport for the step agent's mail tools (both
    // halves of mailbox ownership, §3a OUTBOUND and §3b INBOUND). Outbound
    // (`send`) routes over the control IPC to the supervisor, which performs
    // the actual signed send through the host transport as `address`.
    // `address` is the deployment mailbox address: the same identity the host
    // registered the agent's `CryptoProvider` against, so the outbound mail
    // carries the agent's signature with parity to the in-process path.
    // Inbound (`deps.inbound`, present for the warm single-step agent) makes
    // `mail_read` / `mail_search` / `mail_wait` resolve locally against a fresh
    // committed snapshot of the deployment's substrate `INBOX`; a build that
    // owns no inbound mailbox (the toolless body) leaves it undefined and the
    // inbound methods stay inert. Both `transport` and `address` are the env
    // keys `@intx/tools-mail`'s sidecar bundle declares in its `requires`.
    const transport = createSupervisorBackedTransport(
      deps.outboundMailBridge,
      deps.mailboxAddress,
      deps.inbound,
    );

    // The host owns capability assembly: it builds the RuntimeCapabilities
    // bag here (currently `mail.transport`) and puts it on `env.capabilities`,
    // so a bundle consumes the assembled bag rather than re-wrapping a raw
    // env key of its own. `@intx/tools-mail`'s sidecar bundle (`requires:
    // ["capabilities", "address"]`) resolves `mail.transport` from it.
    const capabilities = createHarnessRuntimeCapabilities({ transport });

    // The step env carries `toolCwd`, `transport`, `address`, and the
    // assembled `capabilities` beyond `BaseEnv`. `toolCwd` is the working
    // tree the posix tools operate on; `capabilities` is what the mail bundle
    // reads; `transport` remains a raw env surface for tool packages that
    // read it directly. `address` is observability-only. These widen the
    // returned `StepEnvBase` structurally, which the buildEnv return type
    // (`StepEnvBase`) accepts (a wider object is assignable to the narrower
    // type).
    const env: StepEnvBase & {
      toolCwd: string;
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
      // The per-step workspace is both the lock boundary and the tree the
      // filesystem tools operate on; a deployed step gets a throwaway
      // scratch dir, so the two coincide.
      toolCwd: workdir,
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
      // Inference resolves its source's secret from the SAME live cell tool
      // credentials resolve from, by `credentialId`, so the step's sources carry
      // no inline key and the child never holds the cipher key. The reader is set
      // whenever a context is present -- a toolless onTrigger body carries a
      // context for the reader alone.
      env.readCurrentMaterial = createInferenceCredentialResolver(
        credentialContext.materialCell,
      );
      // Attach the tool `credentials` capability only for a tool-bearing build. A
      // toolless body has no tool grants and assembles no capability, so it skips
      // the wiring while still reading the run's live material for inference.
      if (deps.toolless !== true) {
        attachStepCredentialWiring(env, {
          materialCell: credentialContext.materialCell,
          resolveGrants: () =>
            // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- resolveStepGrants returns unknown[] at the run-child boundary; the sidecar owns the GrantRule grammar
            credentialContext.resolveStepGrants(stepId) as readonly GrantRule[],
          providers: credentialContext.providers,
        });
      }
    }
    return env;
  };
}

/**
 * Real per-step invoker for a childWorkflow child (INTR-310). It widens the
 * workflow-runtime `StepInvoker` with the child's credentials-backed
 * `authorize` (the seam that resolves each tool call against the run's
 * grants), the child's own per-step inference `sourcesRef` (built fresh per
 * spawn from the env-delivered body sources, disjoint from the top-level's
 * mutable table), and the parent run's `onEvent` funnel (so the child's live
 * inference events reach the hub timeline). Same shape as
 * `SidecarBodyStepInvoker`; the two differ only in whether the build env is
 * tool-bearing (childWorkflow, source-tools) or toolless (onTrigger body).
 */
export type SidecarChildStepInvoker = (
  req: StepInvokeRequest,
  authorize: WorkflowAuthorizeFn,
  sourcesRef: SourcesSnapshotRef,
  onEvent: (event: InferenceEvent) => void,
  credentialContext?: SidecarStepCredentialContext,
) => Promise<StepInvokeResult>;

/**
 * Real per-step invoker for an onTrigger BODY child. Structurally identical to
 * `SidecarChildStepInvoker`; the two differ only in the build env the wrapper
 * uses -- an onTrigger body is guaranteed toolless (the deploy guard rejects a
 * tool-bearing body), so this invoker builds a toolless env, while a
 * childWorkflow step builds a tool-bearing source-tools env. Both run a real
 * agent through `createWorkflowStepInvoker`, resolving inference against the
 * child's own per-step `sourcesRef` and funnelling live events through
 * `onEvent` to the hub timeline.
 */
export type SidecarBodyStepInvoker = (
  req: StepInvokeRequest,
  authorize: WorkflowAuthorizeFn,
  sourcesRef: SourcesSnapshotRef,
  onEvent: (event: InferenceEvent) => void,
  credentialContext?: SidecarStepCredentialContext,
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
   * factory's default (`childInvokeStep` in
   * `createSidecarSubstrateFactory`) runs a real, tool-bearing agent
   * against the child's own staged sources; a test may inject its own.
   *
   * The invoker receives the child's credentials-backed `authorize`
   * alongside the request, mirroring the workflow-process child's
   * `ChildStepInvoker`: the runtime body calls `env.invokeStep` with the
   * request only, so the invoker -- not the runtime -- is the seam that
   * gates each tool call against the run's grants.
   */
  invokeStep: SidecarChildStepInvoker;
  /**
   * Toolless per-step invoker used ONLY for an onTrigger body child's own
   * steps (INTR-310). `createSidecarSpawnSuspendableChild` wires it onto the
   * body env so a guaranteed-toolless body runs through a toolless build env;
   * the tool-bearing `invokeStep` above stays the seam for childWorkflow spawns
   * and for the body's own childWorkflow grandchildren. Absent runs the body's
   * steps through the tool-bearing invoker (a test-only shape).
   */
  bodyInvokeStep?: SidecarBodyStepInvoker;
  /**
   * Every spawned body's plaintext inference-source table, keyed by definition
   * id, decrypted sidecar-side from the run record and delivered through the
   * spawn env. Each body path resolves its own table from here by definition id,
   * so the child never holds the sidecar cipher key. A body whose id is absent --
   * a deployment restored from a legacy record written before body sources moved
   * into the record -- falls back to the on-disk `dataDir` file.
   */
  bodySources: BodyInferenceSources;
  /**
   * Sidecar data dir. The legacy fallback for a body absent from `bodySources`
   * reads its `assets/workflow/<childRef>/sources.json`; the terminal
   * childWorkflow / body paths resolve per-step storage under it. Required.
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
  /**
   * Sidecar-static credential provider registry, shared with the top level.
   * The child's tool-bearing build combines it with the run's live material
   * cell and the child's capped grants to assemble each bundle's consumer
   * `credentials` capability; the toolless body build carries it too so its
   * inference reader resolves against the same material.
   */
  credentialProviders: CredentialProviderRegistry;
  /** Director registry the child runtime uses; defaults to the canonical built-ins. */
  directors?: DirectorRegistry;
  /**
   * Sidecar-local directory of the materialized workflow-definition closure
   * (`env.spawn.closurePackageDir`), the source-ref lineage's only closure.
   * `buildChildRunEnv` loads the child body's declared plugin tool definitions
   * from it so a plugin-contributed `tool:<name>` the child loads is declared
   * when capping the child's inherited grants. A child body that declares no
   * plugin package needs no closure read; absent on a lineage that stages no
   * closure.
   */
  closurePackageDir?: string;
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
 * to the supervisor. It names the shallow `runs/<childRunId>/` prefix, whose
 * merge input is only that level's flat blobs (the prefix read does not
 * recurse), so the write rebuilds the level with just `grants.json`. This is
 * safe ONLY because the sole caller (`capAndPersistChildGrants`) is write-once:
 * it invokes this only when no grants file exists yet, i.e. at the run's birth
 * before the runtime appends any event, so the `runs/<childRunId>/` subtree is
 * empty and nothing is dropped. Invoking it over a populated run would delete
 * that run's committed `events/`/`blobs/` subtrees. A later event append names
 * the nested `runs/<childRunId>/events/` prefix, so its own subtree-delete does
 * not reach `grants.json` one level up -- the sibling survives.
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
 * Abort propagation: the parent-supplied `signal` is wired to the child
 * run's local-abort seam (`RuntimeRunOptions.localAbort`). If it aborts --
 * mid-flight or pre-aborted -- the child's own cancel controller aborts, an
 * in-flight step fails, and the returned promise resolves with
 * `terminalStatus: "failed"`. There is no durable `CancelRequested`: an
 * in-process child writes through the proxy substrate and cannot sign the
 * supervisor cancel one would require.
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
  // No `controlPlanePrincipal`: an in-process child tears down through the
  // runtime's local-abort seam (see the `runtimeRun` call below), not a durable
  // `CancelRequested`, so it never writes a control-plane cancel this repo store
  // would have to sign. Run-body events use the workflow-process `principal`.
  // Created once and shared across every child this factory spawns (the
  // runtime scopes reads/subscribes by runId), so sibling and grandchild
  // spawns route through one repo-store handle rather than a fresh one each.
  const repoStore = createWorkflowRunRepoStore({
    substrate: deps.substrate,
    repoId: deps.workflowRunRepoId,
    principal: deps.principal,
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
  const runChild: RunChildWorkflow = async (
    {
      definition,
      childRunId,
      input,
      parentRunId,
      signal,
      depth,
      maxChildSpawnDepth,
    },
    onEvent,
    credentialMaterial,
  ) => {
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
      onEvent,
      ...(credentialMaterial !== undefined
        ? { materialCell: credentialMaterial }
        : {}),
    });
    try {
      // Thread this rung's depth/ceiling into the child run so its own
      // childWorkflow spawns keep counting against the tree-wide bound (the
      // guard lives in the runtime's runChildWorkflow). Without this the
      // in-process sidecar recursion would reset to depth 0 each rung.
      // A parent abort tears the child down through `runtimeRun`'s local-abort
      // seam, not a control-plane cancel: the in-process child writes through
      // the workflow-run PROXY substrate, which cannot sign the supervisor
      // `CancelRequested` a cancel needs (the kind handler refuses a
      // workflow-process-signed cancel). Local teardown aborts the child's own
      // controller directly, so an in-flight step fails and the child settles
      // `failed` under its own principal -- no durable cancel, no wedge while the
      // parent awaits the terminal below.
      const handle = runtimeRun(rewrittenDefinition, env, {
        runId: childRunId,
        triggerPayload: input,
        depth,
        maxChildSpawnDepth,
        localAbort: signal,
      });
      const result = await handle.complete;
      return { terminalStatus: result.terminalStatus };
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
 * hard error on `next()` and tears the child down locally so the section run
 * ends loudly (failed).
 *
 * Signal-channel lifecycle: unlike `createSidecarRunChild`, which stops the
 * channel in a `finally` around a single awaited terminal, this keeps the
 * channel alive across every park (so `resume` can deliver) and ties its
 * teardown to the run's terminal -- the one lifecycle moment every path
 * funnels through (normal completion, a parent-abort local teardown, an
 * illegal-input-park local teardown, or a runtime failure). Tearing down
 * per-`next()` would leak the channel when a parent abort makes `runOnTrigger`
 * stop calling `next()` mid-park.
 *
 * Abort propagation: the parent-supplied `signal` tears the body down through
 * `createSuspendableChildHandle`'s local-teardown seam -- the in-process body
 * runs under the `workflow-process` principal and cannot sign the control-plane
 * `CancelRequested` a durable cancel needs, so on abort its own step fails and
 * the run settles `failed` (not `cancelled`), and the terminal-tied teardown
 * runs.
 */
export function createSidecarSpawnSuspendableChild(
  deps: SidecarRunChildDeps,
): RunSuspendableChild {
  const directors = deps.directors ?? createDefaultDirectorRegistry();
  const clock = deps.clock ?? defaultClock;
  const newId = deps.newId ?? defaultNewId;
  // No `controlPlanePrincipal`: an in-process body tears down through the shared
  // handle's local-abort seam (`createSuspendableChildHandle`), not a durable
  // `CancelRequested`, so it never writes a control-plane cancel this repo store
  // would have to sign. Run-body events use the workflow-process `principal`.
  const repoStore = createWorkflowRunRepoStore({
    substrate: deps.substrate,
    repoId: deps.workflowRunRepoId,
    principal: deps.principal,
    ref: deps.workflowRunRef,
  });
  // A body's own `childWorkflow` grandchildren spawn terminal-only: the
  // suspendable seam is exercised only by onTrigger sections, and
  // `buildChildRunEnv` wires the body env's `spawnChild` (not
  // `spawnSuspendableChild`), so a nested onTrigger inside a body fails loud
  // rather than silently spawning.
  const runChild = createSidecarRunChild(deps);

  return async (
    {
      definition,
      childRunId,
      input,
      parentRunId,
      signal,
      depth,
      maxChildSpawnDepth,
      resumeFromEvents,
    },
    onEvent,
    credentialMaterial,
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
      // The live event sink is always threaded (both a body step and a
      // grandchild childWorkflow step run a real agent). The BODY env adds a
      // toolless `bodyStepInvoker` when the factory wired one, so a body's
      // guaranteed-toolless steps run through the toolless build env rather than
      // the tool-bearing childWorkflow invoker. A body's own childWorkflow
      // grandchildren, built via the internal `createSidecarRunChild(deps)`
      // above, run through the tool-bearing `deps.invokeStep`.
      onEvent,
      // The run's live credential-material cell so the body's inference resolves
      // its source secret against the parent's current delivery; a grandchild
      // spawned from the body inherits it through the recursive spawnChild.
      ...(credentialMaterial !== undefined
        ? { materialCell: credentialMaterial }
        : {}),
      ...(deps.bodyInvokeStep !== undefined
        ? { bodyStepInvoker: deps.bodyInvokeStep }
        : {}),
    });

    return createSuspendableChildHandle(baseEnv, {
      definition: rewrittenDefinition,
      childRunId,
      input,
      depth,
      maxChildSpawnDepth,
      ...(resumeFromEvents !== undefined ? { resumeFromEvents } : {}),
      signal,
      cleanup: () => signalChannel.stop(),
    });
  };
}

/**
 * Read the parent run's grants, cap them to what `definition` declares, and
 * persist the capped set as the child run's own `runs/<childRunId>/grants.json`
 * -- the file the next spawn hop (a childWorkflow grandchild) reads back as its
 * ceiling. Returns the capped grants so a caller building a fresh child env can
 * also key its credentials snapshot on them.
 *
 * `definition` MUST be the PRE-rewrite body, its childWorkflow grandchildren
 * still INLINE: `collectDeclaredResources` skips a `{ ref }` body, so a
 * rewritten definition would drop the grandchild's declared resources and
 * under-authorize it. Every birth path materializes a run's grants file, so a
 * missing parent file is a defect, not a run that legitimately holds none --
 * fail closed. The cap only removes rules (the parent stays the ceiling),
 * matching the top level's "authority bounded by declared capabilities" model.
 *
 * The grants file is WRITE-ONCE per run: a run's authorization ceiling is fixed
 * at its birth, so this reads back an existing `runs/<childRunId>/grants.json`
 * rather than rewriting it. Re-writing on a resume re-drive is not just
 * redundant, it is CORRUPTING: `writeChildRunGrants` commits at the shallow
 * `runs/<childRunId>/` prefix through a substrate writer separate from the
 * runtime's event-log writer, so a re-write racing the runtime's replay
 * re-appends on the shared repo regresses another run's event seq (a
 * single-writer-invariant violation), and its wholesale subtree-delete drops the
 * run's committed `events/`/`blobs/` (the prefix read is non-recursive, so the
 * merge cannot carry them forward). The read-back returns the same capped set
 * the birth-time write persisted, so a fresh child-env caller keys its
 * credentials snapshot on identical grants either way.
 */
async function capAndPersistChildGrants(args: {
  deps: SidecarRunChildDeps;
  directors: ReturnType<typeof createDefaultDirectorRegistry>;
  definition: WorkflowDefinition;
  childRunId: string;
  parentRunId: string;
}): Promise<readonly unknown[]> {
  const { deps, directors, definition, childRunId, parentRunId } = args;
  const existingChildGrants = await readRunGrants({
    repoStore: deps.substrate,
    anchorRunId: deps.workflowRunRepoId.id,
    runId: childRunId,
  });
  if (existingChildGrants !== undefined) return existingChildGrants;
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
  const pluginDefs: PluginToolDefinitions =
    deps.closurePackageDir === undefined
      ? new Map()
      : await loadWorkflowPluginToolDefinitionsFromClosure({
          packageDir: deps.closurePackageDir,
          plugins: collectDeclaredPluginNames(definition),
        });
  const declaredResources = collectDeclaredResources(
    definition,
    directors,
    pluginDefs,
  );
  const childGrants = filterGrantsToDeclaredResources(
    parentGrants,
    declaredResources,
  );
  await writeChildRunGrants({
    substrate: deps.substrate,
    workflowRunRepoId: deps.workflowRunRepoId,
    principal: deps.principal,
    ref: deps.workflowRunRef,
    childRunId,
    grants: childGrants,
  });
  return childGrants;
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
   * Toolless body-step invoker for the onTrigger body path. Present ONLY when
   * this env hosts an onTrigger body: `createSidecarSpawnSuspendableChild`
   * passes it so the body's (guaranteed toolless) agent steps run for real. A
   * childWorkflow env runs its steps through the real, tool-bearing
   * `deps.invokeStep` instead.
   */
  bodyStepInvoker?: SidecarBodyStepInvoker;
  /**
   * Per-run live inference-event sink, threaded from the parent run's event
   * channel. Required for both paths -- a childWorkflow step and an onTrigger
   * body step both run a real agent whose inference the hub stream must see, so
   * a missing sink is a wiring defect, not a silent drop.
   */
  onEvent: (event: InferenceEvent) => void;
  /**
   * The parent run's live credential-material cell. Threaded so the child's
   * inference resolves its source secret by `credentialId` against the run's
   * current delivery -- reached live through the shared reference on a rotation.
   * Absent when a non-sidecar executor carries no credential material, in which
   * case no credential context is assembled and the child's inference reader
   * stays unset.
   */
  materialCell?: CredentialMaterialCell;
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
    materialCell,
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
  // Enumerate any `loop` bodies nested in this spawned body so a loop can run
  // here (the runtime needs `env.loopFns` + `env.spawnLoopIteration`, wired
  // below). A loop body stays inline on its primitive, so mint a ref-keyed copy
  // for the loop-iteration host, keep the pre-rewrite form for the per-iteration
  // grant cap, and merge each loop body's own childWorkflow grandchildren into
  // `grandchildMap` so a grandchild spawned from a loop body resolves in-memory.
  // Mirrors the top-level loop-body registration in run-child.ts.
  const loopBodies = enumerateInlineLoopBodies(definition);
  const loopBodiesMap = new Map<string, WorkflowDefinition>();
  const loopBodyPreRewrite = new Map<string, WorkflowDefinition>();
  for (const loopBody of loopBodies) {
    loopBodyPreRewrite.set(loopBody.ref, loopBody.definition);
    const bodyRewrite = rewriteInlineChildWorkflowBodies(loopBody.definition);
    loopBodiesMap.set(loopBody.ref, bodyRewrite.workflow);
    for (const grandchild of bodyRewrite.bodies) {
      grandchildMap.set(grandchild.ref, grandchild.definition);
    }
  }
  // Resolve the loop while/carry fns from the closure so a loop in this body can
  // run. Only load when the body contains a loop. If it declares a loop but no
  // closure is wired, that is a wiring defect -- fail loud, mirroring the
  // establish-time posture rather than deferring to a mid-run resolve failure.
  let loopFns: LoopFnRegistry | undefined;
  if (loopBodies.length > 0) {
    if (deps.closurePackageDir === undefined) {
      throw new Error(
        "sidecar child: a loop is nested in this spawned body but deps.closurePackageDir is missing; the loop while/carry fns cannot be resolved",
      );
    }
    loopFns = await loadWorkflowLoopFnsFromClosure({
      packageDir: deps.closurePackageDir,
    });
    eagerlyResolveLoopFns(
      [
        rewrittenDefinition,
        ...loopBodiesMap.values(),
        ...grandchildMap.values(),
      ],
      loopFns,
    );
  }
  // Read the parent's grants, cap them to what this body declares, and persist
  // the capped set as the child's own `runs/<childRunId>/grants.json` (the file
  // a grandchild reads as its ceiling). `definition` here is the childWorkflow-
  // inline form, so the cap keeps a grandchild's declared resources.
  const childGrants = await capAndPersistChildGrants({
    deps,
    directors,
    definition,
    childRunId,
    parentRunId,
  });
  // The child's credentials snapshot applies the capped grant set
  // uniformly across every step the child definition declares, keyed on
  // each step's id (the same shape the deploy-time and per-run snapshot
  // assemblies produce). The in-process child has no per-step mail
  // address, so the snapshot's `address` mirrors the step id --
  // `createCredentialsBackedAuthorize` reads only `grants`.
  const contentHash = await hashGrants(childGrants);
  const credentialsSnapshot: CredentialsSnapshot = {
    steps: rewrittenDefinition.stepOrder.map((stepId) => ({
      stepId,
      address: stepId,
      grants: childGrants,
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
  // Both the terminal childWorkflow path and the onTrigger BODY path run a real
  // agent that resolves inference against its OWN per-step source table, keyed by
  // the rewritten ref, and funnels live events to the parent run's channel. So
  // `onEvent` is required for both; a missing one is a wiring defect, not a
  // silent drop.
  if (onEvent === undefined) {
    throw new Error(
      "sidecar child: onEvent is missing; child inference events would be silently dropped from the hub stream",
    );
  }
  const childOnEvent = onEvent;
  // Resolve fresh per spawn into a `sourcesRef` disjoint from the top-level's
  // mutable table, so a top-level source rotation never leaks into a child. The
  // sources are delivered plaintext through the spawn env (`deps.bodySources`),
  // decrypted sidecar-side, so the child never holds the cipher key.
  const sourcesRef: SourcesSnapshotRef = {
    current: await resolveBodyStepSources(deps, rewrittenDefinition.id),
  };
  // Recursive `spawnChild`: a grandchild embedded inline in this rung is
  // resolved from the in-memory map lifted above and flows back into this same
  // `runChild` callback. Inject THIS run's event funnel (mirroring the rung-0
  // wrap in run-child.ts) so the grandchild's agent steps ride this run's
  // channel; the runtime env keeps the narrow `SpawnChildWorkflow` (no event
  // slot). Sub-namespace scoping holds at every depth via `childRunId`.
  const spawnHost = createInMemorySpawnChild({
    bodies: grandchildMap,
    runChild,
  });
  const spawnChild: WorkflowRuntimeEnv["spawnChild"] = (spawnInput) =>
    spawnHost(spawnInput, childOnEvent, materialCell);
  // Assemble the per-step credential context from the run's live material cell
  // (threaded in from the parent) when one is present, so the child's inference
  // resolves its source secret by `credentialId` against the parent's current
  // delivery. The childWorkflow path is tool-bearing, so its context resolves
  // the child's capped grants (mirroring the top level's snapshot lookup) and
  // carries the sidecar-static providers for the tool `credentials` capability.
  // The toolless BODY path needs the inference reader only: its build env sets
  // the reader but attaches no tool wiring (gated on `toolless`), so its grants
  // resolver is never read. Absent when no material was threaded (a non-sidecar
  // executor), which leaves the child's inference reader unset.
  const childCredentialContext: SidecarStepCredentialContext | undefined =
    materialCell === undefined
      ? undefined
      : {
          materialCell,
          resolveStepGrants: (stepId) => {
            const entry = credentialsSnapshot.steps.find(
              (step) => step.stepId === baseStepId(stepId),
            );
            if (entry === undefined) {
              throw new Error(
                `sidecar child credential wiring: credentials snapshot has no entry for step ${baseStepId(stepId)}`,
              );
            }
            return entry.grants;
          },
          providers: deps.credentialProviders,
        };
  const bodyCredentialContext: SidecarStepCredentialContext | undefined =
    materialCell === undefined
      ? undefined
      : {
          materialCell,
          resolveStepGrants: () => [],
          providers: deps.credentialProviders,
        };
  // Per-step invocation seam. The runtime body invokes `env.invokeStep` with
  // the request alone; the wrapper forwards the child's credentials-backed
  // authorize (so each tool call gates against the inherited grants), the run's
  // `sourcesRef`, the event funnel, and the credential context (so the step's
  // inference reads the run's live material). The childWorkflow path runs a real
  // tool-bearing agent (`deps.invokeStep`, the source-tools arm); the onTrigger
  // BODY path runs a toolless agent (`bodyStepInvoker`). Both read the same
  // `sourcesRef`.
  let invokeStep: WorkflowRuntimeEnv["invokeStep"] = (req) =>
    deps.invokeStep(
      req,
      authorize,
      sourcesRef,
      childOnEvent,
      childCredentialContext,
    );
  if (bodyStepInvoker !== undefined) {
    invokeStep = (req) =>
      bodyStepInvoker(
        req,
        authorize,
        sourcesRef,
        childOnEvent,
        bodyCredentialContext,
      );
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
    ...(loopFns !== undefined ? { loopFns } : {}),
  };
  // Wire loop-iteration spawning for a `loop` nested in this body. Assigned
  // AFTER the env literal because the iteration host closes over `env`: a loop
  // iteration re-enters THIS body env, so a nested loop composes and inherits
  // the body's toolless step invoker, capped grants, and in-memory spawnChild.
  //
  // This deliberately REPLICATES the top-level loop host in run-child.ts rather
  // than sharing a helper. The two live in different packages and differ in the
  // grants seam (here `capAndPersistChildGrants` is in scope and called
  // directly; the top level injects it as a binding), and the loop-in-body
  // crash-resume path is not yet proven identical to the top level's. Extract a
  // shared helper only once a deployed loop-in-body resume test shows the two
  // paths match.
  //
  // Boundary: the suspendable-child seam services APPROVAL parks only. A loop
  // iteration in a body that awaits an externally-delivered signal, or re-arms
  // an onTrigger, inherits that approvals-only limit and cannot park on an
  // external input channel.
  if (loopFns !== undefined) {
    const loopIterationHost = createInMemorySpawnSuspendableChild({
      bodies: loopBodiesMap,
      runSuspendableChild: async (loopInput, _onEvent) => {
        const preRewriteBody = loopBodyPreRewrite.get(loopInput.definitionRef);
        if (preRewriteBody === undefined) {
          throw new Error(
            `sidecar child: no pre-rewrite loop body for ref ${loopInput.definitionRef}`,
          );
        }
        // Cap the iteration's grants against the body's own grants (the
        // iteration's parent IS the body run) and persist them before the
        // iteration appends its first event.
        await capAndPersistChildGrants({
          deps,
          directors,
          definition: preRewriteBody,
          childRunId: loopInput.childRunId,
          parentRunId: loopInput.parentRunId,
        });
        const iterationSignalChannel = createWorkflowHostSignalChannel({
          repoStore: deps.substrate,
          principal: deps.principal,
          repoId: deps.workflowRunRepoId,
          ref: deps.workflowRunRef,
          runId: loopInput.childRunId,
          readState: () => emptyState(loopInput.childRunId),
          newId: () => newId("sig"),
          clock,
        });
        return createLoopIterationHandle(env, {
          definition: loopInput.definition,
          childRunId: loopInput.childRunId,
          input: loopInput.input,
          depth: loopInput.depth,
          maxChildSpawnDepth: loopInput.maxChildSpawnDepth,
          ...(loopInput.resumeFromEvents !== undefined
            ? { resumeFromEvents: loopInput.resumeFromEvents }
            : {}),
          signal: loopInput.signal,
          signalChannel: iterationSignalChannel,
          cleanup: () => iterationSignalChannel.stop(),
        });
      },
    });
    env.spawnLoopIteration = (spawnInput) =>
      loopIterationHost(spawnInput, childOnEvent);
  }
  return { env, signalChannel, definition: rewrittenDefinition };
}

/**
 * Resolve a spawned body's per-step inference sources by definition id. The
 * primary path is sidecar-mediated: the sources arrive plaintext in
 * `deps.bodySources`, decrypted sidecar-side from the sealed run record, so the
 * child holds no cipher key.
 *
 * LEGACY FALLBACK: a body whose id is absent from the delivered set -- a
 * deployment restored from a record written before body sources moved into the
 * record -- reads its on-disk plaintext `sources.json`. That file predates the
 * change and is plaintext, so the fallback constructs no cipher and the child
 * stays key-free. Removable once no restorable record predates the
 * record-carried body sources (after the reconnect re-push has re-persisted
 * every live deployment).
 */
async function resolveBodyStepSources(
  deps: SidecarRunChildDeps,
  definitionId: string,
): Promise<StepInferenceSourceTable> {
  const delivered = deps.bodySources[definitionId];
  if (delivered !== undefined) {
    return delivered;
  }
  if (deps.dataDir === undefined) {
    throw new Error(
      `sidecar child: body ${definitionId} is absent from the delivered sources and deps.dataDir is missing, so its legacy on-disk sources cannot be read`,
    );
  }
  return readChildStepInferenceSources(deps.dataDir, definitionId);
}

/**
 * Legacy fallback reader (see `resolveBodyStepSources`): a spawned body's
 * plaintext per-step pins from `${dataDir}/assets/workflow/<childRef>/sources.json`,
 * staged by a pre-record deploy. Parsed through the same
 * `parseStepInferenceSources` boundary the top-level `STEP_INFERENCE_SOURCES` env
 * entry uses. Only reached for a body the delivered set does not carry, so a
 * missing or malformed file is a defect and surfaces loudly.
 */
async function readChildStepInferenceSources(
  dataDir: string,
  childRef: string,
): Promise<StepInferenceSourceTable> {
  const sourcesPath = path.join(
    dataDir,
    "assets",
    "workflow",
    childRef,
    "sources.json",
  );
  let raw: string;
  try {
    raw = await fs.promises.readFile(sourcesPath, "utf8");
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    throw new Error(
      `sidecar child: failed to read child inference sources at ${sourcesPath}: ${reason}`,
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

    // INBOUND half of mailbox ownership (§3b). One watch registry per child,
    // created at boot and shared by BOTH the step agent's supervisor-backed
    // transport (its `watch` registers callbacks here, backing `mail_wait`) and
    // the child's control loop (which fires each `mailbox.notify` into it). The
    // registry rides out on the returned bindings so `runWorkflowChild` routes
    // notifications to this same instance. The child mailbox reader opens a
    // fresh committed snapshot of the deployment's substrate `INBOX` per read,
    // over the same substrate handle / repo id / principal / ref the mail-part
    // reader uses, so a read taken after a `mailbox.notify` observes the message
    // the supervisor just committed.
    const mailboxWatchRegistry = createMailboxWatchRegistry();
    const transportInbound: SupervisorBackedTransportInbound = {
      reader: createChildMailboxReader({
        substrate,
        repoId: workflowRunRepoId,
        principal,
        ref: validated.WORKFLOW_RUN_REF,
      }),
      watchRegistry: mailboxWatchRegistry,
      // The child holds no sender-key registry, so signature verification is not
      // yet possible here; `fetchFull` reports every message's signature status
      // as "unknown". A future sender-key surface would replace this resolver.
      getCrypto: () => undefined,
      // The write methods (setFlags / clearFlags / expunge) route through this
      // bridge to the supervisor, the sole mailbox writer, instead of flushing
      // the run ref from the child.
      mutationBridge: env.mailboxMutationBridge,
    };

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
      // Activate the warm agent's inbound mail surface: `mail_read` /
      // `mail_search` / `mail_wait` resolve against the deployment's committed
      // substrate `INBOX` through this bundle. The body build below omits it
      // (a toolless body owns no inbound mailbox).
      inbound: transportInbound,
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

    // childWorkflow step build env (INTR-310). A childWorkflow child's steps run
    // real, TOOL-BEARING agents through the same source-tools arm the top level
    // uses: the child holds the live re-verified definition, so each step agent
    // carries live `toolFactories` fed from `req.agent.toolFactories`, and the
    // source arm materializes declared plugins from the shared closure. Built
    // COLD per invocation -- no `durableConversation`/warm hooks (a fan-out
    // branch is a fresh run per spawn) and no `inbound` (a spawned child owns no
    // warm inbound mailbox; inbound-reading tools stay inert). The source arm
    // records no tool-mark floor (a source tool's bare `tool:<name>` grant is
    // already in the credentials snapshot), so the floor recorder throw-asserts
    // that invariant.
    const coldChildBuildStepEnv = createSidecarStepBuildEnv({
      dataDir: validated.SIDECAR_DATA_DIR,
      workflowRunRepoId,
      signer: conversationSigner,
      mailboxAddress: env.spawn.mailboxAddress,
      stepCount: env.spawn.stepCount,
      outboundMailBridge: env.outboundMailBridge,
      cache: stepToolCache,
      adapters: childAdapterRegistry,
      recordToolMarkFloor: () => {
        throw new Error(
          "source-tools child build-env must not record a tool-mark floor",
        );
      },
      toolless: false,
      sourceTools: true,
      closurePackageDir: env.spawn.closurePackageDir,
    });
    // childWorkflow step invoker (INTR-310). Mirrors `bodyInvokeStep` below but
    // tool-bearing: it runs a real agent through `createWorkflowStepInvoker`,
    // resolving inference against the child's own per-step `sourcesRef` (staged
    // at deploy, read fresh per spawn) and funnelling live events to the parent
    // run's channel. `buildChildRunEnv` threads in the run's `credentialContext`
    // (the live material cell, the child's capped grants, the sidecar-static
    // providers), so the tool-bearing build attaches each bundle's `credentials`
    // capability and the step's inference resolves its source secret against the
    // run's live material. Absent when no material was threaded, in which case a
    // tool that declares a credential consumer fails closed and loud at its own
    // `resolve("credentials")`, never silently. Wired as `childRunDeps.invokeStep`,
    // so it also covers a body's own childWorkflow grandchildren.
    const childInvokeStep: SidecarChildStepInvoker = (
      req,
      authorize,
      sourcesRef,
      onEvent,
      credentialContext,
    ) =>
      createWorkflowStepInvoker({
        workflowAuthorize: authorize,
        buildEnv: (buildReq) =>
          coldChildBuildStepEnv(buildReq, sourcesRef, credentialContext),
        agentFactory: stepAgentFactory,
        sourcesRef,
        onEvent,
      })(req);

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
      credentialContext,
    ) =>
      createWorkflowStepInvoker({
        workflowAuthorize: authorize,
        // A body is guaranteed toolless, so the cold build env attaches no tool
        // credential wiring; it still sets the inference reader from the context's
        // live material cell so the body's inference resolves its source secret
        // against the run's current delivery.
        buildEnv: (buildReq) =>
          coldBodyBuildStepEnv(buildReq, sourcesRef, credentialContext),
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
    // over its lifetime stream; the returned drain handle carries the lifetime
    // `done` promise the step-invoker folds into the warm entry so eviction
    // drains it, plus the per-turn settle barrier the warm step gates each
    // reply turn on. The key is the step identity, the same key the durable
    // store is filed under. Absent for a multi-step deploy (no durable
    // registry).
    const driveReplies:
      | ((key: string, stream: AgentEventStream) => ConnectorReplyDrain)
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
              // Build the full RFC 5322 References chain from the deployment's
              // committed mailbox: locate the parent by its Message-Id and
              // return its own References plus its Message-Id, so a reply
              // carries the whole conversational ancestry rather than a
              // single element. The reader opens a fresh committed snapshot,
              // so a parent committed just before this reply is visible. A
              // parent miss (the first reply on a fresh thread, a malformed
              // id) returns undefined and the transport derives [inReplyTo].
              resolveReferences: (inReplyTo) =>
                resolveMailboxReferences(transportInbound.reader, inReplyTo),
              onReplySent: (receipt) =>
                durableConversation.get(key).onReplySent(receipt),
            })
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
      // Plaintext body sources decrypted sidecar-side from the run record; each
      // body path resolves its own table from here by definition id.
      bodySources: parseBodyInferenceSources(validated.WORKFLOW_BODY_SOURCES),
      dataDir: validated.SIDECAR_DATA_DIR,
      evaluateGrants: evaluateGrantsAdapter,
      // Shared with the top level's `buildStepEnv`: the child's tool-bearing
      // build combines it with the run's live material and capped grants; the
      // toolless body build carries it for its inference reader.
      credentialProviders,
      // The shared closure the child re-walks to cap its inherited grants at
      // its declared capabilities. Source-ref only, so always present here.
      closurePackageDir: env.spawn.closurePackageDir,
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
      // A loop iteration runs under the inherited env (not `buildChildRunEnv`),
      // so it is the one body birth path that writes no grants file of its own.
      // Materialize it here -- capping the container run's grants to the loop
      // body's declared resources -- so the body's childWorkflow grandchild
      // spawn is authorized. `definition` is the PRE-rewrite loop body.
      materializeLoopIterationGrants: async ({
        parentRunId,
        childRunId,
        definition,
      }) => {
        await capAndPersistChildGrants({
          deps: childRunDeps,
          directors: childRunDeps.directors ?? createDefaultDirectorRegistry(),
          definition,
          childRunId,
          parentRunId,
        });
      },
      scheduler,
      evaluateGrants: evaluateGrantsAdapter,
      loadParkedApproval,
      readParkedApprovalOps,
      // The same registry the warm agent's transport registers `watch`
      // callbacks into; `runWorkflowChild` routes each `mailbox.notify` to it.
      mailboxWatchRegistry,
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
