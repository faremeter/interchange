// Sidecar substrate factory for `runWorkflowChildFromProcessEnv`. Closes
// over the production substrate, the host scheduler, and the sidecar's
// grant-rule evaluator.
//
// Single-writer architecture: the supervisor is the workflow-run ref's
// only writer. The child opens a bare read-only store against the shared
// data dir and exposes a proxy whose `writeTreePreservingPrefix` forwards
// writes over the control IPC into the supervisor's substrate.

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
import type {
  AnnotatedPluginFactory,
  DirectorRegistry,
  ToolDeclaration,
} from "@intx/agent";
import { createDefaultDirectorRegistry } from "@intx/agent";
import {
  builtinCredentialProviders,
  createCredentialProviderRegistry,
  createHarnessRuntimeCapabilities,
  createInferenceCredentialResolver,
  driveConnectorReplies,
  type AgentEventStream,
  type ConnectorReplyDrain,
  type CredentialMaterialCell,
  type CredentialProviderRegistry,
} from "@intx/harness";
import { createSSHSignature } from "@intx/crypto";
import { parseRunAddress } from "@intx/types";
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
  walkWorkflowSteps,
  LOOP_BODY_DESCENT,
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
import { createWorkflowRunBlobSubstrate } from "../adapters/blob-substrate";
import { createWorkflowRunRepoStore } from "../adapters/repo-store";
import {
  createInMemorySpawnChild,
  createInMemorySpawnSuspendableChild,
  type RunChildWorkflow,
  type RunSuspendableChild,
} from "../adapters/spawn-child";
import {
  createWorkflowStepInvoker,
  type StepEnvBase,
} from "../adapters/step-invoker";
import {
  createDurableConversationRegistry,
  reconstructDurableConversation,
  type DurableConversationRegistry,
} from "../conversation-state";
import { readRunGrants, runGrantsPath } from "../run-grants";
import {
  adaptHostScheduler,
  createWorkflowHostScheduler,
  createWorkflowHostSignalChannel,
} from "../seams/index";
import {
  hashGrants,
  isErrnoNotFound,
  type CredentialsSnapshot,
} from "../supervisor/index";
import {
  loadWorkflowLoopFnsFromClosure,
  loadWorkflowPluginFactoriesFromClosure,
  loadWorkflowPluginToolDefinitionsFromClosure,
} from "../workflow-definition-loader";
import type { SubstrateFactory, SubstrateFactoryEnv } from "./from-process-env";
import { createMailboxWatchRegistry } from "./mailbox-watch-registry";
import type { ChildOutboundMailBridge } from "./outbound-mail-bridge";
import type { LoadParkedApproval } from "./parked-correlations";
import { createProxyWorkflowRunRepoStore } from "./proxy-repo-store";
import {
  createCredentialsBackedAuthorize,
  type CredentialsSnapshotRef,
  type GrantEvaluator,
  type RunWorkflowChildBindings,
  type SourcesSnapshotRef,
} from "./run-child";
import {
  attachStepCredentialWiring,
  attachStepTools,
  createToolBearingAgentFactory,
  deriveToolMarkFloorGrants,
  type StepToolCacheConfig,
  type StepToolMaterialization,
} from "./step-tools";
import {
  createSupervisorBackedTransport,
  type SupervisorBackedTransportInbound,
} from "./supervisor-backed-transport";

// No child-side pack-push pipeline: the supervisor's substrate is wrapped
// with the pack-pushing facade at the sidecar boot edge, so every
// workflow-run write fires the hub push. The child's proxy forwards
// `writeTreePreservingPrefix` over IPC into that wrapped substrate.

/**
 * Substrate-config keys the binary forwards into the factory. The helper
 * enforces presence-and-non-empty against this allowlist.
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
  // `{ [definitionId]: { [stepId]: InferenceSource[] } }` of every spawned
  // body's plaintext sources, decrypted sidecar-side. Always serialized, so a
  // missing key is a serialization bug and fails loud here.
  WORKFLOW_BODY_SOURCES: "string > 0",
  // Per-step tool-loader caps threaded from the boot edge; positive-finite
  // number strings at this boundary.
  SIDECAR_CACHE_MAX_BYTES: "string > 0",
  SIDECAR_REGISTRY_MAX_TARBALL_BYTES: "string > 0",
  // JSON-encoded custom inference adapter manifest; always serialized by the
  // boot edge, so a missing key is a serialization bug. Re-validated against
  // `AdapterManifest` in `parseAdapterManifest` before any module imports.
  SIDECAR_ADAPTER_MANIFEST: "string > 0",
}).onUndeclaredKey("ignore");

/**
 * Parse a substrate-config cap entry into a positive finite number. The boot
 * edge already validated it; this re-parse keeps the typed-config contract
 * honest rather than trusting the wire blindly.
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
 * Per-step inference-source table from `STEP_INFERENCE_SOURCES`, parsed and
 * validated once at construction and seeded into the run loop's mutable
 * sources reference. Each value is the step's ordered failover chain (element
 * 0 active), so the list is non-empty.
 */
const StepInferenceSourceTable = type({
  "[string]": InferenceSource.array().atLeastLength(1),
});
type StepInferenceSourceTable = typeof StepInferenceSourceTable.infer;

/**
 * Parse and validate the JSON-encoded `STEP_INFERENCE_SOURCES` entry.
 * Malformed input is rejected here with a structured error rather than
 * deferred to a deep-stack `buildEnv` failure.
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
 * Per-body per-step inference-source table from `WORKFLOW_BODY_SOURCES`, keyed
 * by definition id. Empty when the deployment spawns no bodies.
 */
const BodyInferenceSources = type({
  "[string]": StepInferenceSourceTable,
});
type BodyInferenceSources = typeof BodyInferenceSources.infer;

/** Parse and validate `WORKFLOW_BODY_SOURCES`; mirrors `parseStepInferenceSources`. */
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
 * Parse and validate the JSON-encoded `SIDECAR_ADAPTER_MANIFEST` entry.
 *
 * Trust boundary: the config is operator-supplied, so this re-validation is
 * defense-in-depth, not a trust upgrade (the channel already carries the
 * signing private key). Adapter specifiers must resolve from both the
 * sidecar's and the child's module roots, and adapter modules must be
 * import-side-effect-free: top-level side effects would run in the parent
 * and in every child.
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
 * Resolve a step's failover chain from the table. A lookup miss is a
 * supervisor-side programmer error. A scoped id resolves to its base: deploy
 * pins one source per base step, so every iteration shares it.
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
 * Pinned tool-package materialization. The sidecar process owns the
 * deploy-tree read; the factory calls this only on the pinned arm.
 */
type MaterializeStepTools = (args: {
  dataDir: string;
  mailboxAddress: string;
  stepId: string;
  stepCount: number;
  storeDir: string;
  cache: StepToolCacheConfig;
}) => Promise<StepToolMaterialization>;

/**
 * Dependencies `createSidecarSubstrateFactory` closes over. Tool
 * materialization and the child-grant cap are required: this package does
 * not import the modules that implement them. `createBareRepoStore` stays
 * optional so tests can inject an in-memory store.
 */
interface SidecarSubstrateFactoryDeps {
  materializeStepTools: MaterializeStepTools;
  collectDeclaredResources: (
    definition: WorkflowDefinition,
    directors: DirectorRegistry,
    pluginDefs: ReadonlyMap<string, readonly ToolDeclaration[]>,
  ) => ReadonlySet<string>;
  collectDeclaredCredentialConsumers: (
    definition: WorkflowDefinition,
    directors: DirectorRegistry,
    pluginDefs: ReadonlyMap<string, readonly ToolDeclaration[]>,
  ) => ReadonlySet<string>;
  filterGrantsToDeclaredResources: (
    parentGrants: readonly unknown[],
    declared: ReadonlySet<string>,
    credentialConsumers: ReadonlySet<string>,
  ) => readonly unknown[];
  /**
   * Override the bare-store constructor; tests inject an in-memory stub.
   * The bare store backs the child's read-only operations; writes go through
   * the proxy over IPC into the supervisor's substrate.
   */
  createBareRepoStore?: (config: {
    dataDir: string;
    signingKey: { publicKey: Uint8Array; privateKey: Uint8Array };
  }) => RepoStore;
}

/**
 * `CommitSigner` for per-step isogit stores: the factory's Ed25519 keypair in
 * `sshsig` shape, so agent-state commits match the production store's signing
 * surface.
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
 * Root directory for one step invocation's agent-state storage and workspace.
 * A distinct isogit repo OUTSIDE the workflow-run repo's tree (a nested git
 * repo would collide with its write contract), under a dedicated
 * `workflow-step-state/` sibling subtree, isolated per run and step.
 *
 * Resume-attempt invariant: a suspended step commits under `attempt-N` for its
 * attempt at suspend time; crash-resume reopens the SAME `attempt-N` store so
 * `rehydrateGates` finds the pending-op the delivered decision correlates
 * against. Reopening `attempt-1` for a retried step would rehydrate an empty
 * store and hang; `createSidecarStepBuildEnv` asserts this on the cold path.
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
 * Per-run scratch root (`workflow-step-state/<repoId>/runs/<runId>/`) for the
 * cold path's per-step stores; reclaiming it drops every step/attempt the run
 * produced in one removal.
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
 * Stable per-agent scratch root for the WARM single-step agent (workspace +
 * tool materialization), keyed by step identity like the durable conversation
 * store -- not by the first-message runId -- so the cached agent reuses one
 * workspace across every message and after respawn. Reclaimed on undeploy;
 * rooted under a `warm/` sibling of the cold `runs/` subtree so one undeploy
 * sweep reclaims both keyings.
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
 * per-attempt isogit store. Returns an empty list when the store directory is
 * absent: `createIsogitStore` would `mkdir` and init a fresh repo for a
 * non-existent dir, so the `directoryExists` guard keeps the read a read.
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
 * Read a warm (single-step) parked agent's pending operations from its
 * conversation-store mirror under `agent-state/<stepId>/`. Reconstructed
 * read-only -- NOT through `DurableConversationRegistry.acquire`, whose first
 * acquire writes a substrate restore and would front-run the warm agent's own
 * restore ordering (a respawned child has not rebuilt the live store when
 * re-registration runs). Empty when no durable state exists.
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
 * Project pending operations to the minimal approval records the resume
 * classifier needs: only `approval` ops, keeping correlationId and the
 * optional deadline. The runtime reconstructs the lost `SignalAwaited` from
 * those alone and must not see the reactor's internals.
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
   * Deployment mailbox address threaded into the child: locates each step's
   * deploy tree for tool materialization AND is the step agent's outbound
   * mail `address` (§3a).
   */
  mailboxAddress: string;
  /**
   * Step count of the deployed definition (`stepOrder.length`). Selects the
   * head/step collapse in `stepDeployTreeDir`: single-step reads at the head,
   * multi-step at the per-step address.
   */
  stepCount: number;
  /**
   * Child-side outbound-mail bridge (§3a). Wrapped in the supervisor-backed
   * `MessageTransport` the step agent gets as `env.transport`; sends route to
   * the supervisor for the actual signed send -- the agent never holds the
   * signing key.
   */
  outboundMailBridge: ChildOutboundMailBridge;
  /**
   * Inbound surface for the supervisor-backed transport (§3b): the shared
   * watch registry, the mutation bridge for flag writes and expunge, and the
   * call bridge. Absent for a build that owns no inbound mailbox (a spawned
   * child or an onTrigger body), whose transport inbound stays inert.
   */
  inbound?: SupervisorBackedTransportInbound;
  /** Per-step tool-loader caps (cache + registry tarball size). */
  cache: StepToolCacheConfig;
  /**
   * Adapter registry the step agent resolves inference adapters through, set
   * on `env.deps`. Built at boot from the validated manifest so a custom
   * provider source resolves in the child as on the sidecar main path;
   * without it the agent would fall back to built-ins only.
   */
  adapters: AdapterRegistry;
  /**
   * Durable-conversation registry for the warm single-step agent (§3c).
   * When present, swaps the per-run isogit `ContextStore` for a per-agent
   * durable store mirrored to the workflow-run substrate, restoring the prior
   * conversation before the env returns. Absent for a multi-step deploy,
   * whose agents are not warm/long-lived.
   */
  durableConversation?: DurableConversationRegistry;
  /**
   * Record the step's tool-mark floor grants, keyed by base step id. The env
   * builder is the only place the child holds the loaded factories' static
   * `definitions`, so it derives the floor here; the grant evaluator merges it
   * under the snapshot's grants at authorization time. Keyed by base id so a
   * `map` iteration shares its base step's floor.
   */
  recordToolMarkFloor: (baseStepId: string, grants: GrantRule[]) => void;
  /**
   * Feed the step agent's OWN evaluated tool factories into the
   * materialization slot instead of reading a pinned manifest off the deploy
   * tree. The source-ref lineage stages no manifest, so this arm runs a
   * source workflow's tools. No tool-mark floor is recorded: a source tool's
   * bare `definition.name` grant already came from the capability walk, so
   * the snapshot authorizes it directly.
   */
  sourceTools: boolean;
  /**
   * Sidecar-local directory of the materialized workflow-definition closure,
   * source-ref only. The source arm materializes each step agent's declared
   * plugin packages from this already-laid-out closure (no re-download) and
   * feeds the resulting factories into the per-step plugin chain -- the only
   * channel that reaches an `env.plugins` a source workflow's posix bundle
   * consumes. Undefined on a pinned-tool build.
   */
  closurePackageDir?: string;
  /** Pinned tool-package materialization. Unused when `sourceTools` is set. */
  materializeStepTools: MaterializeStepTools;
}

/**
 * Materialize a source-ref step agent's declared plugin factories from the
 * frozen closure (already laid out on disk; load-only, no re-download).
 * Empty when no plugins are declared; fails closed when plugins are declared
 * but the closure dir is absent -- dropping them would run the workflow
 * without the tools it declared.
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
 * Per-run credential inputs the step's `credentials` wiring resolves from:
 * the live material cell and grants resolver ride in from the run child;
 * the provider registry is sidecar-static. Absent when no material cell was
 * threaded, which leaves the step's inference reader and tool credentials
 * unwired.
 */
interface SidecarStepCredentialContext {
  readonly materialCell: CredentialMaterialCell;
  /** The step's grants, resolved live by base step id (typed `unknown[]`; cast to `GrantRule[]` where `evaluateGrants` reads them). */
  readonly resolveStepGrants: (stepId: string) => readonly unknown[];
  readonly providers: CredentialProviderRegistry;
}

/**
 * The step-invoker `buildEnv` callback the workflow-host adapter consumes,
 * pulled out so per-step env construction is observable without the full
 * substrate. Resolves the per-step `InferenceSource`, stands up per-step
 * isogit storage and workspace, and surfaces construction failures here (the
 * single-step path always runs a real agent against real storage).
 */
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
    // Resolve against the live table each build so a rotation that landed
    // before this build is reflected in the agent it constructs (a built warm
    // agent does not pass through here again).
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
    // The resolver guarantees a non-empty chain; assert it so the reactor's
    // initial-source pin (element 0) is a checked fact.
    const activeSource = sources[0];
    if (activeSource === undefined) {
      throw new Error(
        `sidecar workflow-child step invoker buildEnv: empty InferenceSource chain pinned for stepId ${JSON.stringify(stepId)}`,
      );
    }

    // Root the per-step scratch (workspace + tool cache + apply-state). Cold
    // keys per run/step/attempt and reclaims the run's whole subtree on
    // completion; warm keys per agent so the cached agent reuses one workspace
    // across messages and respawns, reclaimed on undeploy. The `runs/` and
    // `warm/` sub-roots are disjoint so neither sweep touches the other.
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
    // Conversation storage: warm single-step agents need the conversation to
    // survive respawn, so it is backed by a per-agent durable store mirrored
    // to the workflow-run substrate (§3c), restoring the prior conversation
    // before the reactor loads. A multi-step deploy keeps the per-run isogit
    // store; only the conversation context is durable across runs (workdir +
    // tools stay per-run).
    const storage: ContextStore & AuditStore =
      deps.durableConversation !== undefined
        ? (await deps.durableConversation.acquire(stepId)).storage
        : await createIsogitStore(storeDir, deps.signer);

    // Cold-path resume keying guard for the `stepStorageRoot` invariant: an
    // approval resume must find a `suspendedCall`-bearing pending op for its
    // correlationId (the reactor re-runs the approved call); an async-tool
    // marker shares `kind: "approval"` but has no `suspendedCall`, so on
    // resume the reactor clears its gate without re-running. A miss means the
    // wrong attempt's store was reopened (gateless reactor, silent hang) or
    // only an async marker matched (call silently skipped) -- fail loud here,
    // the single seam that opened the store and knows the gate must be
    // present. Warm keys per agent and rehydrates from a different lifecycle,
    // so this is cold-path only; an `"input"` resume names a re-arm channel,
    // never a gate, so it is exempt.
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

    // Assemble the step's tool runtime. Two arms:
    //
    //   - Source-ref (`sourceTools`): feed the step agent's OWN evaluated
    //     `req.agent.toolFactories` into the slot (bare-named
    //     `AnnotatedToolFactory`s); the source deploy stages no manifest, so
    //     `materializeStepTools` would find nothing here. Plugin factories
    //     (no agent slot, so this arm cannot carry them) materialize from the
    //     frozen closure into the same `pluginFactories` slot pinned packages
    //     fill.
    //
    //   - Pinned packages (`materializeStepTools`): materialize the pinned
    //     closure from its on-disk deploy tree, rooted per step under
    //     `storeDir` so concurrent steps never collide on the tarball cache
    //     or apply-state. No manifest yields empty tools; a broken manifest
    //     surfaces loudly.
    const materialization: StepToolMaterialization =
      deps.sourceTools === true
        ? {
            factories: req.agent.toolFactories.map((factory) => ({
              packageName: factory.id,
              declaredCredentials: [],
              factory,
            })),
            pluginFactories: await materializeSourcePluginFactories(deps, req),
          }
        : await deps.materializeStepTools({
            dataDir: deps.dataDir,
            mailboxAddress: deps.mailboxAddress,
            stepId,
            stepCount: deps.stepCount,
            storeDir,
            cache: deps.cache,
          });

    // Derive and record the tool-mark floor from the loaded factories' static
    // definitions: a pinned tool never reached the hub's capability walk, so
    // the floor lets the evaluator authorize it against its own static mark.
    // Keyed by base step id so a `map` iteration shares its base step's
    // floor. Skipped on the source-ref lineage, whose tools already carry a
    // `tool:<name>` grant from the walk.
    if (deps.sourceTools !== true) {
      deps.recordToolMarkFloor(
        baseStepId(stepId),
        deriveToolMarkFloorGrants(
          materialization.factories.map((f) => ({
            packageName: f.packageName,
            definitions: f.factory.definitions,
          })),
        ),
      );
    }

    // Supervisor-backed transport for the step agent's mail tools (§3a/§3b).
    // `send` routes over control IPC to the supervisor, which signs through
    // the host transport as `address`; inbound (warm single-step only)
    // forwards every other mailbox method, failing as unwired when absent.
    const transport = createSupervisorBackedTransport(
      deps.outboundMailBridge,
      deps.mailboxAddress,
      deps.inbound,
    );

    // The host assembles the `RuntimeCapabilities` bag (currently
    // `mail.transport`) onto `env.capabilities`; bundles consume the bag
    // rather than re-wrapping a raw env key.
    const capabilities = createHarnessRuntimeCapabilities({ transport });

    // The step env extends `BaseEnv` with the tool/transport fields below.
    const env: StepEnvBase & {
      toolCwd: string;
      transport: MessageTransport;
      address: string;
      capabilities: RuntimeCapabilities;
    } = {
      // Full ordered failover chain, pinned to element 0; the reactor fails
      // over forward through `sources`.
      sources,
      defaultSource: activeSource.id,
      storage,
      workdir,
      // The workspace is both the lock boundary and the filesystem tools'
      // tree; for a deployed step the two coincide.
      toolCwd: workdir,
      audit: storage,
      directors: createDefaultDirectorRegistry(),
      // Boot-built adapter registry (built-ins + custom), so a custom-provider
      // source resolves as on the sidecar main path.
      deps: createDependencies(deps.adapters),
      transport,
      address: deps.mailboxAddress,
      capabilities,
    };
    // Carry the materialized tool runtime to the `agentFactory` via the env's
    // symbol-keyed slot; the step-invoker spreads the env, and spread
    // preserves own symbol-keyed properties.
    attachStepTools(env, materialization);
    // Attach the credential wiring so `agentFactory` can assemble each
    // bundle's consumer-scoped `credentials` capability. Grants are wired as
    // a THUNK, resolved only when a package needs the capability, so a step
    // with no credential-consuming package (e.g. a self-discovery resume
    // before the grants barrier) never faults on a missing snapshot; a
    // credential-consuming tool with no context threaded fails closed at its
    // own `resolve("credentials")`.
    if (credentialContext !== undefined) {
      // Inference resolves its source secret from the same live cell, by
      // `credentialId`; the step's sources carry no inline key and the child
      // never holds the cipher key.
      env.readCurrentMaterial = createInferenceCredentialResolver(
        credentialContext.materialCell,
      );
      attachStepCredentialWiring(env, {
        materialCell: credentialContext.materialCell,
        resolveGrants: () =>
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- resolveStepGrants returns unknown[]; evaluateGrants reads GrantRule
          credentialContext.resolveStepGrants(stepId) as readonly GrantRule[],
        providers: credentialContext.providers,
      });
    }
    return env;
  };
}

/**
 * Per-step invoker for a spawned child's steps (childWorkflow and onTrigger
 * bodies alike). Widens the runtime `StepInvoker` with the child's
 * credentials-backed `authorize`, its own per-step `sourcesRef` (built fresh
 * per spawn, disjoint from the top level's), and the parent run's `onEvent`
 * funnel.
 */
export type SidecarChildStepInvoker = (
  req: StepInvokeRequest,
  authorize: WorkflowAuthorizeFn,
  sourcesRef: SourcesSnapshotRef,
  onEvent: (event: InferenceEvent) => void,
  credentialContext?: SidecarStepCredentialContext,
) => Promise<StepInvokeResult>;

/**
 * Inputs for the sidecar's in-process child runtime, lifted out of
 * `createSidecarSubstrateFactory` so the implementation is exercisable in
 * isolation.
 *
 * Sub-namespace scoping: the child runs under `childRunId`, which the runtime
 * threads through every `repoStore` / `blobs` / `signalChannel` call, so its
 * events land under `runs/<childRunId>/events/` in the parent's repo.
 *
 * Substrate identity: the child reuses the parent's wrapped `RepoStore`, so a
 * successful child write fires the same hub pack push; the parent's
 * workflow-process principal is reused verbatim.
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
   * Step invoker the child runtime delegates per-step invocations to. Child
   * stepIds are disjoint from the parent's, so the parent's
   * `STEP_INFERENCE_SOURCES`-pinned `buildStepEnv` would error on every child
   * step; callers supply a SEPARATE invoker that runs a real tool-bearing
   * agent against the child's own staged sources.
   *
   * Also receives the child's credentials-backed `authorize`: the runtime
   * calls `env.invokeStep` with the request only, so the invoker is the seam
   * that gates each tool call against the run's grants.
   */
  invokeStep: SidecarChildStepInvoker;
  /**
   * Every spawned body's plaintext inference-source table, keyed by definition
   * id, decrypted sidecar-side and delivered through the spawn env (the child
   * never holds the cipher key). A body absent from the set -- a legacy
   * record predating record-carried body sources -- falls back to the on-disk
   * file.
   */
  bodySources: BodyInferenceSources;
  /** Sidecar data dir: legacy fallback source file, and the root for per-step storage. */
  dataDir?: string;
  /** Grant evaluator the child's `authorize` delegates to; the parent factory's adapter, so child and parent steps resolve against the same grammar. */
  evaluateGrants: GrantEvaluator;
  /** Sidecar-static credential provider registry, shared with the top level; combined with the run's live material cell and capped grants per build. */
  credentialProviders: CredentialProviderRegistry;
  /** Director registry the child runtime uses; defaults to the canonical built-ins. */
  directors?: DirectorRegistry;
  /**
   * Sidecar-local directory of the materialized workflow-definition closure
   * (source-ref only). `buildChildRunEnv` loads the body's declared plugin
   * tool definitions from it so plugin-contributed `tool:<name>` grants are
   * declared when capping the child's inherited grants. Absent on a lineage
   * that stages no closure.
   */
  closurePackageDir?: string;
  /** Clock for timestamp generation; defaults to `() => new Date()`. */
  clock?: () => Date;
  /**
   * Random id generator for run ids, signal ids, timer ids; defaults to
   * a monotonic counter combined with a random suffix.
   */
  newId?: (prefix: string) => string;
  /**
   * Collect the resources the child body declares, the credential consumers
   * it instantiates, and narrow the parent grant array to that pair. The
   * process that boots the child supplies all three. This factory persists
   * the filter's array and does not import the walk.
   */
  collectDeclaredResources: (
    definition: WorkflowDefinition,
    directors: DirectorRegistry,
    pluginDefs: ReadonlyMap<string, readonly ToolDeclaration[]>,
  ) => ReadonlySet<string>;
  collectDeclaredCredentialConsumers: (
    definition: WorkflowDefinition,
    directors: DirectorRegistry,
    pluginDefs: ReadonlyMap<string, readonly ToolDeclaration[]>,
  ) => ReadonlySet<string>;
  filterGrantsToDeclaredResources: (
    parentGrants: readonly unknown[],
    declared: ReadonlySet<string>,
    credentialConsumers: ReadonlySet<string>,
  ) => readonly unknown[];
}

/**
 * Write a spawned child's inherited grants to its own
 * `runs/<childRunId>/grants.json`. The write names the shallow
 * `runs/<childRunId>/` prefix, so it rebuilds that level with just
 * `grants.json`; safe ONLY because the sole caller is write-once at run
 * birth, before any event is appended -- over a populated run it would delete
 * the committed `events/`/`blobs/` subtrees. A later event append names the
 * nested `events/` prefix, so it never reaches `grants.json` one level up.
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
 * Construct the `RunChildWorkflow` callback the spawn-child adapter delegates
 * to: builds a fresh `WorkflowRuntimeEnv` scoped to `childRunId`, invokes
 * `runtimeRun`, and returns the child's terminal status.
 *
 * Abort propagation: the parent's `signal` wires to the child's local-abort
 * seam, so an abort fails the in-flight step and the run settles `failed`.
 * No durable `CancelRequested`: an in-process child cannot sign one.
 *
 * Resource lifecycle: the per-run signal channel is `stop()`ped in a finally
 * so its `subscribeKind` loop tears down before the callback returns.
 */
export function createSidecarRunChild(
  deps: SidecarRunChildDeps,
): RunChildWorkflow {
  const directors = deps.directors ?? createDefaultDirectorRegistry();
  const clock = deps.clock ?? defaultClock;
  const newId = deps.newId ?? defaultNewId;
  // No `controlPlanePrincipal`: an in-process child tears down through the
  // local-abort seam, never a durable cancel. One store shared across every
  // child this factory spawns.
  const repoStore = createWorkflowRunRepoStore({
    substrate: deps.substrate,
    repoId: deps.workflowRunRepoId,
    principal: deps.principal,
    ref: deps.workflowRunRef,
  });
  // Self-referential so a child env's recursive `spawnChild` routes grandchild
  // spawns back through the same adapter; recursion bottoms out at a rung with
  // no `childWorkflow`. Sub-namespace scoping holds at every depth.
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
      // Terminal: the caller awaits this child's terminal; no signal is
      // addressable to it, so a park has nothing to answer it.
      hasUpstreamSignalResolver: false,
      ...(credentialMaterial !== undefined
        ? { materialCell: credentialMaterial }
        : {}),
    });
    try {
      // Thread this rung's depth/ceiling into the child run so its own
      // childWorkflow spawns keep counting against the tree-wide bound
      // (otherwise the recursion would reset to depth 0 each rung). A parent
      // abort tears the child down through the local-abort seam -- the proxy
      // substrate cannot sign a `CancelRequested` -- so an in-flight step
      // fails and the child settles `failed` under its own principal.
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
 * Construct the `RunSuspendableChild` callback the suspendable-spawn adapter
 * delegates to: the park-aware analog of {@link createSidecarRunChild},
 * returning a live {@link SuspendableChildHandle} the caller drives across
 * the body's approval parks.
 *
 * Park surfacing: the env's `onPark` translates control-plane parks into the
 * handle's `next()` stream; an `"approval"` park is proxied up on the same
 * correlation and the granted decision returns through `resume` onto the
 * child's own signal channel. An `"input"` park (a nested onTrigger re-arm)
 * is unserviceable -- the caller proxies approvals only -- and surfaces as a
 * hard error on `next()` rather than hanging.
 *
 * Signal-channel lifecycle: kept alive across every park (so `resume` can
 * deliver) and torn down at the run's terminal -- per-`next()` teardown would
 * leak the channel when a parent abort stops `next()` mid-park.
 *
 * Abort propagation: the parent's `signal` tears the body down through
 * `createSuspendableChildHandle`'s local-teardown seam; the body cannot sign
 * a durable `CancelRequested`, so on abort its step fails and the run settles
 * `failed` (not `cancelled`).
 */
export function createSidecarSpawnSuspendableChild(
  deps: SidecarRunChildDeps,
): RunSuspendableChild {
  const directors = deps.directors ?? createDefaultDirectorRegistry();
  const clock = deps.clock ?? defaultClock;
  const newId = deps.newId ?? defaultNewId;
  // No `controlPlanePrincipal`: an in-process body tears down through the
  // shared handle's local-abort seam, never a durable cancel.
  const repoStore = createWorkflowRunRepoStore({
    substrate: deps.substrate,
    repoId: deps.workflowRunRepoId,
    principal: deps.principal,
    ref: deps.workflowRunRef,
  });
  // A body's own `childWorkflow` grandchildren spawn terminal-only; a nested
  // onTrigger inside a body fails loud rather than silently spawning.
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
      // The live event sink is always threaded: a body step and a grandchild
      // childWorkflow step both run a real agent through `deps.invokeStep`.
      onEvent,
      // Park-aware: the container drives this body across its parks and
      // relays a decision back onto the body's own channel, so a park here is
      // answerable even though the body run carries no address of its own.
      hasUpstreamSignalResolver: true,
      ...(credentialMaterial !== undefined
        ? { materialCell: credentialMaterial }
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
 * Cap the parent run's grants to what `definition` declares and persist them
 * as the child's own `runs/<childRunId>/grants.json` -- the ceiling the next
 * spawn hop reads back. Returns the capped set so a fresh child env can key
 * its credentials snapshot on them.
 *
 * `definition` MUST be the PRE-rewrite body, its childWorkflow grandchildren
 * still INLINE: both collectors skip a `{ ref }` body, so a rewritten
 * definition would drop the grandchild's declared resources and the factories
 * only its inline body instantiates, and under-authorize it. Every birth path
 * materializes a run's grants file, so a
 * missing parent file is a defect, not a run that legitimately holds none --
 * fail closed. The cap only removes rules (the parent stays the ceiling),
 * matching the top level's "authority bounded by declared capabilities" model.
 *
 * The grants file is WRITE-ONCE per run (a run's ceiling is fixed at birth);
 * a re-write on resume is CORRUPTING: it commits through a writer separate
 * from the runtime's event-log writer, so a re-write racing the replay
 * re-appends on the shared repo and regresses another run's event seq, and
 * its subtree-delete drops the run's committed `events/`/`blobs/`.
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
  const pluginDefs: ReadonlyMap<string, readonly ToolDeclaration[]> =
    deps.closurePackageDir === undefined
      ? new Map<string, readonly ToolDeclaration[]>()
      : await loadWorkflowPluginToolDefinitionsFromClosure({
          packageDir: deps.closurePackageDir,
          plugins: collectDeclaredPluginNames(definition),
        });
  const declaredResources = deps.collectDeclaredResources(
    definition,
    directors,
    pluginDefs,
  );
  const credentialConsumers = deps.collectDeclaredCredentialConsumers(
    definition,
    directors,
    pluginDefs,
  );
  const childGrants = deps.filterGrantsToDeclaredResources(
    parentGrants,
    declaredResources,
    credentialConsumers,
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
 * Build the per-`childRunId` `WorkflowRuntimeEnv` a spawned child runs
 * against: inherit the parent's grants, assemble the child's credentials
 * snapshot, wire per-run repo store / blob substrate / signal channel plus a
 * recursive `spawnChild`. Returns the child's signal channel so the caller can
 * `stop()` it once the child settles. Shared by both child-drive callers.
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
  /** Per-run live inference-event sink from the parent run's channel; required for both paths (a missing sink would silently drop hub-stream events). */
  onEvent: (event: InferenceEvent) => void;
  /**
   * The parent run's live credential-material cell, so the child's inference
   * resolves its source secret by `credentialId` against the current delivery.
   * Absent when a non-sidecar executor carries no material, which leaves the
   * child's inference reader unset.
   */
  materialCell?: CredentialMaterialCell;
  /**
   * Whether a park in this child can be answered from outside it. The two
   * shared seams differ precisely here: a suspendable body's container relays
   * a decision back down, while a terminal child has no address or relay.
   */
  hasUpstreamSignalResolver: boolean;
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
    onEvent,
    materialCell,
  } = args;
  // Lift any inline `childWorkflow` grandchildren to `{ ref }` form (the shape
  // the runtime dispatches), keeping the lifted definitions in an in-memory
  // map so a grandchild spawns with no on-disk read at any depth.
  const { workflow: rewrittenDefinition, bodies: grandchildBodies } =
    rewriteInlineChildWorkflowBodies(definition);
  const grandchildMap = new Map(
    grandchildBodies.map((b) => [b.ref, b.definition]),
  );
  // Register any nested `loop` bodies so a loop can run here: a ref-keyed copy
  // for the loop-iteration host, the pre-rewrite form for the per-iteration
  // grant cap, and each loop body's own grandchildren merged into
  // `grandchildMap`. Mirrors the top-level registration in run-child.ts.
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
  // Resolve loop while/carry fns from the closure only when the body contains
  // a loop; a loop with no closure wired is a wiring defect and fails loud.
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
  // Cap the parent's grants to what this body declares and persist them as the
  // child's own grants file (a grandchild's ceiling). `definition` is the
  // childWorkflow-inline form, so the cap keeps a grandchild's resources.
  const childGrants = await capAndPersistChildGrants({
    deps,
    directors,
    definition,
    childRunId,
    parentRunId,
  });
  // The child's credentials snapshot applies the capped grant set uniformly
  // across every step the child definition declares, keyed on each step's id.
  // The in-process child has no per-step mail address, so `address` mirrors
  // the step id (the authorize reads only `grants`).
  //
  // "Every step" reaches past `stepOrder` into this body's `loop` bodies: a
  // loop iteration runs under the inherited env, so its steps authorize
  // against THIS snapshot under their own plain ids -- a `stepOrder`-only
  // snapshot would leave a nested loop with no entry.
  const credentialStepIds = new Set<string>();
  walkWorkflowSteps({
    definition: rewrittenDefinition,
    descent: LOOP_BODY_DESCENT,
    context: "sidecar child credentials snapshot: ",
    visit: ({ stepId }) => {
      credentialStepIds.add(stepId);
    },
  });
  const contentHash = await hashGrants(childGrants);
  const credentialsSnapshot: CredentialsSnapshot = {
    steps: [...credentialStepIds].map((stepId) => ({
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
  // `env.authorize` binds to the inherited credentials snapshot and delegates
  // to the parent factory's grant evaluator; the `invokeStep` wrapper consults
  // it per tool call, and an action step's `EffectContext` calls it directly
  // per effect.
  const credentialsRef: CredentialsSnapshotRef = {
    current: credentialsSnapshot,
  };
  const authorize = createCredentialsBackedAuthorize(
    credentialsRef,
    deps.evaluateGrants,
  );
  const drain = createNoopDrainController(rewrittenDefinition);
  // Both paths run a real agent funnelling live events to the parent run's
  // channel, so `onEvent` is required; a missing one is a wiring defect.
  if (onEvent === undefined) {
    throw new Error(
      "sidecar child: onEvent is missing; child inference events would be silently dropped from the hub stream",
    );
  }
  const childOnEvent = onEvent;
  // Fresh per spawn, disjoint from the top level's mutable table so a
  // top-level rotation never leaks into a child; sources arrive plaintext,
  // decrypted sidecar-side.
  const sourcesRef: SourcesSnapshotRef = {
    current: await resolveBodyStepSources(deps, rewrittenDefinition.id),
  };
  // Recursive `spawnChild`: an inline grandchild resolves from the in-memory
  // map and flows back into this same `runChild`; its agent steps ride this
  // run's event funnel.
  const spawnHost = createInMemorySpawnChild({
    bodies: grandchildMap,
    runChild,
  });
  const spawnChild: WorkflowRuntimeEnv["spawnChild"] = (spawnInput) =>
    spawnHost(spawnInput, childOnEvent, materialCell);
  // Assemble the per-step credential context from the run's live material
  // cell when one is present, so the child's inference resolves its source
  // secret by `credentialId` against the parent's delivery and tool
  // `credentials` resolve against the capped grants; absent when no material
  // was threaded, leaving the inference reader unset.
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
  // Per-step invocation seam: the runtime invokes `env.invokeStep` with the
  // request alone; the wrapper forwards the child's authorize, `sourcesRef`,
  // event funnel, and credential context. One invoker serves both paths,
  // running a real tool-bearing agent against the same `sourcesRef`.
  const invokeStep: WorkflowRuntimeEnv["invokeStep"] = (req) =>
    deps.invokeStep(
      req,
      authorize,
      sourcesRef,
      childOnEvent,
      childCredentialContext,
    );
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
    hasUpstreamSignalResolver: args.hasUpstreamSignalResolver,
    ...(loopFns !== undefined ? { loopFns } : {}),
  };
  // Wire loop-iteration spawning for a `loop` nested in this body, assigned
  // AFTER the env literal because the iteration host closes over `env`: an
  // iteration re-enters THIS body env, inheriting its step invoker, capped
  // grants, and in-memory spawnChild. Replicates the top-level loop host in
  // run-child.ts (the grants seam differs: here
  // `capAndPersistChildGrants` is called directly, the top level injects it).
  //
  // Boundary: the suspendable seam services APPROVAL parks only, so an
  // iteration awaiting an external signal, or re-arming an onTrigger, cannot
  // park on an external input channel.
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
        // Cap the iteration's grants against the body run's own and persist
        // them before the iteration appends its first event.
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
 * primary path is the delivered `deps.bodySources`, decrypted sidecar-side
 * (the child holds no cipher key).
 *
 * LEGACY FALLBACK: a body absent from the delivered set -- a record written
 * before body sources moved into the record -- reads its on-disk plaintext
 * `sources.json`. Removable once no restorable record predates that move.
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
 * staged by a pre-record deploy. Only reached for a body the delivered set
 * does not carry, so a missing or malformed file is a defect.
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
 * Build a `SubstrateFactory` closed over the supplied dependencies (the
 * sidecar binding passes tool materialization and the grant cap).
 *
 * Construction: validate the `substrateConfig` against the typed schema;
 * open the bare read-only `RepoStore`; wrap it in the write-forwarding proxy
 * (the supervisor runs writes under its per-repo lock); start the host
 * scheduler; build the `invokeStep` and `spawnChild` adapters; return the
 * bindings.
 */
export function createSidecarSubstrateFactory(
  deps: SidecarSubstrateFactoryDeps,
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

    // Build the child's adapter registry eagerly at boot so a bad specifier
    // crashes loudly at construction rather than degrading to built-ins-only
    // at first resolve; the sidecar's closure registry cannot cross the fork.
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

    // Proxy substrate: writes forward over IPC into the supervisor's
    // substrate; reads consult the bare store. The supervisor is the ref's
    // sole writer, so child writes never race its claim-check writes.
    const substrate: RepoStore = createProxyWorkflowRunRepoStore({
      bareStore,
      bridge: env.substrateWriteBridge,
      workflowRunRepoId,
    });

    // INBOUND half of mailbox ownership (§3b): one watch registry per child,
    // shared by the step agent's transport (`watch` registers callbacks here,
    // backing `mail_wait`) and the control loop (which fires each
    // `mailbox.notify` into it). Rides out on the bindings so the run loop
    // routes notifications to the same instance.
    const mailboxWatchRegistry = createMailboxWatchRegistry();
    const transportInbound: SupervisorBackedTransportInbound = {
      watchRegistry: mailboxWatchRegistry,
      mutationBridge: env.mailboxMutationBridge,
      callBridge: env.mailboxCallBridge,
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

    // The single-step / top-level path runs a real agent: the env builder
    // stands up real per-step storage/workdir/audit/directors rooted under
    // the run; the step-invoker instantiates the agent and drives the
    // input-to-reply turn.
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

    // Durable-conversation registry for the warm single-step agent (§3c),
    // built only when the deployment is warm-kept: the sole long-lived
    // agent's conversation must survive child respawn, so it is mirrored to
    // the workflow-run substrate at a per-agent path; a multi-step deploy
    // leaves this `undefined` (per-step agents are not warm). On respawn the
    // child rebuilds it empty and each store restores its prior snapshot on
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

    // Per-step tool-mark floor grants, keyed by base step id: derived and
    // recorded by the env builder from the step's materialized factories,
    // merged under the snapshot's grants at authorization so a pinned tool
    // authorizes against its own static mark. Lives for the factory's
    // lifetime so a warm agent's floor -- recorded on its first build -- stays
    // available for later tool calls.
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
      materializeStepTools: deps.materializeStepTools,
      // Source-ref is the only deploy lineage: the child runs each step
      // agent's own evaluated tool factories from the materialized closure.
      sourceTools: true,
      closurePackageDir: env.spawn.closurePackageDir,
      // Warm agent's inbound mail surface; the spawned-child build omits it
      // (a spawned child owns no warm inbound mailbox).
      inbound: transportInbound,
      ...(durableConversation !== undefined ? { durableConversation } : {}),
    });

    // The tool-bearing agent factory attaches the per-step env's materialized
    // tool runtime to the `AgentDefinition`, builds the plugin chain, and
    // wraps `agent.close()` so every plugin (LSP subprocess included) and
    // tool bundle tears down with the agent on every exit path.
    const stepAgentFactory = createToolBearingAgentFactory();

    // The credential provider registry, built once from the sidecar-static
    // built-ins and shared by every per-step build; the per-run material and
    // grants ride in separately at each invoke.
    const credentialProviders = createCredentialProviderRegistry(
      builtinCredentialProviders(),
    );

    // Spawned-child step build env: every spawned child's steps -- a
    // childWorkflow child's and an onTrigger body's alike -- run real,
    // TOOL-BEARING agents through the same source-tools arm the top level
    // uses. Built COLD per invocation (no warm hooks, no inbound: each spawn
    // is a fresh run). No tool-mark floor is recorded -- the recorder
    // throw-asserts that -- because a source tool's bare `tool:<name>` grant
    // is already in the snapshot.
    //
    // The source arm also keeps the body's tools scoped by construction: a
    // body child runs under the PARENT deployment's mailbox/stepCount, so a
    // colliding body step id would read the PARENT step's tools off a deploy
    // tree; feeding each agent its own evaluated `req.agent.toolFactories`
    // never consults that tree.
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
      materializeStepTools: deps.materializeStepTools,
      sourceTools: true,
      closurePackageDir: env.spawn.closurePackageDir,
    });
    // Spawned-child step invoker: runs a real agent per step, resolving
    // inference against the child's own `sourcesRef` and funnelling live
    // events to the parent run's channel. `buildChildRunEnv` threads in the
    // run's `credentialContext`; a tool declaring a credential consumer fails
    // closed at its own `resolve("credentials")` when none was threaded.
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

    // Adapt the workflow-runtime `StepInvoker` shape onto the host's
    // `ChildStepInvoker` shape. The host's `onEvent` is the child's per-run
    // event-channel sink (`onEvent -> event-channel sender -> supervisor ->
    // publishWorkflowInferenceEvent -> hub timeline`).
    //
    // `authorize` is the child's credentials-backed closure threaded in from
    // `run-child.ts`. The runtime gates EVERY tool call through `env.authorize`
    // (`resource = tool:<name>`, `action = "invoke"`), so each call resolves
    // against the per-step grant snapshot the supervisor pushed over control
    // IPC. The deploy-time capability walk bounds the toolset a deploy may
    // carry; the grant snapshot decides which of those the agent may invoke.
    //
    // A fresh invoker is built per invocation so the agent's event stream
    // subscribes to THIS step's `onEvent`; the env builder and agent factory
    // are pinned, the event sink and authorize vary per step.
    //
    // `warmCache` (§3b) is the run-loop's per-deployment warm-agent cache,
    // present only for the single-step long-lived deployment: when supplied
    // the adapter builds the agent once and reuses it across messages;
    // otherwise instantiate-send-teardown per step.
    // Run-boundary durability flush (§3c): when warm-kept, mirror the warm
    // agent's conversation snapshot to the substrate after each message's send
    // settles, keyed by the step identity the env builder filed the durable
    // store under. Absent for a multi-step deploy.
    const onRunBoundary: ((key: string) => Promise<void>) | undefined =
      durableConversation !== undefined
        ? async (key: string) => {
            await durableConversation.get(key).mirrorToSubstrate();
          }
        : undefined;

    // Connector-thread seed (§3c): when warm-kept, route each mail-derived
    // inbound message onto the warm agent's connector thread before its send
    // so the reply path has thread state. Keyed by the step identity.
    const seedInbound:
      | ((key: string, message: InboundMessage) => Promise<void>)
      | undefined =
      durableConversation !== undefined
        ? async (key: string, message: InboundMessage) => {
            await durableConversation.get(key).seedInbound(message);
          }
        : undefined;

    // Connector reply drain (§3c): when warm-kept, drive the warm agent's
    // outbound replies through the shared drain -- compose a threaded reply
    // from the durable store, send it via `bridge.submit` (the same
    // signed-send path the agent's own transport uses), then advance the
    // thread from the receipt. Established once per warm agent over its
    // lifetime stream; the handle carries the lifetime `done` promise and the
    // per-turn settle barrier the warm step gates each reply turn on.
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
                  {
                    // Connector replies do not carry References. A bare
                    // inReplyTo on any other send must not be completed
                    // from this mailbox.
                    completeReferences: true,
                  },
                ),
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
        // Combine the per-run credential wiring (live material cell and
        // step-grants resolver, ridden in from the run child) with the
        // sidecar-static provider registry, so the agentFactory can assemble
        // each bundle's consumer-scoped `credentials` capability.
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
      // Merge the step's pinned-tool floor grants (recorded by the env
      // builder from the materialized factories) under the snapshot's
      // grants: the floor supplies the `tool:<name>` authority a pinned tool
      // never got from the hub's walk. ADDITIVE -- `evaluateGrants` ranks by
      // specificity then effect, so a declared `deny` still beats the derived
      // `ask`/`allow`. A missing floor entry (`?? []`) can only fail MORE
      // closed, never open a hole.
      const floor = toolMarkFloorByStep.get(baseStepId(stepId)) ?? [];
      const result = await evaluateGrants(
        // The credentials snapshot types `grants` as `readonly unknown[]`;
        // this factory casts at the boundary where `evaluateGrants` reads
        // the rows.
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- credentialsSnapshot.steps[*].grants is typed unknown[]; evaluateGrants reads GrantRule
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
      // Plaintext body sources decrypted sidecar-side; each body path resolves
      // its own table by definition id.
      bodySources: parseBodyInferenceSources(validated.WORKFLOW_BODY_SOURCES),
      dataDir: validated.SIDECAR_DATA_DIR,
      evaluateGrants: evaluateGrantsAdapter,
      // Shared with the top level's `buildStepEnv`.
      credentialProviders,
      // The shared closure the child re-walks to cap its inherited grants.
      // Source-ref only, so always present.
      closurePackageDir: env.spawn.closurePackageDir,
      collectDeclaredResources: deps.collectDeclaredResources,
      collectDeclaredCredentialConsumers:
        deps.collectDeclaredCredentialConsumers,
      filterGrantsToDeclaredResources: deps.filterGrantsToDeclaredResources,
    };
    // Terminal childWorkflow executor; `run-child` builds the in-memory
    // resolver from this plus the lifted-body map, so an owned inline child
    // spawns with no on-disk read.
    const runChild = createSidecarRunChild(childRunDeps);

    // An onTrigger section runs each event's body as a suspendable child;
    // `run-child` builds the in-memory body resolver from this plus the
    // lifted-body map, so a body resolves in-process with no on-disk read and
    // no separate per-body re-verify (the parent's re-verify covers every
    // inline body).
    const runSuspendableChild =
      createSidecarSpawnSuspendableChild(childRunDeps);

    // The top-level run's scratch survives for inspection until allocation
    // cleanup; internal cold runs reclaim their own scratch once they finish.
    let cleanupRunStorage: ((runId: string) => Promise<void>) | undefined;
    if (!env.spawn.warmKeep) {
      const topLevelRun = parseRunAddress(env.spawn.mailboxAddress);
      if (topLevelRun === null)
        throw new Error(
          `Workflow mailbox ${env.spawn.mailboxAddress} is not a run address`,
        );
      cleanupRunStorage = async (runId) => {
        if (runId === topLevelRun.runId) return;
        await fs.promises.rm(
          runStepStorageRoot({
            dataDir: validated.SIDECAR_DATA_DIR,
            workflowRunRepoId,
            runId,
          }),
          { recursive: true, force: true },
        );
      };
    }

    // Recover a parked correlation's approval snapshot for re-registration
    // enumeration. Wired unconditionally (unlike `cleanupRunStorage`): the
    // `warmKeep` branch selects the durable read -- cold reads the per-attempt
    // isogit store, warm reconstructs the agent's conversation state.
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
    // resume classifier, off the same cold/warm read as `loadParkedApproval`:
    // the enumeration needed when the correlationId never reached the log
    // (crash-across-park), projected to the minimal records the runtime
    // reconstructs `SignalAwaited` from.
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
      // A loop iteration runs under the inherited env, so it is the one birth
      // path that writes no grants file of its own; materialize it here
      // (capping the container run's grants to the body's declared resources)
      // so the body's grandchild spawn is authorized. `definition` is the
      // PRE-rewrite loop body.
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
      // The registry the warm agent's transport registers `watch` callbacks
      // into; `runWorkflowChild` routes each `mailbox.notify` to it.
      mailboxWatchRegistry,
      ...(cleanupRunStorage !== undefined ? { cleanupRunStorage } : {}),
    };
    return bindings;
  };
}
