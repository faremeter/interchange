// `runWorkflowChild` -- the workflow-process child's runtime body.
//
// The package-owned binary at `packages/workflow-host/bin/workflow-child`
// is a thin wrapper that parses `process.env`, opens stdin/stdout for
// the control channel, accepts the inherited event-channel fd, builds
// the substrate `RepoStore`, and invokes this function. Tests bypass
// the binary and call `runWorkflowChild` directly with mock streams
// and an in-memory substrate.
//
// The signature accepts every I/O and substrate handle as an injected
// dependency. Nothing inside this function reads `process.env` or
// reaches into a singleton; the binary's job is to bridge the
// process-shaped surfaces to this function's typed opts.
//
// Lifecycle:
//   1. Open the control channel and event channel using the IPC
//      primitives. Verify the supervisor's first signed control frame
//      by virtue of the receiver iterator's per-frame signature check.
//   2. Construct the `WorkflowRuntimeEnv` from the production env
//      adapters (RepoStore, BlobSubstrate, StepInvoker, SpawnChild)
//      and the substrate-shaped seams (signal channel; scheduler is a
//      host-process singleton supplied by the binary).
//   3. Discover any in-flight runs via the workflow-run repo's `runs/`
//      subdirectory and call `runtimeRun` with `resumeFromEvents` for
//      each one whose log lacks a terminal event.
//   4. Emit `ready` on the control channel.
//   5. Loop on control-channel frames:
//        - `trigger.fired` -> first-fire the deployment's top-level run via
//          `runtimeRun` (the supervisor only sends this for an absent log).
//        - `grants-updated` -> replace the credentialsSnapshot.
//        - `drain` -> forward to the drain controller (no-op here).
//        - `shutdown` -> stop accepting new triggers and exit the
//          loop.
//
// The `WorkflowAuthorize` closure evaluates grants against the active
// `credentialsSnapshot`. The snapshot's initial value can arrive in
// the spawn-time env bootstrap (multi-step deploys whose host wires
// the snapshot up-front) or via the first `grants-updated` control
// frame; the closure re-reads the closure-local snapshot on every
// invocation so a live update applies to subsequent steps without
// reconstructing the env.
//
// The DrainController is wired here against the production
// `createWorkflowHostDrainController`: on receipt of the supervisor's
// `drain` control mail the controller flips its signal, the runtime
// body observes the change at its four observation points, and the
// `behaviorFor` resolver derived from the loaded `WorkflowDefinition`
// classifies each in-flight step as cancel-mode or wait-mode. The
// supervisor's recycle policy is OS-driven (drain, SIGTERM, SIGKILL,
// respawn) and does not require a child-side control frame.

import { getLogger } from "@intx/log";
import { generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";

import type {
  Principal,
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions/substrate";
import type { DirectorRegistry } from "@intx/agent";
import {
  rewriteInlineOnTriggerBodies,
  rewriteInlineChildWorkflowBodies,
  enumerateInlineLoopBodies,
} from "@intx/workflow";
import type { AuthzCallResult } from "@intx/inference";

import type {
  ActionHandler,
  RunResult,
  Scheduler,
  ReadParkedApprovalOps,
  StepInvokeRequest,
  StepInvokeResult,
  StepInvoker,
  SpawnChildWorkflow,
  SpawnSuspendableChild,
  LoopFnRegistry,
  WorkflowAuthorizeFn,
  WorkflowDefinition,
  WorkflowPark,
  WorkflowRun,
  WorkflowRuntimeEnv,
} from "@intx/workflow";
import {
  baseStepId,
  createDefaultActionInvoker,
  createInMemoryEffectLedger,
  createLoopIterationHandle,
  emptyState,
  runtimeRun,
} from "@intx/workflow";

import {
  createWorkflowHostDrainController,
  type WorkflowHostDrainController,
} from "../drain-controller";

import type { InferenceSource, MailPartReader } from "@intx/types/runtime";
import type { CredentialDelivery } from "@intx/types/sidecar";

import { createWorkflowRunRepoStore } from "../adapters/repo-store";
import { createWorkflowRunBlobSubstrate } from "../adapters/blob-substrate";
import { createMailPartReader } from "../adapters/mail-part-store";
import type {
  HostSpawnSuspendableChild,
  HostSpawnChild,
  RunSuspendableChild,
  RunChildWorkflow,
} from "../adapters/spawn-child";
import {
  createInMemorySpawnSuspendableChild,
  createInMemorySpawnChild,
} from "../adapters/spawn-child";
import {
  createControlChannelSender,
  createEventChannelSender,
  receiveControlChannel,
  type ControlChannelSender,
  type ControlPayload,
  type EventPayload,
  type FrameWriter,
  type NdjsonReader,
  type NdjsonWriter,
} from "../ipc/index";
import { runBodyThenCleanup } from "../run-body-then-cleanup";
import { createWorkflowHostSignalChannel } from "../seams/signal-channel";
import type { CredentialsSnapshot } from "../supervisor/credentials";
import { hashGrants } from "../supervisor/credentials";

import type { SpawnTimeEnv } from "./env-bootstrap";
import { loadVerifiedWorkflowDefinitionFromClosure } from "./verified-definition-loader";
import {
  loadWorkflowActionHandlersFromClosure,
  loadWorkflowDirectorRegistryFromClosure,
  loadWorkflowLoopFnsFromClosure,
} from "../workflow-definition-loader";
import { discoverInFlightRuns } from "./self-discovery";
import {
  collectParkedApprovalCorrelations,
  type LoadParkedApproval,
} from "./parked-correlations";
import type { ChildOutboundMailBridge } from "./outbound-mail-bridge";
import type { ChildMailboxMutationBridge } from "./mailbox-mutation-bridge";
import type { MailboxWatchRegistry } from "./mailbox-watch-registry";
import { createWarmAgentCache, type WarmAgentCache } from "./warm-agent-cache";

const logger = getLogger(["workflow-host", "child"]);

/**
 * `WorkflowAuthorize` closure factory shape. The child's authorize
 * evaluates a `(resource, action)` request against the active
 * credentialsSnapshot for the originating step. The closure used here
 * is intentionally permissive on missing context: the workflow runtime
 * supplies `stepId` from the `AuthorizeContext` it threads through
 * every step's invoker, so a bare-call without a step id is a
 * programming error rather than a security-sensitive path. The closure
 * surfaces it loudly.
 *
 * Read-site: the closure consults a mutable reference so a
 * `grants-updated` control frame swaps the snapshot in place without
 * the caller having to reconstruct the closure.
 */
export type CredentialsSnapshotRef = {
  current: CredentialsSnapshot | null;
};

/**
 * The deployment's decrypted credential material and per-handle descriptors,
 * held through a mutable reference and swapped wholesale on a rotation push (a
 * revoked credential arrives by omission, so the swap evicts it). The secret
 * lives ONLY here -- read at tool-invoke time through the gated capability --
 * and is never copied into a snapshot, event, or state.
 */
export type CredentialMaterialRef = {
  current: CredentialDelivery | null;
};

/**
 * Per-step inference-source table the build path reads through a mutable
 * reference, keyed by stepId. Each value is the step's ordered failover
 * chain (element 0 is the active source). The single-step build resolves
 * its sources from `current` at build time, so a rotation that writes
 * `current` before the first build is reflected in the built agent. A
 * warm agent that is already built does not re-read this ref, so rotating
 * its live sources is out of this ref's scope.
 */
export type SourcesSnapshotRef = {
  current: Record<string, InferenceSource[]>;
};

/**
 * Construct the workflow-level authorize closure backed by a
 * mutable credentialsSnapshot reference.
 *
 * The closure looks up the step's grants by `stepId`, then delegates
 * to the caller-supplied grant evaluator. The evaluator slot exists so
 * the host wires its own grant-rule semantics without leaking the
 * grant-rule grammar into the workflow-host package; tests inject a
 * spy that records inputs.
 */
export type GrantEvaluator = (input: {
  resource: string;
  action: string;
  stepId: string;
  attempt: number | undefined;
  runId: string | undefined;
  grants: readonly unknown[];
}) => Promise<AuthzCallResult>;

export function createCredentialsBackedAuthorize(
  ref: CredentialsSnapshotRef,
  evaluate: GrantEvaluator,
): WorkflowAuthorizeFn {
  return async (resource, action, ctx) => {
    const stepId = ctx?.stepId;
    if (stepId === undefined) {
      throw new Error(
        "workflow-child authorize: missing stepId in AuthorizeContext; the runtime body must thread it through every step invocation",
      );
    }
    const snapshot = ref.current;
    if (snapshot === null) {
      throw new Error(
        "workflow-child authorize: no credentialsSnapshot active; the supervisor must push one before any step runs",
      );
    }
    // The credentials snapshot is keyed per base step; a map iteration's
    // scoped id `<base>[<index>]` resolves to its base entry so every
    // iteration shares the base step's grants. `baseStepId` is the identity
    // on an unscoped id, so a plain step is unaffected.
    const lookupStepId = baseStepId(stepId);
    const entry = snapshot.steps.find((s) => s.stepId === lookupStepId);
    if (entry === undefined) {
      const scopedNote =
        lookupStepId === stepId
          ? ""
          : ` (normalized from scoped invocation id ${stepId})`;
      throw new Error(
        `workflow-child authorize: credentialsSnapshot has no entry for stepId ${lookupStepId}${scopedNote}`,
      );
    }
    return evaluate({
      resource,
      action,
      stepId,
      attempt: ctx?.attempt,
      runId: ctx?.runId,
      grants: entry.grants,
    });
  };
}

/**
 * The workflow-host child's drain controller is the production
 * implementation defined in `../drain-controller.ts`. The control-loop
 * calls `requestDrain()` on receipt of the supervisor's `drain`
 * control mail; the controller flips its signal and the runtime body
 * observes the change at its four observation points. The `behaviorFor`
 * resolver consults the loaded `WorkflowDefinition`.
 */
export type DrainController = WorkflowHostDrainController;

/**
 * Step-invoker shape the child binds. Widens the workflow-runtime
 * `StepInvoker` with an `onEvent` callback the harness fires for
 * every `InferenceEvent` it emits during the step's run, plus the
 * child's credentials-backed `authorize` closure so the step agent's
 * tool-invocation gate evaluates the per-step grants the supervisor
 * pushed (rather than the host minting its own authorize that cannot
 * see the live credentials snapshot). The child's `buildRuntimeEnv`
 * constructs the per-step `onEvent` closure (wrapping the
 * HMAC-authenticated event-channel sender) and threads both here so
 * every event reaches the supervisor over the wire and every tool
 * call resolves against the agent's grants. The runtime-runtime
 * `StepInvoker` exposed via `WorkflowRuntimeEnv` remains the narrower
 * shape -- the child wraps this binding into a `StepInvoker` inside
 * `buildRuntimeEnv` so the workflow-runtime never sees the
 * host-specific surface.
 *
 * The `warmCache` argument carries the run-loop's per-deployment
 * warm-agent cache (design §3b) when the deployment is a warm candidate,
 * and `undefined` otherwise. The binding forwards it to the step-invoker
 * adapter, which builds-or-reuses the cached agent on a warm cache and
 * keeps instantiate-send-teardown when it is absent. The cache is owned
 * by the run-loop (`runWorkflowChild`), not the binding: the binding
 * only reads it through to the adapter.
 */
/**
 * Per-run credential inputs the top-level step invoker carries to the
 * substrate: the live material cell the control channel writes each delivery
 * into, and a resolver for a step's grants (which the substrate gates
 * credential use against). The substrate combines these with its own static
 * provider registry to assemble each tool bundle's `credentials` capability.
 *
 * Grants are typed `readonly unknown[]` here: this package owns no grant
 * grammar (the credentials snapshot's grants are `unknown[]` throughout), so
 * the substrate casts to its `GrantRule` shape at its own boundary, exactly
 * as the grant evaluator does. The cell is read live per use, so a rotation or
 * a revoking re-push reaches an already-shaped handle without a rebuild.
 */
export interface CredentialWiring {
  readonly materialRef: CredentialMaterialRef;
  readonly resolveStepGrants: (stepId: string) => readonly unknown[];
}

export type ChildStepInvoker = (
  req: StepInvokeRequest,
  onEvent: (event: EventPayload) => void,
  authorize: WorkflowAuthorizeFn,
  warmCache: WarmAgentCache | undefined,
  sourcesRef: SourcesSnapshotRef,
  credentialWiring: CredentialWiring,
  mailPartReader: MailPartReader,
) => Promise<StepInvokeResult>;

/**
 * Bindings the binary owns: per-deployment substrate identity,
 * principal credentials, the runtime-supplied callbacks the
 * adapter-layer cannot construct from `process.env` alone. Tests
 * supply a fully in-memory bindings object so `runWorkflowChild` runs
 * without touching disk.
 */
export interface RunWorkflowChildBindings {
  /** Workflow-run substrate (per-deployment workflow-run repo). */
  substrate: SubstrateRepoStore;
  /** Per-deployment workflow-run repo identity. */
  workflowRunRepoId: RepoId;
  /** Workflow-run repo ref the child reads/writes. */
  workflowRunRef: string;
  /**
   * Substrate-shaped principal the child presents on every workflow-run
   * read/write. Per the IPC threat model the child holds no private
   * key of its own; the principal here is a substrate-level identity
   * the host's substrate accepts for `runs/<runId>/` writes.
   */
  principal: Principal;
  /**
   * Step-invoker callback the runtime body invokes per step. The
   * shape is the workflow-runtime `StepInvoker` widened with an
   * `onEvent` slot so the harness can emit `InferenceEvent` frames
   * up through the event channel for every step invocation. The
   * production binary wires this against `createWorkflowStepInvoker`
   * with the host's per-step env builder; tests inject a stub.
   */
  invokeStep: ChildStepInvoker;
  /**
   * Terminal child-spawn callback the runtime body invokes for a
   * `childWorkflow` primitive when the deployment embeds NO inline child
   * import (the map `run-child` lifts is empty). Optional and, in practice,
   * only a test seam: a production deployment that carries a childWorkflow
   * always has a non-empty lifted-body map and routes through the in-memory
   * resolver built from `runChild` below, and one that carries none never
   * invokes this. A workflow that reaches a childWorkflow with neither this
   * nor `runChild` wired fails loud at spawn.
   */
  spawnChild?: SpawnChildWorkflow;
  /**
   * Raw in-process terminal child executor. `run-child` builds the in-memory
   * childWorkflow resolver from this executor plus the lifted-body map it
   * extracts after loading the definition -- the parent's own re-verified
   * closure -- so an owned inline child resolves with NO on-disk read. Parallel
   * to `runSuspendableChild` for onTrigger bodies. Optional for the same
   * reason: a child that embeds no childWorkflow import omits it.
   */
  runChild?: RunChildWorkflow;
  /**
   * Raw in-process suspendable-child executor. `run-child` builds the in-memory
   * onTrigger-body resolver from this executor plus the bodies map it extracts
   * AFTER re-evaluating the closure -- the substrate factory cannot build that
   * resolver because the bodies map does not exist pre-eval. Optional: a child
   * that runs no onTrigger section omits it.
   */
  runSuspendableChild?: RunSuspendableChild;
  /** Host-process scheduler singleton. The child consumes the same instance. */
  scheduler: Scheduler;
  /** Grant evaluator wired against the host's grant-rule grammar. */
  evaluateGrants: GrantEvaluator;
  /**
   * Reclaim the local-disk scratch a run produced once the run has
   * reached its terminal status. The host owns the on-disk layout
   * (`<dataDir>/workflow-step-state/<repoId>/runs/<runId>/`), so the
   * teardown lives next to the path construction in the substrate
   * factory and the run-loop merely fires it at the run-completion
   * moment it observes. Invoked ONLY on the cold (non-warm) path -- a
   * warm deployment's single agent reuses one stable workspace across
   * runs, so deleting per run would wipe a live conversation's files
   * mid-stream. A cleanup failure is logged and swallowed: it is a
   * disk-reclamation best-effort, never a correctness gate on the run's
   * terminal status. Optional so tests and the recursive child-workflow
   * adapter (which roots no per-run scratch of its own) can omit it.
   */
  cleanupRunStorage?: (runId: string) => Promise<void>;
  /**
   * Recover the durable approval snapshot for a parked control-plane
   * correlation, so the child can answer a supervisor
   * `parked-correlations.request` (the supervisor's re-registration path
   * after a re-establishment). The child owns enumeration -- it walks its
   * own reduced run state for `awaiting-signal` steps on control-plane
   * channels -- but the snapshot lives in per-step durable storage whose
   * on-disk layout (cold vs warm) the host owns, so the read is a host
   * binding next to `cleanupRunStorage`. Optional so tests inject a stub and
   * the recursive child-workflow adapter (which roots no per-step approval
   * storage) can omit it; a production child that enumerates a parked
   * control-plane step with no binding wired throws rather than silently
   * dropping the correlation the hub is waiting to register.
   */
  loadParkedApproval?: LoadParkedApproval;
  /**
   * Enumerate the durable pending approval operations a crashed-mid-invocation
   * step left behind, so the resume classifier can recover a step that crashed
   * across the park boundary (durable `StepStarted`, unflushed `SignalAwaited`)
   * as `awaiting-signal` rather than failing the run. Reads the same per-step
   * durable storage as `loadParkedApproval` (cold isogit / warm substrate), so
   * it is a host binding for the same reason. Optional so tests inject a stub
   * and the recursive child-workflow adapter can omit it; absent, a crashed
   * invocation step settles as a terminal failure, the pre-recovery behavior.
   */
  readParkedApprovalOps?: ReadParkedApprovalOps;
  /**
   * Mailbox watch registry backing the warm agent's `mail_wait` (INBOUND half
   * of mailbox ownership, §3b). The host's substrate factory builds ONE
   * instance at child boot, shares it with the step agent's supervisor-backed
   * transport (whose `watch` registers callbacks into it), and exposes it here
   * so the control loop routes each `mailbox.notify` frame to the same
   * registry's `fire`. Optional: a deploy that wires no mail surface (and the
   * recursive child-workflow adapter) omits it, and an inbound `mailbox.notify`
   * frame is then logged and dropped. A test may instead inject a registry
   * directly through `RunWorkflowChildOpts.mailboxWatchRegistry`, which takes
   * precedence.
   */
  mailboxWatchRegistry?: MailboxWatchRegistry;
  /** Optional clock override; production wires `() => new Date()`. */
  clock?: () => Date;
  /** Optional id generator override; production wires a monotonic one. */
  newId?: (prefix: string) => string;
  /**
   * Optional bootstrap credentialsSnapshot. The host's production
   * wiring supplies this for multi-step deploys whose snapshot is
   * baked at spawn time; tests can pre-seed it directly. Absent
   * value defers to the first `grants-updated` control frame.
   */
  initialCredentialsSnapshot?: CredentialsSnapshot;
  /**
   * Bootstrap per-step inference-source table (keyed by stepId), parsed
   * from the spawn env by the host's substrate factory. Seeds the
   * mutable `sourcesRef` the build path reads. Absent value defers to an
   * empty table, so a step with no pinned source fails loudly at build
   * rather than resolving a default.
   */
  initialSources?: Record<string, InferenceSource[]>;
  /**
   * Bootstrap credential material for the deployment's tools, decrypted
   * hub-side and delivered on the deploy frame so it is resident before any
   * step runs. Seeds the mutable `credentialMaterialRef` the gated capability
   * reads. Absent when the deployment binds no credentials; a later
   * `credentials-updated` control frame refreshes it on rotation.
   */
  initialCredentialMaterial?: CredentialDelivery;
  /**
   * Optional override for the child's Ed25519 keypair factory. The
   * child mints a fresh keypair at startup, holds the private half
   * in its own address space, signs every upstream control frame
   * with it, and publishes the public half in the `ready` frame so
   * the supervisor can verify subsequent upstream frames. Production
   * wires this against `@intx/crypto`'s `generateKeyPair`;
   * tests inject a deterministic factory so they can assert on the
   * published key. The supervisor's private key is NEVER threaded
   * into the child -- the child holds only its own private half.
   */
  ipcChildKeyPairFactory?: () => Promise<{
    privateKey: Uint8Array;
    publicKey: Uint8Array;
  }>;
}

export interface RunWorkflowChildOpts {
  /** Parsed spawn-time env. */
  env: SpawnTimeEnv;
  /** Control-channel reader (supervisor -> child). */
  controlReader: NdjsonReader;
  /**
   * Control-channel writer back to the supervisor. The child does not
   * sign frames here today (the only upstream control frame, the
   * `ready` signal, rides as an unsigned wire shape because the
   * supervisor receives it on its trusted side). Future upstream
   * frames will adopt the same envelope-and-signature contract the
   * downstream side enforces; the writer slot exists today so the
   * control-channel boundary is symmetric in shape.
   */
  controlWriter: NdjsonWriter;
  /**
   * Event-channel writer (child -> supervisor). The child publishes
   * verified `InferenceEvent` frames the harness emits up through
   * here. Tests inject an in-memory writer; production wires the
   * inherited socketpair fd into a FrameWriter.
   */
  eventWriter: FrameWriter;
  /** Bindings the binary or test harness owns. */
  bindings: RunWorkflowChildBindings;
  /**
   * Optional pre-built upstream control sender the child uses to emit
   * `ready` and (today) `pack.push.request` frames. Defaults to a
   * sender minted internally against the child's own Ed25519 keypair.
   * The process-shaped wrapper `runWorkflowChildFromProcessEnv`
   * supplies a pre-built sender so the same Ed25519-signed surface is
   * shared with the pack-push bridge it constructs against the
   * substrate factory.
   */
  upstreamSender?: ControlChannelSender;
  /**
   * Optional substrate-write bridge whose `handleMergeRequest` and
   * `handleWriteResponse` the child's control loop invokes when the
   * matching downstream frames land. When omitted, inbound
   * `substrate.merge.request` / `substrate.write.response` frames are
   * logged at warn-level and dropped -- the wire shapes are
   * well-formed but nobody on the child side asked for them. The
   * pre-built bridge is the path the process-shaped wrapper takes so
   * the substrate factory's proxy `RepoStore` can resolve writes
   * against it.
   */
  substrateWriteBridge?: SubstrateWriteResponseSink;
  /**
   * Optional outbound-mail bridge (OUTBOUND half of mailbox ownership,
   * §3a). The step agent's mail tools are backed by a transport whose
   * `send` routes through this bridge: it emits an `outbound.message`
   * upstream control frame and resolves the agent's mail-tool `send`
   * once the supervisor's matching `outbound.result` lands. The
   * control loop routes the downstream `outbound.result` frame to the
   * bridge's `handleResult` and invokes `cancelAll` on any exit path so
   * a pending send does not leak an awaiter after the supervisor tears
   * the IPC down. When omitted, inbound `outbound.result` frames are
   * logged at warn-level and dropped -- the wire shape is well-formed
   * but no agent on the child side asked for an outbound send.
   */
  outboundMailBridge?: ChildOutboundMailBridge;
  /**
   * Optional mailbox-mutation bridge (INBOUND half of mailbox ownership,
   * §3b). The step agent's mail tools mutate the INBOX -- flag writes and
   * `expunge` -- through a transport whose write methods route through
   * this bridge: it emits a `mailbox.mutate.request` upstream control
   * frame and resolves once the supervisor's matching
   * `mailbox.mutate.response` lands. The control loop routes the
   * downstream response frame to the bridge's `handleResult` and invokes
   * `cancelAll` on any exit path so a pending mutation does not leak an
   * awaiter after the supervisor tears the IPC down. When omitted,
   * inbound `mailbox.mutate.response` frames are logged at warn-level and
   * dropped -- the wire shape is well-formed but no agent on the child
   * side asked for a mutation.
   */
  mailboxMutationBridge?: ChildMailboxMutationBridge;
  /**
   * Optional mailbox watch registry (INBOUND half of mailbox ownership,
   * design §3b). The supervisor -- the sole mail owner -- commits an arrived
   * message to the workflow-run substrate mailbox and fires a `mailbox.notify`
   * control frame; the control loop routes that frame to this registry's
   * `fire`, which delivers a typed `exists` `MailboxEvent` to the callbacks the
   * step agent's supervisor-backed transport registered through `watch`
   * (backing `mail_wait`). When omitted, an inbound `mailbox.notify` frame is
   * logged at warn-level and dropped -- the wire shape is well-formed but no
   * watcher on the child side asked for inbound events.
   */
  mailboxWatchRegistry?: MailboxWatchRegistry;
}

/**
 * Narrow interface the child's control loop calls when downstream
 * substrate-write frames arrive, plus the `cancelAll` shutdown
 * surface the loop invokes on any exit path. Decouples the loop from
 * the bridge's `submit` side so a test can drop in a recording sink
 * without standing up the full bridge.
 */
export interface SubstrateWriteResponseSink {
  handleMergeRequest(
    data: Extract<ControlPayload, { type: "substrate.merge.request" }>["data"],
  ): void;
  handleWriteResponse(
    data: Extract<ControlPayload, { type: "substrate.write.response" }>["data"],
  ): void;
  cancelAll(reason: string): void;
}

/**
 * Public result the test harness inspects. Production binaries discard
 * the return value (the process exits when this function resolves);
 * tests assert on the discovered-run ids and the active credentials
 * snapshot to verify the loop's behaviour without scraping logs.
 */
export interface RunWorkflowChildResult {
  /** RunIds the child resumed at startup. */
  resumedRunIds: readonly string[];
  /** RunIds the child started from `trigger.fired` after `ready`. */
  triggeredRunIds: readonly string[];
  /** Snapshot active at function return. */
  finalCredentialsSnapshot: CredentialsSnapshot | null;
}

/**
 * Run the workflow-process child. Resolves once the control channel
 * emits `shutdown` (or ends without a frame, in which case the loop
 * exits cleanly).
 */
export async function runWorkflowChild(
  opts: RunWorkflowChildOpts,
): Promise<RunWorkflowChildResult> {
  const credentialsRef: CredentialsSnapshotRef = {
    current: opts.bindings.initialCredentialsSnapshot ?? null,
  };
  const sourcesRef: SourcesSnapshotRef = {
    current: opts.bindings.initialSources ?? {},
  };
  const credentialMaterialRef: CredentialMaterialRef = {
    current: opts.bindings.initialCredentialMaterial ?? null,
  };
  // The per-run credential wiring the top-level step invoker carries to the
  // substrate: the live material cell and a resolver for a step's grants from
  // the same credentials snapshot `authorize` reads. Built once over the two
  // refs; every step build reads them live, so a rotation -- or a revoking
  // re-push that swaps a ref -- is reflected without rebuilding the wiring.
  const credentialWiring: CredentialWiring = {
    materialRef: credentialMaterialRef,
    resolveStepGrants: (stepId) => {
      const snapshot = credentialsRef.current;
      if (snapshot === null) {
        throw new Error(
          `workflow-child credential wiring: no credentials snapshot for step ${stepId}; a tool-bearing step cannot resolve its grants before the run carries any`,
        );
      }
      const entry = snapshot.steps.find(
        (step) => step.stepId === baseStepId(stepId),
      );
      if (entry === undefined) {
        throw new Error(
          `workflow-child credential wiring: credentials snapshot has no entry for step ${baseStepId(stepId)}`,
        );
      }
      return entry.grants;
    },
  };
  const clock = opts.bindings.clock ?? defaultClock;
  const newId = opts.bindings.newId ?? defaultNewId;

  // Mint the child's own upstream-signing keypair. The private half
  // never leaves this address space; the public half rides on the
  // `ready` frame's payload so the supervisor can verify subsequent
  // upstream frames against it.
  const childKeyPair = await (
    opts.bindings.ipcChildKeyPairFactory ?? generateKeyPair
  )();

  const runtimeRepoStore = createWorkflowRunRepoStore({
    substrate: opts.bindings.substrate,
    repoId: opts.bindings.workflowRunRepoId,
    principal: opts.bindings.principal,
    ref: opts.bindings.workflowRunRef,
  });

  const eventSender = createEventChannelSender({
    hmacKey: opts.env.hmacKey,
    channelId: opts.env.channelId,
    writer: opts.eventWriter,
  });

  // Re-verify barrier at the load boundary. Source-ref is the only deploy
  // lineage: the inert projection is a non-executable approval surface (agents
  // carry `modelSources`/no `inference`, tool factories are plain data), so the
  // child EVALUATES the pinned code closure to a live definition and re-verifies
  // by projecting it back to inert and hashing (`computeLiveDefinitionHash`)
  // against `opts.env.definitionHash`; a divergent closure fails closed. The
  // load happens once before both the resume loop and the trigger loop, so the
  // same verified definition serves every fresh trigger AND every resume.
  //
  // Post-verify structural rewrite: the re-verify above hashed the closure's
  // INLINE onTrigger bodies (matching the frozen approval); now lift each to a
  // `{ ref }` so the runtime dispatches to the body child, and keep the
  // extracted body definitions in an in-memory map. The suspendable-child
  // resolver runs each body from THIS map -- the parent's already-re-verified
  // closure -- with no disk read and no separate per-body re-verify. The rewrite
  // MUST follow the re-verify: rewriting first would diverge from the frozen
  // inline-body hash.
  const verifiedDefinition = await loadVerifiedWorkflowDefinitionFromClosure({
    packageDir: opts.env.closurePackageDir,
    approvedHash: opts.env.definitionHash,
  });
  const { workflow, bodies } = rewriteInlineOnTriggerBodies(verifiedDefinition);
  let definition: WorkflowDefinition = workflow;
  const bodiesMap = new Map<string, WorkflowDefinition>(
    bodies.map((b) => [b.ref, b.definition]),
  );

  // An owned `childWorkflow` import embeds its child inline in the parent's
  // definition (folded into the parent's hash and approval), so it is already
  // covered by the re-verify above. Lift each inline child to an internal
  // `{ ref }` -- the form the runtime dispatches -- and keep the lifted
  // definitions in an in-memory map. The terminal childWorkflow resolver below
  // runs each child from THIS map, with no on-disk asset read and no separate
  // per-child re-verify.
  const childRewrite = rewriteInlineChildWorkflowBodies(definition);
  definition = childRewrite.workflow;
  const childBodiesMap = new Map(
    childRewrite.bodies.map((b) => [b.ref, b.definition]),
  );

  // A loop iteration runs its body as a suspendable child through the same seam
  // an onTrigger body uses, so register each top-level loop body in `bodiesMap`
  // under its `<workflowId>__<stepId>` ref. Unlike an onTrigger or childWorkflow
  // body, a loop keeps its body INLINE on the primitive -- both hash layers
  // project the body inline, so rewriting the primitive would change every
  // existing loop's hash. `enumerateInlineLoopBodies` mints a ref-keyed COPY and
  // leaves the primitive untouched, and a given step is exactly one primitive
  // kind, so a loop ref never collides with an onTrigger or childWorkflow ref.
  //
  // A loop body may itself contain a `childWorkflow` grandchild: rewrite the
  // COPY's inline children to `{ ref }` (the primitive, and thus the hash, is
  // untouched) and fold the extracted grandchildren into `childBodiesMap` HERE
  // -- before the eager loop-fn/handler resolution and the terminal-childWorkflow
  // host selection below, both of which read `childBodiesMap`. Merging later
  // would resolve a grandchild's refs mid-run instead of at establish, and would
  // leave `childBodiesMap` empty for a deployment whose only child is a
  // loop-body grandchild, so its spawn would find no wired terminal host.
  for (const loopBody of enumerateInlineLoopBodies(definition)) {
    const bodyRewrite = rewriteInlineChildWorkflowBodies(loopBody.definition);
    bodiesMap.set(loopBody.ref, bodyRewrite.workflow);
    for (const grandchild of bodyRewrite.bodies) {
      childBodiesMap.set(grandchild.ref, grandchild.definition);
    }
  }

  // Directors resolve from the pinned closure so a custom director authored in
  // the workflow's own package runs. Loading directors OUTSIDE the
  // definition-hash re-verify is safe: the approved hash pins each director's
  // id + config (which director runs cannot change post-approval) and the
  // closure's SRI pins its module bytes. Folding directors into the hash would
  // be redundant, so it is deliberately not done -- see
  // `loadWorkflowDirectorRegistryFromClosure`.
  const directors = await loadWorkflowDirectorRegistryFromClosure({
    packageDir: opts.env.closurePackageDir,
  });

  // Loop `while`/`carry` functions resolve from the pinned closure's
  // `interchange.loops` module, loaded alongside the directors and OUTSIDE the
  // definition-hash re-verify for the same reason: the approved hash pins each
  // ref string and the closure's SRI pins the module bytes. Resolve every loop
  // ref reachable from the definition (its own loop bodies, and the lifted
  // onTrigger/childWorkflow bodies, which share this same registry at runtime)
  // eagerly here, so a deployment that declares a loop whose fn the closure
  // does not export fails at establish rather than mid-run.
  const loopFns = await loadWorkflowLoopFnsFromClosure({
    packageDir: opts.env.closurePackageDir,
  });
  eagerlyResolveLoopFns(
    [definition, ...bodiesMap.values(), ...childBodiesMap.values()],
    loopFns,
  );

  // Action handlers resolve from the pinned closure's `interchange.actions`
  // module, on the same terms as loop fns. Resolve every action handler ref
  // reachable from the definition eagerly here (recursing into loop bodies,
  // where an action body is the common case), so a deployment that declares an
  // action whose handler the closure does not export fails at establish rather
  // than mid-run.
  const actionResolver = await loadWorkflowActionHandlersFromClosure({
    packageDir: opts.env.closurePackageDir,
  });
  eagerlyResolveActionHandlers(
    [definition, ...bodiesMap.values(), ...childBodiesMap.values()],
    actionResolver,
  );

  // Suspendable-child resolver (onTrigger section bodies and loop bodies),
  // selected ONCE per deployment: the bodies map is immutable and the per-run
  // `onEvent` is injected later in `buildRuntimeEnv`. Resolve each body from the
  // parent's in-memory closure (already re-verified above) via the raw executor
  // binding. A deployment that carries bodies but whose host wired no executor is
  // a misconfiguration -- fail loud at startup rather than silently falling back
  // to a disk read (the exact behaviour this arm exists to avoid). A deployment
  // with no suspendable body leaves the host undefined; its slot is never
  // invoked.
  let suspendableChildHost: HostSpawnSuspendableChild | undefined;
  if (bodiesMap.size > 0) {
    const executor = opts.bindings.runSuspendableChild;
    if (executor === undefined) {
      throw new Error(
        "workflow-child: source-ref deployment carries suspendable bodies " +
          "(onTrigger sections or loop bodies) but the host wired no " +
          "runSuspendableChild executor; cannot resolve bodies in-memory",
      );
    }
    suspendableChildHost = createInMemorySpawnSuspendableChild({
      bodies: bodiesMap,
      runSuspendableChild: executor,
    });
  }

  // Terminal childWorkflow resolver, selected ONCE per deployment. When the
  // definition embeds any inline child (the lifted map is non-empty), resolve
  // each from that in-memory map via the raw terminal executor -- the parent's
  // own re-verified closure -- so an owned child spawns with no disk read. A
  // deployment that embeds a childWorkflow but whose host wired no executor is
  // a misconfiguration and fails loud at startup rather than falling back to a
  // disk read. A definition with no inline child keeps the injected binding (a
  // test seam); its childWorkflow slot is never invoked.
  // `HostSpawnChild` (a call-arg `onEvent`) like the suspendable host: the
  // resolver is deployment-scoped but each run injects its own event sink in
  // `buildRuntimeEnv`. The two fallback arms ignore the sink.
  let spawnChild: HostSpawnChild;
  if (childBodiesMap.size > 0) {
    const executor = opts.bindings.runChild;
    if (executor === undefined) {
      throw new Error(
        "workflow-child: deployment embeds childWorkflow imports but the " +
          "host wired no runChild executor; cannot resolve children in-memory",
      );
    }
    spawnChild = createInMemorySpawnChild({
      bodies: childBodiesMap,
      runChild: executor,
    });
  } else if (opts.bindings.spawnChild !== undefined) {
    const injected = opts.bindings.spawnChild;
    spawnChild = (input, _onEvent) => injected(input);
  } else {
    // No inline child and no injected binding: a workflow that nonetheless
    // reaches a childWorkflow spawn fails loud here rather than silently
    // completing against a child that never ran.
    spawnChild = async ({ definitionRef }, _onEvent) => {
      throw new Error(
        `workflow-child: childWorkflow ${definitionRef} reached the runtime ` +
          `but no child executor is wired`,
      );
    };
  }

  const authorize = createCredentialsBackedAuthorize(
    credentialsRef,
    opts.bindings.evaluateGrants,
  );

  const drainController = createWorkflowHostDrainController({ definition });

  // Warm-agent cache (design §3b). Built only when the deployment is a
  // warm candidate (the single-step long-lived agent the deploy
  // projection marked). The cache lives in this run-loop's address
  // space, holds the constructed agent across messages, and is evicted
  // -- running the wrapped `agent.close()` that kills the LSP subprocess
  // -- at the loop's teardown points (the shutdown frame and the
  // exit-path `finally` below). A multi-step deployment leaves this
  // `undefined`, so its steps keep instantiate-send-teardown and no
  // multi-step agent is ever warm-kept.
  const warmCache: WarmAgentCache | undefined = opts.env.warmKeep
    ? createWarmAgentCache()
    : undefined;

  // Construct the upstream control-channel sender up-front. The
  // supervisor's `waitForReady` consumes the `ready` frame and the
  // upstream-control pump consumes every subsequent upstream payload
  // (`pack.push.request`, `terminal.event`, `recycle.request`) on the
  // same iterator. Building the sender here lets the resume loop
  // below attach a terminal-event emitter onto every resumed run's
  // `complete` promise without re-deriving the sender lazily.
  const upstreamSender =
    opts.upstreamSender ??
    createControlChannelSender({
      privateKeySeed: childKeyPair.privateKey,
      channelId: opts.env.channelId,
      writer: opts.controlWriter,
    });

  // Self-discovery before announcing `ready`. The runtime body must
  // see every in-flight run before the supervisor starts forwarding
  // `trigger.fired` frames; otherwise a fresh trigger could land
  // ahead of a resume and the runtime would commit a duplicate run
  // entry for the same id.
  const discovered = await discoverInFlightRuns({
    substrate: opts.bindings.substrate,
    repoId: opts.bindings.workflowRunRepoId,
    runtimeRepoStore,
  });
  const resumedRunIds: string[] = [];
  // One-driver-per-run claim. A runId present here is already being
  // driven by a live `runtimeRun` in this process (a resume below, or an
  // earlier trigger). The trigger.fire path consults it to refuse
  // spawning a second concurrent driver for the same runId: two drivers
  // race to settle the same residual and the loser throws an uncaught
  // TransitionError into its fire-and-forget continuation. Each site
  // removes its entry when the run reaches terminal.
  const runsInFlight = new Map<string, WorkflowRun>();
  for (const run of discovered) {
    const env = buildRuntimeEnv({
      runId: run.runId,
      bindings: opts.bindings,
      runtimeRepoStore,
      authorize,
      directors,
      suspendableChildHost,
      bodiesMap,
      spawnChild,
      loopFns,
      actionResolver,
      clock,
      newId,
      drainController,
      warmCache,
      sourcesRef,
      credentialWiring,
      onEvent: (event) => {
        void eventSender.send(event).catch((cause) => {
          logger.error`event-channel send failed during resume run ${run.runId}: ${String(cause)}`;
        });
      },
      upstreamSender,
    });
    const handle = runtimeRun(definition, env, {
      runId: run.runId,
      resumeFromEvents: run.seedEvents,
    });
    runsInFlight.set(run.runId, handle);
    // Fire-and-forget: the runtime body's `complete` settles when the
    // run reaches a terminal phase; the child's control-loop does not
    // block on resumed runs. The supervisor's dispatch loop / drain
    // accumulator subscribes to the resumed run's terminal via the
    // `terminal.event` upstream frame the child emits below.
    void handle.complete
      .then((result) => {
        reclaimRunStorageIfCold({
          warmKeep: opts.env.warmKeep,
          cleanupRunStorage: opts.bindings.cleanupRunStorage,
          runId: run.runId,
        });
        runsInFlight.delete(run.runId);
        return emitTerminalEvent(upstreamSender, result);
      })
      .catch((cause) => {
        runsInFlight.delete(run.runId);
        logger.error`resumed run ${run.runId} failed: ${String(cause)}`;
      });
    resumedRunIds.push(run.runId);
  }

  // `ready` rides over the control channel back to the supervisor.
  // The supervisor's `waitForReady` consumes it on its receive side.
  // The upstream sender is constructed above so the resume loop can
  // attach a terminal-event emitter onto every resumed run's
  // `complete` promise; the same sender lives behind the pack-push
  // bridge the process wrapper builds (when the caller supplies one),
  // so the upstream frame sequence is monotonic across `ready`,
  // every `pack.push.request`, every `terminal.event`, and any
  // future child-originated upstream payload. Upstream frames are
  // signed by the child's own private key; the `ready` payload
  // publishes the matching public half so the supervisor can verify
  // every subsequent upstream frame.
  await upstreamSender.send({
    type: "ready",
    data: {
      childPid: process.pid,
      childPublicKey: hexEncode(childKeyPair.publicKey),
    },
  });

  // Report self-discovered runs so the supervisor seeds its cohort
  // tracking before the dispatch loop starts.
  await upstreamSender.send({
    type: "resumed.runs",
    data: { runIds: resumedRunIds },
  });

  const triggeredRunIds: string[] = [];

  // Control-loop. The receiver iterator yields one verified payload
  // per call; any signature/channelId/seq violation crashes the
  // receiver via `onCrash` and ends the iterator.
  const iter = receiveControlChannel({
    publicKey: opts.env.hostPublicKey,
    channelId: opts.env.channelId,
    reader: opts.controlReader,
    onCrash: (reason) => {
      logger.error`workflow-child control channel crash: ${reason}`;
    },
  });

  // Resolve the mailbox watch registry the control loop routes `mailbox.notify`
  // frames to. Production wires it on the bindings (the substrate factory builds
  // one instance and shares it with the warm agent's supervisor-backed
  // transport); a test may inject one directly through the opts, which wins.
  // Both absent leaves inbound `mailbox.notify` frames logged and dropped -- a
  // deploy with no wired mail surface.
  const mailboxWatchRegistry =
    opts.mailboxWatchRegistry ?? opts.bindings.mailboxWatchRegistry;

  const runControlLoop = async (): Promise<void> => {
    for await (const payload of iter) {
      if (
        await handleControlPayload(payload, {
          env: opts.env,
          bindings: opts.bindings,
          credentialsRef,
          runtimeRepoStore,
          definition,
          authorize,
          directors,
          suspendableChildHost,
          bodiesMap,
          spawnChild,
          loopFns,
          actionResolver,
          clock,
          newId,
          eventSender,
          upstreamSender,
          drainController,
          triggeredRunIds,
          runsInFlight,
          warmCache,
          sourcesRef,
          credentialMaterialRef,
          credentialWiring,
          ...(opts.substrateWriteBridge !== undefined
            ? { substrateWriteBridge: opts.substrateWriteBridge }
            : {}),
          ...(opts.outboundMailBridge !== undefined
            ? { outboundMailBridge: opts.outboundMailBridge }
            : {}),
          ...(opts.mailboxMutationBridge !== undefined
            ? { mailboxMutationBridge: opts.mailboxMutationBridge }
            : {}),
          ...(mailboxWatchRegistry !== undefined
            ? { mailboxWatchRegistry }
            : {}),
        })
      ) {
        // shutdown received; the shutdown case already cancelled any
        // pending substrate writes before returning true.
        break;
      }
    }
  };

  const cleanupControlLoop = async (): Promise<void> => {
    // Any exit path -- clean (iterator end), dirty (thrown error),
    // shutdown (already cancelled, repeat is a no-op on an empty map)
    // -- cancels every still-pending substrate write so the runtime
    // call site that fired the write surfaces a structured rejection
    // rather than awaiting indefinitely on a control channel the
    // supervisor has already torn down.
    if (opts.substrateWriteBridge !== undefined) {
      opts.substrateWriteBridge.cancelAll("workflow-child control loop exited");
    }
    // Same contract for outbound mail: a step agent's mail-tool send
    // that is still awaiting the supervisor's `outbound.result` when
    // the control loop exits must surface a structured rejection rather
    // than hang on a torn-down channel.
    if (opts.outboundMailBridge !== undefined) {
      opts.outboundMailBridge.cancelAll("workflow-child control loop exited");
    }
    // Same contract for mailbox mutations: a step agent's flag or
    // `expunge` still awaiting the supervisor's `mailbox.mutate.response`
    // when the control loop exits must surface a structured rejection
    // rather than hang on a torn-down channel.
    if (opts.mailboxMutationBridge !== undefined) {
      opts.mailboxMutationBridge.cancelAll(
        "workflow-child control loop exited",
      );
    }
    // Evict the warm-agent cache (design §3b) on every exit path:
    // graceful (shutdown frame -> iterator end), dirty (thrown error),
    // or the control channel closing. Eviction runs the wrapped
    // `agent.close()` that disposes plugins and kills the LSP
    // subprocess, so no warm agent or LSP outlives the run-loop. On a
    // production hard kill (recycle/SIGKILL) the process dies before
    // this runs, but the OS reaps the LSP grandchild regardless; this
    // path covers the graceful teardown the eviction contract names.
    if (warmCache !== undefined) {
      await warmCache.evictAll("workflow-child control loop exited");
    }
  };

  // Run the control loop, then always run the cleanup above. A failing
  // eviction (the wrapped agent close rejects when a plugin/LSP disposer
  // fails) surfaces on a clean exit, but must not mask a control-loop
  // error already unwinding -- so it is logged, not rethrown, in that case.
  await runBodyThenCleanup(
    runControlLoop,
    cleanupControlLoop,
    (cause) =>
      logger.error`workflow-child: warm-agent eviction failed while unwinding a control-loop error; surfacing the control-loop error, eviction failure: ${cause instanceof Error ? cause.message : String(cause)}`,
  );

  return {
    resumedRunIds,
    triggeredRunIds,
    finalCredentialsSnapshot: credentialsRef.current,
  };
}

/**
 * Handle a single control-channel payload. Returns `true` when the
 * payload signals shutdown so the caller exits the loop; otherwise
 * `false`.
 */
async function handleControlPayload(
  payload: ControlPayload,
  ctx: {
    env: SpawnTimeEnv;
    bindings: RunWorkflowChildBindings;
    credentialsRef: CredentialsSnapshotRef;
    runtimeRepoStore: ReturnType<typeof createWorkflowRunRepoStore>;
    definition: WorkflowDefinition;
    authorize: WorkflowAuthorizeFn;
    directors: DirectorRegistry;
    suspendableChildHost: HostSpawnSuspendableChild | undefined;
    bodiesMap: ReadonlyMap<string, WorkflowDefinition>;
    spawnChild: HostSpawnChild;
    loopFns: LoopFnRegistry;
    actionResolver: (ref: string) => ActionHandler;
    clock: () => Date;
    newId: (prefix: string) => string;
    eventSender: ReturnType<typeof createEventChannelSender>;
    upstreamSender: ControlChannelSender;
    drainController: DrainController;
    triggeredRunIds: string[];
    runsInFlight: Map<string, WorkflowRun>;
    warmCache: WarmAgentCache | undefined;
    sourcesRef: SourcesSnapshotRef;
    credentialMaterialRef: CredentialMaterialRef;
    credentialWiring: CredentialWiring;
    substrateWriteBridge?: SubstrateWriteResponseSink;
    outboundMailBridge?: ChildOutboundMailBridge;
    mailboxMutationBridge?: ChildMailboxMutationBridge;
    mailboxWatchRegistry?: MailboxWatchRegistry;
  },
): Promise<boolean> {
  switch (payload.type) {
    case "trigger.fire": {
      // One driver per runId. If this child is already driving this
      // runId -- self-discovery resumed it, or an earlier trigger opened
      // it -- a duplicate/stale trigger frame (which carries the local part
      // of the deployment's mail address as the runId and no resumeFromEvents)
      // must NOT spawn a second `runtimeRun`. A
      // second concurrent driver would race the live one to settle the
      // same residual and the loser throws an uncaught TransitionError,
      // and even a driver that avoided the throw would double-emit the
      // terminal. The live driver's completion continuation owns the
      // single terminal emission; the supervisor's terminal-event-driven
      // `markConsumed` consumes the original message off that one terminal,
      // so no work is dropped by declining here. Record the runId and signal
      // "handled, not shutdown" the same way the normal trigger case
      // returns, without awaiting the live handle's `complete` inline
      // (that would block the control loop).
      if (ctx.runsInFlight.has(payload.data.runId)) {
        ctx.triggeredRunIds.push(payload.data.runId);
        return false;
      }
      // The supervisor resolved the inbound mail to the run's input (the
      // conversation text plus references to attachment bytes it committed to
      // the workflow-run substrate) and shipped it in the frame. It becomes
      // the run's trigger payload; the one-step workflow's first step defaults
      // its input selector to `trigger.payload` (defineWorkflow's default-input
      // convention), so the step input resolves to the inbound message and
      // `agent.send` receives it once its attachment references are resolved to
      // bytes at send time.
      const triggerPayload = payload.data.payload;
      const env = buildRuntimeEnv({
        runId: payload.data.runId,
        bindings: ctx.bindings,
        runtimeRepoStore: ctx.runtimeRepoStore,
        authorize: ctx.authorize,
        directors: ctx.directors,
        suspendableChildHost: ctx.suspendableChildHost,
        bodiesMap: ctx.bodiesMap,
        spawnChild: ctx.spawnChild,
        loopFns: ctx.loopFns,
        actionResolver: ctx.actionResolver,
        clock: ctx.clock,
        newId: ctx.newId,
        drainController: ctx.drainController,
        warmCache: ctx.warmCache,
        sourcesRef: ctx.sourcesRef,
        credentialWiring: ctx.credentialWiring,
        onEvent: (event) => {
          void ctx.eventSender.send(event).catch((cause) => {
            logger.error`event-channel send failed during run ${payload.data.runId}: ${String(cause)}`;
          });
        },
        upstreamSender: ctx.upstreamSender,
      });
      const handle: WorkflowRun = runtimeRun(ctx.definition, env, {
        runId: payload.data.runId,
        consumedMessageId: payload.data.messageId,
        triggerPayload,
      });
      ctx.runsInFlight.set(payload.data.runId, handle);
      // Fan the run's terminal status back to the supervisor over the
      // upstream control channel. The supervisor's dispatch loop and
      // any armed drainTimeout accumulator subscribe through the
      // per-cohort broadcaster the supervisor owns; the broadcaster
      // settles when this frame lands. The runtime body commits the
      // terminal event to the workflow-run substrate as part of the
      // same lifecycle moment, so the on-disk audit chain and the
      // peer notification originate from the same code path.
      void handle.complete
        .then((result) => {
          reclaimRunStorageIfCold({
            warmKeep: ctx.env.warmKeep,
            cleanupRunStorage: ctx.bindings.cleanupRunStorage,
            runId: payload.data.runId,
          });
          ctx.runsInFlight.delete(payload.data.runId);
          return emitTerminalEvent(ctx.upstreamSender, result);
        })
        .catch((cause) => {
          ctx.runsInFlight.delete(payload.data.runId);
          logger.error`triggered run ${payload.data.runId} failed: ${String(cause)}`;
        });
      ctx.triggeredRunIds.push(payload.data.runId);
      return false;
    }
    case "grants-updated": {
      // The supervisor pushes the fresh snapshot inline. Replace the
      // closure-local snapshot reference so every subsequent
      // `authorize` call against the credentials-backed closure
      // (`createCredentialsBackedAuthorize`) reads the new per-step
      // grants without reconstructing the workflow env. The optional
      // `stepHashes` cross-check is informational: when present, a
      // mismatch against the snapshot's per-step contentHash crashes
      // the child rather than silently honoring a desynchronized
      // push.
      const snapshot: CredentialsSnapshot = {
        steps: payload.data.snapshot.steps.map((s) => ({
          stepId: s.stepId,
          address: s.address,
          grants: s.grants,
          contentHash: s.contentHash,
        })),
      };
      if (payload.data.stepHashes !== undefined) {
        for (const step of snapshot.steps) {
          const expected = payload.data.stepHashes[step.stepId];
          if (expected !== undefined && expected !== step.contentHash) {
            throw new Error(
              `workflow-child grants-updated: stepHashes pin for ${step.stepId} (${expected}) does not match snapshot contentHash (${step.contentHash})`,
            );
          }
        }
      }
      ctx.credentialsRef.current = snapshot;
      return false;
    }
    case "credentials-updated": {
      // Replace the in-memory credential material wholesale. A revoked
      // credential arrives by omission -- its material entry is absent from
      // the delivery -- so the swap evicts it. Atomic whole-object assignment,
      // so a concurrent reader never observes a torn cell. The secret stays on
      // this ref only; nothing here copies it into a snapshot, event, or state.
      ctx.credentialMaterialRef.current = payload.data.delivery;
      return false;
    }
    case "signal.deliver": {
      // Drop a delivery for a run this child is not driving. The dispatch path
      // only ever targets a live run id, but a stale or mis-routed frame -- a
      // synthetic body-child id, or a run that crashed and has not been
      // re-discovered -- must not commit an orphan `SignalReceived` to a log no
      // awaiter is tailing. `runsInFlight` is the one-driver authority on which
      // runs this child drives.
      if (!ctx.runsInFlight.has(payload.data.runId)) {
        logger.warn`signal.deliver for run ${payload.data.runId} which is not in flight; dropping (signalName=${payload.data.signalName})`;
        return false;
      }
      // Land the signal as a `SignalReceived` commit on the run's
      // event log. The signal-channel substrate's `subscribeKind`
      // peer (the per-run signal channel installed at run start) is
      // what resolves any pending `awaitNext` awaiter -- the
      // control-loop's job is just to commit. Constructing an
      // ad-hoc signal channel scoped to this runId keeps the
      // control-loop free of per-run signal-channel bookkeeping
      // while still routing through the canonical writer path.
      //
      // The deliver path writes through `writeTreePreservingPrefix`,
      // which the sidecar's substrate factory wraps with a pack-push
      // hook. The hook emits a `pack.push.request` on the upstream
      // control channel and awaits the supervisor's matching
      // `pack.push.response` on the same downstream stream this
      // iterator pulls from. Awaiting the deliver inline blocks the
      // iterator from pulling the response that resolves the deliver
      // -- a deadlock observed end-to-end with the workflow-run
      // pack-pushing wrapper. Fire the deliver off the loop so the
      // iterator continues pumping `pack.push.response` (and any other
      // downstream payload) while the deliver settles in the
      // background. A commit failure surfaces via the logger; the
      // runtime body's `signalChannel.awaitNext` peer either resolves
      // (deliver landed) or remains pending until a subsequent
      // delivery.
      const transientSignalChannel = createWorkflowHostSignalChannel({
        repoStore: ctx.bindings.substrate,
        principal: ctx.bindings.principal,
        repoId: ctx.bindings.workflowRunRepoId,
        ref: ctx.bindings.workflowRunRef,
        runId: payload.data.runId,
        readState: () => emptyState(payload.data.runId),
        newId: () => ctx.newId("sig"),
        clock: ctx.clock,
      });
      void (async () => {
        try {
          await transientSignalChannel.deliver(
            payload.data.signalName,
            payload.data.payload,
            payload.data.signalId,
          );
        } catch (cause) {
          const reason = cause instanceof Error ? cause.message : String(cause);
          logger.warn`signal.deliver commit failed runId=${payload.data.runId} signalName=${payload.data.signalName}: ${reason}`;
        } finally {
          await transientSignalChannel.stop();
        }
      })();
      return false;
    }
    case "drain": {
      // The supervisor's `drain` control mail flips the controller's
      // signal. The runtime body's four observation points read the
      // signal on their next tick; cancel-mode steps abort their
      // local controllers, wait-mode steps continue. The
      // supervisor's drainTimeout accumulator (host-side) escalates
      // to a signed CancelRequested if cancel-mode work outlasts the
      // deadline.
      logger.info`workflow-child drain requested (deadlineMs=${String(payload.data.deadlineMs)})`;
      ctx.drainController.requestDrain();
      return false;
    }
    case "shutdown": {
      logger.info`workflow-child shutdown requested (${payload.data.reason})`;
      if (ctx.substrateWriteBridge !== undefined) {
        ctx.substrateWriteBridge.cancelAll("workflow-child shutdown requested");
      }
      return true;
    }
    case "sources-updated": {
      // Live inference-source rotation for the warm single-step agent. The
      // wire boundary (`SourcesUpdatedData`) already guaranteed the list is
      // non-empty, its ids are unique, and its head is the default, so this
      // trusts the frame and does not re-validate it.
      //
      // Only a single-step deployment rotates sources: its sole step's id
      // is the sole key in the sources table, so the whole table is
      // replaced. A multi-step deployment has no single per-agent source
      // identity to swap and is never routed a sources-updated frame;
      // assert it so a mis-route fails loudly rather than corrupting the
      // table.
      if (ctx.definition.stepOrder.length !== 1) {
        throw new Error(
          `workflow-child sources-updated: only a single-step deployment can rotate sources; got ${String(ctx.definition.stepOrder.length)} steps`,
        );
      }
      const stepId = ctx.definition.stepOrder[0];
      if (stepId === undefined) {
        throw new Error(
          "workflow-child sources-updated: single-step deployment has no step id",
        );
      }
      // A sources-updated only reaches a warm single-step deployment, which
      // always builds a warm cache. An absent cache is a routing bug, not a
      // silent no-op.
      if (ctx.warmCache === undefined) {
        throw new Error(
          "workflow-child sources-updated: no warm cache; a sources rotation must target a warm single-step deployment",
        );
      }
      // Swap the built warm agent first (a no-op when none is built yet),
      // then update the table the next cold build reads. Applying to the
      // agent first means a rotation racing eviction -- a closed-agent
      // `setSources` throw -- leaves the table untouched rather than ahead
      // of a half-applied swap.
      ctx.warmCache.applySources(
        payload.data.sources,
        payload.data.defaultSource,
      );
      ctx.sourcesRef.current = { [stepId]: payload.data.sources };
      return false;
    }
    case "ready": {
      // `ready` is a child->supervisor frame; receiving one on the
      // child's downstream side is a protocol violation that the
      // sender should not be able to produce against the typed union.
      throw new Error(
        "workflow-child received a `ready` frame on its inbound control channel; this is a supervisor-only payload",
      );
    }
    case "recycle.request": {
      // `recycle.request` is the child->supervisor self-initiated
      // recycle path; receiving one on the child's downstream side is
      // the same shape of protocol violation as a downstream `ready`.
      throw new Error(
        "workflow-child received a `recycle.request` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "substrate.write.request": {
      // `substrate.write.request` is the child->supervisor proxied
      // write path; receiving one on the child's downstream side is a
      // protocol violation in the same shape as a downstream `ready`.
      throw new Error(
        "workflow-child received a `substrate.write.request` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "substrate.merge.response": {
      // `substrate.merge.response` is the child->supervisor merge
      // result frame; receiving one on the child's downstream side is
      // a protocol violation in the same shape as a downstream
      // `ready`.
      throw new Error(
        "workflow-child received a `substrate.merge.response` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "terminal.event": {
      // `terminal.event` is the child->supervisor terminal-run
      // notification frame; receiving one on the child's downstream
      // side is a protocol violation in the same shape as a downstream
      // `ready` or `recycle.request`.
      throw new Error(
        "workflow-child received a `terminal.event` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "park.notify": {
      // `park.notify` is the child->supervisor suspension-notification
      // frame; receiving one on the child's downstream side is a
      // protocol violation in the same shape as a downstream
      // `terminal.event`.
      throw new Error(
        "workflow-child received a `park.notify` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "outbound.message": {
      // `outbound.message` is the child->supervisor outbound-mail
      // request frame; receiving one on the child's downstream side is a
      // protocol violation in the same shape as a downstream `ready`.
      throw new Error(
        "workflow-child received an `outbound.message` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "outbound.result": {
      // Route the supervisor's signed-send result to the outbound-mail
      // bridge if one is wired. A result that lands without an active
      // bridge means a stale supervisor frame for which no awaiter
      // exists; log and drop rather than throwing so the runtime keeps
      // progressing.
      if (ctx.outboundMailBridge === undefined) {
        logger.warn`workflow-child outbound.result received without a bridge wired; requestId=${payload.data.requestId} dropped`;
        return false;
      }
      ctx.outboundMailBridge.handleResult(payload.data);
      return false;
    }
    case "mailbox.notify": {
      // Route the supervisor's new-mail notification to the child's watch
      // registry so a step agent's `watch`/`mail_wait` observes the arrival.
      // A notify that lands without a registry means no watcher on the child
      // side asked for inbound events; log and drop rather than throwing so
      // the runtime keeps progressing (mirrors the `outbound.result` arm).
      if (ctx.mailboxWatchRegistry === undefined) {
        logger.warn`workflow-child mailbox.notify received without a watch registry wired; mailbox=${payload.data.mailbox} uid=${String(payload.data.uid)} dropped`;
        return false;
      }
      ctx.mailboxWatchRegistry.fire(payload.data.mailbox, {
        type: "exists",
        uid: payload.data.uid,
        headers: payload.data.headers,
      });
      return false;
    }
    case "mailbox.mutate.request": {
      // `mailbox.mutate.request` is the child->supervisor mailbox-mutation
      // request frame; receiving one on the child's downstream side is a
      // protocol violation in the same shape as a downstream
      // `outbound.message`.
      throw new Error(
        "workflow-child received a `mailbox.mutate.request` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "mailbox.mutate.response": {
      // Route the supervisor's applied-mutation result to the
      // mailbox-mutation bridge if one is wired. A response that lands
      // without an active bridge means a stale supervisor frame for which
      // no awaiter exists; log and drop rather than throwing so the
      // runtime keeps progressing (mirrors the `outbound.result` arm).
      if (ctx.mailboxMutationBridge === undefined) {
        logger.warn`workflow-child mailbox.mutate.response received without a bridge wired; requestId=${payload.data.requestId} dropped`;
        return false;
      }
      ctx.mailboxMutationBridge.handleResult(payload.data);
      return false;
    }
    case "substrate.merge.request": {
      // Route the request to the substrate-write bridge if one is
      // wired. A request that lands without an active bridge means a
      // stale supervisor frame for which no awaiter exists; log and
      // drop rather than throwing so the runtime keeps progressing.
      if (ctx.substrateWriteBridge === undefined) {
        logger.warn`workflow-child substrate.merge.request received without a bridge wired; requestId=${payload.data.requestId} dropped`;
        return false;
      }
      ctx.substrateWriteBridge.handleMergeRequest(payload.data);
      return false;
    }
    case "substrate.write.response": {
      // Route the response to the substrate-write bridge if one is
      // wired. A response that lands without an active bridge means a
      // stale supervisor frame for which no awaiter exists; log and
      // drop rather than throwing so the runtime keeps progressing.
      if (ctx.substrateWriteBridge === undefined) {
        logger.warn`workflow-child substrate.write.response received without a bridge wired; requestId=${payload.data.requestId} dropped`;
        return false;
      }
      ctx.substrateWriteBridge.handleWriteResponse(payload.data);
      return false;
    }
    case "parked-correlations.request": {
      // Answer the supervisor's re-registration enumeration from durable
      // state. Awaiting inline is safe -- unlike `signal.deliver`, this
      // reads (self-discovery + the snapshot binding) and sends one upstream
      // reply without awaiting any downstream frame, so it cannot deadlock
      // the iterator against a response it is itself blocking. A store
      // inconsistency (an enumerated park with no durable snapshot, or no
      // binding to recover one) throws out of the loop like the other
      // invariant-violation arms rather than dropping a correlation the hub
      // is waiting to register.
      const parked = await collectParkedApprovalCorrelations({
        substrate: ctx.bindings.substrate,
        repoId: ctx.bindings.workflowRunRepoId,
        runtimeRepoStore: ctx.runtimeRepoStore,
        ...(ctx.bindings.loadParkedApproval !== undefined
          ? { loadParkedApproval: ctx.bindings.loadParkedApproval }
          : {}),
      });
      await ctx.upstreamSender.send({
        type: "parked-correlations.response",
        data: { requestId: payload.data.requestId, parked },
      });
      return false;
    }
    case "resumed.runs": {
      // `resumed.runs` is the child->supervisor self-discovery report;
      // receiving one on the child's downstream side is a protocol
      // violation in the same shape as a downstream `ready`.
      throw new Error(
        "workflow-child received a `resumed.runs` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "parked-correlations.response": {
      // `parked-correlations.response` is the child->supervisor reply frame;
      // receiving one on the child's downstream side is a protocol violation
      // in the same shape as a downstream `substrate.merge.response`.
      throw new Error(
        "workflow-child received a `parked-correlations.response` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
  }
}

/**
 * Construct a `WorkflowRuntimeEnv` for one run. Each run gets its own
 * `BlobSubstrate` and `SignalChannel` because both are per-run by
 * shape; the substrate handle and per-deployment `RepoStore` adapter
 * are shared across runs.
 */
/**
 * Force-resolve every loop `while`/`carry` ref reachable from these definitions
 * against the registry, so a missing loop fn surfaces at establish rather than
 * when the loop is first driven mid-run. Recurses into a loop's inline body (a
 * nested loop resolves against the same shared registry). The caller passes the
 * lifted onTrigger/childWorkflow bodies separately, since those are `{ ref }` in
 * the top-level definition and this walk does not descend into them.
 */
function eagerlyResolveLoopFns(
  definitions: readonly WorkflowDefinition[],
  loopFns: LoopFnRegistry,
): void {
  const visit = (def: WorkflowDefinition): void => {
    for (const step of Object.values(def.steps)) {
      if (step.kind === "loop") {
        // Each call throws (fail closed) if the ref names no export, or an
        // export that is not a function.
        loopFns(step.while);
        loopFns(step.carry);
        visit(step.body);
      }
    }
  };
  for (const def of definitions) visit(def);
}

/**
 * Force-resolve every `action` handler ref reachable from these definitions
 * against the resolver, so a missing action handler surfaces at establish
 * rather than when the action is first invoked mid-run. Recurses into loop
 * bodies (an action body is the common loop shape). The caller passes the
 * lifted onTrigger/childWorkflow bodies separately, as with loop fns.
 */
function eagerlyResolveActionHandlers(
  definitions: readonly WorkflowDefinition[],
  actionResolver: (ref: string) => ActionHandler,
): void {
  const visit = (def: WorkflowDefinition): void => {
    for (const step of Object.values(def.steps)) {
      if (step.kind === "action") {
        // Throws (fail closed) if the handler names no export, or a non-function.
        actionResolver(step.handler);
      } else if (step.kind === "loop") {
        visit(step.body);
      }
    }
  };
  for (const def of definitions) visit(def);
}

function buildRuntimeEnv(args: {
  runId: string;
  bindings: RunWorkflowChildBindings;
  runtimeRepoStore: ReturnType<typeof createWorkflowRunRepoStore>;
  authorize: WorkflowAuthorizeFn;
  directors: DirectorRegistry;
  suspendableChildHost: HostSpawnSuspendableChild | undefined;
  bodiesMap: ReadonlyMap<string, WorkflowDefinition>;
  spawnChild: HostSpawnChild;
  loopFns: LoopFnRegistry;
  actionResolver: (ref: string) => ActionHandler;
  clock: () => Date;
  newId: (prefix: string) => string;
  drainController: DrainController;
  warmCache: WarmAgentCache | undefined;
  sourcesRef: SourcesSnapshotRef;
  credentialWiring: CredentialWiring;
  onEvent: (event: EventPayload) => void;
  upstreamSender: ControlChannelSender;
}): WorkflowRuntimeEnv {
  const signalChannel = createWorkflowHostSignalChannel({
    repoStore: args.bindings.substrate,
    principal: args.bindings.principal,
    repoId: args.bindings.workflowRunRepoId,
    ref: args.bindings.workflowRunRef,
    runId: args.runId,
    readState: () => emptyState(args.runId),
    newId: () => args.newId("sig"),
    clock: args.clock,
  });
  const blobs = createWorkflowRunBlobSubstrate({
    substrate: args.bindings.substrate,
    repoId: args.bindings.workflowRunRepoId,
    principal: args.bindings.principal,
    runId: args.runId,
    ref: args.bindings.workflowRunRef,
  });
  // Reader for inbound-mail parts, a sibling of `blobs` over the same
  // workflow-run repo. The step invoker resolves a `Mail` part's `ref` to its
  // bytes through it at `agent.send` time; the supervisor committed the bytes
  // before the trigger. Deployment-scoped (the ref encodes the owning run), so
  // one reader resolves any run's parts.
  const mailPartReader = createMailPartReader({
    substrate: args.bindings.substrate,
    repoId: args.bindings.workflowRunRepoId,
    principal: args.bindings.principal,
    ref: args.bindings.workflowRunRef,
  });
  // Wrap the step invoker so every `InferenceEvent` the harness emits
  // funnels through the per-run `onEvent` closure, which forwards
  // the event up the HMAC-authenticated event channel. The wrap is
  // the only translation point between the workflow-runtime's
  // narrow `StepInvoker` shape (no event slot) and the host's
  // `ChildStepInvoker` shape (carries onEvent), so the workflow-
  // runtime never has to know an event firehose exists.
  const invokeStep: StepInvoker = async (req) => {
    return args.bindings.invokeStep(
      req,
      args.onEvent,
      args.authorize,
      args.warmCache,
      args.sourcesRef,
      args.credentialWiring,
      mailPartReader,
    );
  };
  // Adapt the host binding (which takes the run's `onEvent` sink) down to the
  // runtime's narrow `SpawnSuspendableChild` by injecting THIS run's event
  // funnel -- the same closure `invokeStep` forwards -- so a body's live
  // inference events ride the parent run's event channel to the hub stream
  // (and inherit its loud-on-failure logging), while the runtime env keeps the
  // narrow contract with no event slot.
  const hostSuspendable = args.suspendableChildHost;
  const spawnSuspendableChild: SpawnSuspendableChild | undefined =
    hostSuspendable === undefined
      ? undefined
      : (spawnInput) => hostSuspendable(spawnInput, args.onEvent);
  // Same adaptation for the terminal childWorkflow seam: inject THIS run's event
  // funnel so a child's live inference events ride the parent run's channel,
  // while the runtime env keeps the narrow `SpawnChildWorkflow` (no event slot).
  const spawnChild: SpawnChildWorkflow = (spawnInput) =>
    args.spawnChild(spawnInput, args.onEvent);
  const env: WorkflowRuntimeEnv = {
    repoStore: args.runtimeRepoStore,
    scheduler: args.bindings.scheduler,
    signalChannel,
    blobs,
    directors: args.directors,
    authorize: args.authorize,
    invokeStep,
    spawnChild,
    // Resolve a loop's `while`/`carry` refs against the closure's loop module.
    // Every ref was force-resolved at establish, so a lookup here cannot fail
    // for a definition that passed startup.
    loopFns: args.loopFns,
    // Wire the suspendable-child seam only when the host supplied it; a child
    // that never runs an onTrigger section omits the binding, and the runtime
    // body fails loud if a workflow reaches a section the env did not wire.
    ...(spawnSuspendableChild !== undefined ? { spawnSuspendableChild } : {}),
    clock: args.clock,
    newId: args.newId,
    drain: args.drainController,
    // Forward a control-plane suspension up the same upstream control
    // channel `terminal.event` rides, so the supervisor can stamp the
    // deployment identity and register the correlation at the hub. The
    // runtime body fires this once per fresh park on a reserved
    // `signalName(correlationId)` channel.
    onPark: (park) => {
      void emitParkNotify(args.upstreamSender, park);
    },
    // Let the resume classifier recover a step that crashed across the park
    // boundary. Absent (tests, the recursive child-workflow adapter) leaves a
    // crashed invocation a terminal failure.
    ...(args.bindings.readParkedApprovalOps !== undefined
      ? { readParkedApprovalOps: args.bindings.readParkedApprovalOps }
      : {}),
  };
  // The suspendable-loop executor runs each iteration's body under THIS run's
  // inherited env (its real tool-bearing invokeStep, invokeAction, credentials
  // authorize, effect ledger, and durable shared repoStore/blobs), giving the
  // body only its own substrate-backed signal channel -- the createLoopIteration
  // env model plus park capability, distinct from an onTrigger body's fresh
  // capped/toolless env. Resolved from the bodies map by ref (loop bodies were
  // registered there at establish) and wrapped with this run's event funnel.
  // Assigned AFTER env construction because it closes over `env`.
  const loopIterationHost = createInMemorySpawnSuspendableChild({
    bodies: args.bodiesMap,
    runSuspendableChild: async (loopInput, _onEvent) => {
      const childSignalChannel = createWorkflowHostSignalChannel({
        repoStore: args.bindings.substrate,
        principal: args.bindings.principal,
        repoId: args.bindings.workflowRunRepoId,
        ref: args.bindings.workflowRunRef,
        runId: loopInput.childRunId,
        readState: () => emptyState(loopInput.childRunId),
        newId: () => args.newId("sig"),
        clock: args.clock,
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
        signalChannel: childSignalChannel,
        cleanup: () => childSignalChannel.stop(),
      });
    },
  });
  env.spawnLoopIteration = (spawnInput) =>
    loopIterationHost(spawnInput, args.onEvent);

  // Action handlers run against a per-run effect ledger. The ledger is
  // IN-MEMORY, and that is correct -- not a shortcut -- on the deployed store:
  // appends are immediate-durable single-ref commits, `runAction` flushes
  // StepStarted durably before the effect, and the runtime never re-invokes a
  // crashed action (a mid-action crash settles the step failed; a loop-body
  // action leaves a non-empty child log that fails the iteration loud rather
  // than re-running). So the ledger is never consulted across a crash; its
  // cross-crash exactly-once rests on that store-consistency invariant, which
  // the store layer owns. A durable ledger here would re-enforce a constraint
  // a lower layer already guarantees. Within a single invocation the ledger
  // still dedups a handler that performs the same effect twice.
  const effects = createInMemoryEffectLedger();
  env.effects = effects;
  env.invokeAction = createDefaultActionInvoker(
    args.authorize,
    effects,
    args.actionResolver,
  );
  return env;
}

/**
 * Forward a control-plane suspension to the supervisor over the upstream
 * control channel. Fired from `env.onPark` each time a workflow agent step
 * parks on a reserved `signalName(correlationId)` channel. The supervisor's
 * `park.notify` arm stamps the deployment identity it owns and sends a
 * `signal.correlation.register` frame to the hub.
 *
 * Best-effort like `emitTerminalEvent`'s send: a transport failure is logged,
 * not rethrown. A lost frame means the correlation is not registered and the
 * parked run cannot be resumed until it is re-registered; the failure surfaces
 * structurally as a run that never resumes rather than a silent lifecycle
 * corruption. The register at the hub is idempotent, so a re-park resume's
 * re-emit is safe.
 */
export function emitParkNotify(
  upstreamSender: ControlChannelSender,
  park: WorkflowPark,
): Promise<void> {
  return upstreamSender
    .send({
      type: "park.notify",
      data: {
        runId: park.runId,
        correlationId: park.correlationId,
        parkKind: park.parkKind,
        ...(park.approvalSnapshot !== undefined
          ? { snapshot: park.approvalSnapshot }
          : {}),
      },
    })
    .catch((cause) => {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`park.notify upstream send failed for runId=${park.runId} correlationId=${park.correlationId}: ${message}`;
    });
}

/**
 * Mirror a run's terminal status back to the supervisor over the
 * upstream control channel. Fired once per run from the resume and
 * trigger.fire paths' `complete` continuation. The supervisor's
 * per-cohort terminal broadcaster fans the event out to the dispatch
 * loop and any armed drainTimeout accumulator subscribed for the
 * runId.
 *
 * The frame mirrors the run's committed terminal event: every field --
 * `kind`, `seq`, `at`, and (for `RunFailed`) `error.message` -- is
 * sourced from that event, which is why the frame's `seq` matches the
 * on-disk audit-log entry. `terminalStatus` is only the cross-check: the
 * found event's `kind` must agree with it. A missing terminal event, or
 * one whose kind disagrees, is a runtime producer bug (the runtime
 * commits the terminal event last), and emitting a frame anyway would
 * desync the supervisor from the durable log that `discoverInFlightRuns`
 * reads on resume -- the supervisor would settle a run the on-disk log
 * still shows in-flight. So this throws instead: no frame keeps the
 * supervisor and the durable log agreeing that the run is unsettled, and
 * the next recycle/restart resumes it. The throw propagates to the
 * caller's `complete` continuation, which logs it.
 *
 * Errors flowing out of `upstreamSender.send` are a different case --
 * a transport send failure, logged but not rethrown. The supervisor's
 * dispatch loop is the authoritative settler through its cohort abort
 * signal, so a lost frame surfaces structurally as a wedged dispatch
 * rather than a silent lifecycle failure. The invariant throws above run
 * before the send so that catch never swallows them.
 */
export function emitTerminalEvent(
  upstreamSender: ControlChannelSender,
  result: RunResult,
): Promise<void> {
  // Recover the terminal event from the committed event log. The runtime
  // body commits the terminal event last; walking from the end finds it in
  // one step without rebuilding the state machine.
  let terminalEvent: (typeof result.events)[number] | null = null;
  for (let i = result.events.length - 1; i >= 0; i -= 1) {
    const candidate = result.events[i];
    if (candidate === undefined) continue;
    if (
      candidate.kind === "RunCompleted" ||
      candidate.kind === "RunFailed" ||
      candidate.kind === "RunCancelled"
    ) {
      terminalEvent = candidate;
      break;
    }
  }
  if (terminalEvent === null) {
    throw new Error(
      `emitTerminalEvent: run ${result.runId} terminated as ${result.terminalStatus} but its committed event log carries no terminal event (the runtime commits it last; this is a producer bug)`,
    );
  }
  const expectedKind =
    result.terminalStatus === "completed"
      ? "RunCompleted"
      : result.terminalStatus === "cancelled"
        ? "RunCancelled"
        : "RunFailed";
  if (terminalEvent.kind !== expectedKind) {
    throw new Error(
      `emitTerminalEvent: run ${result.runId} terminated as ${result.terminalStatus} but its committed terminal event is ${terminalEvent.kind}`,
    );
  }
  // The RunFailed-missing-error.message case the supervisor's
  // `synthesizeTerminalEvent` guards is unreachable here: `result.events`
  // is typed `WorkflowEvent[]`, and `RunFailed.error.message` is a
  // non-optional `string`, so a RunFailed reached here always carries one.
  // The supervisor needs that guard because it parses untrusted JSON.
  let payload: Extract<ControlPayload, { type: "terminal.event" }>["data"];
  if (terminalEvent.kind === "RunCompleted") {
    payload = {
      runId: result.runId,
      seq: terminalEvent.seq,
      kind: "RunCompleted",
      at: terminalEvent.at,
    };
  } else if (terminalEvent.kind === "RunCancelled") {
    payload = {
      runId: result.runId,
      seq: terminalEvent.seq,
      kind: "RunCancelled",
      at: terminalEvent.at,
    };
  } else {
    payload = {
      runId: result.runId,
      seq: terminalEvent.seq,
      kind: "RunFailed",
      at: terminalEvent.at,
      error: { message: terminalEvent.error.message },
    };
  }
  return upstreamSender
    .send({
      type: "terminal.event",
      data: payload,
    })
    .catch((cause) => {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`terminal.event upstream send failed for runId=${result.runId}: ${message}`;
    });
}

/**
 * Reclaim a completed run's local-disk scratch on the COLD path.
 *
 * Gated on `!warmKeep`: a warm deployment's single agent reuses one
 * stable workspace across runs (the substrate factory roots its scratch
 * per agent, not per run), so per-run deletion there would wipe a live
 * conversation's files mid-stream. On the cold path each run rebuilds
 * its agent + scratch, so once the run is terminal nothing reopens its
 * `runs/<runId>/` subtree (resume reads the substrate run log, not local
 * step state) and the subtree is safe to drop.
 *
 * Best-effort: a reclamation failure is logged and swallowed -- it must
 * never gate the run's terminal status or the upstream terminal.event.
 */
function reclaimRunStorageIfCold(opts: {
  warmKeep: boolean;
  cleanupRunStorage: ((runId: string) => Promise<void>) | undefined;
  runId: string;
}): void {
  if (opts.warmKeep) return;
  if (opts.cleanupRunStorage === undefined) return;
  void opts.cleanupRunStorage(opts.runId).catch((cause) => {
    const message = cause instanceof Error ? cause.message : String(cause);
    logger.warn`workflow-step-state cleanup failed for runId=${opts.runId}: ${message}`;
  });
}

function defaultClock(): Date {
  return new Date();
}

let idCounter = 0;
function defaultNewId(prefix: string): string {
  idCounter += 1;
  return `${prefix}-${String(idCounter)}-${Math.random().toString(36).slice(2, 8)}`;
}

/** Re-export the hash helper so callers can verify the snapshot's pin. */
export { hashGrants };
