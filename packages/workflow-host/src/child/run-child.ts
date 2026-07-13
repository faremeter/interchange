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
//        - `trigger.fired` -> open a new run via `runtimeRun`.
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

import { type } from "arktype";

import { getLogger } from "@intx/log";
import { generateKeyPair } from "@intx/crypto";
import { base64Decode, hexEncode } from "@intx/types";

import type {
  Principal,
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions/substrate";
import {
  readProcessingEntry,
  workflowDefinitionEnvelopeSchema,
} from "@intx/hub-sessions/substrate";
import {
  extractPartByPath,
  parseHeaderSection,
  parseMimePart,
} from "@intx/mime";
import type { DirectorRegistry } from "@intx/agent";
import { createDefaultDirectorRegistry } from "@intx/agent";
import type { AuthzCallResult } from "@intx/inference";

import type {
  RunResult,
  Scheduler,
  StepInvokeRequest,
  StepInvokeResult,
  StepInvoker,
  SpawnChildWorkflow,
  WorkflowAuthorizeFn,
  WorkflowDefinition,
  WorkflowRun,
  WorkflowRuntimeEnv,
} from "@intx/workflow";
import { emptyState, runtimeRun } from "@intx/workflow";

import {
  createWorkflowHostDrainController,
  type WorkflowHostDrainController,
} from "../drain-controller";

import type { InferenceSource } from "@intx/types/runtime";

import { createWorkflowRunRepoStore } from "../adapters/repo-store";
import { createWorkflowRunBlobSubstrate } from "../adapters/blob-substrate";
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
import { createWorkflowHostSignalChannel } from "../seams/signal-channel";
import type { CredentialsSnapshot } from "../supervisor/credentials";
import { hashGrants } from "../supervisor/credentials";

import type { SpawnTimeEnv } from "./env-bootstrap";
import { discoverInFlightRuns } from "./self-discovery";
import type { ChildOutboundMailBridge } from "./outbound-mail-bridge";
import { createWarmAgentCache, type WarmAgentCache } from "./warm-agent-cache";

const logger = getLogger(["workflow-host", "child"]);

const WORKFLOW_JSON_PATH = "workflow.json";

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
    const entry = snapshot.steps.find((s) => s.stepId === stepId);
    if (entry === undefined) {
      throw new Error(
        `workflow-child authorize: credentialsSnapshot has no entry for stepId ${stepId}`,
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
export type ChildStepInvoker = (
  req: StepInvokeRequest,
  onEvent: (event: EventPayload) => void,
  authorize: WorkflowAuthorizeFn,
  warmCache: WarmAgentCache | undefined,
  sourcesRef: SourcesSnapshotRef,
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
  /** Workflow-asset repo identity (used to load `workflow.json`). */
  workflowDefinitionRepoId: RepoId;
  /** Workflow-asset ref the deploy orchestrator wrote to. */
  workflowDefinitionRef: string;
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
   * Child-spawn callback the runtime body invokes for `childWorkflow`
   * primitives. The production binary wires this against
   * `createWorkflowSpawnChild`; tests inject a stub.
   */
  spawnChild: SpawnChildWorkflow;
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
  /** Optional director registry; defaults to the canonical built-ins. */
  directors?: DirectorRegistry;
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
  const directors = opts.bindings.directors ?? createDefaultDirectorRegistry();
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

  const definition = await loadWorkflowDefinition(opts.bindings);

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
      clock,
      newId,
      drainController,
      warmCache,
      sourcesRef,
      onEvent: (event) => {
        void eventSender.send(event).catch((cause) => {
          logger.error`event-channel send failed during resume run ${run.runId}: ${String(cause)}`;
        });
      },
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
        return emitTerminalEvent(upstreamSender, result);
      })
      .catch((cause) => {
        logger.error`resumed run ${run.runId} failed: ${String(cause)}`;
      })
      .finally(() => {
        runsInFlight.delete(run.runId);
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

  try {
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
          clock,
          newId,
          eventSender,
          upstreamSender,
          drainController,
          triggeredRunIds,
          runsInFlight,
          warmCache,
          sourcesRef,
          ...(opts.substrateWriteBridge !== undefined
            ? { substrateWriteBridge: opts.substrateWriteBridge }
            : {}),
          ...(opts.outboundMailBridge !== undefined
            ? { outboundMailBridge: opts.outboundMailBridge }
            : {}),
        })
      ) {
        // shutdown received; the shutdown case already cancelled any
        // pending substrate writes before returning true.
        break;
      }
    }
  } finally {
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
  }

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
    clock: () => Date;
    newId: (prefix: string) => string;
    eventSender: ReturnType<typeof createEventChannelSender>;
    upstreamSender: ControlChannelSender;
    drainController: DrainController;
    triggeredRunIds: string[];
    runsInFlight: Map<string, WorkflowRun>;
    warmCache: WarmAgentCache | undefined;
    sourcesRef: SourcesSnapshotRef;
    substrateWriteBridge?: SubstrateWriteResponseSink;
    outboundMailBridge?: ChildOutboundMailBridge;
  },
): Promise<boolean> {
  switch (payload.type) {
    case "trigger.fire": {
      // One driver per runId. If this child is already driving this
      // runId -- self-discovery resumed it, or an earlier trigger opened
      // it -- the supervisor's re-fire (which carries `runId = messageId`
      // and no resumeFromEvents) must NOT spawn a second `runtimeRun`. A
      // second concurrent driver would race the live one to settle the
      // same residual and the loser throws an uncaught TransitionError,
      // and even a driver that avoided the throw would double-emit the
      // terminal. The live driver's completion continuation owns the
      // single terminal emission; the supervisor's terminal-event-driven
      // `markConsumed` consumes the message off that one terminal, so no
      // work is dropped by declining here. Record the runId (the
      // supervisor did fire a trigger and it was accepted) and signal
      // "handled, not shutdown" the same way the normal trigger case
      // returns, without awaiting the live handle's `complete` inline
      // (that would block the control loop).
      if (ctx.runsInFlight.has(payload.data.runId)) {
        ctx.triggeredRunIds.push(payload.data.runId);
        return false;
      }
      // Resolve the inbound mail bytes for this messageId from the
      // claim-check processing entry the supervisor created when it
      // dequeued the message. The bytes become the run's trigger
      // payload; the one-step workflow's first step defaults its input
      // selector to `trigger.payload` (defineWorkflow's default-input
      // convention), so the step input resolves to the inbound message
      // and `agent.send` receives it. A missing or unreadable entry
      // surfaces loudly -- the run cannot proceed without its input,
      // and silently running the agent with empty input would mask a
      // real mailbox-ownership failure.
      const triggerPayload = await resolveTriggerPayload({
        substrate: ctx.bindings.substrate,
        principal: ctx.bindings.principal,
        workflowRunRepoId: ctx.bindings.workflowRunRepoId,
        mailboxAddress: ctx.env.mailboxAddress,
        messageId: payload.data.messageId,
      });
      const env = buildRuntimeEnv({
        runId: payload.data.runId,
        bindings: ctx.bindings,
        runtimeRepoStore: ctx.runtimeRepoStore,
        authorize: ctx.authorize,
        directors: ctx.directors,
        clock: ctx.clock,
        newId: ctx.newId,
        drainController: ctx.drainController,
        warmCache: ctx.warmCache,
        sourcesRef: ctx.sourcesRef,
        onEvent: (event) => {
          void ctx.eventSender.send(event).catch((cause) => {
            logger.error`event-channel send failed during run ${payload.data.runId}: ${String(cause)}`;
          });
        },
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
          return emitTerminalEvent(ctx.upstreamSender, result);
        })
        .catch((cause) => {
          logger.error`triggered run ${payload.data.runId} failed: ${String(cause)}`;
        })
        .finally(() => {
          ctx.runsInFlight.delete(payload.data.runId);
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
    case "signal.deliver": {
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
  }
}

/**
 * Construct a `WorkflowRuntimeEnv` for one run. Each run gets its own
 * `BlobSubstrate` and `SignalChannel` because both are per-run by
 * shape; the substrate handle and per-deployment `RepoStore` adapter
 * are shared across runs.
 */
function buildRuntimeEnv(args: {
  runId: string;
  bindings: RunWorkflowChildBindings;
  runtimeRepoStore: ReturnType<typeof createWorkflowRunRepoStore>;
  authorize: WorkflowAuthorizeFn;
  directors: DirectorRegistry;
  clock: () => Date;
  newId: (prefix: string) => string;
  drainController: DrainController;
  warmCache: WarmAgentCache | undefined;
  sourcesRef: SourcesSnapshotRef;
  onEvent: (event: EventPayload) => void;
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
    );
  };
  return {
    repoStore: args.runtimeRepoStore,
    scheduler: args.bindings.scheduler,
    signalChannel,
    blobs,
    directors: args.directors,
    authorize: args.authorize,
    invokeStep,
    spawnChild: args.bindings.spawnChild,
    clock: args.clock,
    newId: args.newId,
    drain: args.drainController,
  };
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

/**
 * Resolve the run's trigger payload from the inbound mail message the
 * supervisor moved to the claim-check processing queue. Reads the
 * processing entry by messageId (a read-only snapshot of the
 * `refs/heads/events` tip that cannot race the supervisor's
 * `markConsumed` write), decodes the inlined raw MIME bytes, and
 * extracts the conversation text the agent's `agent.send` receives.
 *
 * Defensive: a missing processing entry, an entry with no inlined
 * bytes, or unparseable mail all throw. The run cannot proceed without
 * its input, and a placeholder would mask a mailbox-ownership failure.
 */
async function resolveTriggerPayload(args: {
  substrate: SubstrateRepoStore;
  principal: Principal;
  workflowRunRepoId: RepoId;
  mailboxAddress: string;
  messageId: string;
}): Promise<string> {
  const entry = await readProcessingEntry(
    args.substrate,
    args.principal,
    args.workflowRunRepoId,
    args.mailboxAddress,
    args.messageId,
  );
  if (entry === null) {
    throw new Error(
      `workflow-child trigger.fire: no claim-check processing entry for messageId ${args.messageId} at ${args.mailboxAddress}; the run has no input to deliver to the agent`,
    );
  }
  const rawMessageBase64 = entry.envelope.rawMessage;
  if (rawMessageBase64 === undefined) {
    throw new Error(
      `workflow-child trigger.fire: processing entry for messageId ${args.messageId} carries no inlined rawMessage; the supervisor must inline the inbound mail bytes for the child to deliver them as the step input`,
    );
  }
  const raw = base64Decode(rawMessageBase64);
  return extractConversationText(raw, args.messageId);
}

/**
 * Extract the conversation body text from a raw inbound MIME message.
 *
 * Three on-wire shapes are handled, matching every producer the mail
 * bus accepts:
 *   1. The Interchange assembler's `multipart/signed` envelope whose
 *      first part is a `multipart/mixed` body carrying the text at part
 *      path `1.1`.
 *   2. A `multipart/signed` envelope wrapping a bare `text/plain` part
 *      (a sender that signs without the `multipart/mixed` wrapper); the
 *      text is at part path `1`.
 *   3. A flat top-level `text/plain` message (no multipart structure at
 *      all); the body is the bytes after the header section.
 *
 * The top-level `Content-Type` selects the shape: only a `multipart/*`
 * root walks into parts; anything else reads the single body directly.
 * This mirrors the conversation branch of mail-memory's `fetchFull`
 * while also tolerating the flat single-part case the in-process agent
 * accepts, so a non-standard inbound mail still delivers its text to
 * the agent rather than crashing the run.
 */
function extractConversationText(raw: Uint8Array, messageId: string): string {
  const { headers, bodyOffset } = parseHeaderSection(raw);
  const rootMime = (headers.get("content-type") ?? "")
    .split(";")[0]
    ?.trim()
    .toLowerCase();
  if (rootMime === undefined || !rootMime.startsWith("multipart/")) {
    // Flat single-part message: the body is everything after the
    // header section.
    return new TextDecoder("utf-8", { fatal: false }).decode(
      raw.subarray(bodyOffset),
    );
  }
  let part1: ReturnType<typeof parseMimePart>;
  try {
    part1 = parseMimePart(extractPartByPath(raw, "1"));
  } catch (cause) {
    throw new Error(
      `workflow-child trigger.fire: cannot parse inbound mail part 1 for messageId ${messageId}`,
      { cause },
    );
  }
  const part1Mime = (part1.contentType.split(";")[0] ?? "")
    .trim()
    .toLowerCase();
  const bodyBytes = part1Mime.startsWith("multipart/")
    ? parseMimePart(extractPartByPath(raw, "1.1")).body
    : part1.body;
  return new TextDecoder("utf-8", { fatal: false }).decode(bodyBytes);
}

/**
 * Load the `WorkflowDefinition` from the workflow asset repo's deploy
 * ref. Mirrors the sibling `spawn-child` adapter's working-tree-read
 * pattern -- the deploy orchestrator's `writeTree` materializes
 * `workflow.json` under the substrate's repo dir, so a flat
 * `fs.readFile` returns the bytes without round-tripping through git.
 */
async function loadWorkflowDefinition(
  bindings: RunWorkflowChildBindings,
): Promise<WorkflowDefinition> {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const dir = bindings.substrate.getRepoDir(bindings.workflowDefinitionRepoId);
  const workflowPath = path.join(dir, WORKFLOW_JSON_PATH);
  let raw: string;
  try {
    raw = await fs.readFile(workflowPath, "utf8");
  } catch (cause) {
    throw new Error(
      `workflow-child: cannot read ${WORKFLOW_JSON_PATH} for ${bindings.workflowDefinitionRepoId.kind}/${bindings.workflowDefinitionRepoId.id} on ${bindings.workflowDefinitionRef}`,
      { cause },
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new Error(
      `workflow-child: ${WORKFLOW_JSON_PATH} for ${bindings.workflowDefinitionRepoId.kind}/${bindings.workflowDefinitionRepoId.id} on ${bindings.workflowDefinitionRef} is not valid JSON`,
      { cause },
    );
  }
  const validated = workflowDefinitionEnvelopeSchema(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `workflow-child: ${WORKFLOW_JSON_PATH} for ${bindings.workflowDefinitionRepoId.kind}/${bindings.workflowDefinitionRepoId.id} on ${bindings.workflowDefinitionRef} failed envelope validation: ${validated.summary}`,
    );
  }
  // The envelope schema enforces the structural shape; the
  // discriminated narrow over every primitive variant lives downstream
  // in the runtime body. The sibling `spawn-child` adapter follows the
  // same pattern at the same boundary.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- envelope schema enforces structural shape; primitive narrows live downstream in the runtime body
  return validated as unknown as WorkflowDefinition;
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
