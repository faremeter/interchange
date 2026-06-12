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
//        - `recycle` -> no-op (recycle path lands elsewhere).
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
// DrainController is a NO-OP PLACEHOLDER in this commit. Recycle is
// also a no-op. Both seams exist so the control-loop's `drain` and
// `recycle` cases compile against a typed shape; the wired-up
// implementations land in their own commits.

import { type } from "arktype";

import { getLogger } from "@intx/log";
import { generateKeyPair } from "@intx/crypto-node";

import type {
  Principal,
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions";
import { workflowDefinitionEnvelopeSchema } from "@intx/hub-sessions";
import type { DirectorRegistry } from "@intx/agent";
import { createDefaultDirectorRegistry } from "@intx/agent";
import type { AuthzCallResult } from "@intx/inference";

import type {
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

import { createWorkflowRunRepoStore } from "../adapters/repo-store";
import { createWorkflowRunBlobSubstrate } from "../adapters/blob-substrate";
import {
  createControlChannelSender,
  createEventChannelSender,
  hexEncode,
  receiveControlChannel,
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
 * Placeholder drain controller. The control-loop's `drain` case calls
 * `request(opts)` and the supervisor expects a settled promise; the
 * no-op here keeps the loop compiling and behaving as documented for
 * this commit (no in-flight cancellation, no audit frame). The real
 * controller lands in its own commit and replaces this implementation
 * wholesale; nothing inside the child depends on the placeholder's
 * specific shape beyond the `request` method's signature.
 */
export interface DrainController {
  request(opts: { deadlineMs: number; reason: string }): Promise<void>;
}

function createNoopDrainController(): DrainController {
  return {
    async request(opts) {
      logger.info`workflow-child drain requested (deadlineMs=${String(opts.deadlineMs)} reason=${opts.reason}); placeholder no-op until the drain controller lands`;
    },
  };
}

/**
 * Step-invoker shape the child binds. Widens the workflow-runtime
 * `StepInvoker` with an `onEvent` callback the harness fires for
 * every `InferenceEvent` it emits during the step's run. The child's
 * `buildRuntimeEnv` constructs the per-step `onEvent` closure
 * (wrapping the HMAC-authenticated event-channel sender) and threads
 * it here so every event reaches the supervisor over the wire. The
 * runtime-runtime `StepInvoker` exposed via `WorkflowRuntimeEnv`
 * remains the narrower shape -- the child wraps this binding into a
 * `StepInvoker` inside `buildRuntimeEnv` so the workflow-runtime
 * never sees the host-specific surface.
 */
export type ChildStepInvoker = (
  req: StepInvokeRequest,
  onEvent: (event: EventPayload) => void,
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
   * Optional override for the child's Ed25519 keypair factory. The
   * child mints a fresh keypair at startup, holds the private half
   * in its own address space, signs every upstream control frame
   * with it, and publishes the public half in the `ready` frame so
   * the supervisor can verify subsequent upstream frames. Production
   * wires this against `@intx/crypto-node`'s `generateKeyPair`;
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
  for (const run of discovered) {
    const env = buildRuntimeEnv({
      runId: run.runId,
      bindings: opts.bindings,
      runtimeRepoStore,
      authorize,
      directors,
      clock,
      newId,
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
    // Fire-and-forget: the runtime body's `complete` settles when the
    // run reaches a terminal phase; the child's control-loop does not
    // block on resumed runs.
    void handle.complete.catch((cause) => {
      logger.error`resumed run ${run.runId} failed: ${String(cause)}`;
    });
    resumedRunIds.push(run.runId);
  }

  // `ready` rides over the control channel back to the supervisor.
  // The supervisor's `waitForReady` consumes it on its receive side.
  // Re-use `createControlChannelSender` so the wire shape is the same
  // as the downstream's; the supervisor's `receiveControlChannel`
  // accepts both directions through one decoder. Upstream frames are
  // signed by the child's own private key; the `ready` payload
  // publishes the matching public half so the supervisor can verify
  // every subsequent upstream frame.
  const upstreamSender = createControlChannelSender({
    privateKeySeed: childKeyPair.privateKey,
    channelId: opts.env.channelId,
    writer: opts.controlWriter,
  });
  await upstreamSender.send({
    type: "ready",
    data: {
      childPid: process.pid,
      childPublicKey: hexEncode(childKeyPair.publicKey),
    },
  });

  const drainController = createNoopDrainController();
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
        drainController,
        triggeredRunIds,
      })
    ) {
      // shutdown received; exit the loop.
      break;
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
    drainController: DrainController;
    triggeredRunIds: string[];
  },
): Promise<boolean> {
  switch (payload.type) {
    case "trigger.fire": {
      const env = buildRuntimeEnv({
        runId: payload.data.runId,
        bindings: ctx.bindings,
        runtimeRepoStore: ctx.runtimeRepoStore,
        authorize: ctx.authorize,
        directors: ctx.directors,
        clock: ctx.clock,
        newId: ctx.newId,
        onEvent: (event) => {
          void ctx.eventSender.send(event).catch((cause) => {
            logger.error`event-channel send failed during run ${payload.data.runId}: ${String(cause)}`;
          });
        },
      });
      const handle: WorkflowRun = runtimeRun(ctx.definition, env, {
        runId: payload.data.runId,
        consumedMessageId: payload.data.messageId,
      });
      void handle.complete.catch((cause) => {
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
    case "signal.deliver": {
      // Land the signal as a `SignalReceived` commit on the run's
      // event log. The signal-channel substrate's `subscribeKind`
      // peer (the per-run signal channel installed at run start) is
      // what resolves any pending `awaitNext` awaiter -- the
      // control-loop's job is just to commit. Constructing an
      // ad-hoc signal channel scoped to this runId keeps the
      // control-loop free of per-run signal-channel bookkeeping
      // while still routing through the canonical writer path.
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
      try {
        await transientSignalChannel.deliver(
          payload.data.signalName,
          payload.data.payload,
          payload.data.signalId,
        );
      } finally {
        await transientSignalChannel.stop();
      }
      return false;
    }
    case "drain": {
      await ctx.drainController.request({
        deadlineMs: payload.data.deadlineMs,
        reason: "control-channel drain",
      });
      return false;
    }
    case "recycle": {
      logger.info`workflow-child recycle requested (${payload.data.reason}); not yet implemented`;
      return false;
    }
    case "shutdown": {
      logger.info`workflow-child shutdown requested (${payload.data.reason})`;
      return true;
    }
    case "sources-updated": {
      logger.info`workflow-child sources-updated: ${JSON.stringify(payload.data)}`;
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
    return args.bindings.invokeStep(req, args.onEvent);
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
  };
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
