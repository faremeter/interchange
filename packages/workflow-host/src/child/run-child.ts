// `runWorkflowChild` -- the workflow-process child's runtime body. The
// sidecar binary parses `process.env`, opens the IPC streams, builds the
// substrate, and invokes this function; tests call it directly with mock
// streams and an in-memory substrate. Every I/O and substrate handle is an
// injected dependency -- nothing here reads `process.env` or reaches into a
// singleton.
//
// Lifecycle: open the control and event channels; construct the
// `WorkflowRuntimeEnv`; resume any in-flight runs; emit `ready`; loop on
// control frames (`trigger.fired` starts the top-level run, `grants-updated`
// swaps the credentials snapshot, `drain` forwards to the drain controller,
// `shutdown` exits).
//
// `authorize` evaluates grants against the active `credentialsSnapshot`
// (seeded at spawn or by the first `grants-updated` frame), re-reading the
// snapshot per invocation so a live update applies without rebuilding the
// env. The drain controller flips a signal the runtime body observes at its
// observation points; the supervisor's recycle policy is OS-driven and needs
// no child-side frame.

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
  eagerlyResolveLoopFns,
} from "@intx/workflow";
import type { AuthzCallResult } from "@intx/inference";

import type {
  ActionHandler,
  RunResult,
  RuntimeWorkflowRun,
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
import { createCancellationBarrier } from "./cancellation-barrier";
import { createWorkflowRunBlobSubstrate } from "../adapters/blob-substrate";
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
import type { ChildMailboxCallBridge } from "./mailbox-call-bridge";
import { createSupervisorBackedMailPartReader } from "./supervisor-backed-mail-part-reader";
import type { ChildMailboxMutationBridge } from "./mailbox-mutation-bridge";
import type { MailboxWatchRegistry } from "./mailbox-watch-registry";
import { createWarmAgentCache, type WarmAgentCache } from "./warm-agent-cache";
import { mergeCredentialDelivery } from "./credential-cell";

const logger = getLogger(["workflow-host", "child"]);

/**
 * Mutable reference to the active credentials snapshot. A
 * `grants-updated` control frame swaps it in place without
 * reconstructing the authorize closure.
 */
export type CredentialsSnapshotRef = {
  current: CredentialsSnapshot | null;
};

/**
 * Mutable reference to the deployment's decrypted credential material. A
 * `credentials-updated` frame MERGES into it (by credentialId / consumer
 * handle; a `revoke` list drops entries) rather than replacing it: the cell
 * has several independently-scoped producers. The secret lives only here,
 * read at tool-invoke time through the gated capability, never copied into a
 * snapshot, event, or state.
 */
export type CredentialMaterialRef = {
  current: CredentialDelivery | null;
};

/**
 * Mutable per-step inference-source table the build path reads (each value
 * is the step's ordered failover chain, element 0 active). A rotation that
 * lands before the first build is reflected; an already-built warm agent
 * never re-reads this ref.
 */
export type SourcesSnapshotRef = {
  current: Record<string, InferenceSource[]>;
};

/**
 * Grant evaluator the credentials-backed authorize delegates to. The
 * slot lets the host wire its own grant-rule grammar without leaking
 * it into this package; tests inject a spy.
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
    // A map iteration's scoped id `<base>[<index>]` resolves to its base
    // entry so every iteration shares the base step's grants.
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

/** Production drain controller; see `../drain-controller.ts`. */
export type DrainController = WorkflowHostDrainController;

/**
 * Step-invoker shape the child binds: the runtime `StepInvoker` widened with
 * an `onEvent` callback the harness fires per `InferenceEvent` and the
 * child's credentials-backed `authorize` (per-step grants the supervisor
 * pushed). `buildRuntimeEnv` wraps it back into a narrow `StepInvoker`.
 *
 * `warmCache` (§3b) is the run-loop's per-deployment warm-agent cache,
 * present only for a warm single-step deployment; the binding only reads it
 * through to the step-invoker adapter.
 */
/**
 * Per-run credential inputs the top-level step invoker carries to the
 * substrate: the live material cell and a step-grants resolver. Grants are
 * typed `readonly unknown[]` here (this package owns no grant grammar); the
 * substrate casts to its `GrantRule` shape at its own boundary. The cell is
 * read live per use, so a rotation reaches an already-shaped handle without
 * a rebuild.
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
 * Per-deployment bindings the binary owns (substrate identity, principal,
 * runtime callbacks the adapter layer cannot construct from `process.env`
 * alone). Tests supply a fully in-memory object so `runWorkflowChild` runs
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
   * Substrate-shaped principal for every workflow-run read/write. Per the IPC
   * threat model the child holds no private key; this is the substrate-level
   * identity the host accepts for `runs/<runId>/` writes.
   */
  principal: Principal;
  /**
   * Step-invoker callback the runtime body invokes per step (the runtime
   * `StepInvoker` widened with `onEvent` so the harness emits
   * `InferenceEvent` frames up the event channel). Production wires it
   * against `createWorkflowStepInvoker` with the host's env builder; tests
   * inject a stub.
   */
  invokeStep: ChildStepInvoker;
  /**
   * Terminal child-spawn callback used only when the deployment embeds no
   * inline childWorkflow (the lifted-body map is empty). In practice a test
   * seam: production routes through the in-memory resolver built from
   * `runChild`. A workflow reaching a childWorkflow with neither wired fails
   * loud at spawn.
   */
  spawnChild?: SpawnChildWorkflow;
  /**
   * Raw in-process terminal child executor; `run-child` builds the in-memory
   * childWorkflow resolver from this plus the lifted-body map, so an inline
   * child resolves with no on-disk read. Optional: a child with no
   * childWorkflow import omits it.
   */
  runChild?: RunChildWorkflow;
  /**
   * Raw in-process suspendable-child executor; `run-child` builds the
   * onTrigger-body resolver from this after re-evaluating the closure (the
   * bodies map does not exist pre-eval). Optional: a child with no onTrigger
   * section omits it.
   */
  runSuspendableChild?: RunSuspendableChild;
  /**
   * Materialize a loop iteration's own grants file (the parent run's grants
   * capped to the loop body's declared resources). A loop iteration runs
   * through the inherited env, not `buildChildRunEnv`, so it is the one birth
   * path that writes no grants file of its own -- without it the body's
   * childWorkflow grandchild spawn is refused as under-authorized. Optional
   * for a host that never spawns a grandchild from a loop body. `definition`
   * must be the PRE-rewrite loop body (grandchild still inline) so the cap
   * keeps the grandchild's resources.
   */
  materializeLoopIterationGrants?: (args: {
    parentRunId: string;
    childRunId: string;
    definition: WorkflowDefinition;
  }) => Promise<void>;
  /** Host-process scheduler singleton. The child consumes the same instance. */
  scheduler: Scheduler;
  /** Grant evaluator wired against the host's grant-rule grammar. */
  evaluateGrants: GrantEvaluator;
  /**
   * Reclaim a run's local-disk scratch once it reaches terminal, cold path
   * only (a warm agent reuses one workspace across runs; per-run deletion
   * would wipe a live conversation's files). Best-effort: a failure is
   * logged and swallowed, never a gate on the run's terminal status.
   */
  cleanupRunStorage?: (runId: string) => Promise<void>;
  /**
   * Recover the durable approval snapshot for a parked correlation, answering
   * the supervisor's `parked-correlations.request`. The snapshot lives in
   * per-step durable storage whose layout (cold vs warm) the host owns, so
   * the read is a host binding. Optional so tests inject a stub; a production
   * child enumerating a parked step with no binding throws rather than
   * silently dropping the correlation the hub is waiting to register.
   */
  loadParkedApproval?: LoadParkedApproval;
  /**
   * Enumerate the durable pending approval operations a crashed step left
   * behind, so the resume classifier can recover a crash across the park
   * boundary as `awaiting-signal`. Reads the same per-step storage as
   * `loadParkedApproval`, so it is a host binding for the same reason. Absent,
   * a crashed step settles as a terminal failure (pre-recovery behavior).
   */
  readParkedApprovalOps?: ReadParkedApprovalOps;
  /**
   * Mailbox watch registry backing the warm agent's `mail_wait` (INBOUND
   * half of mailbox ownership, §3b): built at child boot, shared with the
   * transport, and exposed so the control loop routes each `mailbox.notify`
   * frame to the same instance. Absent, inbound notifications are logged and
   * dropped. `RunWorkflowChildOpts.mailboxWatchRegistry` takes precedence.
   */
  mailboxWatchRegistry?: MailboxWatchRegistry;
  /** Optional clock override; production wires `() => new Date()`. */
  clock?: () => Date;
  /** Optional id generator override; production wires a monotonic one. */
  newId?: (prefix: string) => string;
  /** Bootstrap credentialsSnapshot (multi-step deploys bake it at spawn); absent defers to the first `grants-updated` frame. */
  initialCredentialsSnapshot?: CredentialsSnapshot;
  /**
   * Bootstrap per-step inference-source table, parsed from the spawn env.
   * Seeds the mutable `sourcesRef` the build path reads; absent defers to an
   * empty table, so a step with no pinned source fails loudly at build.
   */
  initialSources?: Record<string, InferenceSource[]>;
  /**
   * Bootstrap credential material for the deployment's tools, decrypted
   * hub-side and delivered on the deploy frame so it is resident before any
   * step runs. Seeds the mutable `credentialMaterialRef`. Absent when the
   * deployment binds no credentials; a later `credentials-updated` frame
   * refreshes it on rotation.
   */
  initialCredentialMaterial?: CredentialDelivery;
  /**
   * Override for the child's Ed25519 keypair factory. The child mints a
   * fresh keypair at startup, keeps the private half in its own address
   * space, signs every upstream frame with it, and publishes the public half
   * in `ready` so the supervisor can verify subsequent frames. Tests inject a
   * deterministic factory to assert on the published key.
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
   * Control-channel writer back to the supervisor. The only upstream frame
   * today, `ready`, is unsigned; the slot keeps the boundary symmetric.
   */
  controlWriter: NdjsonWriter;
  /** Event-channel writer (child -> supervisor) for verified `InferenceEvent` frames; production wires the inherited socketpair fd. */
  eventWriter: FrameWriter;
  /** Bindings the binary or test harness owns. */
  bindings: RunWorkflowChildBindings;
  /**
   * Pre-built upstream control sender for `ready` and `pack.push.request`
   * frames; defaults to a sender minted against the child's own keypair. The
   * process-shaped wrapper supplies one so the signed surface is shared with
   * its pack-push bridge.
   */
  upstreamSender?: ControlChannelSender;
  /**
   * Substrate-write bridge the control loop invokes on the matching
   * downstream frames; omitted, they are logged and dropped. The
   * process-shaped wrapper supplies one so the proxy `RepoStore` can resolve
   * writes against it.
   */
  substrateWriteBridge?: SubstrateWriteResponseSink;
  /**
   * Outbound-mail bridge (OUTBOUND half of mailbox ownership, §3a). The
   * transport's `send` routes through it: an `outbound.message` frame goes
   * upstream and the send resolves when `outbound.result` lands. The control
   * loop routes that frame to `handleResult` and calls `cancelAll` on exit so
   * a pending send does not leak an awaiter. Omitted, `outbound.result` frames
   * are logged and dropped.
   */
  outboundMailBridge?: ChildOutboundMailBridge;
  /**
   * Mailbox-mutation bridge (INBOUND half of mailbox ownership, §3b). The
   * step agent's flag writes and `expunge` route through it; the control loop
   * routes `mailbox.mutate.response` to `handleResult` and calls `cancelAll`
   * on exit. Omitted, those frames are logged and dropped.
   */
  mailboxMutationBridge?: ChildMailboxMutationBridge;
  /**
   * Mailbox-call bridge for every mailbox method that is not `send` and not a
   * flag/expunge mutation. The control loop routes `mailbox.call.response` to
   * `handleResult` and cancels it on exit; omitted, a response is logged and
   * dropped.
   */
  mailboxCallBridge?: ChildMailboxCallBridge;
  /**
   * Mailbox watch registry (INBOUND half of mailbox ownership, §3b). The
   * control loop routes each `mailbox.notify` frame to this registry's `fire`,
   * which delivers `exists` events to the `watch` callbacks backing
   * `mail_wait`. Omitted, notifications are logged and dropped.
   */
  mailboxWatchRegistry?: MailboxWatchRegistry;
}

/**
 * Downstream substrate-write frame surface the control loop calls, plus the
 * `cancelAll` shutdown arm for any exit path. Decouples the loop from the
 * bridge's `submit` side so a test can drop in a recording sink.
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
 * Result the test harness inspects; production binaries discard it
 * (the process exits when this function resolves).
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
  // Per-run credential wiring for the top-level step invoker: the live
  // material cell and a step-grants resolver from the same snapshot
  // `authorize` reads. Built once; every step build reads the refs live, so
  // a rotation is reflected without rebuilding.
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
  // `ready` frame so the supervisor can verify upstream frames.
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

  // Re-verify barrier at the load boundary: evaluate the pinned closure to a
  // live definition, project back to inert, and hash against
  // `opts.env.definitionHash`; a divergent closure fails closed. Runs once
  // before both the resume and trigger loops, so one verified definition
  // serves every trigger and resume.
  //
  // Post-verify structural rewrite: lift each inline onTrigger body to a
  // `{ ref }` and keep the extracted bodies in an in-memory map, so the
  // suspendable-child resolver runs each body from the parent's
  // already-re-verified closure with no disk read and no per-body re-verify.
  // The rewrite must follow the re-verify: rewriting first would diverge from
  // the frozen inline-body hash.
  const verifiedDefinition = await loadVerifiedWorkflowDefinitionFromClosure({
    packageDir: opts.env.closurePackageDir,
    approvedHash: opts.env.definitionHash,
  });
  const { workflow, bodies } = rewriteInlineOnTriggerBodies(verifiedDefinition);
  let definition: WorkflowDefinition = workflow;
  const bodiesMap = new Map<string, WorkflowDefinition>(
    bodies.map((b) => [b.ref, b.definition]),
  );

  // An inline `childWorkflow` is folded into the parent's hash, so the
  // re-verify above already covers it. Lift each to an internal `{ ref }` and
  // keep the lifted definitions in an in-memory map for the terminal resolver.
  const childRewrite = rewriteInlineChildWorkflowBodies(definition);
  definition = childRewrite.workflow;
  const childBodiesMap = new Map(
    childRewrite.bodies.map((b) => [b.ref, b.definition]),
  );

  // A loop iteration runs its body as a suspendable child through the same
  // seam an onTrigger body uses, so register each top-level loop body in
  // `bodiesMap` under its `<workflowId>__<stepId>` ref. A loop keeps its body
  // INLINE on the primitive, so `enumerateInlineLoopBodies` mints a ref-keyed
  // copy and leaves the primitive untouched.
  //
  // A loop body may itself contain a `childWorkflow` grandchild: rewrite the
  // copy's inline children to `{ ref }` and fold the extracted grandchildren
  // into `childBodiesMap` here, before the eager resolution and terminal-host
  // selection below read it. Keep each body's PRE-rewrite form keyed by ref:
  // the iteration grants cap re-walks it, and capping the rewritten `{ ref }`
  // form would skip -- and so under-authorize -- the grandchild's resources.
  const loopBodyPreRewrite = new Map<string, WorkflowDefinition>();
  for (const loopBody of enumerateInlineLoopBodies(definition)) {
    loopBodyPreRewrite.set(loopBody.ref, loopBody.definition);
    const bodyRewrite = rewriteInlineChildWorkflowBodies(loopBody.definition);
    bodiesMap.set(loopBody.ref, bodyRewrite.workflow);
    for (const grandchild of bodyRewrite.bodies) {
      childBodiesMap.set(grandchild.ref, grandchild.definition);
    }
  }

  // Directors resolve from the pinned closure so a custom director authored in
  // the workflow's own package runs. Loading them outside the re-verify is
  // safe: the approved hash pins each director's id + config and the closure's
  // SRI pins its module bytes.
  const directors = await loadWorkflowDirectorRegistryFromClosure({
    packageDir: opts.env.closurePackageDir,
  });

  // Loop `while`/`carry` fns resolve from the pinned closure's
  // `interchange.loops` module, outside the re-verify for the same reason as
  // directors. Resolved eagerly so a declared loop the closure does not
  // export fails at establish rather than mid-run.
  const loopFns = await loadWorkflowLoopFnsFromClosure({
    packageDir: opts.env.closurePackageDir,
  });
  eagerlyResolveLoopFns(
    [definition, ...bodiesMap.values(), ...childBodiesMap.values()],
    loopFns,
  );

  // Action handlers resolve from the pinned closure's `interchange.actions`
  // module, on the same terms as loop fns: resolved eagerly (recursing into
  // loop bodies) so a declared action the closure does not export fails at
  // establish rather than mid-run.
  const actionResolver = await loadWorkflowActionHandlersFromClosure({
    packageDir: opts.env.closurePackageDir,
  });
  eagerlyResolveActionHandlers(
    [definition, ...bodiesMap.values(), ...childBodiesMap.values()],
    actionResolver,
  );

  // Suspendable-child resolver (onTrigger and loop bodies), selected once per
  // deployment; the per-run `onEvent` is injected later in `buildRuntimeEnv`.
  // A deployment that carries bodies but wired no executor is a
  // misconfiguration -- fail loud at startup rather than falling back to a
  // disk read. A deployment with no suspendable body leaves the host
  // undefined; its slot is never invoked.
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

  // Terminal childWorkflow resolver, selected once per deployment. With any
  // inline child, resolve each from the in-memory map via the raw terminal
  // executor (the parent's own re-verified closure), so an owned child spawns
  // with no disk read; a deployment that embeds a childWorkflow but wired no
  // executor fails loud at startup. A definition with no inline child keeps
  // the injected binding (a test seam); its slot is never invoked.
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
    // No inline child and no injected binding: a workflow reaching a
    // childWorkflow spawn fails loud rather than silently completing against
    // a child that never ran.
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

  // Warm-agent cache (§3b), built only for a warm candidate (the single-step
  // long-lived agent): holds the constructed agent across messages and is
  // evicted at the loop's teardown points. A multi-step deployment leaves
  // this `undefined`, so its steps keep instantiate-send-teardown.
  const warmCache: WarmAgentCache | undefined = opts.env.warmKeep
    ? createWarmAgentCache()
    : undefined;

  // Construct the upstream sender up-front so the resume loop can attach a
  // terminal-event emitter onto every resumed run's `complete` promise
  // without re-deriving it lazily.
  const upstreamSender =
    opts.upstreamSender ??
    createControlChannelSender({
      privateKeySeed: childKeyPair.privateKey,
      channelId: opts.env.channelId,
      writer: opts.controlWriter,
    });

  // Self-discovery before `ready`: the body must see every in-flight run
  // before the supervisor forwards `trigger.fired`, or a fresh trigger could
  // land ahead of a resume and commit a duplicate run entry.
  const discovered = await discoverInFlightRuns({
    substrate: opts.bindings.substrate,
    repoId: opts.bindings.workflowRunRepoId,
    runtimeRepoStore,
  });
  const resumedRunIds: string[] = [];
  // One-driver-per-run claim: a runId present here is already driven by a
  // live `runtimeRun`. The trigger.fire path consults it to refuse a second
  // concurrent driver -- two drivers race to settle the same residual and the
  // loser throws an uncaught TransitionError. Each site removes its entry at
  // terminal.
  const runsInFlight = new Map<string, RuntimeWorkflowRun>();
  const cancellationBarrier = createCancellationBarrier(
    runtimeRepoStore,
    upstreamSender,
  );
  for (const run of discovered) {
    const env = buildRuntimeEnv({
      runId: run.runId,
      bindings: opts.bindings,
      runtimeRepoStore,
      authorize,
      directors,
      suspendableChildHost,
      bodiesMap,
      loopBodyPreRewrite,
      spawnChild,
      loopFns,
      actionResolver,
      clock,
      newId,
      drainController,
      warmCache,
      sourcesRef,
      credentialWiring,
      ...(opts.mailboxCallBridge !== undefined
        ? { mailboxCallBridge: opts.mailboxCallBridge }
        : {}),
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
    // Fire-and-forget: `complete` settles at a terminal phase; the control
    // loop does not block on resumed runs. The supervisor subscribes to the
    // terminal via the `terminal.event` upstream frame emitted below.
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

  // `ready` rides over the control channel back to the supervisor, whose
  // `waitForReady` consumes it; the payload publishes the child's public key
  // so the supervisor can verify subsequent upstream frames.
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

  // Control loop. The receiver iterator yields one verified payload per call;
  // any signature/channelId/seq violation crashes the receiver via `onCrash`
  // and ends the iterator.
  const iter = receiveControlChannel({
    publicKey: opts.env.hostPublicKey,
    channelId: opts.env.channelId,
    reader: opts.controlReader,
    onCrash: (reason) => {
      logger.error`workflow-child control channel crash: ${reason}`;
    },
  });

  // The watch registry the control loop routes `mailbox.notify` frames to;
  // the opts-level injection wins over the bindings, and both absent leaves
  // notifications logged and dropped.
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
          loopBodyPreRewrite,
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
          cancellationBarrier,
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
          ...(opts.mailboxCallBridge !== undefined
            ? { mailboxCallBridge: opts.mailboxCallBridge }
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
    cancellationBarrier.close("workflow-child control loop exited");
    // Every exit path cancels still-pending substrate writes, outbound sends,
    // mailbox mutations, and mailbox calls so their awaiters surface a
    // structured rejection instead of hanging on a torn-down channel.
    if (opts.substrateWriteBridge !== undefined) {
      opts.substrateWriteBridge.cancelAll("workflow-child control loop exited");
    }
    if (opts.outboundMailBridge !== undefined) {
      opts.outboundMailBridge.cancelAll("workflow-child control loop exited");
    }
    if (opts.mailboxMutationBridge !== undefined) {
      opts.mailboxMutationBridge.cancelAll(
        "workflow-child control loop exited",
      );
    }
    if (opts.mailboxCallBridge !== undefined) {
      opts.mailboxCallBridge.cancelAll("workflow-child control loop exited");
    }
    // Evict the warm-agent cache on every exit path; eviction runs the
    // wrapped `agent.close()` (disposes plugins, kills the LSP subprocess),
    // so no warm agent or LSP outlives the run-loop.
    if (warmCache !== undefined) {
      await warmCache.evictAll("workflow-child control loop exited");
    }
  };

  // Run the control loop, then always run the cleanup above. A failing
  // eviction surfaces on a clean exit but must not mask a control-loop error
  // already unwinding -- logged, not rethrown, in that case.
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
    loopBodyPreRewrite: ReadonlyMap<string, WorkflowDefinition>;
    spawnChild: HostSpawnChild;
    loopFns: LoopFnRegistry;
    actionResolver: (ref: string) => ActionHandler;
    clock: () => Date;
    newId: (prefix: string) => string;
    eventSender: ReturnType<typeof createEventChannelSender>;
    upstreamSender: ControlChannelSender;
    drainController: DrainController;
    triggeredRunIds: string[];
    runsInFlight: Map<string, RuntimeWorkflowRun>;
    cancellationBarrier: ReturnType<typeof createCancellationBarrier>;
    warmCache: WarmAgentCache | undefined;
    sourcesRef: SourcesSnapshotRef;
    credentialMaterialRef: CredentialMaterialRef;
    credentialWiring: CredentialWiring;
    substrateWriteBridge?: SubstrateWriteResponseSink;
    outboundMailBridge?: ChildOutboundMailBridge;
    mailboxMutationBridge?: ChildMailboxMutationBridge;
    mailboxCallBridge?: ChildMailboxCallBridge;
    mailboxWatchRegistry?: MailboxWatchRegistry;
  },
): Promise<boolean> {
  switch (payload.type) {
    case "trigger.fire": {
      // One driver per runId: a duplicate/stale trigger for a runId this
      // child already drives (a resume or an earlier trigger) must not spawn
      // a second `runtimeRun` -- two drivers race to settle the same
      // residual, the loser throws an uncaught TransitionError, and even a
      // surviving driver would double-emit the terminal. The live driver's
      // completion owns the single terminal emission, so nothing is dropped
      // by declining here.
      if (ctx.runsInFlight.has(payload.data.runId)) {
        ctx.triggeredRunIds.push(payload.data.runId);
        return false;
      }
      // The supervisor resolved the inbound mail to the run's input
      // (conversation text plus attachment-byte references) and shipped it in
      // the frame; the first step's default input selector reads
      // `trigger.payload`, so the step input resolves to the inbound message.
      const triggerPayload = payload.data.payload;
      const env = buildRuntimeEnv({
        runId: payload.data.runId,
        bindings: ctx.bindings,
        runtimeRepoStore: ctx.runtimeRepoStore,
        authorize: ctx.authorize,
        directors: ctx.directors,
        suspendableChildHost: ctx.suspendableChildHost,
        bodiesMap: ctx.bodiesMap,
        loopBodyPreRewrite: ctx.loopBodyPreRewrite,
        spawnChild: ctx.spawnChild,
        loopFns: ctx.loopFns,
        actionResolver: ctx.actionResolver,
        clock: ctx.clock,
        newId: ctx.newId,
        drainController: ctx.drainController,
        warmCache: ctx.warmCache,
        sourcesRef: ctx.sourcesRef,
        credentialWiring: ctx.credentialWiring,
        ...(ctx.mailboxCallBridge !== undefined
          ? { mailboxCallBridge: ctx.mailboxCallBridge }
          : {}),
        onEvent: (event) => {
          void ctx.eventSender.send(event).catch((cause) => {
            logger.error`event-channel send failed during run ${payload.data.runId}: ${String(cause)}`;
          });
        },
        upstreamSender: ctx.upstreamSender,
      });
      const handle = runtimeRun(ctx.definition, env, {
        runId: payload.data.runId,
        consumedMessageId: payload.data.messageId,
        triggerPayload,
      });
      ctx.runsInFlight.set(payload.data.runId, handle);
      // Fan the run's terminal status back to the supervisor; the runtime
      // commits the terminal event to the substrate in the same lifecycle
      // moment, so the audit chain and the peer notification originate from
      // the same code path.
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
      // Replace the closure-local snapshot reference so every subsequent
      // `authorize` call reads the new grants without reconstructing the env.
      // The optional `stepHashes` cross-check crashes the child on a mismatch
      // rather than honoring a desynchronized push.
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
      // Merge into the live cell (see `mergeCredentialDelivery`) rather than
      // replace it: the cell has several independently-scoped producers, so a
      // swap would evict another producer's credentials. One atomic assignment
      // keeps readers from observing a torn cell. The secret stays on this
      // ref only.
      ctx.credentialMaterialRef.current = mergeCredentialDelivery(
        ctx.credentialMaterialRef.current,
        payload.data.delivery,
        payload.data.revoke,
      );
      return false;
    }
    case "signal.deliver": {
      // Drop a delivery for a run this child is not driving: a stale or
      // mis-routed frame must not commit an orphan `SignalReceived` to a log
      // no awaiter is tailing. `runsInFlight` is the authority.
      if (!ctx.runsInFlight.has(payload.data.runId)) {
        logger.warn`signal.deliver for run ${payload.data.runId} which is not in flight; dropping (signalName=${payload.data.signalName})`;
        return false;
      }
      // Land the signal as a `SignalReceived` commit on the run's event log;
      // the per-run signal channel's `subscribeKind` peer resolves any pending
      // `awaitNext` awaiter.
      //
      // The deliver writes through the pack-pushing proxy: it emits
      // `pack.push.request` upstream and awaits `pack.push.response` on this
      // SAME downstream stream. Awaiting inline would deadlock the iterator
      // against the response it is blocking on (observed end-to-end), so the
      // deliver fires off the loop and the iterator keeps pumping. A commit
      // failure surfaces via the logger; the `awaitNext` peer resolves or
      // stays pending until a later delivery.
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
    case "cancel.prepare": {
      // Preparation flushes through the same IPC stream; keep consuming replies.
      void ctx.cancellationBarrier
        .prepare(payload.data)
        .then(() =>
          ctx.runsInFlight
            .get(payload.data.runId)
            ?.applyCommittedCancellation(),
        )
        .catch((error: unknown) => {
          logger.error`Failed to apply cancellation for ${payload.data.runId}: ${error instanceof Error ? error.message : String(error)}`;
        });
      return false;
    }
    case "cancel.committed": {
      ctx.cancellationBarrier.complete(payload.data);
      return false;
    }
    case "cancel.prepared": {
      throw new Error("workflow-child received an upstream cancellation reply");
    }
    case "drain": {
      // Flip the drain controller's signal; the runtime body's observation
      // points read it on their next tick. The supervisor's drainTimeout
      // accumulator escalates to a signed CancelRequested if cancel-mode work
      // outlasts the deadline.
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
      // Live inference-source rotation for the warm single-step agent. Only a
      // single-step deployment rotates sources (its sole step's id is the
      // table's sole key, so the whole table is replaced); assert the step
      // count so a mis-route fails loudly rather than corrupting the table.
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
      // always builds a warm cache; an absent cache is a routing bug.
      if (ctx.warmCache === undefined) {
        throw new Error(
          "workflow-child sources-updated: no warm cache; a sources rotation must target a warm single-step deployment",
        );
      }
      // Swap the built warm agent first (a no-op when none is built), then
      // update the table the next cold build reads: a rotation racing
      // eviction leaves the table untouched rather than ahead of a
      // half-applied swap.
      ctx.warmCache.applySources(
        payload.data.sources,
        payload.data.defaultSource,
      );
      ctx.sourcesRef.current = { [stepId]: payload.data.sources };
      return false;
    }
    case "ready": {
      // `ready` is child->supervisor; receiving one downstream is a
      // protocol violation the sender should not be able to produce.
      throw new Error(
        "workflow-child received a `ready` frame on its inbound control channel; this is a supervisor-only payload",
      );
    }
    case "recycle.request": {
      // Child->supervisor frame; receiving one downstream is the same
      // shape of protocol violation as a downstream `ready`.
      throw new Error(
        "workflow-child received a `recycle.request` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "substrate.write.request": {
      // Child->supervisor proxied write; receiving one downstream is a
      // protocol violation in the same shape as a downstream `ready`.
      throw new Error(
        "workflow-child received a `substrate.write.request` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "substrate.merge.response": {
      // Child->supervisor merge result; receiving one downstream is a
      // protocol violation in the same shape as a downstream `ready`.
      throw new Error(
        "workflow-child received a `substrate.merge.response` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "terminal.event": {
      // Child->supervisor terminal-run notification; receiving one
      // downstream is a protocol violation like a downstream `ready`.
      throw new Error(
        "workflow-child received a `terminal.event` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "park.notify": {
      // Child->supervisor suspension notification; receiving one
      // downstream is a protocol violation like a downstream
      // `terminal.event`.
      throw new Error(
        "workflow-child received a `park.notify` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "outbound.message": {
      // Child->supervisor outbound-mail request; receiving one
      // downstream is a protocol violation like a downstream `ready`.
      throw new Error(
        "workflow-child received an `outbound.message` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "outbound.result": {
      // Route the supervisor's send result to the outbound-mail bridge when
      // wired; without one the frame is stale -- log and drop rather than
      // throw.
      if (ctx.outboundMailBridge === undefined) {
        logger.warn`workflow-child outbound.result received without a bridge wired; requestId=${payload.data.requestId} dropped`;
        return false;
      }
      ctx.outboundMailBridge.handleResult(payload.data);
      return false;
    }
    case "mailbox.notify": {
      // Route the supervisor's new-mail notification to the watch registry
      // so a step agent's `watch`/`mail_wait` observes the arrival; without
      // one, log and drop (mirrors the `outbound.result` arm).
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
      // Child->supervisor mailbox-mutation request; receiving one
      // downstream is a protocol violation like a downstream
      // `outbound.message`.
      throw new Error(
        "workflow-child received a `mailbox.mutate.request` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "mailbox.mutate.response": {
      // Route the supervisor's applied-mutation result to the
      // mailbox-mutation bridge when wired; otherwise log and drop.
      if (ctx.mailboxMutationBridge === undefined) {
        logger.warn`workflow-child mailbox.mutate.response received without a bridge wired; requestId=${payload.data.requestId} dropped`;
        return false;
      }
      ctx.mailboxMutationBridge.handleResult(payload.data);
      return false;
    }
    case "mailbox.call.request": {
      // Child->supervisor mailbox-call frame; receiving one downstream is
      // a protocol violation like a downstream `mailbox.mutate.request`.
      throw new Error(
        "workflow-child received a `mailbox.call.request` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "mailbox.call.response": {
      // Route the supervisor's answer to the mailbox-call bridge when wired;
      // otherwise log and drop.
      if (ctx.mailboxCallBridge === undefined) {
        logger.warn`workflow-child mailbox.call.response received without a bridge wired; requestId=${payload.data.requestId} dropped`;
        return false;
      }
      ctx.mailboxCallBridge.handleResult(payload.data);
      return false;
    }
    case "substrate.merge.request": {
      // Route the request to the substrate-write bridge when wired;
      // otherwise log and drop.
      if (ctx.substrateWriteBridge === undefined) {
        logger.warn`workflow-child substrate.merge.request received without a bridge wired; requestId=${payload.data.requestId} dropped`;
        return false;
      }
      ctx.substrateWriteBridge.handleMergeRequest(payload.data);
      return false;
    }
    case "substrate.write.response": {
      // Route the response to the substrate-write bridge when wired;
      // otherwise log and drop.
      if (ctx.substrateWriteBridge === undefined) {
        logger.warn`workflow-child substrate.write.response received without a bridge wired; requestId=${payload.data.requestId} dropped`;
        return false;
      }
      ctx.substrateWriteBridge.handleWriteResponse(payload.data);
      return false;
    }
    case "parked-correlations.request": {
      // Answer the supervisor's re-registration enumeration from durable
      // state. Awaiting inline is safe: unlike `signal.deliver`, this reads
      // and sends one upstream reply without awaiting any downstream frame,
      // so it cannot deadlock the iterator. A store inconsistency throws
      // rather than dropping a correlation the hub is waiting to register.
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
      // Child->supervisor self-discovery report; receiving one downstream
      // is a protocol violation like a downstream `ready`.
      throw new Error(
        "workflow-child received a `resumed.runs` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
    case "parked-correlations.response": {
      // Child->supervisor reply frame; receiving one downstream is a
      // protocol violation like a downstream `substrate.merge.response`.
      throw new Error(
        "workflow-child received a `parked-correlations.response` frame on its inbound control channel; this is a child-only upstream payload",
      );
    }
  }
}

/**
 * Construct a `WorkflowRuntimeEnv` for one run. Each run gets its own
 * `BlobSubstrate` and `SignalChannel` (both are per-run by shape); the
 * substrate handle and per-deployment `RepoStore` adapter are shared.
 */

/**
 * Force-resolve every `action` handler ref reachable from these definitions
 * (recursing into loop bodies), so a missing handler surfaces at establish
 * rather than when the action is first invoked mid-run.
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

function unwiredMailPartReader(): MailPartReader {
  return {
    read(ref) {
      return Promise.reject(
        new Error(
          `mail part reader: this child has no mailbox call bridge; cannot read ${ref}`,
        ),
      );
    },
  };
}

function buildRuntimeEnv(args: {
  runId: string;
  bindings: RunWorkflowChildBindings;
  runtimeRepoStore: ReturnType<typeof createWorkflowRunRepoStore>;
  authorize: WorkflowAuthorizeFn;
  directors: DirectorRegistry;
  suspendableChildHost: HostSpawnSuspendableChild | undefined;
  bodiesMap: ReadonlyMap<string, WorkflowDefinition>;
  loopBodyPreRewrite: ReadonlyMap<string, WorkflowDefinition>;
  spawnChild: HostSpawnChild;
  loopFns: LoopFnRegistry;
  actionResolver: (ref: string) => ActionHandler;
  clock: () => Date;
  newId: (prefix: string) => string;
  drainController: DrainController;
  warmCache: WarmAgentCache | undefined;
  sourcesRef: SourcesSnapshotRef;
  credentialWiring: CredentialWiring;
  mailboxCallBridge?: ChildMailboxCallBridge;
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
  // The supervisor committed each part before the trigger; a step asks it for
  // the bytes. A child with no call bridge cannot read them; inlined text
  // never asks.
  const mailPartReader =
    args.mailboxCallBridge === undefined
      ? unwiredMailPartReader()
      : createSupervisorBackedMailPartReader({
          callBridge: args.mailboxCallBridge,
          runId: args.runId,
        });
  // Wrap the step invoker so every `InferenceEvent` funnels through the
  // per-run `onEvent` closure, which forwards it up the HMAC-authenticated
  // event channel -- the only translation between the runtime's narrow
  // `StepInvoker` and the host's `ChildStepInvoker`.
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
  // runtime's narrow `SpawnSuspendableChild` by injecting this run's event
  // funnel, so a body's live inference events ride the parent run's channel;
  // the live credential-material cell rides the same seam.
  const hostSuspendable = args.suspendableChildHost;
  const spawnSuspendableChild: SpawnSuspendableChild | undefined =
    hostSuspendable === undefined
      ? undefined
      : (spawnInput) =>
          hostSuspendable(
            spawnInput,
            args.onEvent,
            args.credentialWiring.materialRef,
          );
  // Same adaptation for the terminal childWorkflow seam.
  const spawnChild: SpawnChildWorkflow = (spawnInput) =>
    args.spawnChild(
      spawnInput,
      args.onEvent,
      args.credentialWiring.materialRef,
    );
  const env: WorkflowRuntimeEnv = {
    repoStore: args.runtimeRepoStore,
    scheduler: args.bindings.scheduler,
    signalChannel,
    blobs,
    directors: args.directors,
    authorize: args.authorize,
    invokeStep,
    spawnChild,
    // The deployment's addressable run: parks register with the hub through
    // the notify sink below and decisions deliver back onto this run's channel.
    hasUpstreamSignalResolver: true,
    // Every loop ref was force-resolved at establish, so a lookup cannot fail
    // for a definition that passed startup.
    loopFns: args.loopFns,
    // Wire the suspendable-child seam only when the host supplied it; the
    // runtime body fails loud if a workflow reaches an unwired section.
    ...(spawnSuspendableChild !== undefined ? { spawnSuspendableChild } : {}),
    clock: args.clock,
    newId: args.newId,
    drain: args.drainController,
    // Forward a control-plane suspension up the same channel `terminal.event`
    // rides, so the supervisor can stamp the deployment identity and register
    // the correlation at the hub.
    onPark: (park) => {
      void emitParkNotify(args.upstreamSender, park);
    },
    // Let the resume classifier recover a step that crashed across the park
    // boundary; absent (tests, the recursive adapter) leaves a crashed
    // invocation a terminal failure.
    ...(args.bindings.readParkedApprovalOps !== undefined
      ? { readParkedApprovalOps: args.bindings.readParkedApprovalOps }
      : {}),
  };
  // The suspendable-loop executor runs each iteration's body under this run's
  // inherited env (invokeStep, invokeAction, authorize, effect ledger, shared
  // repoStore/blobs), giving the body only its own substrate-backed signal
  // channel -- distinct from an onTrigger body's fresh capped env. Assigned
  // AFTER env construction because it closes over `env`.
  const loopIterationHost = createInMemorySpawnSuspendableChild({
    bodies: args.bodiesMap,
    runSuspendableChild: async (loopInput, _onEvent) => {
      // Materialize this iteration's grants file BEFORE the body runs (its
      // first event append), so a childWorkflow grandchild spawned from the
      // body reads it as authority. The ordering is load-bearing: the grants
      // write is write-once and its shallow-prefix rebuild is safe only while
      // the iteration's subtree is still empty. The cap must walk the
      // PRE-rewrite loop body (grandchild still inline); the rewritten body in
      // `bodiesMap` would skip the grandchild's resources. Sidecar-only seam:
      // the in-process host keeps no per-run grants file, so an absent binding
      // leaves the grants unmaterialized.
      const preRewriteBody = args.loopBodyPreRewrite.get(
        loopInput.definitionRef,
      );
      if (preRewriteBody === undefined) {
        throw new Error(
          `workflow-child: loop iteration ${loopInput.childRunId} has no ` +
            `pre-rewrite body registered for ref ${loopInput.definitionRef}`,
        );
      }
      await args.bindings.materializeLoopIterationGrants?.({
        parentRunId: loopInput.parentRunId,
        childRunId: loopInput.childRunId,
        definition: preRewriteBody,
      });
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

  // Action handlers run against a per-run effect ledger. In-memory is
  // correct -- not a shortcut -- on the deployed store: `runAction` flushes
  // `StepStarted` durably before the effect and the runtime never re-invokes
  // a crashed action, so the ledger is never consulted across a crash. Its
  // cross-crash exactly-once rests on that store-consistency invariant, which
  // the store layer owns; a durable ledger here would re-enforce a constraint
  // a lower layer already guarantees. Within one invocation the ledger still
  // dedups a handler that performs the same effect twice.
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
 * control channel, fired from `env.onPark` each time a step parks; the
 * supervisor stamps the deployment identity and registers the correlation at
 * the hub.
 *
 * Best-effort: a transport failure is logged, not rethrown. A lost frame
 * surfaces structurally as a run that never resumes; the hub register is
 * idempotent, so a re-park re-emit is safe.
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
 * Mirror a run's terminal status back to the supervisor over the upstream
 * control channel, fired once per run from the resume and trigger.fire paths'
 * `complete` continuation.
 *
 * Every frame field is sourced from the run's committed terminal event (the
 * runtime commits it last); `terminalStatus` is only the cross-check. A
 * missing or disagreeing terminal event is a producer bug, and emitting a
 * frame anyway would desync the supervisor from the durable log
 * `discoverInFlightRuns` reads on resume -- so this throws instead: no frame
 * keeps supervisor and log agreeing the run is unsettled, and the next
 * recycle/restart resumes it. The throw propagates to the caller's
 * `complete` continuation, which logs it.
 *
 * A transport send failure is different: logged, not rethrown. The
 * supervisor's dispatch loop is the authoritative settler, so a lost frame
 * surfaces as a wedged dispatch rather than a silent lifecycle failure. The
 * invariant throws run before the send, so that catch never swallows them.
 */
export function emitTerminalEvent(
  upstreamSender: ControlChannelSender,
  result: RunResult,
): Promise<void> {
  // The runtime commits the terminal event last, so walking from the end
  // finds it without rebuilding the state machine.
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
  // The supervisor's `synthesizeTerminalEvent` guards a missing RunFailed
  // error.message (it parses untrusted JSON); here the type is a non-optional
  // `string`, so the case is unreachable.
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
 * Gated on `!warmKeep`: a warm agent reuses one stable workspace across runs,
 * so per-run deletion would wipe a live conversation's files mid-stream. On
 * the cold path each run rebuilds its agent + scratch, so once terminal
 * nothing reopens its `runs/<runId>/` subtree and it is safe to drop.
 *
 * Best-effort: a failure is logged and swallowed, never gating the run's
 * terminal status or the upstream terminal.event.
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
