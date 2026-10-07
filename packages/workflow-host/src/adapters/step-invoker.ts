// Production `WorkflowRuntimeEnv.StepInvoker` adapter.
//
// Per step: build the agent env (minus `authorize`, which the adapter
// wraps from `WorkflowAuthorizeFn` + the per-call `AuthorizeContext`),
// synthesize the inbound message for the resolved input, drive it
// through `agent.send`, and return the reply/turn (or an approval
// suspend). The agent is torn down on every exit path.
//
// Abort: a pre-aborted signal short-circuits before the env build; a
// mid-step abort closes the agent and rejects with an `AbortError`.
//
// Warm-keep mode (design §3b): with `warmCache`, the agent is built
// once on the first message and reused. The cache owns the agent's
// lifetime -- this adapter never closes it or drains its forwarder, and
// a turn abort cancels only the in-flight send. Multi-step steps pass
// no cache.

import {
  createAgent,
  type Agent,
  type AgentDefinition,
  type AuthorizeFn,
  type BaseEnv,
  type SendResult,
} from "@intx/agent";
import { getLogger } from "@intx/log";
import { createInboundMessage, extractAddrSpec, isMessageId } from "@intx/mime";
import type {
  InboundMessage,
  InferenceEvent,
  InferenceSource,
  Mail,
  MailPartReader,
  MessageAttachment,
} from "@intx/types/runtime";
import { isMail } from "@intx/types/runtime";
import type {
  AuthorizeContext,
  StepInvokeRequest,
  StepInvokeResult,
  StepInvoker,
  WorkflowAuthorizeFn,
} from "@intx/workflow";

import type {
  WarmAgentCache,
  WarmEventSinkRef,
  WarmReplyDrive,
} from "../child/warm-agent-cache";
import { runBodyThenCleanup } from "../run-body-then-cleanup";

const logger = getLogger(["workflow-host", "step-invoker"]);

/**
 * Per-step env contributions the caller owns: everything on `BaseEnv`
 * except `authorize`, which the adapter constructs from
 * `WorkflowAuthorizeFn` + the per-call `AuthorizeContext`. Invoked once
 * per step; async so callers can allocate per-step resources.
 */
export type StepEnvBase = Omit<BaseEnv, "authorize">;

export interface WorkflowStepInvokerOpts {
  /**
   * Workflow-level authorize callback. The adapter wraps it into the
   * per-step `AuthorizeFn` closure with the per-call `AuthorizeContext`
   * embedded.
   */
  workflowAuthorize: WorkflowAuthorizeFn;
  /**
   * Build the per-step env minus `authorize`. The returned env's
   * `storage`, `workdir`, and other agent-runtime fields belong to that
   * one step and are torn down with the agent. Receives the request so
   * per-step paths can be derived from the run id.
   */
  buildEnv: (req: StepInvokeRequest) => Promise<StepEnvBase>;
  /**
   * Agent factory override. Defaults to `@intx/agent`'s `createAgent`;
   * tests inject a stub.
   */
  agentFactory?: <EnvReq extends BaseEnv>(
    def: AgentDefinition<EnvReq>,
    env: EnvReq,
  ) => Promise<Agent>;
  /**
   * Observability sink for the per-step agent's event stream. When
   * supplied, the adapter subscribes `agent.stream()` before send and
   * forwards every `InferenceEvent` here; the subscription is torn
   * down with the agent on every exit path. Forwarding is best-effort:
   * a throwing sink is logged and swallowed so a downstream failure
   * cannot abort the step. Omitted, the stream is never consumed.
   */
  onEvent?: (event: InferenceEvent) => void;
  /**
   * Warm-agent cache (design §3b). When supplied the agent is built
   * once on the first invocation and reused instead of torn down per
   * send. The cache owns the agent's lifetime: `close()` runs only at
   * the run-loop's eviction, never in this adapter's `finally`. The
   * run-loop supplies a cache only for a single-step long-lived
   * deployment; multi-step steps omit it.
   */
  warmCache?: WarmAgentCache;
  /**
   * Live per-step inference-source table the run-loop rotates in place.
   * Warm path only: after storing the freshly built agent the adapter
   * re-applies the table so a rotation that landed during the async
   * build is not lost.
   */
  sourcesRef?: { current: Record<string, InferenceSource[]> };
  /**
   * Run-boundary hook (design §3c durability). Warm path only: awaited
   * in the `finally` after each send settles, so the conversation
   * snapshot flushes to the durable substrate before the next message.
   * Awaited (not fire-and-forget) so a respawn cannot lose this
   * message's turns; a flush failure rejects the step. `key` is the
   * step identity (`authzContext.stepId`), the warm cache's key.
   */
  onRunBoundary?: (key: string) => Promise<void>;
  /**
   * Seed hook (design §3c threading). Warm path only: called before
   * `agent.send` for a mail-derived `InboundMessage`, so the durable
   * connector thread (threadRoot / lastMessageId / replyTo) is
   * committed before the reply is produced. Awaited; a seed failure
   * rejects the step. Only mail-derived inbound messages seed --
   * approval resumes and synthesized strings do not advance the thread.
   */
  seedInbound?: (key: string, message: InboundMessage) => Promise<void>;
  /**
   * Connector reply-drain hook (design §3c). Warm path only: invoked
   * ONCE at the first-message build with the step identity and the
   * agent's lifetime event stream. The sidecar's drain composes a
   * threaded reply from the durable connector thread on every
   * `connector.reply` and sends it through the outbound bridge. The
   * returned handle exposes the drain's lifetime `done` promise
   * (folded into the warm entry's forward promise so eviction drains
   * the reply loop) and the per-turn settle barrier (`replySeq` /
   * `waitForReplyAfter`) the step gates each reply turn on.
   */
  driveReplies?: (
    key: string,
    stream: ReturnType<Agent["stream"]>,
  ) => WarmReplyDrive;
  /**
   * Reader for the run's inbound-mail parts. When the step input is a
   * decoded `Mail`, part `ref`s resolve to committed bytes through this
   * reader. Supplied for the top-level run's steps; absent for body
   * steps, where a byte-requiring part is refused loudly.
   */
  mailPartReader?: MailPartReader;
}

/** Construct the production `WorkflowRuntimeEnv.StepInvoker` adapter. */
export function createWorkflowStepInvoker(
  opts: WorkflowStepInvokerOpts,
): StepInvoker {
  const agentFactory = opts.agentFactory ?? createAgent;
  return async (req) => invokeStep(opts, agentFactory, req);
}

async function invokeStep(
  opts: WorkflowStepInvokerOpts,
  agentFactory: NonNullable<WorkflowStepInvokerOpts["agentFactory"]>,
  req: StepInvokeRequest,
): Promise<StepInvokeResult> {
  if (req.signal.aborted) {
    // Skip the env build on an already-aborted signal: it may allocate
    // resources whose disposer would run anyway.
    throw abortError(req.signal);
  }

  if (opts.warmCache !== undefined) {
    return invokeWarmStep(opts, opts.warmCache, agentFactory, req);
  }
  return invokeColdStep(opts, agentFactory, req);
}

/**
 * Instantiate-send-teardown path: build the agent, send one message,
 * close on every exit path. Used by multi-step steps and any deployment
 * without a warm cache.
 */
async function invokeColdStep(
  opts: WorkflowStepInvokerOpts,
  agentFactory: NonNullable<WorkflowStepInvokerOpts["agentFactory"]>,
  req: StepInvokeRequest,
): Promise<StepInvokeResult> {
  const agent = await buildStepAgent(opts, agentFactory, req);

  // Subscribe BEFORE `agent.send` so the inbound `inference.start` and
  // per-turn events are captured. `message.received` is the single
  // exclusion: an assembly-internal dequeue signal, expressed to the
  // audit chain as the `message.run.started` / `message.run.ended`
  // bracket instead.
  const eventForward = subscribeAgentEvents(agent, opts.onEvent);

  // Route the close through runBodyThenCleanup so a wrapped-close
  // failure (a plugin/LSP disposer rejecting) surfaces on a clean step
  // but never masks a step error already unwinding from send.
  // `agent.close()` ends the forwarder's loop, and the forwarder never
  // rejects, so awaiting it in cleanup masks nothing.
  return runBodyThenCleanup(
    async () =>
      stepResultFromSend(
        await sendWithAbort(agent, req, {
          closeOnAbort: true,
          mailPartReader: opts.mailPartReader,
          // Only the warm path seeds connector state.
          seedInbound: undefined,
        }),
      ),
    async () => {
      try {
        await agent.close();
      } finally {
        await eventForward;
      }
    },
    (cause) =>
      logger.error`step invoker: agent.close failed while unwinding a step error; surfacing the step error, close failure: ${cause instanceof Error ? cause.message : String(cause)}`,
  );
}

/** The cause class is not part of the `WarmReplyDrive` contract; match by name. */
function describeReplyFailure(cause: unknown): string {
  if (cause instanceof Error && cause.name === "NoActiveConnectorThreadError") {
    return (
      "the warm agent has no active connector thread, so its auto-reply " +
      "names nobody to reply to and was never sent"
    );
  }
  return "the warm agent's auto-reply send failed";
}

/**
 * Warm-keep path (design §3b): build the agent once on the first
 * invocation, reuse it after. The cache owns the agent's lifetime --
 * this adapter never closes it or drains its forwarder; a turn abort
 * cancels only the in-flight send and the agent survives for the next
 * message. Each invocation points the entry's mutable event sink at
 * its own `onEvent` before send and clears it after.
 */
async function invokeWarmStep(
  opts: WorkflowStepInvokerOpts,
  warmCache: WarmAgentCache,
  agentFactory: NonNullable<WorkflowStepInvokerOpts["agentFactory"]>,
  req: StepInvokeRequest,
): Promise<StepInvokeResult> {
  const key = req.authzContext.stepId;
  if (key === undefined) {
    // The runtime must thread `stepId` through every invocation; an
    // absent id would collide distinct steps onto one cached agent.
    throw new Error(
      "workflow step invoker: warm-keep requires authzContext.stepId; the runtime must thread the step id through every invocation",
    );
  }
  let agent = warmCache.acquire(key);
  if (agent === null) {
    // Lazy first-message build; the stream is consumed once, for the
    // agent's whole life, through the entry's mutable sink ref.
    agent = await buildStepAgent(opts, agentFactory, req);
    const eventSinkRef: WarmEventSinkRef = { current: null };
    const eventForward = subscribeAgentEvents(agent, (event) => {
      const sink = eventSinkRef.current;
      if (sink !== null) sink(event);
    });
    // Establish the reply drain (design §3c) as a second consumer of the
    // agent's lifetime stream. Fold its `done` promise into the stored
    // forwarder so eviction drains both; the per-turn barrier is stored
    // so every message gates its reply turn on a durable send. Present
    // only on the warm mail path.
    const replyDrive =
      opts.driveReplies !== undefined
        ? opts.driveReplies(key, agent.stream())
        : null;
    const lifetimeForward =
      replyDrive !== null
        ? Promise.all([eventForward, replyDrive.done]).then(() => undefined)
        : eventForward;
    warmCache.store(key, agent, eventSinkRef, lifetimeForward, replyDrive);
    // Re-apply the live source table: a rotation during the async build
    // hit the empty cache as a no-op, so apply it now that the entry
    // exists. Element 0 is the default per the wire boundary.
    const live = opts.sourcesRef?.current[key];
    const head = live?.[0];
    if (live !== undefined && head !== undefined) {
      warmCache.applySources(live, head.id);
    }
  }

  if (opts.onEvent !== undefined) {
    warmCache.setEventSink(key, opts.onEvent);
  }
  // Bind the seed hook to this step's identity.
  const seedInbound = opts.seedInbound;
  // Snapshot the barrier before the send: the agent resolves send in the
  // same step it pushes `connector.reply`, so a post-send snapshot would
  // miss this turn's reply.
  const replyDrive = warmCache.getReplyDrive(key);
  const replySeqBeforeSend = replyDrive !== null ? replyDrive.replySeq() : 0;
  try {
    const sendResult = await sendWithAbort(agent, req, {
      closeOnAbort: false,
      mailPartReader: opts.mailPartReader,
      seedInbound:
        seedInbound !== undefined
          ? (message) => seedInbound(key, message)
          : undefined,
    });
    const stepResult = stepResultFromSend(sendResult);
    // Gate the return on this turn's reply being durably sent, so the run
    // parks only after the auto-reply reaches the transport. Only a reply
    // turn produces a `connector.reply`; a suspended turn must NOT await
    // the barrier (that reply never arrives). Fail the turn rather than
    // claim a reply that never went out; the mail is still consumed, since
    // a replay could not compose the same reply.
    if (replyDrive !== null && sendResult.type === "reply") {
      const settlement = await replyDrive.waitForReplyAfter(replySeqBeforeSend);
      if (!settlement.ok) {
        throw new Error(
          `workflow step invoker: ${describeReplyFailure(settlement.cause)}; ` +
            "failing the turn, so the inbound mail is consumed without a " +
            "reply rather than replayed",
          { cause: settlement.cause },
        );
      }
    }
    return stepResult;
  } finally {
    // Do NOT close the agent or drain its forwarder: both are owned by
    // the warm cache. Clear the per-message sink so a stray event
    // between messages is dropped, then flush the conversation snapshot
    // to the durable substrate (design §3c) so a respawn before the
    // next message resumes from this message's turns.
    warmCache.clearEventSink(key);
    if (opts.onRunBoundary !== undefined) {
      await opts.onRunBoundary(key);
    }
  }
}

/**
 * Build the per-step agent: assemble `BaseEnv`, wrap the workflow-typed
 * authorize, and instantiate through the factory. Shared by the cold
 * path and the warm path's first-message build.
 */
async function buildStepAgent(
  opts: WorkflowStepInvokerOpts,
  agentFactory: NonNullable<WorkflowStepInvokerOpts["agentFactory"]>,
  req: StepInvokeRequest,
): Promise<Agent> {
  const envBase = await opts.buildEnv(req);
  const authorize = wrapAuthorize(opts.workflowAuthorize, req.authzContext);
  const env: BaseEnv = { ...envBase, authorize };
  return agentFactory(req.agent, env);
}

/**
 * Drive one `agent.send`, racing it against the step's abort signal.
 *
 * The delivered message depends on the resume kind: a first invocation
 * sends the synthesized input; an `"approval"` resume sends a correlated
 * `InboundMessage` whose `interchangeCorrelationId` matches the
 * rehydrated gate in the reactor's `tryCorrelate`; an `"input"` resume
 * sends the decision as plain synthesized content, like a first
 * invocation. Every path goes through `agent.send`, so a gate park
 * settles as `"suspended"` regardless of delivery.
 *
 * `closeOnAbort` selects abort semantics: `true` (cold path) leaves the
 * in-flight send to settle via `agent.close()` in the caller's `finally`
 * and does not thread the signal into `agent.send`; `false` (warm path)
 * threads the signal so a mid-turn abort cancels only that turn and the
 * agent survives. In both modes a pre-send or mid-send abort rejects
 * with the abort error so the abort attribution wins.
 */
async function sendWithAbort(
  agent: Agent,
  req: StepInvokeRequest,
  cfg: {
    closeOnAbort: boolean;
    mailPartReader: MailPartReader | undefined;
    seedInbound: ((message: InboundMessage) => Promise<void>) | undefined;
  },
): Promise<SendResult> {
  // Re-check after the async env build; the caller can fire the abort
  // in between.
  if (req.signal.aborted) throw abortError(req.signal);
  // Resolve the step input; a build failure (bad resume shape,
  // unresolvable attachment) rejects the step.
  const { message, mailInbound } = await buildSendMessage(
    req,
    cfg.mailPartReader,
  );
  // Seed the warm agent's connector thread (design §3c) before the send;
  // only a mail-derived inbound advances the thread. Awaited so the
  // durable flush completes before the reply is produced.
  if (mailInbound !== null && cfg.seedInbound !== undefined) {
    await cfg.seedInbound(mailInbound);
  }
  let abortListener: (() => void) | null = null;
  try {
    return await new Promise<SendResult>((resolve, reject) => {
      // A mid-build abort must not attach a listener to an already-aborted
      // signal that never fires again, or the send would hang to timeout.
      if (req.signal.aborted) {
        reject(abortError(req.signal));
        return;
      }
      const onAbort = (): void => {
        // Reject here so the abort attribution wins whether the cold
        // path's close or the warm path's threaded signal settles the
        // send first.
        reject(abortError(req.signal));
      };
      abortListener = onAbort;
      req.signal.addEventListener("abort", onAbort, { once: true });
      const sendOpts = cfg.closeOnAbort ? undefined : { signal: req.signal };
      agent.send(message, sendOpts).then(resolve, (cause: unknown) => {
        reject(cause instanceof Error ? cause : new Error(String(cause)));
      });
    });
  } finally {
    if (abortListener !== null) {
      req.signal.removeEventListener("abort", abortListener);
    }
  }
}

/**
 * Subscribe the agent's event stream and forward every `InferenceEvent`
 * to `onEvent`. The returned promise settles when `agent.close()`
 * terminates the stream iterator; with no `onEvent` the stream is never
 * consumed. A throwing sink is logged and swallowed so a downstream
 * failure cannot abort the step; an iterator failure is logged at warn.
 */
function subscribeAgentEvents(
  agent: Agent,
  onEvent: ((event: InferenceEvent) => void) | undefined,
): Promise<void> {
  if (onEvent === undefined) {
    return Promise.resolve();
  }
  const events = agent.stream();
  return (async () => {
    try {
      for await (const event of events) {
        if (event.type === "message.received") continue;
        try {
          onEvent(event);
        } catch (cause) {
          logger.error`step-invoker event sink threw forwarding ${event.type}: ${cause instanceof Error ? cause.message : String(cause)}`;
        }
      }
    } catch (cause) {
      logger.warn`step-invoker event forwarder terminated: ${cause instanceof Error ? cause.message : String(cause)}`;
    }
  })();
}

/**
 * Wrap the workflow-typed authorize into the agent harness's
 * `AuthorizeFn`, capturing the per-call `AuthorizeContext`; the agent
 * layer's generic context slot is ignored. Same shape the `runlocal`
 * step invoker uses.
 */
function wrapAuthorize(
  workflowAuthorize: WorkflowAuthorizeFn,
  authzContext: AuthorizeContext,
): AuthorizeFn {
  return async (resource, action) =>
    workflowAuthorize(resource, action, authzContext);
}

/**
 * Translate a settled `SendResult` into the step result. A `"reply"`
 * becomes `{ output: { reply, turn } }`. A `"suspended"` becomes a
 * `{ suspend: { correlationId, kind, approvalSnapshot } }` park the
 * runtime resumes via `resume`; a resumed cycle that re-parks flows
 * back through here as another suspend.
 */
function stepResultFromSend(result: SendResult): StepInvokeResult {
  if (result.type === "suspended") {
    // The reactor parks only on an approval gate, so a suspend must
    // carry a snapshot. Classify a snapshot-less suspend here rather
    // than emitting an ambiguous suspend the runtime would reject later.
    if (result.approvalSnapshot === undefined) {
      throw new Error(
        `reactor suspended on correlation ${result.correlationId} with no ` +
          `approval snapshot; a snapshot-less suspend is not a supported ` +
          `approval park`,
      );
    }
    return {
      suspend: {
        correlationId: result.correlationId,
        kind: "approval",
        approvalSnapshot: result.approvalSnapshot,
      },
    };
  }
  return { output: { reply: result.reply, turn: result.turn } };
}

/**
 * Resolve the step input into the value `agent.send` receives:
 * - `"approval"` resume: a full `InboundMessage` stamped with
 *   `resume.correlationId`, so the reactor's `tryCorrelate` matches the
 *   rehydrated gate (a plain string would drop the id and never match).
 * - a mail-derived `Mail`: an `InboundMessage` projecting its parts,
 *   resolving non-inline bytes through the reader.
 * - anything else: synthesized plain text; `agent.send` stamps its own
 *   synthetic addressing.
 */
async function buildSendMessage(
  req: StepInvokeRequest,
  mailPartReader: MailPartReader | undefined,
): Promise<{
  message: string | InboundMessage;
  mailInbound: InboundMessage | null;
}> {
  if (req.resume !== undefined && req.resume.kind !== "input") {
    return {
      message: createInboundMessage({
        from: "signal@local",
        to: "agent@local",
        content: synthesizeInputContent(req.resume.decision),
        interchangeType: "conversation.message",
        correlationId: req.resume.correlationId,
      }),
      mailInbound: null,
    };
  }
  const rawInput = req.resume === undefined ? req.input : req.resume.decision;
  // The strict `isMail` guard keeps an arbitrary step value from
  // matching; this branch is the one that carries real threading
  // headers, so `mailInbound` surfaces for the warm path's seed.
  if (isMail(rawInput)) {
    const message = await buildInboundMessageFromMail(rawInput, mailPartReader);
    return { message, mailInbound: message };
  }
  return { message: synthesizeInputContent(rawInput), mailInbound: null };
}

function usableAddr(raw: string | undefined): string | undefined {
  if (raw === undefined) return undefined;
  try {
    return extractAddrSpec(raw);
  } catch {
    return undefined;
  }
}

/** The hub routes out-of-band, never on `To`; unresolvable entries are dropped. */
function usableAddrs(raw: string[]): string[] {
  const resolved: string[] = [];
  for (const entry of raw) {
    const addr = usableAddr(entry);
    if (addr !== undefined) resolved.push(addr);
  }
  return resolved;
}

/**
 * Project a decoded `Mail` into the agent's `InboundMessage`. Inline
 * plain-text parts become the conversation body; every other part
 * becomes an attachment, resolving bytes through the reader. Real
 * sender/recipient and threading headers ride through; content is
 * omitted when empty (`createInboundMessage` rejects an empty string).
 * The reader is required only when a part's bytes must actually be read
 * -- a text-only mail whose parts all inlined still delivers, while a
 * byte-requiring part with no reader is refused loudly.
 */
async function buildInboundMessageFromMail(
  mail: Mail,
  mailPartReader: MailPartReader | undefined,
): Promise<InboundMessage> {
  const noReader = (): Error =>
    new Error(
      "workflow step invoker: a mail part's bytes must be read but the step has no mail-part reader wired; inbound parts are not supported for this step",
    );
  const textPieces: string[] = [];
  const attachments: MessageAttachment[] = [];
  for (const part of mail.parts) {
    // Only inline plain text is body; anything else (attachment
    // disposition, non-plain-text type) is an attachment so its bytes
    // and filename survive.
    const isBody =
      part.disposition !== "attachment" && part.contentType === "text/plain";
    if (isBody) {
      if (part.text !== undefined) {
        textPieces.push(part.text);
        continue;
      }
      if (mailPartReader === undefined) throw noReader();
      textPieces.push(
        new TextDecoder("utf-8", { fatal: false }).decode(
          await mailPartReader.read(part.ref),
        ),
      );
      continue;
    }
    if (mailPartReader === undefined) throw noReader();
    attachments.push({
      name: part.filename ?? part.contentType,
      contentType: part.contentType,
      data: await mailPartReader.read(part.ref),
    });
  }
  const content = textPieces.join("\n").trim();
  // Forward only well-formed RFC 2822 identifiers: a headerless-derived
  // (sha256) or malformed Message-Id is a valid claim-check key but not
  // a valid identifier, and passing it would throw. When omitted,
  // createInboundMessage synthesizes a valid one.
  const validReferences = mail.headers.references?.filter(isMessageId) ?? [];
  const from = usableAddr(mail.headers.from);
  // A sender's `Interchange-Correlation-ID` is deliberately not
  // forwarded: resolving one clears a parked gate.
  return createInboundMessage({
    ...(from !== undefined ? { from } : {}),
    to: usableAddrs(mail.headers.to),
    ...(mail.headers.subject !== undefined
      ? { subject: mail.headers.subject }
      : {}),
    ...(mail.headers.messageId !== undefined &&
    isMessageId(mail.headers.messageId)
      ? { messageId: mail.headers.messageId }
      : {}),
    ...(mail.headers.inReplyTo !== undefined &&
    isMessageId(mail.headers.inReplyTo)
      ? { inReplyTo: mail.headers.inReplyTo }
      : {}),
    ...(validReferences.length > 0 ? { references: validReferences } : {}),
    ...(content.length > 0 ? { content } : {}),
    ...(attachments.length > 0 ? { attachments } : {}),
    interchangeType: "conversation.message",
  });
}

/**
 * Encode the step's resolved `input` as the synthetic inbound content:
 * a string passes verbatim; anything else is JSON-stringified (round-
 * trips through the agent's synthetic mail boundary). Non-serializable
 * inputs (functions, symbols, `undefined`) surface as a thrown error
 * rather than a silent `"undefined"` string.
 */
function synthesizeInputContent(input: unknown): string {
  if (typeof input === "string") return input;
  const encoded = JSON.stringify(input);
  if (encoded === undefined) {
    throw new Error(
      `workflow step invoker: input of typeof ${typeof input} is not JSON-serializable; the step's input selector must resolve to a serializable value`,
    );
  }
  return encoded;
}

/**
 * The abort rejection. Mirrors the DOMException-shaped abort errors the
 * inference harness emits so consumers match a stable shape.
 */
function abortError(signal: AbortSignal): Error {
  const reason = signal.reason;
  if (reason instanceof Error) return reason;
  return new DOMException("aborted", "AbortError");
}
