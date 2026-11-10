// Shared connector reply drain for the agent harness.
//
// A director emits a `connector.reply` event when the agent produces an
// outbound reply. Draining that event means: compose the threading
// headers, send the reply through the transport, then report the send
// receipt and the References the reply carried, so the caller can make the
// sent reply the parent of the next one. The caller's `composeReply`
// decides the recipient: the turn's `passthrough` sender when one is set,
// otherwise the active connector thread. When it returns no recipient, the
// drain drops the reply and logs it. The drain composes when the agent
// emits the reply, not when the turn starts. This module owns that loop so
// both the harness composition layer (`createHarness`) and the warm
// workflow-host agent path drive replies through one implementation rather
// than each keeping its own copy.
//
// The loop subscribes an agent event stream and serializes every reply
// through a single chain: two replies fired in quick succession do not
// interleave their compose / send / onReplySent sequence -- the second
// waits for the first's receipt to advance the thread before composing
// against it. A per-reply failure (compose, send, or onReplySent) is
// surfaced to `onSendFailed` and the reply is dropped with the thread left
// at its pre-send state; an abnormal stream termination (e.g. an agent
// stream backpressure violation) is surfaced to `onTerminated`. Neither
// escapes the returned `done` promise -- it always resolves -- so a caller
// can await teardown without guarding a rejection.

import type { Agent } from "@intx/agent";
import { getLogger } from "@intx/log";
import type { OutboundMessage, SendReceipt } from "@intx/types/runtime";

import type { ConnectorReplyParts } from "./connector-router";

const logger = getLogger(["interchange", "harness", "reply-drain"]);

/**
 * The agent event stream the drain consumes -- exactly `agent.stream()`'s
 * type. The stream yields the reactor's full emitted-event union (wider than
 * `InferenceEvent`: it also carries `message.received`), so the drain accepts
 * that union and lets every non-`connector.reply` event flow past untouched.
 */
export type AgentEventStream = ReturnType<Agent["stream"]>;

export interface ConnectorReplyDrainOpts {
  /** The agent event stream to drain. Each `connector.reply` sends a reply. */
  stream: AgentEventStream;
  /**
   * Produce the threading headers (`to`, `cc`, `inReplyTo`, `references`,
   * `subject`) for the reply. Returns `null` when the reply has no
   * recipient; the drain then drops the reply without a send, logs it, and
   * settles it as `ok` with no receipt. Throws when no thread is active;
   * the throw is caught per reply and routed to `onSendFailed`.
   */
  composeReply: () => ConnectorReplyParts | null;
  /**
   * Send the composed reply. The drain builds the `OutboundMessage` from
   * `composeReply()`'s parts plus the reply content and a
   * `conversation.message` type; the caller's `send` routes it to the
   * transport / outbound bridge.
   */
  send: (message: OutboundMessage) => Promise<SendReceipt>;
  /**
   * Advance connector state after a successful send. `references` is the
   * References chain the reply carried: `composeReply()`'s `references`, or
   * empty when the parts had none. May be synchronous (the in-process
   * router's `onReplySent`) or asynchronous (a durable store that persists
   * the advanced state); the drain awaits it before composing the next
   * reply.
   */
  onReplySent: (
    receipt: SendReceipt,
    references: readonly string[],
  ) => void | Promise<void>;
  /**
   * Invoked when `composeReply`, `send`, or `onReplySent` throws for one
   * reply. The reply is dropped and the connector thread stays at its
   * pre-send value. The drain awaits the callback (so an async callback's
   * rejection is observed and logged, not left as an unhandled rejection)
   * and absorbs any error it raises.
   */
  onSendFailed?: (cause: unknown) => void | Promise<void>;
  /**
   * Invoked when the stream's `for await` loop exits abnormally -- the
   * documented case is a backpressure error thrown by the agent event
   * stream. After it fires the drain no longer forwards replies. Awaited
   * and absorbed the same way as `onSendFailed`.
   */
  onTerminated?: (cause: unknown) => void | Promise<void>;
}

/**
 * The settled outcome of one reply the drain processed. `ok` distinguishes a
 * durably-sent reply (the send acked and `onReplySent` advanced the thread)
 * from a failed one (compose, send, or `onReplySent` threw). A caller gating a
 * side effect on the reply reaching the transport awaits the barrier and acts
 * only on `ok: true`; `ok: false` carries the failure `cause` so the caller can
 * surface it rather than treat the reply as sent. A reply that `composeReply`
 * gave no recipient settles as `ok: true` with a `null` receipt: nothing was
 * sent, and nothing is left to send.
 */
export type ReplySettlement =
  | { readonly ok: true; readonly receipt: SendReceipt | null }
  | { readonly ok: false; readonly cause: unknown };

export interface ConnectorReplyDrain {
  /**
   * Settles once the drain loop has exited and its last pending reply has
   * drained. Always resolves -- per-reply and terminal failures are routed
   * to the callbacks, never thrown out of here -- so a caller can await it
   * on teardown without guarding a rejection.
   */
  readonly done: Promise<void>;
  /**
   * Signal the loop to stop at the next event. The loop also exits on its
   * own when the underlying stream ends (e.g. the agent closes); `stop()`
   * is the cooperative early exit for a caller tearing down before then.
   */
  stop(): void;
  /**
   * The count of replies that have SETTLED so far -- sent-and-acked or failed.
   * Monotonic. A per-turn caller captures this BEFORE the `agent.send` that may
   * produce a reply, then, for a turn that did produce a `connector.reply`,
   * awaits `waitForReplyAfter(captured)` to block until THIS turn's reply
   * settles. The capture-before-send ordering is required: the agent resolves
   * `agent.send` in the same synchronous step that pushes the `connector.reply`
   * onto this drain's stream, so the reply is not yet enqueued when `send`
   * resolves -- a post-send snapshot would miss it.
   */
  replySeq(): number;
  /**
   * Resolve once more than `n` replies have settled -- i.e. the reply at index
   * `n` (the `(n + 1)`th reply the drain processed) has settled -- with that
   * reply's settlement. Because the warm agent is strictly serial and the drain
   * is FIFO, a turn that captured `n` from `replySeq()` before its send and
   * produced exactly one reply awaits reply `n` here.
   *
   * When the drain loop exits (stream end, `stop()`, or an abnormal
   * termination) before reply `n` settles, resolves with a failure settlement
   * rather than hanging, so a caller awaiting a reply that will never arrive
   * fails its turn instead of blocking forever.
   */
  waitForReplyAfter(n: number): Promise<ReplySettlement>;
}

async function invokeAbsorbing(
  callback: (cause: unknown) => void | Promise<void>,
  cause: unknown,
  label: string,
): Promise<void> {
  try {
    await callback(cause);
  } catch (callbackError) {
    logger.error`${label} callback threw: ${callbackError}`;
  }
}

/**
 * Drive an agent's `connector.reply` events out through a transport. Returns
 * immediately with a handle; the drain runs in the background until the
 * stream ends or `stop()` is called.
 */
export function driveConnectorReplies(
  opts: ConnectorReplyDrainOpts,
): ConnectorReplyDrain {
  let stopped = false;
  // Reply sends are serialized through `replyChain` so two replies fired in
  // quick succession do not interleave their compose / send / onReplySent
  // sequence -- the second waits for the first's receipt to advance the
  // thread before composing its own.
  let replyChain: Promise<void> = Promise.resolve();

  // Per-turn settle barrier. `settlements[i]` is the outcome of the `i`th reply
  // the drain processed; `settlements.length` is the monotonic settled count a
  // caller snapshots through `replySeq()`. Waiters block until the settled
  // count passes their target index, then resolve with that reply's outcome. A
  // reply is recorded here on BOTH success and failure so a waiter never hangs;
  // the outcome's `ok` tells the caller which happened.
  const settlements: ReplySettlement[] = [];
  let terminated = false;
  type Waiter = { target: number; resolve: (s: ReplySettlement) => void };
  let waiters: Waiter[] = [];

  const terminalSettlement = (): ReplySettlement => ({
    ok: false,
    cause: new Error(
      "connector reply drain terminated before the reply was sent",
    ),
  });

  function settlementAt(index: number): ReplySettlement {
    const settlement = settlements[index];
    if (settlement === undefined) {
      // Reached only if a waiter resolves for an index the drain never
      // recorded -- an internal invariant break, surfaced loudly rather than
      // handed back as a silent fallback.
      throw new Error(
        `connector reply drain: settlement ${String(index)} missing though ` +
          `${String(settlements.length)} replies have settled`,
      );
    }
    return settlement;
  }

  function recordSettlement(settlement: ReplySettlement): void {
    settlements.push(settlement);
    const settledCount = settlements.length;
    const stillWaiting: Waiter[] = [];
    for (const waiter of waiters) {
      if (settledCount > waiter.target) {
        waiter.resolve(settlementAt(waiter.target));
      } else {
        stillWaiting.push(waiter);
      }
    }
    waiters = stillWaiting;
  }

  function releaseWaitersOnTermination(): void {
    terminated = true;
    const outstanding = waiters;
    waiters = [];
    for (const waiter of outstanding) {
      // A waiter whose reply settled before teardown gets its real outcome; one
      // whose reply never arrived (the drain stopped first) gets a terminal
      // failure so the caller fails its turn rather than blocking.
      waiter.resolve(
        settlements.length > waiter.target
          ? settlementAt(waiter.target)
          : terminalSettlement(),
      );
    }
  }

  const done = (async () => {
    try {
      for await (const event of opts.stream) {
        if (stopped) break;
        if (event.type !== "connector.reply") continue;
        const content = event.data.content;
        replyChain = replyChain.then(async () => {
          try {
            const parts = opts.composeReply();
            if (parts === null) {
              logger.warn`Dropping connector reply: the message it answers names no reply address`;
              recordSettlement({ ok: true, receipt: null });
              return;
            }
            const receipt = await opts.send({
              ...parts,
              content,
              type: "conversation.message",
            });
            await opts.onReplySent(receipt, parts.references ?? []);
            recordSettlement({ ok: true, receipt });
          } catch (cause) {
            // The reply is dropped and the connector thread stays at its
            // pre-send value. Surface the loss to `onSendFailed` in addition
            // to the operator-facing log so programmatic consumers (retries,
            // alerting) can observe what the log alone hides. Record the
            // failure on the barrier too, so a per-turn caller awaiting this
            // reply sees `ok: false` rather than treating it as sent.
            logger.error`Failed to send connector reply: ${cause}`;
            if (opts.onSendFailed !== undefined) {
              await invokeAbsorbing(opts.onSendFailed, cause, "onSendFailed");
            }
            recordSettlement({ ok: false, cause });
          }
        });
      }
    } catch (cause) {
      // The agent's stream throws on backpressure violations; log and exit.
      // The reply path stops working but the caller's other consumers keep
      // running until teardown. Surface the loss to `onTerminated` so
      // programmatic consumers (alerting, watchdogs) can observe it.
      logger.warn`Reply-drain stream terminated: ${cause}`;
      if (opts.onTerminated !== undefined) {
        await invokeAbsorbing(opts.onTerminated, cause, "onTerminated");
      }
    } finally {
      // Drain the pending reply before the loop exits so its settlement is
      // recorded and a caller awaiting `done` sees a settled state. Then
      // release any barrier waiter still blocked on a reply that will never
      // arrive, so a per-turn caller cannot hang past teardown.
      await replyChain;
      releaseWaitersOnTermination();
    }
  })();

  return {
    done,
    stop() {
      stopped = true;
    },
    replySeq() {
      return settlements.length;
    },
    waitForReplyAfter(n: number): Promise<ReplySettlement> {
      if (settlements.length > n) {
        return Promise.resolve(settlementAt(n));
      }
      if (terminated) {
        return Promise.resolve(terminalSettlement());
      }
      return new Promise<ReplySettlement>((resolve) => {
        waiters.push({ target: n, resolve });
      });
    },
  };
}
