// Shared connector reply drain: a director emits `connector.reply` when
// the agent produces an outbound reply; this module composes the
// threading headers, sends, then advances `lastMessageId` from the
// receipt. Used by the harness composition layer and the warm
// workflow-host agent path.
//
// Replies serialize through a single chain so the second waits for the
// first's receipt. A per-reply failure is surfaced to `onSendFailed`
// with the thread at its pre-send state; an abnormal stream exit goes
// to `onTerminated`. Neither escapes `done` -- it always resolves.

import type { Agent } from "@intx/agent";
import { getLogger } from "@intx/log";
import type { OutboundMessage, SendReceipt } from "@intx/types/runtime";

import type { ConnectorReplyParts } from "./connector-router";

const logger = getLogger(["interchange", "harness", "reply-drain"]);

/** `agent.stream()`'s event type; non-`connector.reply` events flow past untouched. */
export type AgentEventStream = ReturnType<Agent["stream"]>;

export interface ConnectorReplyDrainOpts {
  /** The agent event stream to drain. Each `connector.reply` sends a reply. */
  stream: AgentEventStream;
  /**
   * Produce the threading headers for the active connector thread.
   * Throws when no thread is active; the throw is routed to
   * `onSendFailed`.
   */
  composeReply: () => ConnectorReplyParts;
  /** Send the composed reply; the caller routes it to the transport / outbound bridge. */
  send: (message: OutboundMessage) => Promise<SendReceipt>;
  /**
   * Resolve the full References ancestry for a reply to `inReplyTo`.
   * Returns `undefined` when the parent cannot be located (first reply
   * on a fresh thread, or a malformed id); the drain then omits
   * `references`. `createHarness` omits it and the transport derives
   * `[inReplyTo]`.
   */
  resolveReferences?: (inReplyTo: string) => Promise<string[] | undefined>;
  /** Advance connector state after a successful send; awaited before the next reply composes. */
  onReplySent: (receipt: SendReceipt) => void | Promise<void>;
  /**
   * Invoked when one reply's compose, send, or `onReplySent` throws. The
   * reply is dropped and the thread stays at its pre-send value. Awaited
   * and absorbed, so a callback rejection is logged, not left unhandled.
   */
  onSendFailed?: (cause: unknown) => void | Promise<void>;
  /**
   * Invoked when the stream's `for await` loop exits abnormally (the
   * documented case: a backpressure error). After it fires the drain no
   * longer forwards replies. Awaited and absorbed like `onSendFailed`.
   */
  onTerminated?: (cause: unknown) => void | Promise<void>;
}

/**
 * Outcome of one processed reply: `ok: true` when the send acked and
 * `onReplySent` advanced the thread; `ok: false` carries the `cause`.
 */
export type ReplySettlement =
  | { readonly ok: true; readonly receipt: SendReceipt }
  | { readonly ok: false; readonly cause: unknown };

export interface ConnectorReplyDrain {
  /** Settles once the loop has exited and its last pending reply drained; always resolves. */
  readonly done: Promise<void>;
  /**
   * Signal the loop to stop at the next event. The loop also exits on its
   * own when the underlying stream ends (e.g. the agent closes).
   */
  stop(): void;
  /**
   * Monotonic count of settled replies (sent-and-acked or failed). A
   * per-turn caller snapshots this BEFORE `agent.send`: the reply is
   * pushed onto the drain's stream in the same synchronous step `send`
   * resolves, so a post-send snapshot would miss it.
   */
  replySeq(): number;
  /**
   * Resolve with reply `n`'s settlement once more than `n` replies have
   * settled; if the loop exits first, resolve with a failure settlement
   * rather than hanging.
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
  // Serialize reply sends so two quick replies do not interleave their
  // compose / send / onReplySent sequence.
  let replyChain: Promise<void> = Promise.resolve();

  // Per-turn settle barrier: `settlements[i]` is the outcome of the `i`th
  // reply; `settlements.length` is the monotonic count `replySeq()`
  // reports. A reply is recorded on both success and failure so a waiter
  // never hangs.
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
      // Invariant break: a waiter resolved for an index the drain never
      // recorded. Surface loudly rather than silently falling back.
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
      // Settled replies get their real outcome; replies that never arrived
      // get a terminal failure so the caller fails rather than blocking.
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
            // Resolve the References ancestry for the parent, when a
            // resolver is supplied; a miss leaves `references` unset.
            const references =
              opts.resolveReferences !== undefined &&
              parts.inReplyTo !== undefined
                ? await opts.resolveReferences(parts.inReplyTo)
                : undefined;
            const receipt = await opts.send({
              ...parts,
              content,
              type: "conversation.message",
              ...(references !== undefined && references.length > 0
                ? { references }
                : {}),
            });
            await opts.onReplySent(receipt);
            recordSettlement({ ok: true, receipt });
          } catch (cause) {
            // The reply is dropped and the thread stays at its pre-send
            // value; surface the loss and record it on the barrier so a
            // per-turn caller sees `ok: false`.
            logger.error`Failed to send connector reply: ${cause}`;
            if (opts.onSendFailed !== undefined) {
              await invokeAbsorbing(opts.onSendFailed, cause, "onSendFailed");
            }
            recordSettlement({ ok: false, cause });
          }
        });
      }
    } catch (cause) {
      // The agent's stream throws on backpressure violations; log, exit,
      // and surface the loss to `onTerminated`.
      logger.warn`Reply-drain stream terminated: ${cause}`;
      if (opts.onTerminated !== undefined) {
        await invokeAbsorbing(opts.onTerminated, cause, "onTerminated");
      }
    } finally {
      // Drain the pending reply so `done` sees a settled state, then
      // release barrier waiters blocked on a reply that will never arrive.
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
