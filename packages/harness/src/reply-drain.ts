// Shared connector reply drain for the agent harness.
//
// A director emits a `connector.reply` event when the agent produces an
// outbound reply on its connector thread. Draining that event means:
// compose the threading headers for the active thread, send the reply
// through the transport, then advance the thread's `lastMessageId` from
// the send receipt. This module owns that loop so both the harness
// composition layer (`createHarness`) and the warm workflow-host agent
// path drive replies through one implementation rather than each keeping
// its own copy.
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
   * Produce the threading headers (`to`, `cc`, `inReplyTo`, `subject`) for
   * the active connector thread. Throws when no thread is active; the throw
   * is caught per reply and routed to `onSendFailed`.
   */
  composeReply: () => ConnectorReplyParts;
  /**
   * Send the composed reply. The drain builds the `OutboundMessage` from
   * `composeReply()`'s parts plus the reply content and a
   * `conversation.message` type; the caller's `send` routes it to the
   * transport / outbound bridge.
   */
  send: (message: OutboundMessage) => Promise<SendReceipt>;
  /**
   * Advance connector state after a successful send. May be synchronous
   * (the in-process router's `onReplySent`) or asynchronous (a durable
   * store that persists the advanced `lastMessageId`); the drain awaits it
   * before composing the next reply.
   */
  onReplySent: (receipt: SendReceipt) => void | Promise<void>;
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

  const done = (async () => {
    try {
      for await (const event of opts.stream) {
        if (stopped) break;
        if (event.type !== "connector.reply") continue;
        const content = event.data.content;
        replyChain = replyChain.then(async () => {
          try {
            const parts = opts.composeReply();
            const receipt = await opts.send({
              ...parts,
              content,
              type: "conversation.message",
            });
            await opts.onReplySent(receipt);
          } catch (cause) {
            // The reply is dropped and the connector thread stays at its
            // pre-send value. Surface the loss to `onSendFailed` in addition
            // to the operator-facing log so programmatic consumers (retries,
            // alerting) can observe what the log alone hides.
            logger.error`Failed to send connector reply: ${cause}`;
            if (opts.onSendFailed !== undefined) {
              await invokeAbsorbing(opts.onSendFailed, cause, "onSendFailed");
            }
          }
        });
      }
      // Drain the pending reply before the loop exits so a caller awaiting
      // `done` sees a settled state.
      await replyChain;
    } catch (cause) {
      // The agent's stream throws on backpressure violations; log and exit.
      // The reply path stops working but the caller's other consumers keep
      // running until teardown. Surface the loss to `onTerminated` so
      // programmatic consumers (alerting, watchdogs) can observe it.
      logger.warn`Reply-drain stream terminated: ${cause}`;
      if (opts.onTerminated !== undefined) {
        await invokeAbsorbing(opts.onTerminated, cause, "onTerminated");
      }
    }
  })();

  return {
    done,
    stop() {
      stopped = true;
    },
  };
}
