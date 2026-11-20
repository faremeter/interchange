// Shared connector reply drain for the agent harness.
//
// A director emits a `connector.reply` event when the agent produces an
// outbound reply. Draining that event means: compose the threading
// headers, send the reply through the transport, then report the send
// receipt and the References the reply carried, so the caller can make the
// sent reply the parent of the next one. The caller's `composeReply`
// decides the recipient: the turn's `passthrough` sender when one is set,
// otherwise the active connector thread. When it returns no recipient, the
// drain drops the reply and logs it. A turn in `none` mode makes
// `composeReply` throw `TurnAnswersNoMailError`, and the drain reports that
// as a compose failure. The drain composes when the agent emits the reply,
// not when the turn starts. `sendConnectorReply` sends one reply, and the
// drain calls it for each event. The harness composition layer
// (`createHarness`) uses the drain. A caller that sends each reply from its
// own turn, such as the workflow-host step invoker, calls
// `sendConnectorReply` directly.
//
// The loop subscribes an agent event stream and serializes every reply
// through a single chain: two replies fired in quick succession do not
// interleave their compose / send / onReplySent sequence -- the second
// waits for the first's receipt to advance the thread before composing
// against it. The drain reports each per-reply failure to `onSendFailed`.
// A compose or send failure drops the reply and leaves the thread at its
// pre-send state. A persist failure (`onReplySent` threw) comes after the
// reply went out. An abnormal stream termination (e.g. an agent
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

export interface ConnectorReplySendOpts {
  /**
   * Produce the threading headers (`to`, `cc`, `inReplyTo`, `references`,
   * `subject`) for the reply. Returns `null` when the reply has no
   * recipient; `sendConnectorReply` then drops the reply without a send,
   * logs it, and settles it as `ok` with no receipt. Throws when no thread
   * is active; `sendConnectorReply` catches the throw and settles the reply
   * as failed in the `compose` phase.
   */
  composeReply: () => ConnectorReplyParts | null;
  /**
   * Send the composed reply. `sendConnectorReply` builds the
   * `OutboundMessage` from `composeReply()`'s parts plus the reply content
   * and a `conversation.message` type; the caller's `send` routes it to the
   * transport / outbound bridge.
   */
  send: (message: OutboundMessage) => Promise<SendReceipt>;
  /**
   * Advance connector state after a successful send. `references` is the
   * References chain the reply carried: `composeReply()`'s `references`, or
   * empty when the parts had none. May be synchronous (the in-process
   * router's `onReplySent`) or asynchronous (a durable store that persists
   * the advanced state); `sendConnectorReply` awaits it before it settles.
   */
  onReplySent: (
    receipt: SendReceipt,
    references: readonly string[],
  ) => void | Promise<void>;
}

export interface ConnectorReplyDrainOpts extends ConnectorReplySendOpts {
  /** The agent event stream to drain. Each `connector.reply` sends a reply. */
  stream: AgentEventStream;
  /**
   * Invoked when `composeReply`, `send`, or `onReplySent` throws for one
   * reply. A reply whose `onReplySent` threw already went out, and the
   * connector thread holds whatever `onReplySent` changed before it threw.
   * Any other failed reply is dropped, and the connector thread stays at
   * its pre-send value. The drain awaits the callback (so an async callback's
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
 * The phase in which a reply failed:
 *
 * - `compose`: `composeReply` threw. Nothing was sent.
 * - `send`: `send` threw. The transport did not ack the reply.
 * - `persist`: `onReplySent` threw after the send acked. The reply went out,
 *   but `onReplySent` did not finish recording it.
 */
export type ReplyFailurePhase = "compose" | "send" | "persist";

/**
 * The outcome of one reply that `sendConnectorReply` returns. `ok: true`
 * means the send acked and `onReplySent` advanced the thread. `ok: false`
 * means `composeReply`, `send`, or `onReplySent` threw. It carries the
 * failure `phase` and `cause`, so the caller can surface the failure and can
 * tell a reply that went out from one that did not. A reply that
 * `composeReply` gave no recipient settles as `ok: true` with a `null`
 * receipt: nothing was sent, and nothing is left to send.
 */
export type ReplySettlement =
  | { readonly ok: true; readonly receipt: SendReceipt | null }
  | {
      readonly ok: false;
      readonly phase: ReplyFailurePhase;
      readonly cause: unknown;
    };

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
 * Send one connector reply: compose the threading headers, send the reply
 * with `content` through the transport, then report the receipt and the
 * References the reply carried to `onReplySent`.
 *
 * Never throws. Returns the settlement of the reply:
 *
 * - `{ ok: true, receipt }` when the send acked and `onReplySent` finished.
 * - `{ ok: true, receipt: null }` when `composeReply` returned `null`. The
 *   function drops the reply without a send and logs it.
 * - `{ ok: false, phase, cause }` when `composeReply`, `send`, or
 *   `onReplySent` threw. The function logs the failure with its phase.
 *   Before the `persist` phase, nothing went out and the connector thread
 *   stays at its pre-send value. In the `persist` phase, the reply already
 *   went out, and the connector thread holds whatever `onReplySent` changed
 *   before it threw.
 */
export async function sendConnectorReply(
  opts: ConnectorReplySendOpts,
  content: string,
): Promise<ReplySettlement> {
  let phase: ReplyFailurePhase = "compose";
  try {
    const parts = opts.composeReply();
    if (parts === null) {
      logger.warn`Dropping connector reply: the message it answers names no reply address`;
      return { ok: true, receipt: null };
    }
    phase = "send";
    const receipt = await opts.send({
      ...parts,
      content,
      type: "conversation.message",
    });
    phase = "persist";
    await opts.onReplySent(receipt, parts.references ?? []);
    return { ok: true, receipt };
  } catch (cause) {
    logger.error`Failed to send connector reply in its ${phase} phase: ${cause}`;
    return { ok: false, phase, cause };
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
          const settlement = await sendConnectorReply(opts, content);
          // Surface a failed reply to `onSendFailed` in addition to the
          // operator-facing log so programmatic consumers (retries,
          // alerting) can observe what the log alone hides.
          if (!settlement.ok && opts.onSendFailed !== undefined) {
            await invokeAbsorbing(
              opts.onSendFailed,
              settlement.cause,
              "onSendFailed",
            );
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
      // Drain the pending reply before the loop exits so a caller awaiting
      // `done` sees a settled state.
      await replyChain;
    }
  })();

  return {
    done,
    stop() {
      stopped = true;
    },
  };
}
