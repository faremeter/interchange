// Unit tests for the shared connector reply drain. The drain is exercised
// against a plain async stream and stub compose/send/onReplySent seams --
// no full agent or transport -- so the contract (one send per
// connector.reply, correct threading headers, serialized ordering, and a
// surfaced send failure) is asserted in isolation.

import { describe, expect, test } from "bun:test";

import type {
  InferenceEvent,
  OutboundMessage,
  SendReceipt,
} from "@intx/types/runtime";

import { waitUntil } from "@intx/types/testing";

import { createInboundMessage } from "@intx/mime";

import {
  createConnectorRouter,
  type ConnectorReplyParts,
} from "./connector-router";
import { driveConnectorReplies } from "./reply-drain";

function replyEvent(seq: number, content: string): InferenceEvent {
  return { type: "connector.reply", seq, data: { content } };
}

// A non-reply event the drain must ignore. `reactor.start` carries an empty
// data object, so it is the cheapest event to interleave.
function noiseEvent(seq: number): InferenceEvent {
  return { type: "reactor.start", seq, data: {} };
}

async function* streamOf(
  events: InferenceEvent[],
): AsyncGenerator<InferenceEvent> {
  for (const event of events) {
    yield event;
    // Yield to the microtask queue between events so the drain's reply chain
    // has a chance to interleave, matching the real agent stream's async
    // delivery.
    await Promise.resolve();
  }
}

/**
 * A push-based event stream: a test feeds events with `push` and ends the
 * stream with `end`, so the barrier's capture-then-await ordering can be
 * exercised against a stream that stays open between events.
 *
 * `pulled()` reports how many events the consumer has taken off the queue.
 * The drain consumes serially, so `pulled()` reaching n+1 is a signal that
 * the drain finished its handling of event n -- which is what a test asserting
 * "event n changed nothing" needs, since there is no event of its own to await.
 */
function pushStream(): {
  stream: AsyncGenerator<InferenceEvent>;
  push: (event: InferenceEvent) => void;
  end: () => void;
  pulled: () => number;
} {
  const queue: InferenceEvent[] = [];
  let notify: (() => void) | null = null;
  let ended = false;
  let pulledCount = 0;
  const wake = (): void => {
    const resume = notify;
    notify = null;
    resume?.();
  };
  async function* gen(): AsyncGenerator<InferenceEvent> {
    for (;;) {
      const next = queue.shift();
      if (next !== undefined) {
        pulledCount += 1;
        yield next;
        continue;
      }
      if (ended) return;
      await new Promise<void>((resolve) => {
        notify = resolve;
      });
    }
  }
  return {
    stream: gen(),
    push(event) {
      queue.push(event);
      wake();
    },
    end() {
      ended = true;
      wake();
    },
    pulled: () => pulledCount,
  };
}

describe("driveConnectorReplies", () => {
  test("sends exactly one threaded reply per connector.reply", async () => {
    const sent: OutboundMessage[] = [];
    const receipts: SendReceipt[] = [];
    const parts: ConnectorReplyParts = {
      to: "alice@example.com",
      cc: ["bob@example.com"],
      inReplyTo: "<parent@interchange>",
      subject: "Re: hello",
    };

    const drain = driveConnectorReplies({
      stream: streamOf([
        noiseEvent(1),
        replyEvent(2, "the reply body"),
        noiseEvent(3),
      ]),
      composeReply: () => parts,
      send: async (message) => {
        sent.push(message);
        return { messageId: "<child@interchange>", status: "delivered" };
      },
      onReplySent: (receipt) => {
        receipts.push(receipt);
      },
    });

    await drain.done;

    expect(sent.length).toBe(1);
    const message = sent[0];
    expect(message).toBeDefined();
    expect(message?.type).toBe("conversation.message");
    expect(message?.content).toBe("the reply body");
    expect(message?.to).toBe("alice@example.com");
    expect(message?.cc).toEqual(["bob@example.com"]);
    expect(message?.inReplyTo).toBe("<parent@interchange>");
    expect(message?.subject).toBe("Re: hello");

    expect(receipts.length).toBe(1);
    expect(receipts[0]?.messageId).toBe("<child@interchange>");
  });

  test("sends the References chain composeReply supplies and reports it with the receipt", async () => {
    const sent: OutboundMessage[] = [];
    const reported: (readonly string[])[] = [];

    const drain = driveConnectorReplies({
      stream: streamOf([replyEvent(1, "threaded reply")]),
      composeReply: () => ({
        to: "carol@example.com",
        cc: [],
        inReplyTo: "<carol@interchange>",
        references: ["<earlier@interchange>", "<carol@interchange>"],
      }),
      send: async (message) => {
        sent.push(message);
        return { messageId: "<child@interchange>", status: "delivered" };
      },
      onReplySent: (_receipt, references) => {
        reported.push(references);
      },
    });

    await drain.done;

    expect(sent[0]?.references).toEqual([
      "<earlier@interchange>",
      "<carol@interchange>",
    ]);
    expect(reported).toEqual([
      ["<earlier@interchange>", "<carol@interchange>"],
    ]);
  });

  test("reports an empty chain for parts that carry no References", async () => {
    const sent: OutboundMessage[] = [];
    const reported: (readonly string[])[] = [];

    const drain = driveConnectorReplies({
      stream: streamOf([replyEvent(1, "no ancestor")]),
      composeReply: () => ({ to: "alice@example.com", cc: [] }),
      send: async (message) => {
        sent.push(message);
        return { messageId: "<child@interchange>", status: "delivered" };
      },
      onReplySent: (_receipt, references) => {
        reported.push(references);
      },
    });

    await drain.done;

    expect(sent[0]?.references).toBeUndefined();
    expect(reported).toEqual([[]]);
  });

  test("each thread reply carries the RFC 5322 chain of the message it answers", async () => {
    // Driven through a real router: the first reply answers the inbound
    // message, and the second answers the agent's own first reply.
    const router = createConnectorRouter({ ownAddress: "agent@example.com" });
    const inbound = createInboundMessage({
      from: "alice@example.com",
      to: ["agent@example.com"],
      content: "hello",
      messageId: "<inbound@example.com>",
      references: ["<root@example.com>", "<mid@example.com>"],
    });
    router.commit(router.route(inbound));

    const sentIds = ["<sent-1@example.com>", "<sent-2@example.com>"];
    const sent: OutboundMessage[] = [];
    const drain = driveConnectorReplies({
      stream: streamOf([replyEvent(1, "first"), replyEvent(2, "second")]),
      composeReply: () => router.composeReply(),
      send: async (message) => {
        const messageId = sentIds[sent.length];
        if (messageId === undefined) throw new Error("unexpected third send");
        sent.push(message);
        return { messageId, status: "delivered" };
      },
      onReplySent: (receipt, references) => {
        router.onReplySent(receipt, references);
      },
    });

    await drain.done;

    const inboundChain = [
      ...(inbound.headers.references ?? []),
      "<inbound@example.com>",
    ];
    expect(sent[0]?.inReplyTo).toBe("<inbound@example.com>");
    expect(sent[0]?.references).toEqual(inboundChain);
    expect(sent[1]?.inReplyTo).toBe("<sent-1@example.com>");
    expect(sent[1]?.references).toEqual([
      ...inboundChain,
      "<sent-1@example.com>",
    ]);
  });

  test("drops a reply with no recipient and settles it with no receipt", async () => {
    const { stream, push, end } = pushStream();
    let sendCount = 0;
    let sentCount = 0;
    const failures: unknown[] = [];

    const drain = driveConnectorReplies({
      stream,
      composeReply: () => null,
      send: async () => {
        sendCount += 1;
        return { messageId: "<child@interchange>", status: "delivered" };
      },
      onReplySent: () => {
        sentCount += 1;
      },
      onSendFailed: (cause) => {
        failures.push(cause);
      },
    });

    const barrier = drain.waitForReplyAfter(drain.replySeq());
    push(replyEvent(1, "no one to answer"));
    const settlement = await barrier;

    expect(settlement).toEqual({ ok: true, receipt: null });
    expect(sendCount).toBe(0);
    expect(sentCount).toBe(0);
    expect(failures).toEqual([]);

    end();
    await drain.done;
  });

  test("serializes replies so each composes against the advanced thread", async () => {
    // A minimal connector-thread model: `onReplySent` advances the parent id
    // that the next `composeReply` threads against. If the drain did not
    // serialize compose -> send -> onReplySent, the second reply would
    // compose against the pre-advance id.
    let lastMessageId = "<root@interchange>";
    let nextChildSeq = 0;
    const inReplyToSeen: string[] = [];

    const drain = driveConnectorReplies({
      stream: streamOf([replyEvent(1, "first"), replyEvent(2, "second")]),
      composeReply: () => {
        inReplyToSeen.push(lastMessageId);
        return { to: "alice@example.com", cc: [], inReplyTo: lastMessageId };
      },
      send: async () => {
        nextChildSeq += 1;
        return {
          messageId: `<child-${String(nextChildSeq)}@interchange>`,
          status: "delivered",
        };
      },
      onReplySent: (receipt) => {
        lastMessageId = receipt.messageId;
      },
    });

    await drain.done;

    expect(inReplyToSeen).toEqual([
      "<root@interchange>",
      "<child-1@interchange>",
    ]);
  });

  test("surfaces a send failure and leaves the thread unadvanced", async () => {
    const sentinel = new Error("outbound bridge rejected the send");
    const failures: unknown[] = [];
    let onReplySentCalled = false;

    const drain = driveConnectorReplies({
      stream: streamOf([replyEvent(1, "will fail")]),
      composeReply: () => ({
        to: "alice@example.com",
        cc: [],
        inReplyTo: "<parent@interchange>",
      }),
      send: async () => {
        throw sentinel;
      },
      onReplySent: () => {
        onReplySentCalled = true;
      },
      onSendFailed: (cause) => {
        failures.push(cause);
      },
    });

    await drain.done;

    expect(failures).toEqual([sentinel]);
    // A failed send must not advance the connector thread.
    expect(onReplySentCalled).toBe(false);
  });

  test("does not let a send failure reject the done promise", async () => {
    // The drain must keep running past a per-reply failure: `done` resolves,
    // and a later reply still sends. A rejecting `done` would take down the
    // caller that awaits it on teardown.
    const sent: OutboundMessage[] = [];
    let failCount = 0;

    const drain = driveConnectorReplies({
      stream: streamOf([replyEvent(1, "boom"), replyEvent(2, "ok")]),
      composeReply: () => ({
        to: "alice@example.com",
        cc: [],
        inReplyTo: "<parent@interchange>",
      }),
      send: async (message) => {
        if (message.content === "boom") {
          failCount += 1;
          throw new Error("first send fails");
        }
        sent.push(message);
        return { messageId: "<child@interchange>", status: "delivered" };
      },
      onReplySent: () => undefined,
    });

    await expect(drain.done).resolves.toBeUndefined();
    expect(failCount).toBe(1);
    expect(sent.length).toBe(1);
    expect(sent[0]?.content).toBe("ok");
  });

  test("stop() halts the loop before the next event's reply", async () => {
    // `stop()` is the cooperative early exit a teardown uses before the
    // stream ends on its own. After it is set, the loop breaks at the next
    // event rather than sending its reply.
    const sent: OutboundMessage[] = [];
    let stopHandle: (() => void) | null = null;

    async function* controlledStream(): AsyncGenerator<InferenceEvent> {
      yield replyEvent(1, "first");
      await Promise.resolve();
      // Stop before the second reply is observed.
      stopHandle?.();
      yield replyEvent(2, "second");
    }

    const drain = driveConnectorReplies({
      stream: controlledStream(),
      composeReply: () => ({
        to: "alice@example.com",
        cc: [],
        inReplyTo: "<parent@interchange>",
      }),
      send: async (message) => {
        sent.push(message);
        return { messageId: "<child@interchange>", status: "delivered" };
      },
      onReplySent: () => undefined,
    });
    stopHandle = drain.stop;

    await drain.done;

    expect(sent.length).toBe(1);
    expect(sent[0]?.content).toBe("first");
  });

  test("the settle barrier resolves with the receipt after the send acks", async () => {
    // The per-turn barrier: a caller snapshots `replySeq()` before the reply,
    // then awaits `waitForReplyAfter(snapshot)`, which resolves only once the
    // reply's send has acked and the thread advanced.
    const { stream, push, end } = pushStream();
    let sendAcked = false;

    const drain = driveConnectorReplies({
      stream,
      composeReply: () => ({
        to: "alice@example.com",
        cc: [],
        inReplyTo: "<parent@interchange>",
      }),
      send: async () => {
        sendAcked = true;
        return { messageId: "<child@interchange>", status: "delivered" };
      },
      onReplySent: () => undefined,
    });

    // Snapshot before the reply is pushed: no reply has settled yet.
    const before = drain.replySeq();
    expect(before).toBe(0);
    const barrier = drain.waitForReplyAfter(before);

    push(replyEvent(1, "the reply"));
    const settlement = await barrier;

    // The barrier resolved only after the send acked, and carries the receipt.
    expect(sendAcked).toBe(true);
    expect(settlement.ok).toBe(true);
    if (settlement.ok) {
      expect(settlement.receipt?.messageId).toBe("<child@interchange>");
    }
    // The sequence advanced by exactly one.
    expect(drain.replySeq()).toBe(1);

    end();
    await drain.done;
  });

  test("a turn that emits no reply does not settle the barrier until teardown", async () => {
    // A suspended / no-reply turn produces no `connector.reply`, so the reply
    // sequence does not advance and a barrier awaiting that reply stays pending
    // -- which is why the warm step must NOT await the barrier for such a turn.
    // When the drain ends before the reply arrives, the barrier resolves as a
    // failure rather than hanging forever.
    const { stream, push, end, pulled } = pushStream();

    const drain = driveConnectorReplies({
      stream,
      composeReply: () => ({
        to: "alice@example.com",
        cc: [],
        inReplyTo: "<parent@interchange>",
      }),
      send: async () => ({
        messageId: "<child@interchange>",
        status: "delivered",
      }),
      onReplySent: () => undefined,
    });

    const barrier = drain.waitForReplyAfter(0);
    let settledEarly = false;
    void barrier.then(() => {
      settledEarly = true;
    });

    // A non-reply event flows past without advancing the reply sequence.
    // The drain consumes the stream serially, so it takes the second noise
    // event only after it has finished handling the first: waiting for that
    // second pull puts the assertions strictly after any settlement the first
    // event could have caused, with no interval to guess at.
    push(noiseEvent(1));
    push(noiseEvent(2));
    await waitUntil(() => pulled() >= 2);
    expect(settledEarly).toBe(false);
    expect(drain.replySeq()).toBe(0);

    // Teardown before any reply arrives resolves the barrier as a failure.
    end();
    const settlement = await barrier;
    expect(settlement.ok).toBe(false);
    await drain.done;
  });

  test("a failed send settles the barrier as not-sent", async () => {
    // A send failure must NOT count as a durable reply: the barrier resolves
    // with `ok: false` carrying the cause, distinct from a successful ack, so a
    // per-turn caller fails the turn rather than treating the reply as sent.
    const sentinel = new Error("outbound bridge rejected the send");

    const drain = driveConnectorReplies({
      stream: streamOf([replyEvent(1, "will fail")]),
      composeReply: () => ({
        to: "alice@example.com",
        cc: [],
        inReplyTo: "<parent@interchange>",
      }),
      send: async () => {
        throw sentinel;
      },
      onReplySent: () => undefined,
    });

    await drain.done;

    // The reply settled (the sequence advanced) but as a failure.
    expect(drain.replySeq()).toBe(1);
    const settlement = await drain.waitForReplyAfter(0);
    expect(settlement.ok).toBe(false);
    if (!settlement.ok) {
      expect(settlement.cause).toBe(sentinel);
    }
  });
});
