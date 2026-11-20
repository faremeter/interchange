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

import { createInboundMessage } from "@intx/mime";

import {
  createConnectorRouter,
  type ConnectorReplyParts,
} from "./connector-router";
import { driveConnectorReplies, sendConnectorReply } from "./reply-drain";

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

  test("drops a reply with no recipient without a send or a failure", async () => {
    let composeCount = 0;
    let sendCount = 0;
    let sentCount = 0;
    const failures: unknown[] = [];

    const drain = driveConnectorReplies({
      stream: streamOf([replyEvent(1, "no one to answer")]),
      composeReply: () => {
        composeCount += 1;
        return null;
      },
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

    await drain.done;

    expect(composeCount).toBe(1);
    expect(sendCount).toBe(0);
    expect(sentCount).toBe(0);
    expect(failures).toEqual([]);
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
});

describe("sendConnectorReply", () => {
  const PARTS: ConnectorReplyParts = {
    to: "alice@example.com",
    cc: [],
    inReplyTo: "<parent@interchange>",
    references: ["<root@interchange>", "<parent@interchange>"],
  };
  const RECEIPT: SendReceipt = {
    messageId: "<child@interchange>",
    status: "delivered",
  };

  test("sends the reply and reports the receipt and References", async () => {
    const sent: OutboundMessage[] = [];
    const reported: [SendReceipt, readonly string[]][] = [];

    const settlement = await sendConnectorReply(
      {
        composeReply: () => PARTS,
        send: async (message) => {
          sent.push(message);
          return RECEIPT;
        },
        onReplySent: (receipt, references) => {
          reported.push([receipt, references]);
        },
      },
      "the reply body",
    );

    expect(settlement).toEqual({ ok: true, receipt: RECEIPT });
    expect(sent).toEqual([
      { ...PARTS, content: "the reply body", type: "conversation.message" },
    ]);
    expect(reported).toEqual([
      [RECEIPT, ["<root@interchange>", "<parent@interchange>"]],
    ]);
  });

  test("a reply with no recipient settles with no receipt and no send", async () => {
    let sendCalled = false;
    let onReplySentCalled = false;

    const settlement = await sendConnectorReply(
      {
        composeReply: () => null,
        send: async () => {
          sendCalled = true;
          return RECEIPT;
        },
        onReplySent: () => {
          onReplySentCalled = true;
        },
      },
      "no one to answer",
    );

    expect(settlement).toEqual({ ok: true, receipt: null });
    expect(sendCalled).toBe(false);
    expect(onReplySentCalled).toBe(false);
  });

  test("a compose failure settles in the compose phase without a send", async () => {
    const sentinel = new Error("no thread");
    let sendCalled = false;

    const settlement = await sendConnectorReply(
      {
        composeReply: () => {
          throw sentinel;
        },
        send: async () => {
          sendCalled = true;
          return RECEIPT;
        },
        onReplySent: () => undefined,
      },
      "reply",
    );

    expect(settlement).toEqual({
      ok: false,
      phase: "compose",
      cause: sentinel,
    });
    expect(sendCalled).toBe(false);
  });

  test("a send failure settles in the send phase without onReplySent", async () => {
    const sentinel = new Error("bridge rejected the send");
    let onReplySentCalled = false;

    const settlement = await sendConnectorReply(
      {
        composeReply: () => PARTS,
        send: async () => {
          throw sentinel;
        },
        onReplySent: () => {
          onReplySentCalled = true;
        },
      },
      "reply",
    );

    expect(settlement).toEqual({ ok: false, phase: "send", cause: sentinel });
    expect(onReplySentCalled).toBe(false);
  });

  test("an onReplySent failure after the send settles in the persist phase", async () => {
    const sentinel = new Error("metadata write failed");
    const sent: OutboundMessage[] = [];

    const settlement = await sendConnectorReply(
      {
        composeReply: () => PARTS,
        send: async (message) => {
          sent.push(message);
          return RECEIPT;
        },
        onReplySent: async () => {
          throw sentinel;
        },
      },
      "reply",
    );

    expect(settlement).toEqual({
      ok: false,
      phase: "persist",
      cause: sentinel,
    });
    expect(sent.length).toBe(1);
  });
});
