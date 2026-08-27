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

import type { ConnectorReplyParts } from "./connector-router";
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

  test("sets the full References chain a supplied resolver returns", async () => {
    const sent: OutboundMessage[] = [];
    const resolvedFor: string[] = [];

    const drain = driveConnectorReplies({
      stream: streamOf([replyEvent(1, "threaded reply")]),
      composeReply: () => ({
        to: "alice@example.com",
        cc: [],
        inReplyTo: "<parent@interchange>",
      }),
      resolveReferences: async (inReplyTo) => {
        resolvedFor.push(inReplyTo);
        return [
          "<root@interchange>",
          "<mid@interchange>",
          "<parent@interchange>",
        ];
      },
      send: async (message) => {
        sent.push(message);
        return { messageId: "<child@interchange>", status: "delivered" };
      },
      onReplySent: () => undefined,
    });

    await drain.done;

    // The resolver is consulted with the parent id from composeReply, and the
    // full ancestry it returns rides onto the outbound message verbatim.
    expect(resolvedFor).toEqual(["<parent@interchange>"]);
    expect(sent[0]?.references).toEqual([
      "<root@interchange>",
      "<mid@interchange>",
      "<parent@interchange>",
    ]);
  });

  test("omits references when the resolver reports no parent", async () => {
    // A resolver miss (first reply on a fresh thread, malformed id) returns
    // undefined; the drain then leaves `references` unset so the transport
    // derives a single-element chain from inReplyTo.
    const sent: OutboundMessage[] = [];

    const drain = driveConnectorReplies({
      stream: streamOf([replyEvent(1, "first on the thread")]),
      composeReply: () => ({
        to: "alice@example.com",
        cc: [],
        inReplyTo: "<parent@interchange>",
      }),
      resolveReferences: async () => undefined,
      send: async (message) => {
        sent.push(message);
        return { messageId: "<child@interchange>", status: "delivered" };
      },
      onReplySent: () => undefined,
    });

    await drain.done;

    expect(sent).toHaveLength(1);
    expect(sent[0]?.references).toBeUndefined();
  });

  test("omits references with no resolver supplied", async () => {
    // The createHarness path supplies no resolver; the drain must leave
    // `references` unset, preserving the pre-existing single-element
    // threading the transport derives from inReplyTo.
    const sent: OutboundMessage[] = [];

    const drain = driveConnectorReplies({
      stream: streamOf([replyEvent(1, "no resolver")]),
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

    await drain.done;

    expect(sent).toHaveLength(1);
    expect(sent[0]?.references).toBeUndefined();
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
