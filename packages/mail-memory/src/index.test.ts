/* eslint-disable @typescript-eslint/no-non-null-assertion -- test refs[0]! always follows expect(refs.length) checks */
import { describe, test, expect } from "bun:test";
import { generateKeyPair, createNodeCrypto } from "@intx/crypto";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  generateMessageId,
  type MessageHeaders,
} from "@intx/mime";
import { createInMemoryTransport } from "./index";
import type {
  MailboxEvent,
  MessageRef,
  MessageAttachment,
} from "@intx/types/runtime";

function conversationHeaders(): MessageHeaders {
  return {
    from: "alpha@test.interchange",
    to: ["beta@test.interchange"],
    cc: undefined,
    date: new Date("2026-01-15T12:00:00Z"),
    messageId: generateMessageId("alpha@test.interchange"),
    subject: undefined,
    inReplyTo: undefined,
    references: undefined,
    mimeVersion: "1.0",
    interchangeType: "conversation.message",
    interchangeCorrelationId: undefined,
    interchangeTenantId: undefined,
    interchangeAgentId: undefined,
    interchangeSessionId: undefined,
    interchangeOfferingId: undefined,
    interchangeSchemaVersion: undefined,
    traceparent: undefined,
    tracestate: undefined,
  };
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

async function createTestTransport() {
  const transport = createInMemoryTransport();

  const kpA = await generateKeyPair();
  const kpB = await generateKeyPair();
  const cryptoA = createNodeCrypto(kpA);
  const cryptoB = createNodeCrypto(kpB);

  transport.register("alpha@test.interchange", cryptoA);
  transport.register("beta@test.interchange", cryptoB);

  const alphaTransport = transport.getTransportFor("alpha@test.interchange");
  const betaTransport = transport.getTransportFor("beta@test.interchange");

  return { transport, alphaTransport, betaTransport, cryptoA, cryptoB };
}

// ---------------------------------------------------------------------------
// Test 1: send + watch — verify async delivery
// ---------------------------------------------------------------------------

describe("send and watch", () => {
  test("watch callback fires asynchronously after send returns", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    let callbackFired = false;
    let receivedEvent: MailboxEvent | undefined;

    const unwatch = betaTransport.watch("INBOX", (event) => {
      callbackFired = true;
      receivedEvent = event;
    });

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      subject: "Hello",
      content: "Hello from alpha",
    });

    // After send completes (which includes async steps + microtask callbacks),
    // the watch callback should have fired.
    await new Promise((r) => setTimeout(r, 10));

    expect(callbackFired).toBe(true);
    expect(receivedEvent?.type).toBe("exists");
    if (receivedEvent?.type === "exists") {
      expect(receivedEvent.headers.from).toBe("alpha@test.interchange");
      expect(receivedEvent.headers.interchangeType).toBe(
        "conversation.message",
      );
    }

    unwatch();
  });

  test("unwatch prevents further callbacks", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    let count = 0;
    const unwatch = betaTransport.watch("INBOX", () => {
      count++;
    });

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "first",
    });
    await new Promise((r) => setTimeout(r, 10));
    expect(count).toBe(1);

    unwatch();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "second",
    });
    await new Promise((r) => setTimeout(r, 10));
    expect(count).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Test 2: search by Interchange-Type header
// ---------------------------------------------------------------------------

describe("search", () => {
  test("search by Interchange-Type header finds message", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "test content",
    });

    const refs = await betaTransport.search("INBOX", {
      header: { field: "Interchange-Type", contains: "conversation.message" },
    });

    expect(refs.length).toBe(1);
    expect(refs[0]!.mailbox).toBe("INBOX");
  });

  test("search for non-existing header returns empty", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "test",
    });

    const refs = await betaTransport.search("INBOX", {
      header: { field: "Interchange-Type", contains: "offering.request" },
    });

    expect(refs.length).toBe(0);
  });

  test("search by hasFlags finds flagged messages", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "test",
    });

    const refs = await betaTransport.search("INBOX", {});
    expect(refs.length).toBe(1);

    // Set the $Processed flag.
    await betaTransport.setFlags(refs[0]!, ["$Processed"]);

    // Should NOT find with missingFlags $Processed.
    const unprocessed = await betaTransport.search("INBOX", {
      missingFlags: ["$Processed"],
    });
    expect(unprocessed.length).toBe(0);

    // Should find with hasFlags $Processed.
    const processed = await betaTransport.search("INBOX", {
      hasFlags: ["$Processed"],
    });
    expect(processed.length).toBe(1);
  });

  test("UNKEYWORD $Processed does not find processed messages (test 4 from plan)", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "processed message",
    });

    const refs = await betaTransport.search("INBOX", {});
    await betaTransport.setFlags(refs[0]!, ["$Processed"]);

    const notFound = await betaTransport.search("INBOX", {
      missingFlags: ["$Processed"],
    });
    expect(notFound.length).toBe(0);
  });

  test("search by from address", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "hello",
    });

    const found = await betaTransport.search("INBOX", {
      from: "alpha@test.interchange",
    });
    expect(found.length).toBe(1);

    const notFound = await betaTransport.search("INBOX", {
      from: "nobody@test.interchange",
    });
    expect(notFound.length).toBe(0);
  });

  test("boolean AND composition", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "hello",
    });

    const found = await betaTransport.search("INBOX", {
      and: [
        { from: "alpha@test.interchange" },
        {
          header: {
            field: "Interchange-Type",
            contains: "conversation.message",
          },
        },
      ],
    });
    expect(found.length).toBe(1);

    const notFound = await betaTransport.search("INBOX", {
      and: [
        { from: "alpha@test.interchange" },
        { header: { field: "Interchange-Type", contains: "offering.request" } },
      ],
    });
    expect(notFound.length).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Test 3: fetchFull — verify signatureStatus is "valid"
// ---------------------------------------------------------------------------

describe("fetchFull", () => {
  test("fetchFull returns signatureStatus valid for signed message", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "hello world",
      subject: "Test",
    });

    const refs = await betaTransport.search("INBOX", {});
    expect(refs.length).toBe(1);

    const msg = await betaTransport.fetchFull(refs[0]!);
    expect(msg.signatureStatus).toBe("valid");
    expect(msg.headers.from).toBe("alpha@test.interchange");
    expect(msg.headers.interchangeType).toBe("conversation.message");
  });

  test("fetchFull parses conversation content", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "Hello, beta!",
    });

    const refs = await betaTransport.search("INBOX", {});
    const msg = await betaTransport.fetchFull(refs[0]!);
    expect(msg.content).toBe("Hello, beta!");
    expect(msg.payload).toBeUndefined();
  });

  test("fetchFull parses structured payload", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "offering.request",
      payload: { offeringId: "code-review", parameters: { branch: "main" } },
    });

    const refs = await betaTransport.search("INBOX", {});
    const msg = await betaTransport.fetchFull(refs[0]!);
    expect(msg.signatureStatus).toBe("valid");
    expect(msg.payload).toBeDefined();
    expect(msg.payload!.type).toBe("offering.request");
    expect(msg.payload!.body["offeringId"]).toBe("code-review");
  });

  test("fetchFull populates attachments for a conversation message", async () => {
    const { transport, betaTransport, cryptoA } = await createTestTransport();

    const attachments: MessageAttachment[] = [
      {
        name: "shot.png",
        contentType: "image/png",
        data: new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
      },
    ];
    const content = assembleSignedContent({
      kind: "conversation",
      text: "with image",
      attachments,
    });
    const sig = await createDetachedSignatureFromProvider(content, cryptoA);
    const raw = assembleMessage(conversationHeaders(), content, sig);

    transport.deliver("beta@test.interchange", raw);
    const refs = await betaTransport.search("INBOX", {});
    expect(refs).toHaveLength(1);

    const msg = await betaTransport.fetchFull(refs[0]!);
    expect(msg.signatureStatus).toBe("valid");
    expect(msg.content).toBe("with image");
    expect(msg.attachments).toHaveLength(1);
    const got = msg.attachments![0]!;
    const orig = attachments[0]!;
    expect(got.name).toBe("shot.png");
    expect(got.contentType).toBe("image/png");
    expect(Array.from(got.data)).toEqual(Array.from(orig.data));
  });

  test("fetchFull surfaces a malformed attachment instead of dropping it", async () => {
    const { transport, betaTransport, cryptoA } = await createTestTransport();

    const boundary = "mixed_bad_b64";
    const badContent = new TextEncoder().encode(
      `Content-Type: multipart/mixed; boundary="${boundary}"\r\n\r\n` +
        `--${boundary}\r\n` +
        `Content-Type: text/plain; charset=utf-8\r\n` +
        `Content-Transfer-Encoding: 7bit\r\n\r\n` +
        `hi\r\n` +
        `--${boundary}\r\n` +
        `Content-Type: image/png\r\n` +
        `Content-Transfer-Encoding: base64\r\n` +
        `Content-Disposition: attachment; filename="bad.png"\r\n\r\n` +
        `@@@not-valid-base64@@@\r\n` +
        `--${boundary}--\r\n`,
    );
    const sig = await createDetachedSignatureFromProvider(badContent, cryptoA);
    const raw = assembleMessage(conversationHeaders(), badContent, sig);

    transport.deliver("beta@test.interchange", raw);
    const refs = await betaTransport.search("INBOX", {});
    expect(refs).toHaveLength(1);

    await expect(betaTransport.fetchFull(refs[0]!)).rejects.toThrow();
  });

  test("fetchFull reads a bare text/plain signed conversation body", async () => {
    // A plain signed email (no multipart/mixed wrapper) is a valid
    // conversation message; fetchFull must read its body, not drop it.
    const { transport, betaTransport, cryptoA } = await createTestTransport();

    const bareContent = new TextEncoder().encode(
      `Content-Type: text/plain; charset=utf-8\r\n` +
        `Content-Transfer-Encoding: 7bit\r\n\r\n` +
        `plain signed body`,
    );
    const sig = await createDetachedSignatureFromProvider(bareContent, cryptoA);
    const raw = assembleMessage(conversationHeaders(), bareContent, sig);

    transport.deliver("beta@test.interchange", raw);
    const refs = await betaTransport.search("INBOX", {});
    expect(refs).toHaveLength(1);

    const msg = await betaTransport.fetchFull(refs[0]!);
    expect(msg.content).toBe("plain signed body");
    expect(msg.attachments).toBeUndefined();
  });

  test("fetchHeaders returns parsed headers", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "headers test",
      subject: "Subject Line",
    });

    const refs = await betaTransport.search("INBOX", {});
    const headers = await betaTransport.fetchHeaders(refs[0]!);
    expect(headers.from).toBe("alpha@test.interchange");
    expect(headers.to).toContain("beta@test.interchange");
    expect(headers.subject).toBe("Subject Line");
    expect(headers.interchangeType).toBe("conversation.message");
  });
});

// ---------------------------------------------------------------------------
// Test 5: Threading — REFERENCES algorithm tree structure
// ---------------------------------------------------------------------------

describe("thread", () => {
  test("3 messages in a thread produce correct tree structure", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    // Message A (root).
    const receiptA = await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      subject: "Thread root",
      content: "Message A",
    });

    // Message B (reply to A).
    const receiptB = await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      subject: "Re: Thread root",
      content: "Message B",
      inReplyTo: receiptA.messageId,
    });

    // Message C (reply to B).
    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      subject: "Re: Thread root",
      content: "Message C",
      inReplyTo: receiptB.messageId,
    });

    const threads = await betaTransport.thread("INBOX", "references");
    expect(threads.length).toBeGreaterThan(0);

    // The root thread should have children (a non-trivial tree).
    const allRefs: MessageRef[] = [];
    function collectRefs(nodes: typeof threads) {
      for (const node of nodes) {
        allRefs.push(node.ref);
        collectRefs(node.children);
      }
    }
    collectRefs(threads);
    expect(allRefs.length).toBe(3);
  });
});

// ---------------------------------------------------------------------------
// Test 6: UID monotonicity
// ---------------------------------------------------------------------------

describe("UID ordering", () => {
  test("UIDs are monotonically increasing (1, 2, 3)", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    for (let i = 0; i < 3; i++) {
      await alphaTransport.send({
        to: "beta@test.interchange",
        type: "conversation.message",
        content: `Message ${i + 1}`,
      });
    }

    const refs = await betaTransport.search("INBOX", {});
    expect(refs.length).toBe(3);

    const uids = refs.map((r) => r.uid);
    expect(uids[0]).toBe(1);
    expect(uids[1]).toBe(2);
    expect(uids[2]).toBe(3);
  });
});

// ---------------------------------------------------------------------------
// Test 7: MODSEQ increments on flag change
// ---------------------------------------------------------------------------

describe("MODSEQ", () => {
  test("MODSEQ increments when flags change", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "modseq test",
    });

    const statusBefore = await betaTransport.getMailboxStatus("INBOX");
    const modseqBefore = statusBefore.highestModSeq;

    const refs = await betaTransport.search("INBOX", {});
    await betaTransport.setFlags(refs[0]!, ["$Processed"]);

    const statusAfter = await betaTransport.getMailboxStatus("INBOX");
    expect(statusAfter.highestModSeq).toBeGreaterThan(modseqBefore);
  });
});

// ---------------------------------------------------------------------------
// Test 8: Sending to unknown address throws
// ---------------------------------------------------------------------------

describe("error handling", () => {
  test("send to unknown address throws", async () => {
    const { alphaTransport } = await createTestTransport();

    await expect(
      alphaTransport.send({
        to: "nobody@unknown.test",
        type: "conversation.message",
        content: "hello",
      }),
    ).rejects.toThrow(/not registered/);
  });

  test("fetch non-existent UID throws", async () => {
    const { betaTransport } = await createTestTransport();

    await expect(
      betaTransport.fetchFull({ uid: 9999, mailbox: "INBOX" }),
    ).rejects.toThrow(/not found/);
  });

  test("register throws on duplicate registration", async () => {
    const transport = createInMemoryTransport();
    const kp = await generateKeyPair();
    const crypto = createNodeCrypto(kp);
    transport.register("alpha@test.interchange", crypto);
    expect(() => transport.register("alpha@test.interchange", crypto)).toThrow(
      /already registered/,
    );
  });

  test("getTransportFor throws for unregistered address", () => {
    const transport = createInMemoryTransport();
    expect(() => transport.getTransportFor("nobody@test.interchange")).toThrow(
      /not registered/,
    );
  });
});

// ---------------------------------------------------------------------------
// Registration lifecycle — guards the single-entry-map invariant.
// ---------------------------------------------------------------------------

describe("registration lifecycle", () => {
  test("unregister then re-register works (entry is fully removed)", async () => {
    const transport = createInMemoryTransport();
    const kp = await generateKeyPair();
    const crypto = createNodeCrypto(kp);

    transport.register("alpha@test.interchange", crypto);
    transport.unregister("alpha@test.interchange");
    expect(() =>
      transport.register("alpha@test.interchange", crypto),
    ).not.toThrow();
  });

  test("send from a scoped transport whose address was deregistered throws", async () => {
    const { transport, alphaTransport } = await createTestTransport();
    transport.unregister("alpha@test.interchange");

    await expect(
      alphaTransport.send({
        to: "beta@test.interchange",
        type: "conversation.message",
        content: "hi",
      }),
    ).rejects.toThrow(/deregistered|not registered/);
  });

  test("fetchFull returns signatureStatus 'unknown' after sender is unregistered", async () => {
    const { transport, alphaTransport, betaTransport } =
      await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "hi",
    });

    await new Promise((r) => setTimeout(r, 10));

    const refs = await betaTransport.search("INBOX", {});
    expect(refs.length).toBe(1);

    transport.unregister("alpha@test.interchange");

    const full = await betaTransport.fetchFull(refs[0]!);
    expect(full.signatureStatus).toBe("unknown");
  });
});

// ---------------------------------------------------------------------------
// Additional: mailbox management
// ---------------------------------------------------------------------------

describe("mailbox management", () => {
  test("listMailboxes returns default mailboxes", async () => {
    const { betaTransport } = await createTestTransport();
    const mailboxes = await betaTransport.listMailboxes();
    const names = mailboxes.map((m) => m.name);
    expect(names).toContain("INBOX");
    expect(names).toContain("Sent");
    expect(names).toContain("Drafts");
    expect(names).toContain("Archive");
    expect(names).toContain("Trash");
  });

  test("sent copy appears in Sent mailbox", async () => {
    const { alphaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "sent copy test",
    });

    const sentRefs = await alphaTransport.search("Sent", {});
    expect(sentRefs.length).toBe(1);
  });

  test("expunge removes deleted messages", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "to be deleted",
    });

    const refs = await betaTransport.search("INBOX", {});
    expect(refs.length).toBe(1);

    await betaTransport.setFlags(refs[0]!, ["\\Deleted"]);
    await betaTransport.expunge("INBOX");

    const remaining = await betaTransport.search("INBOX", {});
    expect(remaining.length).toBe(0);
  });

  test("move transfers message to destination mailbox", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "conversation.message",
      content: "to be archived",
    });

    const refs = await betaTransport.search("INBOX", {});
    await betaTransport.move(refs[0]!, "Archive");

    const inboxRefs = await betaTransport.search("INBOX", {});
    expect(inboxRefs.length).toBe(0);

    const archiveRefs = await betaTransport.search("Archive", {});
    expect(archiveRefs.length).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Signature round-trip
// ---------------------------------------------------------------------------

describe("signature round-trip", () => {
  test("signature is valid even with structured message", async () => {
    const { alphaTransport, betaTransport } = await createTestTransport();

    await alphaTransport.send({
      to: "beta@test.interchange",
      type: "offering.request",
      payload: { offeringId: "test", parameters: {} },
      summary: "Test offering",
    });

    const refs = await betaTransport.search("INBOX", {});
    const msg = await betaTransport.fetchFull(refs[0]!);
    expect(msg.signatureStatus).toBe("valid");
  });
});

// ---------------------------------------------------------------------------
// deliver() — federation inbound delivery
// ---------------------------------------------------------------------------

describe("deliver", () => {
  const VALID_MESSAGE = new TextEncoder().encode(
    [
      "From: sender@remote",
      "To: alpha@test.interchange",
      "Date: Thu, 17 Apr 2026 12:00:00 +0000",
      "Message-ID: <fed-1@remote>",
      "Subject: Hello",
      "Content-Type: text/plain",
      "",
      "Body text",
    ].join("\r\n"),
  );

  test("delivers a well-formed message to INBOX", async () => {
    const { transport } = await createTestTransport();
    const alphaTransport = transport.getTransportFor("alpha@test.interchange");

    transport.deliver("alpha@test.interchange", VALID_MESSAGE);

    const refs = await alphaTransport.search("INBOX", {});
    expect(refs).toHaveLength(1);
    const headers = await alphaTransport.fetchHeaders(refs[0]!);
    expect(headers.messageId).toBe("<fed-1@remote>");
    expect(headers.from).toBe("sender@remote");
  });

  test("fires watch callback on delivery", async () => {
    const { transport } = await createTestTransport();
    const alphaTransport = transport.getTransportFor("alpha@test.interchange");

    const events: MailboxEvent[] = [];
    alphaTransport.watch("INBOX", (event) => events.push(event));

    transport.deliver("alpha@test.interchange", VALID_MESSAGE);

    // Watch callbacks are scheduled via queueMicrotask.
    await new Promise((r) => setTimeout(r, 10));
    expect(events).toHaveLength(1);
    expect(events[0]!.type).toBe("exists");
  });

  test("throws for unregistered address", async () => {
    const { transport } = await createTestTransport();

    expect(() =>
      transport.deliver("nobody@test.interchange", VALID_MESSAGE),
    ).toThrow(/not registered/);
  });

  test("throws for missing Message-ID header", async () => {
    const { transport } = await createTestTransport();
    const msg = new TextEncoder().encode(
      ["From: x@y", "Date: Thu, 17 Apr 2026 12:00:00 +0000", "", "body"].join(
        "\r\n",
      ),
    );

    expect(() => transport.deliver("alpha@test.interchange", msg)).toThrow(
      /Message-ID/,
    );
  });

  test("throws for missing From header", async () => {
    const { transport } = await createTestTransport();
    const msg = new TextEncoder().encode(
      [
        "Message-ID: <x@y>",
        "Date: Thu, 17 Apr 2026 12:00:00 +0000",
        "",
        "body",
      ].join("\r\n"),
    );

    expect(() => transport.deliver("alpha@test.interchange", msg)).toThrow(
      /From/,
    );
  });

  test("throws for missing Date header", async () => {
    const { transport } = await createTestTransport();
    const msg = new TextEncoder().encode(
      ["From: x@y", "Message-ID: <x@y>", "", "body"].join("\r\n"),
    );

    expect(() => transport.deliver("alpha@test.interchange", msg)).toThrow(
      /Date/,
    );
  });
});
