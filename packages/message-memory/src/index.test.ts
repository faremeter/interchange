/* eslint-disable @typescript-eslint/no-non-null-assertion -- test refs[0]! always follows expect(refs.length) checks */
import { describe, test, expect } from "bun:test";
import { generateKeyPair, createNodeCrypto } from "@interchange/crypto-node";
import { createInMemoryTransport } from "./index";
import type { MailboxEvent, MessageRef } from "@interchange/types/runtime";

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

async function createTestTransport() {
  const transport = createInMemoryTransport();

  const kpA = await generateKeyPair();
  const kpB = await generateKeyPair();
  const cryptoA = createNodeCrypto(kpA);
  const cryptoB = createNodeCrypto(kpB);

  transport.registerAgent("alpha@test.interchange", cryptoA);
  transport.registerAgent("beta@test.interchange", cryptoB);

  const alphaTransport = transport.getTransportForAgent(
    "alpha@test.interchange",
  );
  const betaTransport = transport.getTransportForAgent("beta@test.interchange");

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
    const body = msg.payload!.body as { offeringId: string };
    expect(body.offeringId).toBe("code-review");
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

  test("registerAgent throws on duplicate registration", async () => {
    const transport = createInMemoryTransport();
    const kp = await generateKeyPair();
    const crypto = createNodeCrypto(kp);
    transport.registerAgent("alpha@test.interchange", crypto);
    expect(() =>
      transport.registerAgent("alpha@test.interchange", crypto),
    ).toThrow(/already registered/);
  });

  test("getTransportForAgent throws for unregistered agent", () => {
    const transport = createInMemoryTransport();
    expect(() =>
      transport.getTransportForAgent("nobody@test.interchange"),
    ).toThrow(/not registered/);
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
