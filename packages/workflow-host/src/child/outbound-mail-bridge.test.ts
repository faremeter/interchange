import { describe, test, expect } from "bun:test";

import { type } from "arktype";

import { base64Decode } from "@intx/types";
import { createInMemoryMailboxStore } from "@intx/mailbox";
import type { StoredEnvelope } from "@intx/mailbox";
import type { MailboxEvent, MessageHeaders } from "@intx/types/runtime";

import { createChildOutboundMailBridge } from "./outbound-mail-bridge";
import {
  createSupervisorBackedTransport,
  type SupervisorBackedTransportInbound,
} from "./supervisor-backed-transport";
import { createMailboxWatchRegistry } from "./mailbox-watch-registry";
import type { ChildMailboxReader } from "./child-mailbox-reader";
import type {
  MailboxSyncKnownState,
  MailboxSyncResult,
  SubstrateMailboxStore,
} from "../adapters/substrate-mailbox-store";
import {
  ControlPayload,
  type ControlChannelSender,
} from "../ipc/control-channel";

/**
 * A `SubstrateMailboxStore` over an in-memory `MailboxStore`, so a transport
 * test seeds real messages without standing up a substrate. `flush` counts its
 * calls (the flag-write tests assert it ran) and `sync` mirrors the substrate
 * backing's QRESYNC delta. `open` returns this same evolving store on every
 * call, modeling committed state a later read observes.
 */
function createSeededReader(): {
  reader: ChildMailboxReader;
  store: SubstrateMailboxStore;
  flushCount: () => number;
} {
  const inner = createInMemoryMailboxStore();
  let flushes = 0;
  let dirty = false;

  const store: SubstrateMailboxStore = {
    get uidValidity() {
      return inner.uidValidity;
    },
    get uidNext() {
      return inner.uidNext;
    },
    get highestModSeq() {
      return inner.highestModSeq;
    },
    get messages() {
      return inner.messages;
    },
    get pendingWrites() {
      return dirty;
    },
    append(raw, envelope, flags) {
      dirty = true;
      return inner.append(raw, envelope, flags);
    },
    readRaw(uid) {
      return inner.readRaw(uid);
    },
    find(uid) {
      return inner.find(uid);
    },
    addFlags(uid, flags) {
      dirty = true;
      return inner.addFlags(uid, flags);
    },
    removeFlags(uid, flags) {
      dirty = true;
      return inner.removeFlags(uid, flags);
    },
    remove(uid) {
      dirty = true;
      inner.remove(uid);
    },
    async flush() {
      flushes++;
      dirty = false;
    },
    sync(known: MailboxSyncKnownState): MailboxSyncResult {
      const highestModSeq = inner.highestModSeq;
      if (known.uidValidity !== inner.uidValidity) {
        return {
          resync: true,
          uidValidity: inner.uidValidity,
          uidNext: inner.uidNext,
          highestModSeq,
          messages: inner.messages.slice(),
        };
      }
      const changed = inner.messages
        .filter((m) => m.modseq > known.highestModSeq)
        .sort((a, b) => a.uid - b.uid);
      return {
        resync: false,
        uidValidity: inner.uidValidity,
        uidNext: inner.uidNext,
        highestModSeq,
        changed,
        vanished: [],
      };
    },
  };

  return {
    reader: { open: async () => store },
    store,
    flushCount: () => flushes,
  };
}

/** Append a minimal RFC 2822 message to a seeded store; returns its UID. */
function seedMessage(
  store: SubstrateMailboxStore,
  fields: { from: string; to: string; subject: string; messageId: string },
  flags: string[] = [],
): number {
  const date = "Mon, 01 Jan 2024 00:00:00 +0000";
  const CRLF = "\r\n";
  const raw = new TextEncoder().encode(
    `From: ${fields.from}${CRLF}` +
      `To: ${fields.to}${CRLF}` +
      `Subject: ${fields.subject}${CRLF}` +
      `Date: ${date}${CRLF}` +
      `Message-ID: ${fields.messageId}${CRLF}` +
      `Content-Type: text/plain${CRLF}${CRLF}` +
      `body of ${fields.subject}`,
  );
  const envelope: StoredEnvelope = {
    messageId: fields.messageId,
    from: fields.from,
    to: [fields.to],
    subject: fields.subject,
    date: new Date(date),
    inReplyTo: undefined,
    references: [],
    interchangeType: undefined,
    interchangeCorrelationId: undefined,
  };
  return store.append(raw, envelope, flags);
}

/** A fresh outbound-mail bridge whose upstream frames are discarded. */
function makeBridge() {
  return createChildOutboundMailBridge({
    upstreamSender: createCapturingSender(),
  });
}

/** Build the inbound wiring with test defaults, overridable per test. */
function makeInbound(
  overrides: Partial<SupervisorBackedTransportInbound> = {},
): SupervisorBackedTransportInbound {
  return {
    reader: createSeededReader().reader,
    watchRegistry: createMailboxWatchRegistry(),
    getCrypto: () => undefined,
    ...overrides,
  };
}

/** Let queued microtasks (the watch registry's async delivery) run. */
async function flushMicrotasks(): Promise<void> {
  await new Promise<void>((resolve) => queueMicrotask(resolve));
}

/**
 * Capture the `outbound.message` frames a bridge emits without standing
 * up the real Ed25519-signed sender. The `seq` accessor is unused by the
 * bridge but required by the `ControlChannelSender` shape.
 */
function createCapturingSender(): ControlChannelSender & {
  sent: Extract<ControlPayload, { type: "outbound.message" }>["data"][];
} {
  const sent: Extract<ControlPayload, { type: "outbound.message" }>["data"][] =
    [];
  return {
    get seq() {
      return sent.length;
    },
    async send(payload: ControlPayload) {
      if (payload.type === "outbound.message") sent.push(payload.data);
    },
    sent,
  };
}

describe("createChildOutboundMailBridge", () => {
  test("submit emits an outbound.message frame and resolves on the matching result", async () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-1",
    });

    const submitted = bridge.submit("agent@example.com", {
      to: "recipient@example.com",
      type: "conversation.message",
      content: "reply text",
    });
    // The frame carries the sender address and the projected message.
    expect(sender.sent).toHaveLength(1);
    const frame = sender.sent[0];
    if (frame === undefined) throw new Error("no frame emitted");
    expect(frame.requestId).toBe("rid-1");
    expect(frame.senderAddress).toBe("agent@example.com");
    expect(frame.message.to).toBe("recipient@example.com");
    expect(frame.message.content).toBe("reply text");
    // The frame validates against the canonical control payload narrow.
    const validated = ControlPayload({ type: "outbound.message", data: frame });
    expect(validated instanceof type.errors).toBe(false);
    expect(bridge.pendingCount).toBe(1);

    bridge.handleResult({
      requestId: "rid-1",
      result: { ok: true, messageId: "<m-1@example.com>", status: "delivered" },
    });
    const receipt = await submitted;
    expect(receipt.messageId).toBe("<m-1@example.com>");
    expect(receipt.status).toBe("delivered");
    expect(bridge.pendingCount).toBe(0);
  });

  test("a failed result rejects the submit so the mail-tool call fails loudly", async () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-2",
    });
    const submitted = bridge.submit("agent@example.com", {
      to: "recipient@example.com",
      type: "conversation.message",
      content: "x",
    });
    bridge.handleResult({
      requestId: "rid-2",
      result: { ok: false, reason: "sender not registered" },
    });
    await expect(submitted).rejects.toThrow(/sender not registered/);
  });

  test("cancelAll rejects every pending send", async () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-3",
    });
    const submitted = bridge.submit("agent@example.com", {
      to: "recipient@example.com",
      type: "conversation.message",
      content: "x",
    });
    bridge.cancelAll("control loop exited");
    await expect(submitted).rejects.toThrow(/cancelled: control loop exited/);
    expect(bridge.pendingCount).toBe(0);
  });

  test("projects the References chain through the wire", async () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-refs",
    });
    void bridge.submit("agent@example.com", {
      to: "recipient@example.com",
      type: "conversation.message",
      content: "threaded reply",
      inReplyTo: "<parent@example.com>",
      references: [
        "<root@example.com>",
        "<mid@example.com>",
        "<parent@example.com>",
      ],
    });
    const frame = sender.sent[0];
    if (frame === undefined) throw new Error("no frame emitted");
    expect(frame.message.inReplyTo).toBe("<parent@example.com>");
    expect(frame.message.references).toEqual([
      "<root@example.com>",
      "<mid@example.com>",
      "<parent@example.com>",
    ]);
    // The projected frame validates against the canonical control payload.
    const validated = ControlPayload({ type: "outbound.message", data: frame });
    expect(validated instanceof type.errors).toBe(false);
  });

  test("base64-roundtrips attachment bytes through the wire projection", async () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-4",
    });
    const data = new Uint8Array([1, 2, 3, 250, 251, 252]);
    void bridge.submit("agent@example.com", {
      to: "recipient@example.com",
      type: "conversation.message",
      content: "with attachment",
      attachments: [
        { name: "f.bin", contentType: "application/octet-stream", data },
      ],
    });
    const frame = sender.sent[0];
    if (frame === undefined) throw new Error("no frame emitted");
    const att = frame.message.attachments?.[0];
    if (att === undefined) throw new Error("attachment not projected");
    expect(att.name).toBe("f.bin");
    expect(base64Decode(att.dataBase64)).toEqual(data);
  });
});

describe("createSupervisorBackedTransport", () => {
  test("send routes through the bridge as the agent's address", async () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-5",
    });
    const transport = createSupervisorBackedTransport(
      bridge,
      "agent@example.com",
      makeInbound(),
    );
    const sendPromise = transport.send({
      to: "recipient@example.com",
      type: "conversation.message",
      content: "via transport",
    });
    expect(sender.sent).toHaveLength(1);
    expect(sender.sent[0]?.senderAddress).toBe("agent@example.com");
    bridge.handleResult({
      requestId: "rid-5",
      result: { ok: true, messageId: "<m-5@example.com>", status: "delivered" },
    });
    const receipt = await sendPromise;
    expect(receipt.messageId).toBe("<m-5@example.com>");
  });

  test("inbound methods throw when constructed without the inbound wiring", async () => {
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "agent@example.com",
    );
    await expect(transport.search("INBOX", {})).rejects.toThrow(
      /is not wired for unified-host step agent/,
    );
    await expect(
      transport.fetchFull({ uid: 1, mailbox: "INBOX" }),
    ).rejects.toThrow(/is not wired for unified-host step agent/);
    expect(() =>
      transport.watch("INBOX", () => {
        /* never reached */
      }),
    ).toThrow(/is not wired for unified-host step agent/);
  });

  test("search resolves against the seeded INBOX", async () => {
    const seeded = createSeededReader();
    seedMessage(seeded.store, {
      from: "alice@example.com",
      to: "agent@example.com",
      subject: "hello",
      messageId: "<a-1@example.com>",
    });
    seedMessage(seeded.store, {
      from: "bob@example.com",
      to: "agent@example.com",
      subject: "unrelated",
      messageId: "<b-1@example.com>",
    });
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "agent@example.com",
      makeInbound({ reader: seeded.reader }),
    );

    const all = await transport.search("INBOX", {});
    expect(all).toHaveLength(2);

    const fromAlice = await transport.search("INBOX", { from: "alice" });
    expect(fromAlice).toHaveLength(1);
    expect(fromAlice[0]?.uid).toBe(1);
    expect(fromAlice[0]?.mailbox).toBe("INBOX");
  });

  test("fetchHeaders and fetchFull resolve against the seeded INBOX", async () => {
    const seeded = createSeededReader();
    const uid = seedMessage(seeded.store, {
      from: "alice@example.com",
      to: "agent@example.com",
      subject: "hello",
      messageId: "<a-1@example.com>",
    });
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "agent@example.com",
      makeInbound({ reader: seeded.reader }),
    );

    const headers = await transport.fetchHeaders({ uid, mailbox: "INBOX" });
    expect(headers.from).toBe("alice@example.com");
    expect(headers.subject).toBe("hello");

    const full = await transport.fetchFull({ uid, mailbox: "INBOX" });
    expect(full.ref.uid).toBe(uid);
    expect(full.headers.messageId).toBe("<a-1@example.com>");
    // No sender key is wired, so the signature is reported unverifiable rather
    // than silently trusted.
    expect(full.signatureStatus).toBe("unknown");
  });

  test("getMailboxStatus counts the seeded INBOX", async () => {
    const seeded = createSeededReader();
    seedMessage(
      seeded.store,
      {
        from: "alice@example.com",
        to: "agent@example.com",
        subject: "seen",
        messageId: "<a-1@example.com>",
      },
      ["\\Seen"],
    );
    seedMessage(seeded.store, {
      from: "bob@example.com",
      to: "agent@example.com",
      subject: "unseen",
      messageId: "<b-1@example.com>",
    });
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "agent@example.com",
      makeInbound({ reader: seeded.reader }),
    );

    const status = await transport.getMailboxStatus("INBOX");
    expect(status.total).toBe(2);
    expect(status.unseen).toBe(1);
  });

  test("watch fires when the registry delivers a mailbox event", async () => {
    const watchRegistry = createMailboxWatchRegistry();
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "agent@example.com",
      makeInbound({ watchRegistry }),
    );

    const events: MailboxEvent[] = [];
    const unsubscribe = transport.watch("INBOX", (event) => {
      events.push(event);
    });

    const headers: MessageHeaders = {
      from: "alice@example.com",
      to: ["agent@example.com"],
      date: "Mon, 01 Jan 2024 00:00:00 +0000",
      messageId: "<a-1@example.com>",
    };
    watchRegistry.fire("INBOX", { type: "exists", uid: 1, headers });
    await flushMicrotasks();

    expect(events).toHaveLength(1);
    expect(events[0]).toEqual({ type: "exists", uid: 1, headers });

    // After unsubscribe a later fire is not observed.
    unsubscribe();
    watchRegistry.fire("INBOX", { type: "exists", uid: 2, headers });
    await flushMicrotasks();
    expect(events).toHaveLength(1);
  });

  test("setFlags mutates the snapshot and flushes it", async () => {
    const seeded = createSeededReader();
    const uid = seedMessage(seeded.store, {
      from: "alice@example.com",
      to: "agent@example.com",
      subject: "hello",
      messageId: "<a-1@example.com>",
    });
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "agent@example.com",
      makeInbound({ reader: seeded.reader }),
    );

    await transport.setFlags({ uid, mailbox: "INBOX" }, ["\\Seen"]);
    expect(seeded.flushCount()).toBe(1);
    expect(seeded.store.find(uid)?.flags.has("\\Seen")).toBe(true);

    await transport.clearFlags({ uid, mailbox: "INBOX" }, ["\\Seen"]);
    expect(seeded.flushCount()).toBe(2);
    expect(seeded.store.find(uid)?.flags.has("\\Seen")).toBe(false);
  });

  test("sync splits new arrivals from flag changes against the known uidNext", async () => {
    const seeded = createSeededReader();
    const uid = seedMessage(seeded.store, {
      from: "alice@example.com",
      to: "agent@example.com",
      subject: "hello",
      messageId: "<a-1@example.com>",
    });
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "agent@example.com",
      makeInbound({ reader: seeded.reader }),
    );
    const uidValidity = seeded.store.uidValidity;

    // A client that has seen nothing (uidNext 1, modseq 0) observes the seeded
    // message as a new arrival, not a flag change.
    const fresh = await transport.sync("INBOX", {
      uidValidity,
      uidNext: 1,
      highestModSeq: 0,
    });
    expect(fresh.fullResyncRequired).toBe(false);
    expect(fresh.newMessages).toEqual([{ uid, mailbox: "INBOX" }]);
    expect(fresh.changed).toHaveLength(0);

    // A flag change on a message the client already holds (uid < known uidNext)
    // reports as `changed`, not a new arrival.
    seeded.store.addFlags(uid, ["\\Seen"]);
    const afterFlag = await transport.sync("INBOX", {
      uidValidity,
      uidNext: seeded.store.uidNext,
      highestModSeq: 1,
    });
    expect(afterFlag.newMessages).toHaveLength(0);
    expect(afterFlag.changed).toEqual([{ uid, flags: ["\\Seen"] }]);
  });

  test("a mismatched uidValidity forces a full resync", async () => {
    const seeded = createSeededReader();
    const uid = seedMessage(seeded.store, {
      from: "alice@example.com",
      to: "agent@example.com",
      subject: "hello",
      messageId: "<a-1@example.com>",
    });
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "agent@example.com",
      makeInbound({ reader: seeded.reader }),
    );

    const result = await transport.sync("INBOX", {
      uidValidity: seeded.store.uidValidity + 1,
      uidNext: 1,
      highestModSeq: 0,
    });
    expect(result.fullResyncRequired).toBe(true);
    expect(result.newMessages).toEqual([{ uid, mailbox: "INBOX" }]);
  });

  test("rejects a mailbox other than the agent's own INBOX", async () => {
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "agent@example.com",
      makeInbound(),
    );
    await expect(transport.search("Archive", {})).rejects.toThrow(
      /owns only the "INBOX" mailbox/,
    );
    await expect(
      transport.fetchFull({ uid: 1, mailbox: "Sent" }),
    ).rejects.toThrow(/owns only the "INBOX" mailbox/);
  });

  test("expunge is rejected so an append-only blob is never removed", async () => {
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "agent@example.com",
      makeInbound(),
    );
    await expect(transport.expunge("INBOX")).rejects.toThrow(
      /append-only.*break hub replication/,
    );
  });

  test("methods for mailboxes the agent does not own stay unsupported", async () => {
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "agent@example.com",
      makeInbound(),
    );
    await expect(
      transport.append("INBOX", {
        ref: { uid: 1, mailbox: "INBOX" },
        headers: {
          from: "a@example.com",
          to: ["agent@example.com"],
          date: "Mon, 01 Jan 2024 00:00:00 +0000",
          messageId: "<x@example.com>",
        },
        flags: [],
        signatureStatus: "unknown",
      }),
    ).rejects.toThrow(/not supported for unified-host step agent/);
    await expect(
      transport.move({ uid: 1, mailbox: "INBOX" }, "Archive"),
    ).rejects.toThrow(/not supported for unified-host step agent/);
  });
});
