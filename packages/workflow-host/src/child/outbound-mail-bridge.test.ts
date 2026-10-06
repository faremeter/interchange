import { describe, test, expect } from "bun:test";

import { type } from "arktype";

import { base64Decode, base64Encode, deriveWorkflowRunId } from "@intx/types";
import type { MailboxEvent, MessageHeaders } from "@intx/types/runtime";
import {
  isMessageTransportError,
  MessageTransportError,
  type MessageTransportCondition,
} from "@intx/types/runtime";

import { createChildOutboundMailBridge } from "./outbound-mail-bridge";
import {
  createSupervisorBackedTransport,
  type SupervisorBackedTransportInbound,
} from "./supervisor-backed-transport";
import { createMailboxWatchRegistry } from "./mailbox-watch-registry";
import type {
  ChildMailboxCallBridge,
  MailboxCall,
  MailboxCallSuccess,
} from "./mailbox-call-bridge";
import type {
  ChildMailboxMutationBridge,
  MailboxMutation,
  MailboxMutationResult,
} from "./mailbox-mutation-bridge";
import {
  ControlPayload,
  type ControlChannelSender,
} from "../ipc/control-channel";

/** A fresh outbound-mail bridge whose upstream frames are discarded. */
function makeBridge() {
  return createChildOutboundMailBridge({
    upstreamSender: createCapturingSender(),
  });
}

// The RFC 5530 condition a refusal named. A refusal naming no condition is a
// different failure from the one under test, so it throws rather than folding
// into the comparison below.
function refusalCondition(cause: unknown): string {
  if (!isMessageTransportError(cause)) {
    throw new Error(`expected a condition, got ${String(cause)}`, { cause });
  }
  return cause.condition;
}

async function rejectedCondition(
  run: () => Promise<unknown>,
): Promise<string | undefined> {
  try {
    await run();
    return undefined;
  } catch (cause) {
    return refusalCondition(cause);
  }
}

/**
 * A `ChildMailboxMutationBridge` that records every submitted mutation and
 * resolves immediately, so a transport test asserts the routed write without a
 * live supervisor.
 */
function createRecordingMutationBridge(
  result: MailboxMutationResult = {},
): ChildMailboxMutationBridge & {
  submitted: MailboxMutation[];
} {
  const submitted: MailboxMutation[] = [];
  return {
    submitted,
    submit(mutation: MailboxMutation) {
      submitted.push(mutation);
      return Promise.resolve(result);
    },
    handleResult() {
      /* no downstream frames in this fake */
    },
    cancelAll() {
      /* nothing pending */
    },
    get pendingCount() {
      return 0;
    },
  };
}

/**
 * A call bridge that records every submitted call and answers with `respond`.
 * The transport tests assert what was forwarded and what came back. They do
 * not open a mailbox.
 */
function createRecordingCallBridge(
  respond: (
    call: MailboxCall,
  ) => Promise<MailboxCallSuccess> | MailboxCallSuccess,
): ChildMailboxCallBridge & { submitted: MailboxCall[] } {
  const submitted: MailboxCall[] = [];
  return {
    submitted,
    submit(call) {
      submitted.push(call);
      return Promise.resolve(respond(call));
    },
    handleResult() {
      /* no downstream frames in this fake */
    },
    cancelAll() {
      /* nothing pending */
    },
    get pendingCount() {
      return 0;
    },
  };
}

/** A call bridge that refuses every call with one supervisor condition. */
function refusingCallBridge(
  condition: MessageTransportCondition,
  reason: string,
): ChildMailboxCallBridge & { submitted: MailboxCall[] } {
  const submitted: MailboxCall[] = [];
  return {
    submitted,
    submit(call) {
      submitted.push(call);
      return Promise.reject(new MessageTransportError(condition, reason));
    },
    handleResult() {
      /* no downstream frames in this fake */
    },
    cancelAll() {
      /* nothing pending */
    },
    get pendingCount() {
      return 0;
    },
  };
}

/** Build the inbound wiring with test defaults, overridable per test. */
function makeInbound(
  overrides: Partial<SupervisorBackedTransportInbound> = {},
): SupervisorBackedTransportInbound {
  return {
    watchRegistry: createMailboxWatchRegistry(),
    mutationBridge: createRecordingMutationBridge(),
    callBridge: createRecordingCallBridge(() => {
      throw new Error("this test did not script a mailbox call");
    }),
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

  test("completeReferences rides the frame only when the caller asks", async () => {
    const sender = createCapturingSender();
    let next = 0;
    const bridge = createChildOutboundMailBridge({
      upstreamSender: sender,
      allocateRequestId: () => `rid-flag-${String(next++)}`,
    });
    const message = {
      to: "recipient@example.com",
      type: "conversation.message" as const,
      content: "reply",
      inReplyTo: "<parent@example.com>",
    };
    void bridge.submit("agent@example.com", message);
    void bridge.submit("agent@example.com", message, {
      completeReferences: true,
    });
    void bridge.submit("agent@example.com", message, {
      completeReferences: false,
    });
    expect(sender.sent[0]?.completeReferences).toBeUndefined();
    expect(sender.sent[1]?.completeReferences).toBe(true);
    expect(sender.sent[2]?.completeReferences).toBeUndefined();
    const flagged = sender.sent[1];
    if (flagged === undefined) throw new Error("flagged frame missing");
    const validated = ControlPayload({
      type: "outbound.message",
      data: flagged,
    });
    expect(validated instanceof type.errors).toBe(false);
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
    await expect(
      transport.watch("INBOX", () => {
        /* never reached */
      }),
    ).rejects.toThrow(/is not wired for unified-host step agent/);
  });

  // Search and watch both forward the mailbox name and surface whatever the
  // supervisor answered. The child does not treat INBOX as special and does
  // not invent a condition of its own.
  test("search and watch surface the supervisor refusal for every mailbox", async () => {
    const callBridge = refusingCallBridge(
      "CANNOT",
      "supervisor refused the mailbox",
    );
    const address = "run_agent@example.com";
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      address,
      makeInbound({ callBridge }),
    );
    const runId = deriveWorkflowRunId(address);
    const mailboxes = ["INBOX", "Drafts", "inbox", "", "INBOX/Sub"];

    for (const mailbox of mailboxes) {
      const searchRefusal = await rejectedCondition(() =>
        transport.search(mailbox, {}),
      );
      const watchRefusal = await rejectedCondition(() =>
        transport.watch(mailbox, () => undefined),
      );
      expect(`${mailbox}: ${String(watchRefusal)}`).toBe(
        `${mailbox}: ${String(searchRefusal)}`,
      );
      expect(searchRefusal).toBe("CANNOT");
    }

    expect(callBridge.submitted).toEqual(
      mailboxes.flatMap((mailbox) => [
        { runId, op: "search", mailbox, query: {} },
        { runId, op: "watch", mailbox },
      ]),
    );
  });

  test("an unwired inbound surface refuses both with the same condition", async () => {
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "agent@example.com",
    );

    const searchRefusal = await rejectedCondition(() =>
      transport.search("INBOX", {}),
    );
    const watchRefusal = await rejectedCondition(() =>
      transport.watch("INBOX", () => undefined),
    );

    expect(searchRefusal).toBe("SERVERBUG");
    expect(watchRefusal).toBe(searchRefusal);
  });

  test("search forwards the query and returns the supervisor's refs", async () => {
    const when = new Date("2024-01-02T03:04:05.000Z");
    const refs = [{ uid: 3, mailbox: "Drafts" }];
    const callBridge = createRecordingCallBridge((call) => {
      if (call.op !== "search") throw new Error(`unexpected ${call.op}`);
      return { ok: true, op: "search", requestId: "mc-search", value: refs };
    });
    const address = "run_agent@example.com";
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      address,
      makeInbound({ callBridge }),
    );

    const found = await transport.search("Drafts", {
      from: "alice",
      before: when,
      and: [{ after: when }],
      or: [{ on: when }],
      not: { sentBefore: when, sentAfter: when, sentOn: when },
    });
    expect(found).toEqual(refs);
    expect(callBridge.submitted).toEqual([
      {
        runId: deriveWorkflowRunId(address),
        op: "search",
        mailbox: "Drafts",
        query: {
          from: "alice",
          before: "2024-01-02T03:04:05.000Z",
          and: [{ after: "2024-01-02T03:04:05.000Z" }],
          or: [{ on: "2024-01-02T03:04:05.000Z" }],
          not: {
            sentBefore: "2024-01-02T03:04:05.000Z",
            sentAfter: "2024-01-02T03:04:05.000Z",
            sentOn: "2024-01-02T03:04:05.000Z",
          },
        },
      },
    ]);
  });

  test("an invalid search date fails before the call is sent", async () => {
    const callBridge = createRecordingCallBridge(() => {
      throw new Error("should not submit");
    });
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "run_agent@example.com",
      makeInbound({ callBridge }),
    );
    await expect(
      transport.search("INBOX", { before: new Date("nope") }),
    ).rejects.toThrow(/Invalid Date/);
    expect(callBridge.submitted).toHaveLength(0);
  });

  test("fetch methods project the supervisor's answer, including part bytes", async () => {
    const headers: MessageHeaders = {
      from: "alice@example.com",
      to: ["agent@example.com"],
      subject: "hello",
      messageId: "<a-1@example.com>",
    };
    const data = new Uint8Array([1, 2, 3, 250]);
    const callBridge = createRecordingCallBridge((call) => {
      if (call.op === "fetchHeaders") {
        return {
          ok: true,
          op: "fetchHeaders",
          requestId: "mc-headers",
          value: headers,
        };
      }
      if (call.op === "fetchFull") {
        return {
          ok: true,
          op: "fetchFull",
          requestId: "mc-full",
          value: {
            ref: { uid: 4, mailbox: "Sent" },
            headers,
            flags: ["\\Seen"],
            signatureStatus: "valid",
            content: "body",
            attachments: [
              {
                name: "f.bin",
                contentType: "application/octet-stream",
                dataBase64: base64Encode(data),
                part: "1.2",
              },
            ],
          },
        };
      }
      if (call.op === "fetchPart") {
        return {
          ok: true,
          op: "fetchPart",
          requestId: "mc-part",
          value: {
            contentType: "application/octet-stream",
            contentBase64: base64Encode(data),
            filename: "f.bin",
            disposition: "attachment",
          },
        };
      }
      throw new Error(`unexpected ${call.op}`);
    });
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "run_agent@example.com",
      makeInbound({ callBridge }),
    );
    const ref = { uid: 4, mailbox: "Sent" };

    expect(await transport.fetchHeaders(ref)).toEqual(headers);

    const full = await transport.fetchFull(ref);
    expect(full.ref).toEqual(ref);
    expect(full.headers).toEqual(headers);
    expect(full.flags).toEqual(["\\Seen"]);
    expect(full.signatureStatus).toBe("valid");
    expect(full.content).toBe("body");
    expect(full.attachments).toEqual([
      {
        name: "f.bin",
        contentType: "application/octet-stream",
        data,
        part: "1.2",
      },
    ]);

    const part = await transport.fetchPart(ref, "1.2");
    expect(part).toEqual({
      contentType: "application/octet-stream",
      content: data,
      filename: "f.bin",
      disposition: "attachment",
    });
    expect(callBridge.submitted.map((call) => call.op)).toEqual([
      "fetchHeaders",
      "fetchFull",
      "fetchPart",
    ]);
  });

  test("getMailboxStatus returns the supervisor's counts for the named mailbox", async () => {
    const status = {
      total: 2,
      unseen: 1,
      recent: 0,
      uidNext: 3,
      uidValidity: 9,
      highestModSeq: 4,
    };
    const callBridge = createRecordingCallBridge((call) => {
      if (call.op !== "getMailboxStatus") throw new Error(call.op);
      return {
        ok: true,
        op: "getMailboxStatus",
        requestId: "mc-status",
        value: status,
      };
    });
    const address = "run_agent@example.com";
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      address,
      makeInbound({ callBridge }),
    );

    expect(await transport.getMailboxStatus("Archive")).toEqual(status);
    expect(callBridge.submitted).toEqual([
      {
        runId: deriveWorkflowRunId(address),
        op: "getMailboxStatus",
        mailbox: "Archive",
      },
    ]);
  });

  test("watch delivers an event that arrives before the supervisor accepts", async () => {
    const watchRegistry = createMailboxWatchRegistry();
    let accept: ((value: MailboxCallSuccess) => void) | undefined;
    const callBridge = createRecordingCallBridge(
      () =>
        new Promise<MailboxCallSuccess>((resolve) => {
          accept = resolve;
        }),
    );
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "run_agent@example.com",
      makeInbound({ watchRegistry, callBridge }),
    );

    const events: MailboxEvent[] = [];
    const pending = transport.watch("INBOX", (event) => {
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

    if (accept === undefined) throw new Error("watch was not submitted");
    accept({ ok: true, op: "watch", requestId: "mc-watch" });
    const unsubscribe = await pending;

    unsubscribe();
    watchRegistry.fire("INBOX", { type: "exists", uid: 2, headers });
    await flushMicrotasks();
    expect(events).toHaveLength(1);
  });

  test("a refused watch leaves nothing registered", async () => {
    const watchRegistry = createMailboxWatchRegistry();
    const callBridge = refusingCallBridge("NONEXISTENT", "no such mailbox");
    const address = "run_agent@example.com";
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      address,
      makeInbound({ watchRegistry, callBridge }),
    );
    const events: MailboxEvent[] = [];
    const headers: MessageHeaders = {
      from: "alice@example.com",
      to: ["agent@example.com"],
      date: "Mon, 01 Jan 2024 00:00:00 +0000",
      messageId: "<a-1@example.com>",
    };

    expect(
      await rejectedCondition(() =>
        transport.watch("Drafts", (event) => {
          events.push(event);
        }),
      ),
    ).toBe("NONEXISTENT");
    watchRegistry.fire("Drafts", { type: "exists", uid: 1, headers });
    await flushMicrotasks();
    expect(events).toHaveLength(0);
    expect(callBridge.submitted).toEqual([
      {
        runId: deriveWorkflowRunId(address),
        op: "watch",
        mailbox: "Drafts",
      },
    ]);
  });

  test("a response whose op does not match the request fails", async () => {
    const callBridge = createRecordingCallBridge(() => ({
      ok: true,
      op: "watch",
      requestId: "mc-mismatch",
    }));
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "run_agent@example.com",
      makeInbound({ callBridge }),
    );
    await expect(transport.search("INBOX", {})).rejects.toThrow(
      /does not match/,
    );
  });

  test("setFlags and clearFlags route flag mutations to the supervisor", async () => {
    const bridge = createRecordingMutationBridge();
    const address = "run_flag-agent@example.com";
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      address,
      makeInbound({ mutationBridge: bridge }),
    );
    const runId = deriveWorkflowRunId(address);

    // The write methods route to the supervisor, including a mailbox the
    // child used to refuse before the supervisor could answer.
    await transport.setFlags({ uid: 4, mailbox: "INBOX" }, ["\\Seen"]);
    await transport.clearFlags({ uid: 4, mailbox: "INBOX" }, ["\\Seen"]);
    await transport.setFlags({ uid: 4, mailbox: "Sent" }, ["\\Seen"]);
    expect(bridge.submitted).toEqual([
      { runId, mailbox: "INBOX", op: "addFlags", uid: 4, flags: ["\\Seen"] },
      { runId, mailbox: "INBOX", op: "removeFlags", uid: 4, flags: ["\\Seen"] },
      { runId, mailbox: "Sent", op: "addFlags", uid: 4, flags: ["\\Seen"] },
    ]);
  });

  test("sync forwards the known state and returns the supervisor result", async () => {
    const canned = {
      vanished: [2],
      changed: [{ uid: 3, flags: ["\\Seen"] }],
      newMessages: [{ uid: 8, mailbox: "Archive" }],
      fullResyncRequired: true,
    };
    const callBridge = createRecordingCallBridge((call) => {
      if (call.op !== "sync") throw new Error(call.op);
      return { ok: true, op: "sync", requestId: "mc-sync", value: canned };
    });
    const address = "run_agent@example.com";
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      address,
      makeInbound({ callBridge }),
    );

    const result = await transport.sync("Archive", {
      uidValidity: 9,
      uidNext: 4,
      highestModSeq: 5,
      knownUids: [1, 2, 3],
    });
    expect(result).toEqual(canned);
    expect(callBridge.submitted).toEqual([
      {
        runId: deriveWorkflowRunId(address),
        op: "sync",
        mailbox: "Archive",
        uidNext: 4,
        uidValidity: 9,
        highestModSeq: 5,
      },
    ]);
  });

  test("expunge routes to the supervisor sweep and surfaces the swept uids", async () => {
    const bridge = createRecordingMutationBridge({ expungedUids: [9, 12] });
    const address = "run_expunge-agent@example.com";
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      address,
      makeInbound({ mutationBridge: bridge }),
    );
    const outcome = await transport.expunge("INBOX");
    await transport.expunge("Sent");
    expect(bridge.submitted).toEqual([
      { runId: deriveWorkflowRunId(address), mailbox: "INBOX", op: "expunge" },
      { runId: deriveWorkflowRunId(address), mailbox: "Sent", op: "expunge" },
    ]);
    // The supervisor's swept uids pass through to the caller.
    expect(outcome).toEqual({ expungedUids: [9, 12] });
  });

  test("expunge surfaces a supervisor refusal", async () => {
    const mutationBridge: ChildMailboxMutationBridge = {
      submit() {
        return Promise.reject(
          new MessageTransportError(
            "NONEXISTENT",
            'unknown mailbox "Sent"; only INBOX is writable',
          ),
        );
      },
      handleResult() {
        /* the refusal is the submit rejection */
      },
      cancelAll() {
        /* nothing pending */
      },
      get pendingCount() {
        return 0;
      },
    };
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      "run_agent@example.com",
      makeInbound({ mutationBridge }),
    );
    expect(await rejectedCondition(() => transport.expunge("Sent"))).toBe(
      "NONEXISTENT",
    );
  });

  test("append and move are forwarded, and an append carrying attachments is not sent", async () => {
    const headers: MessageHeaders = {
      from: "a@example.com",
      to: ["agent@example.com"],
      date: "Mon, 01 Jan 2024 00:00:00 +0000",
      messageId: "<x@example.com>",
    };
    const callBridge = createRecordingCallBridge((call) => {
      if (call.op === "append") {
        return {
          ok: true,
          op: "append",
          requestId: "mc-append",
          value: { uid: 6, mailbox: call.mailbox },
        };
      }
      if (call.op === "move") {
        return { ok: true, op: "move", requestId: "mc-move" };
      }
      throw new Error(call.op);
    });
    const address = "run_agent@example.com";
    const transport = createSupervisorBackedTransport(
      makeBridge(),
      address,
      makeInbound({ callBridge }),
    );
    const runId = deriveWorkflowRunId(address);
    const message = {
      ref: { uid: 1, mailbox: "Archive" },
      headers,
      content: "hello",
      flags: [],
      signatureStatus: "unknown" as const,
    };

    expect(await transport.append("Archive", message, ["\\Seen"])).toEqual({
      uid: 6,
      mailbox: "Archive",
    });
    expect(
      await transport.append("Archive", { ...message, attachments: [] }),
    ).toEqual({
      uid: 6,
      mailbox: "Archive",
    });
    await expect(
      transport.append("Archive", {
        ...message,
        attachments: [
          {
            name: "a.bin",
            contentType: "application/octet-stream",
            data: new Uint8Array([1]),
          },
        ],
      }),
    ).rejects.toThrow(/attachments are not carried/);
    await transport.move({ uid: 1, mailbox: "INBOX" }, "Archive");

    expect(callBridge.submitted).toEqual([
      {
        runId,
        op: "append",
        mailbox: "Archive",
        headers,
        content: "hello",
        flags: ["\\Seen"],
      },
      {
        runId,
        op: "append",
        mailbox: "Archive",
        headers,
        content: "hello",
      },
      {
        runId,
        op: "move",
        ref: { uid: 1, mailbox: "INBOX" },
        toMailbox: "Archive",
      },
    ]);
  });
});
