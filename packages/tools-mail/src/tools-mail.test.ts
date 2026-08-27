import { describe, test, expect } from "bun:test";
import type {
  BodyStructure,
  InboundMessage,
  Mailbox,
  MailboxEvent,
  MailboxStatus,
  MessageHeaders,
  MessagePart,
  MessageRef,
  MessageTransport,
  OutboundMessage,
  SearchQuery,
  SendReceipt,
  SyncResult,
  SyncState,
  Thread,
  ListInfo,
  ToolCall,
  Unsubscribe,
} from "@intx/types/runtime";
import {
  createRuntimeCapabilities,
  type RuntimeCapabilities,
} from "@intx/types/runtime-capabilities";

import { createMailTools } from "./index";
import {
  makeMailExpungeHandler,
  makeMailFlagHandler,
  makeMailReadHandler,
  makeMailReplyHandler,
  makeMailSearchHandler,
  makeMailSendHandler,
  makeMailWaitHandler,
} from "./handlers";

// ---------------------------------------------------------------------------
// Mock transport — minimal MessageTransport with hooks for sent-message
// inspection, watch firing, and message enqueueing.
// ---------------------------------------------------------------------------

type WatchCallback = (event: MailboxEvent) => void;

type MockTransport = MessageTransport & {
  getSentMessages(): OutboundMessage[];
  fireWatch(event: MailboxEvent): void;
  enqueueMessage(ref: MessageRef, msg: InboundMessage): void;
  setSearchResult(refs: MessageRef[]): void;
  getFlagCalls(): { op: "set" | "clear"; ref: MessageRef; flags: string[] }[];
  getExpungeCalls(): string[];
  setExpungeResult(uids: number[]): void;
  failMutations(err: Error | null): void;
};

function makeMockTransport(): MockTransport {
  const sentMessages: OutboundMessage[] = [];
  const watchCallbacks: WatchCallback[] = [];
  const messageStore = new Map<string, InboundMessage>();
  let searchResult: MessageRef[] = [];
  const flagCalls: { op: "set" | "clear"; ref: MessageRef; flags: string[] }[] =
    [];
  const expungeCalls: string[] = [];
  let expungeUids: number[] = [];
  let mutationError: Error | null = null;

  function refKey(ref: MessageRef): string {
    return `${ref.mailbox}:${String(ref.uid)}`;
  }

  const transport: MockTransport = {
    getSentMessages() {
      return sentMessages;
    },
    fireWatch(event: MailboxEvent): void {
      for (const cb of watchCallbacks) {
        cb(event);
      }
    },
    enqueueMessage(ref: MessageRef, msg: InboundMessage): void {
      messageStore.set(refKey(ref), msg);
    },
    setSearchResult(refs: MessageRef[]): void {
      searchResult = refs;
    },
    getFlagCalls() {
      return flagCalls;
    },
    getExpungeCalls() {
      return expungeCalls;
    },
    setExpungeResult(uids: number[]): void {
      expungeUids = uids;
    },
    failMutations(err: Error | null): void {
      mutationError = err;
    },

    async send(message: OutboundMessage): Promise<SendReceipt> {
      sentMessages.push(message);
      return {
        messageId: `<msg-${String(Date.now())}@test>`,
        status: "delivered",
      };
    },

    async append(
      mailbox: string,
      message: InboundMessage,
    ): Promise<MessageRef> {
      const ref = { uid: 999, mailbox };
      messageStore.set(refKey(ref), message);
      return ref;
    },

    async listMailboxes(): Promise<Mailbox[]> {
      return [{ name: "INBOX", role: "\\Inbox" }];
    },

    async createMailbox(name: string): Promise<Mailbox> {
      return { name };
    },

    async deleteMailbox(): Promise<void> {
      /* noop */
    },

    async getMailboxStatus(): Promise<MailboxStatus> {
      return {
        total: 0,
        unseen: 0,
        recent: 0,
        uidNext: 1,
        uidValidity: 1,
        highestModSeq: 0,
      };
    },

    async search(_mailbox: string, _query: SearchQuery): Promise<MessageRef[]> {
      return searchResult;
    },

    async thread(): Promise<Thread[]> {
      return [];
    },

    async fetchHeaders(ref: MessageRef): Promise<MessageHeaders> {
      const msg = messageStore.get(refKey(ref));
      if (msg !== undefined) return msg.headers;
      return {
        from: "sender@test",
        to: ["agent@test"],
        date: new Date().toISOString(),
        messageId: `<${String(ref.uid)}@test>`,
      };
    },

    async fetchStructure(): Promise<BodyStructure> {
      return { contentType: "multipart/signed" };
    },

    async fetchPart(): Promise<MessagePart> {
      return { contentType: "text/plain", content: new Uint8Array() };
    },

    async fetchFull(ref: MessageRef): Promise<InboundMessage> {
      const stored = messageStore.get(refKey(ref));
      if (stored !== undefined) return stored;
      return {
        ref,
        headers: {
          from: "sender@test",
          to: ["agent@test"],
          date: new Date().toISOString(),
          messageId: `<${String(ref.uid)}@test>`,
        },
        flags: [],
        content: "hello",
        signatureStatus: "missing",
      };
    },

    async setFlags(ref: MessageRef, flags: string[]): Promise<void> {
      if (mutationError !== null) throw mutationError;
      flagCalls.push({ op: "set", ref, flags });
    },

    async clearFlags(ref: MessageRef, flags: string[]): Promise<void> {
      if (mutationError !== null) throw mutationError;
      flagCalls.push({ op: "clear", ref, flags });
    },

    async move(): Promise<void> {
      /* noop */
    },

    async copy(): Promise<void> {
      /* noop */
    },

    async expunge(mailbox: string): Promise<{ expungedUids: number[] }> {
      if (mutationError !== null) throw mutationError;
      expungeCalls.push(mailbox);
      return { expungedUids: expungeUids };
    },

    watch(_mailbox: string, callback: WatchCallback): Unsubscribe {
      watchCallbacks.push(callback);
      return () => {
        const idx = watchCallbacks.indexOf(callback);
        if (idx !== -1) watchCallbacks.splice(idx, 1);
      };
    },

    async sync(_mailbox: string, _state: SyncState): Promise<SyncResult> {
      return {
        vanished: [],
        changed: [],
        newMessages: [],
        fullResyncRequired: false,
      };
    },

    async createList(address: string, name: string): Promise<ListInfo> {
      return {
        address,
        name,
        memberCount: 0,
        createdAt: new Date().toISOString(),
      };
    },

    async listMembers(): Promise<string[]> {
      return [];
    },

    async subscribe(): Promise<void> {
      /* noop */
    },

    async unsubscribe(): Promise<void> {
      /* noop */
    },
  };

  return transport;
}

function makeInboundMessage(from = "user@test"): InboundMessage {
  return {
    ref: { uid: 0, mailbox: "INBOX" },
    headers: {
      from,
      to: ["agent@local.interchange"],
      date: new Date().toISOString(),
      messageId: `<inbound-${String(Date.now())}@test>`,
      subject: "Test conversation",
    },
    flags: [],
    content: "Hello, agent!",
    signatureStatus: "missing",
  };
}

function makeCapabilities(transport: MessageTransport): RuntimeCapabilities {
  return createRuntimeCapabilities({ "mail.transport": transport });
}

const signal = AbortSignal.timeout(5000);

// ---------------------------------------------------------------------------
// createMailTools factory surface
// ---------------------------------------------------------------------------

describe("createMailTools", () => {
  test("definitions include all mail tools in registered order", () => {
    const tools = createMailTools({
      capabilities: makeCapabilities(makeMockTransport()),
    });

    expect(tools.definitions.map((d) => d.name)).toEqual([
      "mail_send",
      "mail_reply",
      "mail_search",
      "mail_read",
      "mail_wait",
      "mail_flag",
      "mail_expunge",
    ]);
  });

  test("run dispatches each registered tool name", async () => {
    const transport = makeMockTransport();
    const tools = createMailTools({
      capabilities: makeCapabilities(transport),
    });

    const result = await tools.run(
      {
        id: "c1",
        name: "mail_send",
        arguments: { to: "user@test", content: "hi" },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    expect(transport.getSentMessages().length).toBe(1);
  });

  test("run returns Unknown tool error for an unregistered name", async () => {
    const tools = createMailTools({
      capabilities: makeCapabilities(makeMockTransport()),
    });

    const result = await tools.run(
      { id: "c1", name: "not_a_mail_tool", arguments: {} },
      signal,
    );

    expect(result.callId).toBe("c1");
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["error"]).toBe(`Unknown tool: "not_a_mail_tool"`);
  });

  test("run wraps an error thrown from a handler path lacking its own try/catch", async () => {
    // mail_wait calls transport.search outside a per-handler try/catch
    // (only the inner watch path is guarded). A throw from
    // transport.search therefore reaches createMailTools.run, where the
    // top-level wrapper turns it into an isError result.
    const transport = makeMockTransport();
    transport.search = async () => {
      throw new Error("synthetic search failure");
    };

    const tools = createMailTools({
      capabilities: makeCapabilities(transport),
    });

    const result = await tools.run(
      { id: "c1", name: "mail_wait", arguments: { query: {} } },
      signal,
    );

    expect(result.callId).toBe("c1");
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["error"]).toBe("synthetic search failure");
  });

  test("a resolver that throws on mail.transport propagates from createMailTools", () => {
    const capabilities = createRuntimeCapabilities({});

    expect(() => createMailTools({ capabilities })).toThrow(
      /"mail\.transport".*not provided by the host/,
    );
  });

  test("dispose is idempotent", async () => {
    const tools = createMailTools({
      capabilities: makeCapabilities(makeMockTransport()),
    });

    await tools.dispose();
    await tools.dispose();
    // No throw; reaching here is the assertion.
    expect(true).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// mail_send handler
// ---------------------------------------------------------------------------

describe("mail_send handler", () => {
  test("sends a conversation message and returns messageId", async () => {
    const transport = makeMockTransport();
    const handler = makeMailSendHandler(transport);

    const call: ToolCall = {
      id: "s1",
      name: "mail_send",
      arguments: {
        to: "user@test",
        content: "Hello from agent",
        type: "conversation.message",
      },
    };

    const result = await handler(call, signal);

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(typeof result.content["messageId"]).toBe("string");

    expect(transport.getSentMessages().length).toBe(1);
    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.to).toBe("user@test");
    expect(sent.content).toBe("Hello from agent");
    expect(sent.type).toBe("conversation.message");
  });

  test("returns error when 'to' is missing", async () => {
    const handler = makeMailSendHandler(makeMockTransport());

    const result = await handler(
      { id: "s2", name: "mail_send", arguments: { content: "No recipient" } },
      signal,
    );

    expect(result.isError).toBe(true);
  });

  test("returns error when both content and payload are provided", async () => {
    const handler = makeMailSendHandler(makeMockTransport());

    const result = await handler(
      {
        id: "s3",
        name: "mail_send",
        arguments: {
          to: "user@test",
          content: "text",
          payload: { type: "offering.response", version: "1", body: {} },
        },
      },
      signal,
    );

    expect(result.isError).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// mail_reply handler
// ---------------------------------------------------------------------------

describe("mail_reply handler", () => {
  test("fetches parent headers and sends reply with inReplyTo", async () => {
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 10, mailbox: "INBOX" };
    transport.enqueueMessage(parentRef, {
      ref: parentRef,
      headers: {
        from: "user@test",
        to: ["agent@local"],
        date: new Date().toISOString(),
        messageId: "<parent@test>",
        subject: "Original subject",
      },
      flags: [],
      content: "original message",
      signatureStatus: "missing",
    });

    const handler = makeMailReplyHandler(transport);
    const result = await handler(
      {
        id: "r1",
        name: "mail_reply",
        arguments: { ref: parentRef, content: "This is the reply" },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    expect(transport.getSentMessages().length).toBe(1);

    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.to).toBe("user@test");
    expect(sent.inReplyTo).toBe("<parent@test>");
    expect(sent.subject).toBe("Original subject");
    expect(sent.content).toBe("This is the reply");
  });

  test("builds the full References chain from the parent's ancestry", async () => {
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 11, mailbox: "INBOX" };
    transport.enqueueMessage(parentRef, {
      ref: parentRef,
      headers: {
        from: "user@test",
        to: ["agent@local"],
        date: new Date().toISOString(),
        messageId: "<parent@test>",
        references: ["<root@test>", "<mid@test>"],
        subject: "Re: Original subject",
      },
      flags: [],
      content: "original message",
      signatureStatus: "missing",
    });

    const handler = makeMailReplyHandler(transport);
    const result = await handler(
      {
        id: "r3",
        name: "mail_reply",
        arguments: { ref: parentRef, content: "threaded reply" },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.inReplyTo).toBe("<parent@test>");
    // The full ancestry: the parent's own References plus the parent's own
    // Message-Id, in order.
    expect(sent.references).toEqual([
      "<root@test>",
      "<mid@test>",
      "<parent@test>",
    ]);
  });

  test("references falls back to just the parent when it has no ancestry", async () => {
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 12, mailbox: "INBOX" };
    transport.enqueueMessage(parentRef, {
      ref: parentRef,
      headers: {
        from: "user@test",
        to: ["agent@local"],
        date: new Date().toISOString(),
        messageId: "<lonely@test>",
      },
      flags: [],
      content: "thread opener",
      signatureStatus: "missing",
    });

    const handler = makeMailReplyHandler(transport);
    await handler(
      {
        id: "r4",
        name: "mail_reply",
        arguments: { ref: parentRef, content: "first reply" },
      },
      signal,
    );

    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.references).toEqual(["<lonely@test>"]);
  });

  test("returns error when ref is missing", async () => {
    const handler = makeMailReplyHandler(makeMockTransport());

    const result = await handler(
      { id: "r2", name: "mail_reply", arguments: { content: "no ref" } },
      signal,
    );

    expect(result.isError).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// mail_search handler
// ---------------------------------------------------------------------------

describe("mail_search handler", () => {
  test("calls transport.search and returns summaries", async () => {
    const handler = makeMailSearchHandler(makeMockTransport());

    const result = await handler(
      {
        id: "q1",
        name: "mail_search",
        arguments: {
          mailbox: "INBOX",
          query: { from: "user@test" },
          limit: 5,
        },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(Array.isArray(result.content["results"])).toBe(true);
  });

  test("defaults mailbox to INBOX when not specified", async () => {
    const handler = makeMailSearchHandler(makeMockTransport());

    const result = await handler(
      { id: "q2", name: "mail_search", arguments: { query: {} } },
      signal,
    );

    expect(result.isError).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// mail_read handler
// ---------------------------------------------------------------------------

describe("mail_read handler", () => {
  test("fetches full message when parts='full'", async () => {
    const transport = makeMockTransport();
    const ref: MessageRef = { uid: 5, mailbox: "INBOX" };
    transport.enqueueMessage(ref, { ...makeInboundMessage(), ref });

    const handler = makeMailReadHandler(transport);
    const result = await handler(
      { id: "rd1", name: "mail_read", arguments: { ref, parts: "full" } },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["headers"]).toBeDefined();
    expect(result.content["signatureStatus"]).toBe("missing");
  });

  test("fetches only headers when parts='headers'", async () => {
    const transport = makeMockTransport();
    const ref: MessageRef = { uid: 6, mailbox: "INBOX" };
    transport.enqueueMessage(ref, { ...makeInboundMessage(), ref });

    const handler = makeMailReadHandler(transport);
    const result = await handler(
      { id: "rd2", name: "mail_read", arguments: { ref, parts: "headers" } },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["headers"]).toBeDefined();
  });

  test("returns error when ref is missing", async () => {
    const handler = makeMailReadHandler(makeMockTransport());

    const result = await handler(
      { id: "rd3", name: "mail_read", arguments: { parts: "full" } },
      signal,
    );

    expect(result.isError).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// mail_wait handler
// ---------------------------------------------------------------------------

describe("mail_wait handler", () => {
  test("returns immediately when initial search yields a match", async () => {
    const transport = makeMockTransport();
    const ref: MessageRef = { uid: 42, mailbox: "INBOX" };
    transport.enqueueMessage(ref, {
      ...makeInboundMessage("alice@test"),
      ref,
    });
    transport.setSearchResult([ref]);

    const handler = makeMailWaitHandler(transport);
    const result = await handler(
      {
        id: "w1",
        name: "mail_wait",
        arguments: { query: { from: "alice@test" } },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["from"]).toBe("alice@test");
    expect(result.content["ref"]).toEqual(ref);
  });
});

describe("mail_flag handler", () => {
  const ref = { uid: 3, mailbox: "INBOX" };

  test("set adds flags via setFlags", async () => {
    const transport = makeMockTransport();
    const handler = makeMailFlagHandler(transport);
    const result = await handler(
      { id: "f1", name: "mail_flag", arguments: { ref, set: ["\\Deleted"] } },
      signal,
    );
    expect(result.isError).toBeUndefined();
    expect(transport.getFlagCalls()).toEqual([
      { op: "set", ref, flags: ["\\Deleted"] },
    ]);
  });

  test("clear removes flags via clearFlags", async () => {
    const transport = makeMockTransport();
    const handler = makeMailFlagHandler(transport);
    await handler(
      { id: "f2", name: "mail_flag", arguments: { ref, clear: ["\\Seen"] } },
      signal,
    );
    expect(transport.getFlagCalls()).toEqual([
      { op: "clear", ref, flags: ["\\Seen"] },
    ]);
  });

  test("rejects a call that mixes set and clear and touches no transport", async () => {
    const transport = makeMockTransport();
    const handler = makeMailFlagHandler(transport);
    const result = await handler(
      {
        id: "f3",
        name: "mail_flag",
        arguments: { ref, set: ["\\Flagged"], clear: ["\\Seen"] },
      },
      signal,
    );
    expect(result.isError).toBe(true);
    // One direction per call keeps each mutation atomic; neither side fires.
    expect(transport.getFlagCalls()).toHaveLength(0);
  });

  test("rejects a call with neither set nor clear and touches no transport", async () => {
    const transport = makeMockTransport();
    const handler = makeMailFlagHandler(transport);
    const result = await handler(
      { id: "f4", name: "mail_flag", arguments: { ref } },
      signal,
    );
    expect(result.isError).toBe(true);
    expect(transport.getFlagCalls()).toHaveLength(0);
  });

  test("surfaces a rejection as 'not applied' with flag_failed", async () => {
    const transport = makeMockTransport();
    transport.failMutations(new Error("supervisor dropped it"));
    const handler = makeMailFlagHandler(transport);
    const result = await handler(
      { id: "f5", name: "mail_flag", arguments: { ref, set: ["\\Deleted"] } },
      signal,
    );
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("flag_failed");
    expect(String(result.content["error"])).toMatch(
      /flag not applied: supervisor dropped it/,
    );
  });
});

describe("mail_expunge handler", () => {
  test("sweeps INBOX and returns the expunged uids", async () => {
    const transport = makeMockTransport();
    transport.setExpungeResult([4, 7]);
    const handler = makeMailExpungeHandler(transport);
    const result = await handler(
      { id: "e1", name: "mail_expunge", arguments: {} },
      signal,
    );
    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["ok"]).toBe(true);
    expect(result.content["expungedUids"]).toEqual([4, 7]);
    expect(transport.getExpungeCalls()).toEqual(["INBOX"]);
  });

  test("surfaces a rejection as 'not applied' with expunge_failed", async () => {
    const transport = makeMockTransport();
    transport.failMutations(new Error("supervisor dropped it"));
    const handler = makeMailExpungeHandler(transport);
    const result = await handler(
      { id: "e2", name: "mail_expunge", arguments: {} },
      signal,
    );
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("expunge_failed");
  });
});
