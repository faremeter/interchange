import { describe, test, expect } from "bun:test";

import type {
  MessageTransport,
  CryptoProvider,
  ContextStore,
  AuditStore,
  ToolRunner,
  InboundMessage,
  OutboundMessage,
  SendReceipt,
  MessageHeaders,
  MessageRef,
  Mailbox,
  MailboxStatus,
  SearchQuery,
  Thread,
  BodyStructure,
  MessagePart,
  SyncState,
  SyncResult,
  ListInfo,
  MailboxEvent,
  Unsubscribe,
  ConversationTurn,
  PendingOperation,
  TokenUsage,
  ContextCommit,
  ToolCall,
  ToolResult,
  InferenceEvent,
} from "@interchange/types/runtime";
import type { AuditRecord, ErrorRecord } from "@interchange/types/audit";
import type { AuthzCallResult } from "@interchange/inference";
import { createInboundMessage } from "@interchange/mime";
import type {
  ReactorInboundEvent,
  ReactorDirector,
  ReactorState,
  ReactorCapabilities,
} from "@interchange/types/runtime";

import { createHarness } from "./harness";
import { buildMailToolHandlers, buildCombinedRunner } from "./tools";
import { createDefaultDirector } from "./director";
import type { HarnessConfig } from "./config";

// ---------------------------------------------------------------------------
// Mock factory helpers
// ---------------------------------------------------------------------------

function emptyUsage(): TokenUsage {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
}

function makeContextStore(
  opts: { blobs?: Map<string, Uint8Array> } = {},
): ContextStore {
  const blobs = opts.blobs;

  function commit(
    options: { message: string },
    signal?: AbortSignal,
  ): Promise<ContextCommit>;
  function commit(
    turns: ConversationTurn[],
    pendingOperations: PendingOperation[],
    tokenUsage: TokenUsage,
    message: string,
    signal?: AbortSignal,
  ): Promise<ContextCommit>;
  async function commit(
    first: { message: string } | ConversationTurn[],
    _second?: PendingOperation[] | AbortSignal,
    _third?: TokenUsage,
    fourth?: string,
  ): Promise<ContextCommit> {
    const message = Array.isArray(first) ? (fourth ?? "") : first.message;
    return { hash: "mock-hash", message, timestamp: Date.now() };
  }

  return {
    async load() {
      const turns: ConversationTurn[] = [];
      const pendingOperations: PendingOperation[] = [];
      return {
        turns,
        pendingOperations,
        tokenUsage: emptyUsage(),
        connectorState: null,
      };
    },
    setConnectorState() {
      /* noop */
    },
    commit,
    async branch(): Promise<void> {
      /* noop */
    },
    async log(): Promise<ContextCommit[]> {
      return [];
    },
    async readAt(): Promise<ConversationTurn[]> {
      return [];
    },
    async writeBlob(key, bytes) {
      if (blobs === undefined) {
        throw new Error("not implemented");
      }
      blobs.set(key, bytes);
    },
    async readBlob(key) {
      if (blobs === undefined) {
        throw new Error("not implemented");
      }
      const bytes = blobs.get(key);
      if (bytes === undefined) {
        throw new Error(`Blob not found for key: ${key}`);
      }
      return bytes;
    },
    async writePrompt() {
      /* noop */
    },
    async writeResponse() {
      /* noop */
    },
    async writeManifest() {
      /* noop */
    },
    async writeTurns() {
      /* noop */
    },
    async writeMetadata() {
      /* noop */
    },
    async readManifestHistory() {
      throw new Error("not implemented");
    },
  };
}

function makeCrypto(): CryptoProvider {
  const key = new Uint8Array(32);
  return {
    async sign(_content: Uint8Array): Promise<Uint8Array> {
      return new Uint8Array(64);
    },
    async verify(
      _content: Uint8Array,
      _signature: Uint8Array,
      _publicKey: Uint8Array,
    ): Promise<boolean> {
      return true;
    },
    getPublicKey(): Uint8Array {
      return key;
    },
  };
}

function makeToolRunner(): ToolRunner {
  return {
    async run(call: ToolCall): Promise<ToolResult> {
      return { callId: call.id, content: "mock-result" };
    },
  };
}

type WatchCallback = (event: MailboxEvent) => void;

type MockTransport = MessageTransport & {
  getSentMessages(): OutboundMessage[];
  fireWatch(event: MailboxEvent): void;
  getWatchCallbacks(): WatchCallback[];
  enqueueMessage(ref: MessageRef, msg: InboundMessage): void;
};

function makeMockTransport(): MockTransport {
  const sentMessages: OutboundMessage[] = [];
  const watchCallbacks: WatchCallback[] = [];
  const messageStore = new Map<string, InboundMessage>();

  function refKey(ref: MessageRef): string {
    return `${ref.mailbox}:${ref.uid}`;
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
    getWatchCallbacks() {
      return watchCallbacks;
    },
    enqueueMessage(ref: MessageRef, msg: InboundMessage): void {
      messageStore.set(refKey(ref), msg);
    },

    // MessageTransport implementation
    async send(message: OutboundMessage): Promise<SendReceipt> {
      sentMessages.push(message);
      return { messageId: `<msg-${Date.now()}@test>`, status: "delivered" };
    },

    async append(
      _mailbox: string,
      message: InboundMessage,
    ): Promise<MessageRef> {
      const ref = { uid: 999, mailbox: _mailbox };
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
      return [];
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
        messageId: `<${ref.uid}@test>`,
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
          messageId: `<${ref.uid}@test>`,
        },
        flags: [],
        content: "hello",
        signatureStatus: "missing",
      };
    },

    async setFlags(): Promise<void> {
      /* noop */
    },

    async clearFlags(): Promise<void> {
      /* noop */
    },

    async move(): Promise<void> {
      /* noop */
    },

    async copy(): Promise<void> {
      /* noop */
    },

    async expunge(): Promise<void> {
      /* noop */
    },

    watch(
      _mailbox: string,
      callback: (event: MailboxEvent) => void,
    ): Unsubscribe {
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

    async createList(_address: string, name: string): Promise<ListInfo> {
      return {
        address: _address,
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
  return createInboundMessage({
    from,
    to: "agent@local.interchange",
    subject: "Test conversation",
    content: "Hello, agent!",
  });
}

function makeConfig(
  transport: MockTransport,
  overrides: Partial<HarnessConfig> = {},
): HarnessConfig {
  return {
    address: "agent@local.interchange",
    systemPrompt: "You are a helpful agent.",
    provider: {
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      apiKey: "test-key",
      model: "claude-test",
    },
    transport,
    crypto: makeCrypto(),
    storage: makeContextStore(),
    tools: makeToolRunner(),
    onEvent: () => {
      /* noop */
    },
    ...overrides,
  };
}

function waitForEvent(
  events: InferenceEvent[],
  predicate: (e: InferenceEvent) => boolean,
  timeoutMs = 2000,
): Promise<InferenceEvent> {
  return new Promise((resolve, reject) => {
    const deadline = setTimeout(
      () => reject(new Error("Timed out waiting for event")),
      timeoutMs,
    );

    function check() {
      const found = events.find(predicate);
      if (found !== undefined) {
        clearTimeout(deadline);
        resolve(found);
        return;
      }
      setTimeout(check, 10);
    }
    check();
  });
}

// ---------------------------------------------------------------------------
// 1. Lifecycle: start and stop
// ---------------------------------------------------------------------------

describe("Harness lifecycle", () => {
  test("start registers a watch callback on INBOX", () => {
    const transport = makeMockTransport();
    const harness = createHarness(makeConfig(transport));

    expect(transport.getWatchCallbacks().length).toBe(0);

    harness.start();
    expect(transport.getWatchCallbacks().length).toBe(1);

    harness.stop();
  });

  test("stop unsubscribes the watch callback", async () => {
    const transport = makeMockTransport();
    const events: InferenceEvent[] = [];
    const harness = createHarness(
      makeConfig(transport, { onEvent: (e) => events.push(e) }),
    );

    harness.start();
    expect(transport.getWatchCallbacks().length).toBe(1);

    harness.stop();
    expect(transport.getWatchCallbacks().length).toBe(0);

    // Reactor should receive abort signal and emit reactor.done eventually.
    await waitForEvent(events, (e) => e.type === "reactor.done");
  });

  test("start throws if called twice", () => {
    const transport = makeMockTransport();
    const harness = createHarness(makeConfig(transport));
    harness.start();

    expect(() => harness.start()).toThrow("already started");

    harness.stop();
  });
});

// ---------------------------------------------------------------------------
// 2. Message delivery pipeline
// ---------------------------------------------------------------------------

describe("Message delivery pipeline", () => {
  test("watch 'exists' event causes harness to fetch and deliver message to reactor", async () => {
    const transport = makeMockTransport();
    const events: InferenceEvent[] = [];
    let deliveredCount = 0;

    // Director that signals delivery by returning done() on message.received.
    const director: ReactorDirector = {
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          deliveredCount++;
          return caps.done();
        }
        return caps.wait();
      },
    };

    const inboundMsg = makeInboundMessage();
    transport.enqueueMessage(inboundMsg.ref, inboundMsg);

    const harness = createHarness(
      makeConfig(transport, { onEvent: (e) => events.push(e), director }),
    );
    harness.start();

    // Fire a watch event simulating IMAP IDLE notification.
    transport.fireWatch({
      type: "exists",
      uid: inboundMsg.ref.uid,
      headers: inboundMsg.headers,
    });

    // reactor.done signals the director received the message.
    await waitForEvent(events, (e) => e.type === "reactor.done");
    expect(deliveredCount).toBe(1);

    harness.stop();
  });

  test("non-'exists' watch events are ignored", async () => {
    const transport = makeMockTransport();
    let deliveredCount = 0;

    // Director that counts message.received deliveries.
    const director: ReactorDirector = {
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          deliveredCount++;
          return caps.done();
        }
        return caps.wait();
      },
    };

    const harness = createHarness(makeConfig(transport, { director }));
    harness.start();

    transport.fireWatch({ type: "flagsChanged", uid: 1, flags: ["\\Seen"] });
    transport.fireWatch({ type: "expunged", uid: 1 });

    // Give a brief window for any erroneous delivery to appear.
    await new Promise<void>((r) => setTimeout(r, 50));

    expect(deliveredCount).toBe(0);

    harness.stop();
  });

  test("deliver() injects a message directly into the reactor", async () => {
    const transport = makeMockTransport();
    const events: InferenceEvent[] = [];
    let deliveredCount = 0;

    const director: ReactorDirector = {
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          deliveredCount++;
          return caps.done();
        }
        return caps.wait();
      },
    };

    const harness = createHarness(
      makeConfig(transport, { onEvent: (e) => events.push(e), director }),
    );
    harness.start();

    // Wait for reactor to start before delivering.
    await waitForEvent(events, (e) => e.type === "reactor.start");

    const msg = makeInboundMessage();
    harness.deliver(msg);

    // reactor.done signals the director received the message.
    await waitForEvent(events, (e) => e.type === "reactor.done");
    expect(deliveredCount).toBe(1);

    harness.stop();
  });
});

// ---------------------------------------------------------------------------
// 3. Tool name collision detection
// ---------------------------------------------------------------------------

describe("Tool name collision detection", () => {
  test("buildCombinedRunner throws when caller provides a mail_* tool", () => {
    const transport = makeMockTransport();
    const mailHandlers = buildMailToolHandlers(transport);
    const callerTools: ToolRunner = {
      async run(call) {
        return { callId: call.id, content: "ok" };
      },
    };

    expect(() =>
      buildCombinedRunner(mailHandlers, callerTools, [
        { name: "mail_send", description: "test", inputSchema: {} },
      ]),
    ).toThrow('Tool name collision: "mail_send"');
  });

  test("buildCombinedRunner succeeds when no name collisions", () => {
    const transport = makeMockTransport();
    const mailHandlers = buildMailToolHandlers(transport);
    const callerTools: ToolRunner = {
      async run(call) {
        return { callId: call.id, content: "ok" };
      },
    };

    expect(() =>
      buildCombinedRunner(mailHandlers, callerTools, [
        { name: "read_file", description: "test", inputSchema: {} },
        { name: "write_file", description: "test", inputSchema: {} },
      ]),
    ).not.toThrow();
  });

  test("combined runner dispatches mail tools to mail handlers", async () => {
    const transport = makeMockTransport();
    const mailHandlers = buildMailToolHandlers(transport);
    const callerTools: ToolRunner = {
      async run(call) {
        return { callId: call.id, content: "caller-result" };
      },
    };

    const runner = buildCombinedRunner(mailHandlers, callerTools, []);

    const call: ToolCall = {
      id: "c1",
      name: "mail_send",
      arguments: {
        to: "user@test",
        content: "hello",
        type: "conversation.message",
      },
    };

    const result = await runner.run(call, new AbortController().signal);
    expect(result.callId).toBe("c1");
    expect(result.isError).toBeUndefined();

    // Transport should have received the message.
    expect(transport.getSentMessages().length).toBe(1);
  });

  test("combined runner dispatches non-mail tools to caller runner", async () => {
    const transport = makeMockTransport();
    const mailHandlers = buildMailToolHandlers(transport);
    const callerTools: ToolRunner = {
      async run(call) {
        return { callId: call.id, content: "caller-handled" };
      },
    };

    const runner = buildCombinedRunner(mailHandlers, callerTools, []);

    const call: ToolCall = {
      id: "c2",
      name: "read_file",
      arguments: { path: "/tmp/test.txt" },
    };

    const result = await runner.run(call, new AbortController().signal);
    expect(result.content).toBe("caller-handled");
  });
});

// ---------------------------------------------------------------------------
// 4. Mail tool: mail_send
// ---------------------------------------------------------------------------

describe("mail_send tool", () => {
  test("sends a conversation message and returns messageId", async () => {
    const transport = makeMockTransport();
    const handlers = buildMailToolHandlers(transport);
    const sendHandler = handlers.get("mail_send");
    if (sendHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "s1",
      name: "mail_send",
      arguments: {
        to: "user@test",
        content: "Hello from agent",
        type: "conversation.message",
      },
    };

    const result = await sendHandler(call, new AbortController().signal);

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    const content = result.content;
    expect(typeof content["messageId"]).toBe("string");

    expect(transport.getSentMessages().length).toBe(1);
    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.to).toBe("user@test");
    expect(sent.content).toBe("Hello from agent");
    expect(sent.type).toBe("conversation.message");
  });

  test("returns error when 'to' is missing", async () => {
    const transport = makeMockTransport();
    const handlers = buildMailToolHandlers(transport);
    const sendHandler = handlers.get("mail_send");
    if (sendHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "s2",
      name: "mail_send",
      arguments: { content: "No recipient" },
    };

    const result = await sendHandler(call, new AbortController().signal);
    expect(result.isError).toBe(true);
  });

  test("returns error when both content and payload are provided", async () => {
    const transport = makeMockTransport();
    const handlers = buildMailToolHandlers(transport);
    const sendHandler = handlers.get("mail_send");
    if (sendHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "s3",
      name: "mail_send",
      arguments: {
        to: "user@test",
        content: "text",
        payload: { type: "offering.response", version: "1", body: {} },
      },
    };

    const result = await sendHandler(call, new AbortController().signal);
    expect(result.isError).toBe(true);
  });

  test("returns result without pending marker when no correlationId", async () => {
    const transport = makeMockTransport();
    const handlers = buildMailToolHandlers(transport);
    const sendHandler = handlers.get("mail_send");
    if (sendHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "s4",
      name: "mail_send",
      arguments: {
        to: "peer@test",
        content: "invoke request",
        type: "offering.request",
      },
    };

    const result = await sendHandler(call, new AbortController().signal);
    expect(result.pendingMarker).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// 5. Mail tool: mail_reply
// ---------------------------------------------------------------------------

describe("mail_reply tool", () => {
  test("fetches parent headers and sends reply with inReplyTo", async () => {
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 10, mailbox: "INBOX" };
    const parentMsg: InboundMessage = {
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
    };
    transport.enqueueMessage(parentRef, parentMsg);

    const handlers = buildMailToolHandlers(transport);
    const replyHandler = handlers.get("mail_reply");
    if (replyHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "r1",
      name: "mail_reply",
      arguments: {
        ref: parentRef,
        content: "This is the reply",
      },
    };

    const result = await replyHandler(call, new AbortController().signal);

    expect(result.isError).toBeUndefined();
    expect(transport.getSentMessages().length).toBe(1);

    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.to).toBe("user@test");
    expect(sent.inReplyTo).toBe("<parent@test>");
    expect(sent.subject).toBe("Original subject");
    expect(sent.content).toBe("This is the reply");
  });

  test("returns error when ref is missing", async () => {
    const transport = makeMockTransport();
    const handlers = buildMailToolHandlers(transport);
    const replyHandler = handlers.get("mail_reply");
    if (replyHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "r2",
      name: "mail_reply",
      arguments: { content: "no ref" },
    };

    const result = await replyHandler(call, new AbortController().signal);
    expect(result.isError).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 6. Mail tool: mail_search
// ---------------------------------------------------------------------------

describe("mail_search tool", () => {
  test("calls transport.search and returns summaries", async () => {
    const transport = makeMockTransport();
    const handlers = buildMailToolHandlers(transport);
    const searchHandler = handlers.get("mail_search");
    if (searchHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "q1",
      name: "mail_search",
      arguments: {
        mailbox: "INBOX",
        query: { from: "user@test" },
        limit: 5,
      },
    };

    const result = await searchHandler(call, new AbortController().signal);
    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    const content = result.content;
    expect(Array.isArray(content["results"])).toBe(true);
  });

  test("defaults mailbox to INBOX when not specified", async () => {
    const transport = makeMockTransport();
    const handlers = buildMailToolHandlers(transport);
    const searchHandler = handlers.get("mail_search");
    if (searchHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "q2",
      name: "mail_search",
      arguments: { query: {} },
    };

    const result = await searchHandler(call, new AbortController().signal);
    expect(result.isError).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// 7. Mail tool: mail_read
// ---------------------------------------------------------------------------

describe("mail_read tool", () => {
  test("fetches full message when parts='full'", async () => {
    const transport = makeMockTransport();
    const ref: MessageRef = { uid: 5, mailbox: "INBOX" };
    const msg = makeInboundMessage();
    const storedMsg = { ...msg, ref };
    transport.enqueueMessage(ref, storedMsg);

    const handlers = buildMailToolHandlers(transport);
    const readHandler = handlers.get("mail_read");
    if (readHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "rd1",
      name: "mail_read",
      arguments: { ref, parts: "full" },
    };

    const result = await readHandler(call, new AbortController().signal);
    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    const content = result.content;
    expect(content["headers"]).toBeDefined();
    expect(content["signatureStatus"]).toBe("missing");
  });

  test("fetches only headers when parts='headers'", async () => {
    const transport = makeMockTransport();
    const ref: MessageRef = { uid: 6, mailbox: "INBOX" };
    const msg = makeInboundMessage();
    transport.enqueueMessage(ref, { ...msg, ref });

    const handlers = buildMailToolHandlers(transport);
    const readHandler = handlers.get("mail_read");
    if (readHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "rd2",
      name: "mail_read",
      arguments: { ref, parts: "headers" },
    };

    const result = await readHandler(call, new AbortController().signal);
    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    const content = result.content;
    expect(content["headers"]).toBeDefined();
  });

  test("returns error when ref is missing", async () => {
    const transport = makeMockTransport();
    const handlers = buildMailToolHandlers(transport);
    const readHandler = handlers.get("mail_read");
    if (readHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "rd3",
      name: "mail_read",
      arguments: { parts: "full" },
    };

    const result = await readHandler(call, new AbortController().signal);
    expect(result.isError).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 8. Default director decision logic
// ---------------------------------------------------------------------------

describe("Default director", () => {
  function makeCapabilities() {
    return {
      calls: [] as { type: string; args: unknown[] }[],
      infer(model: string, options?: unknown) {
        this.calls.push({ type: "infer", args: [model, options] });
        return { type: "infer" as const, model };
      },
      executeTools(calls: ToolCall[], parallel?: boolean) {
        this.calls.push({ type: "execute_tools", args: [calls, parallel] });
        return {
          type: "execute_tools" as const,
          calls,
          parallel: parallel ?? true,
        };
      },
      suspend(gate: {
        type: import("@interchange/types/runtime").GateType;
        gateId: string;
        timeoutMs: number;
        correlationId?: string;
      }) {
        this.calls.push({ type: "suspend", args: [gate] });
        return { type: "suspend" as const, gate };
      },
      fork(
        mode: import("@interchange/types/runtime").ForkMode,
        forkId: string,
      ) {
        this.calls.push({ type: "fork", args: [mode, forkId] });
        return { type: "fork" as const, mode, forkId };
      },
      reply(content: string) {
        this.calls.push({ type: "reply", args: [content] });
        return { type: "reply" as const, content };
      },
      emit(eventType: `custom.${string}`, data: Record<string, unknown>) {
        this.calls.push({ type: "emit", args: [eventType, data] });
        return { type: "emit" as const, eventType, data };
      },
      checkpoint(message?: string) {
        this.calls.push({ type: "checkpoint", args: [message] });
        return {
          type: "checkpoint" as const,
          message: message ?? "checkpoint",
        };
      },
      compact(compactor: string, reason: string) {
        this.calls.push({ type: "compact", args: [compactor, reason] });
        return { type: "compact" as const, compactor, reason };
      },
      wait() {
        this.calls.push({ type: "wait", args: [] });
        return { type: "wait" as const };
      },
      done() {
        this.calls.push({ type: "done", args: [] });
        return { type: "done" as const };
      },
    };
  }

  function makeState(): import("@interchange/types/runtime").ReactorState {
    return {
      turns: [],
      activeForks: [],
      pendingOperations: [],
      activeGates: [],
      tokenUsage: emptyUsage(),
      lastCycleUsage: null,
      sessionId: "test-session",
    };
  }

  test("message.received triggers infer action", async () => {
    const director = createDefaultDirector("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "message.received",
      message: makeInboundMessage(),
    };

    const actions = await director.decide(event, state, caps);

    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "infer")).toBe(true);
  });

  test("inference.done with tool calls triggers checkpoint and execute_tools", async () => {
    const director = createDefaultDirector("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "claude-test",
        content: [
          {
            type: "tool_call",
            id: "tc1",
            name: "read_file",
            arguments: { path: "/test" },
          },
        ],
        timestamp: 1000,
      },
      usage: emptyUsage(),
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "execute_tools")).toBe(true);
  });

  test("inference.done without tool calls returns checkpoint and reply", async () => {
    const director = createDefaultDirector("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const doneEvent: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "claude-test",
        content: [{ type: "text", text: "Here is my response." }],
        timestamp: 1000,
      },
      usage: emptyUsage(),
    };

    const actions = await director.decide(doneEvent, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];

    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "reply")).toBe(true);
    const replyAction = normalized.find((a) => a.type === "reply");
    if (replyAction === undefined || replyAction.type !== "reply")
      throw new Error("unreachable");
    expect(replyAction.content).toBe("Here is my response.");
  });

  test("tool.done triggers checkpoint and re-infer", async () => {
    const director = createDefaultDirector("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "tool.done",
      result: { callId: "tc1", content: "file contents" },
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "infer")).toBe(true);
  });

  test("inference.error returns checkpoint and reply with error message", async () => {
    const director = createDefaultDirector("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "inference.error",
      error: {
        category: "credential_failure",
        message: "invalid API key",
        statusCode: 401,
      },
      partial: { text: "" },
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);

    const replyAction = normalized.find((a) => a.type === "reply");
    expect(replyAction).toBeDefined();
    const content =
      replyAction?.type === "reply" ? replyAction.content : undefined;
    expect(content).toContain("credential error");
    expect(content).toContain("invalid API key");
  });

  test("abort returns done", async () => {
    const director = createDefaultDirector("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "abort",
      reason: "user_disconnect",
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "done")).toBe(true);
  });

  test("inference.done with empty content returns checkpoint and wait", async () => {
    const director = createDefaultDirector("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "claude-test",
        content: [],
        timestamp: 1000,
      },
      usage: emptyUsage(),
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "wait")).toBe(true);
    expect(normalized.some((a) => a.type === "done")).toBe(false);
    expect(normalized.some((a) => a.type === "reply")).toBe(false);
  });

  test("inference.done with whitespace-only text returns checkpoint and wait", async () => {
    const director = createDefaultDirector("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "claude-test",
        content: [{ type: "text", text: "   \n\t  " }],
        timestamp: 1000,
      },
      usage: emptyUsage(),
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "wait")).toBe(true);
    expect(normalized.some((a) => a.type === "done")).toBe(false);
    expect(normalized.some((a) => a.type === "reply")).toBe(false);
  });

  test("reactive mode inference.done returns checkpoint and wait", async () => {
    const director = createDefaultDirector(
      "claude-test",
      "You are helpful.",
      [],
      {
        mode: "reactive",
      },
    );
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "claude-test",
        content: [{ type: "text", text: "done processing" }],
        timestamp: 1000,
      },
      usage: emptyUsage(),
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "wait")).toBe(true);
  });

  test("reactive mode tool.done returns checkpoint and wait", async () => {
    const director = createDefaultDirector(
      "claude-test",
      "You are helpful.",
      [],
      {
        mode: "reactive",
      },
    );
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "tool.done",
      result: { callId: "tc1", content: "result" },
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "wait")).toBe(true);
    expect(normalized.some((a) => a.type === "infer")).toBe(false);
  });

  test("tool.done batching waits for all results before checkpoint", async () => {
    const director = createDefaultDirector("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    // First trigger inference.done with 2 tool calls to set pendingToolResults.
    const inferDone: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "claude-test",
        content: [
          {
            type: "tool_call",
            id: "tc1",
            name: "read_file",
            arguments: { path: "/a" },
          },
          {
            type: "tool_call",
            id: "tc2",
            name: "read_file",
            arguments: { path: "/b" },
          },
        ],
        timestamp: 1000,
      },
      usage: emptyUsage(),
    };
    await director.decide(inferDone, state, caps);

    // First tool.done — should return empty (still waiting for tc2).
    const toolDone1: ReactorInboundEvent = {
      type: "tool.done",
      result: { callId: "tc1", content: "result1" },
    };
    const actions1 = await director.decide(toolDone1, state, caps);
    const normalized1 = Array.isArray(actions1) ? actions1 : [actions1];
    expect(normalized1).toEqual([]);

    // Second tool.done — all results in, should checkpoint + infer.
    const toolDone2: ReactorInboundEvent = {
      type: "tool.done",
      result: { callId: "tc2", content: "result2" },
    };
    const actions2 = await director.decide(toolDone2, state, caps);
    const normalized2 = Array.isArray(actions2) ? actions2 : [actions2];
    expect(normalized2.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized2.some((a) => a.type === "infer")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 9. Config validation
// ---------------------------------------------------------------------------

describe("Config validation", () => {
  test("throws when address is empty", () => {
    const transport = makeMockTransport();
    expect(() => createHarness(makeConfig(transport, { address: "" }))).toThrow(
      "address",
    );
  });

  test("throws when systemPrompt is empty", () => {
    const transport = makeMockTransport();
    expect(() =>
      createHarness(makeConfig(transport, { systemPrompt: "" })),
    ).toThrow("systemPrompt");
  });

  test("throws when auditStore is provided without authorize", () => {
    const transport = makeMockTransport();
    const auditStore: AuditStore = {
      async commitAudit() {
        /* noop */
      },
      async loadAudit() {
        return [];
      },
      async commitErrors() {
        /* noop */
      },
    };
    expect(() => createHarness(makeConfig(transport, { auditStore }))).toThrow(
      "authorize is required when auditStore is provided",
    );
  });

  test("accepts auditStore with authorize", () => {
    const transport = makeMockTransport();
    const auditStore: AuditStore = {
      async commitAudit() {
        /* noop */
      },
      async loadAudit() {
        return [];
      },
      async commitErrors() {
        /* noop */
      },
    };
    const authorize = async (): Promise<AuthzCallResult> => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    });
    expect(() =>
      createHarness(makeConfig(transport, { auditStore, authorize })),
    ).not.toThrow();
  });

  test("accepts authorize without auditStore", () => {
    const transport = makeMockTransport();
    const authorize = async (): Promise<AuthzCallResult> => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    });
    expect(() =>
      createHarness(makeConfig(transport, { authorize })),
    ).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// 10. Audit integration
// ---------------------------------------------------------------------------

describe("Audit integration", () => {
  function makeAuditStore(): AuditStore & {
    getCommitted(): AuditRecord[][];
  } {
    const committed: AuditRecord[][] = [];
    return {
      async commitAudit(records: AuditRecord[]) {
        committed.push([...records]);
      },
      async loadAudit() {
        return committed.flat();
      },
      async commitErrors() {
        /* noop */
      },
      getCommitted() {
        return committed;
      },
    };
  }

  function allowAll(): Promise<AuthzCallResult> {
    return Promise.resolve({
      effect: "allow" as const,
      matchingGrants: [],
      resolvedBy: null,
    });
  }

  function denyAll(): Promise<AuthzCallResult> {
    return Promise.resolve({
      effect: "deny" as const,
      matchingGrants: [],
      resolvedBy: null,
    });
  }

  // A director that executes a single tool call on message.received,
  // then checkpoints and shuts down on tool.done. This exercises
  // the full audit pipeline without needing a real LLM.
  function makeToolExecDirector(
    toolName: string,
    toolArgs: Record<string, unknown>,
  ): ReactorDirector {
    return {
      async decide(
        event: { type: string },
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          return caps.executeTools([
            { id: `call-${toolName}`, name: toolName, arguments: toolArgs },
          ]);
        }
        if (event.type === "tool.done") {
          return [caps.checkpoint(), caps.done()];
        }
        return caps.done();
      },
    };
  }

  function waitForDone(events: InferenceEvent[]): Promise<void> {
    return new Promise((resolve, reject) => {
      const deadline = setTimeout(
        () => reject(new Error("Timed out waiting for reactor.done")),
        5000,
      );
      const check = () => {
        if (events.some((e) => e.type === "reactor.done")) {
          clearTimeout(deadline);
          resolve();
          return;
        }
        setTimeout(check, 10);
      };
      check();
    });
  }

  test("allowed tool call produces audit record with authz and result", async () => {
    const transport = makeMockTransport();
    const auditStore = makeAuditStore();
    const events: InferenceEvent[] = [];

    const harness = createHarness(
      makeConfig(transport, {
        auditStore,
        authorize: () => allowAll(),
        onEvent: (e) => events.push(e),
        director: makeToolExecDirector("test_tool", { key: "value" }),
      }),
    );

    harness.start();
    harness.deliver(makeInboundMessage());
    await waitForDone(events);

    const records = auditStore.getCommitted().flat();
    expect(records.length).toBe(1);

    const record = records[0];
    if (record === undefined) throw new Error("expected record");

    expect(record.callId).toBe("call-test_tool");
    expect(record.tool).toBe("test_tool");
    expect(record.arguments).toEqual({ key: "value" });
    expect(record.authz).not.toBeNull();
    if (record.authz === null) throw new Error("expected authz");
    expect(record.authz.effect).toBe("allow");
    expect(record.authz.blocked).toBe(false);
    expect(record.result.content).toBe("mock-result");
    expect(record.result.isError).toBe(false);
    expect(record.sessionId).toBeDefined();
    // seq comes from the reactor's tool.done event; verify it matches.
    const toolDoneEvent = events.find(
      (e) =>
        e.type === "tool.done" &&
        e.data.result.callId === "call-test_tool" &&
        !e.data.result.isError,
    );
    if (toolDoneEvent === undefined)
      throw new Error("expected tool.done event");
    expect(record.seq).toBe(toolDoneEvent.seq);
  });

  test("blocked tool call produces audit record with denied authz", async () => {
    const transport = makeMockTransport();
    const auditStore = makeAuditStore();
    const events: InferenceEvent[] = [];

    const harness = createHarness(
      makeConfig(transport, {
        auditStore,
        authorize: () => denyAll(),
        onEvent: (e) => events.push(e),
        director: makeToolExecDirector("secret_tool", { path: "/etc/shadow" }),
      }),
    );

    harness.start();
    harness.deliver(makeInboundMessage());
    await waitForDone(events);

    const records = auditStore.getCommitted().flat();
    expect(records.length).toBe(1);

    const record = records[0];
    if (record === undefined) throw new Error("expected record");

    expect(record.callId).toBe("call-secret_tool");
    expect(record.tool).toBe("secret_tool");
    // Blocked calls never see tool.start, so arguments are not captured.
    expect(record.arguments).toEqual({});
    expect(record.authz).not.toBeNull();
    if (record.authz === null) throw new Error("expected authz");
    expect(record.authz.effect).toBe("deny");
    expect(record.authz.blocked).toBe(true);
    expect(record.authz.blockReason).toBeDefined();
    expect(record.result.isError).toBe(true);
  });

  test("audit records are flushed at shutdown for unflushed records", async () => {
    const transport = makeMockTransport();
    const auditStore = makeAuditStore();
    const events: InferenceEvent[] = [];

    // Director that executes tools but does NOT checkpoint before done.
    // Records should still be flushed via onShutdown.
    const director: ReactorDirector = {
      async decide(
        event: { type: string },
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          return caps.executeTools([
            { id: "call-1", name: "test_tool", arguments: {} },
          ]);
        }
        if (event.type === "tool.done") {
          return caps.done();
        }
        return caps.done();
      },
    };

    const harness = createHarness(
      makeConfig(transport, {
        auditStore,
        authorize: () => allowAll(),
        onEvent: (e) => events.push(e),
        director,
      }),
    );

    harness.start();
    harness.deliver(makeInboundMessage());
    await waitForDone(events);

    // Records should have been flushed via onShutdown (no checkpoint).
    const batches = auditStore.getCommitted();
    expect(batches.length).toBe(1);
    expect(batches[0]?.length).toBe(1);
    expect(batches[0]?.[0]?.callId).toBe("call-1");
  });

  test("checkpoint then shutdown does not double-commit audit records", async () => {
    const transport = makeMockTransport();
    const auditStore = makeAuditStore();
    const events: InferenceEvent[] = [];

    // Director checkpoints before done — both afterCheckpoint and onShutdown
    // fire. The second flush should be a no-op.
    const harness = createHarness(
      makeConfig(transport, {
        auditStore,
        authorize: () => allowAll(),
        onEvent: (e) => events.push(e),
        director: makeToolExecDirector("test_tool", { x: 1 }),
      }),
    );

    harness.start();
    harness.deliver(makeInboundMessage());
    await waitForDone(events);

    // commitAudit should be called exactly once (at checkpoint).
    // The onShutdown flush finds an empty buffer and skips.
    const batches = auditStore.getCommitted();
    expect(batches.length).toBe(1);
    expect(batches[0]?.length).toBe(1);
  });

  test("authorize throwing produces blocked audit record", async () => {
    const transport = makeMockTransport();
    const auditStore = makeAuditStore();
    const events: InferenceEvent[] = [];

    const harness = createHarness(
      makeConfig(transport, {
        auditStore,
        authorize: () => {
          throw new Error("authz service unavailable");
        },
        onEvent: (e) => events.push(e),
        director: makeToolExecDirector("risky_tool", { cmd: "rm -rf /" }),
      }),
    );

    harness.start();
    harness.deliver(makeInboundMessage());
    await waitForDone(events);

    const records = auditStore.getCommitted().flat();
    expect(records.length).toBe(1);

    const record = records[0];
    if (record === undefined) throw new Error("expected record");

    expect(record.tool).toBe("risky_tool");
    expect(record.authz).not.toBeNull();
    if (record.authz === null) throw new Error("expected authz");
    expect(record.authz.blocked).toBe(true);
    expect(record.authz.effect).toBeNull();
    expect(record.result.isError).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 11. Error flushing
// ---------------------------------------------------------------------------

describe("Error flushing", () => {
  function makeErrorAuditStore(): AuditStore & {
    getCommittedErrors(): ErrorRecord[][];
  } {
    const committedErrors: ErrorRecord[][] = [];
    return {
      async commitAudit() {
        /* noop */
      },
      async loadAudit() {
        return [];
      },
      async commitErrors(records: ErrorRecord[]) {
        committedErrors.push([...records]);
      },
      getCommittedErrors() {
        return committedErrors;
      },
    };
  }

  function allowAll(): Promise<AuthzCallResult> {
    return Promise.resolve({
      effect: "allow" as const,
      matchingGrants: [],
      resolvedBy: null,
    });
  }

  function waitForDone(events: InferenceEvent[]): Promise<void> {
    return new Promise((resolve, reject) => {
      const deadline = setTimeout(
        () => reject(new Error("Timed out waiting for reactor.done")),
        5000,
      );
      const check = () => {
        if (events.some((e) => e.type === "reactor.done")) {
          clearTimeout(deadline);
          resolve();
          return;
        }
        setTimeout(check, 10);
      };
      check();
    });
  }

  test("inference.error events are accumulated and flushed at checkpoint", async () => {
    const transport = makeMockTransport();
    const auditStore = makeErrorAuditStore();
    const events: InferenceEvent[] = [];

    // Director that triggers inference (which will fail due to invalid provider
    // URL) and then checkpoints + completes on inference.error.
    const director: ReactorDirector = {
      async decide(
        event: { type: string },
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          return caps.infer("claude-test");
        }
        if (event.type === "inference.error") {
          return [caps.checkpoint("after-error"), caps.done()];
        }
        return caps.done();
      },
    };

    // Use an unreachable URL so inference fails immediately with a network
    // error, causing the reactor to emit inference.error.
    const harness = createHarness(
      makeConfig(transport, {
        provider: {
          provider: "anthropic",
          baseURL: "http://localhost:1",
          apiKey: "test-key",
          model: "claude-test",
        },
        auditStore,
        authorize: () => allowAll(),
        onEvent: (e) => events.push(e),
        director,
      }),
    );

    harness.start();
    harness.deliver(makeInboundMessage());
    await waitForDone(events);

    const batches = auditStore.getCommittedErrors();
    expect(batches.length).toBe(1);
    const record = batches[0]?.[0];
    if (record === undefined) throw new Error("expected error record");
    expect(record.source).toBe("inference");
    expect(record.category).toBeDefined();
    expect(record.message).toBeDefined();
    expect(record.fatal).toBe(false);
    expect(record.sessionId).toBeDefined();
  });

  test("reactor.error (fatal) events are accumulated and flushed", async () => {
    const transport = makeMockTransport();
    const auditStore = makeErrorAuditStore();
    const events: InferenceEvent[] = [];

    // Director that throws on message.received, causing a fatal reactor.error.
    const director: ReactorDirector = {
      async decide(event: { type: string }, _state: ReactorState) {
        if (event.type === "message.received") {
          throw new Error("director explosion");
        }
        return { type: "done" as const };
      },
    };

    const harness = createHarness(
      makeConfig(transport, {
        auditStore,
        authorize: () => allowAll(),
        onEvent: (e) => events.push(e),
        director,
      }),
    );

    harness.start();
    harness.deliver(makeInboundMessage());
    await waitForDone(events);

    const batches = auditStore.getCommittedErrors();
    expect(batches.length).toBe(1);
    const record = batches[0]?.[0];
    if (record === undefined) throw new Error("expected error record");
    expect(record.source).toBe("reactor");
    expect(record.category).toBe("reactor_error");
    expect(record.fatal).toBe(true);
    expect(record.message).toContain("director explosion");
    expect(record.sessionId).toBeDefined();
  });

  test("no commitErrors call when no errors occurred", async () => {
    const transport = makeMockTransport();
    const auditStore = makeErrorAuditStore();
    const events: InferenceEvent[] = [];

    // Director that completes without errors.
    const director: ReactorDirector = {
      async decide(
        event: { type: string },
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          return [caps.checkpoint(), caps.done()];
        }
        return caps.done();
      },
    };

    const harness = createHarness(
      makeConfig(transport, {
        auditStore,
        authorize: () => allowAll(),
        onEvent: (e) => events.push(e),
        director,
      }),
    );

    harness.start();
    harness.deliver(makeInboundMessage());
    await waitForDone(events);

    expect(auditStore.getCommittedErrors().length).toBe(0);
  });

  test("non-fatal reactor.error events are recorded in the error audit trail", async () => {
    const transport = makeMockTransport();
    const auditStore = makeErrorAuditStore();
    const events: InferenceEvent[] = [];

    // Director that triggers a checkpoint (which succeeds) then completes.
    // The reactor emits a non-fatal reactor.error for afterCheckpoint
    // hook failures, but we can simulate by using a director that causes
    // inference (which fails) and then checkpoints + completes.
    const director: ReactorDirector = {
      async decide(
        event: { type: string },
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          return caps.infer("claude-test");
        }
        if (event.type === "inference.error") {
          return [caps.checkpoint("after-error"), caps.done()];
        }
        return caps.done();
      },
    };

    const harness = createHarness(
      makeConfig(transport, {
        provider: {
          provider: "anthropic",
          baseURL: "http://localhost:1",
          apiKey: "test-key",
          model: "claude-test",
        },
        auditStore,
        authorize: () => allowAll(),
        onEvent: (e) => events.push(e),
        director,
      }),
    );

    harness.start();
    harness.deliver(makeInboundMessage());
    await waitForDone(events);

    // The inference.error should be recorded regardless of fatal status.
    const allRecords = auditStore.getCommittedErrors().flat();
    const inferenceErrors = allRecords.filter((r) => r.source === "inference");
    expect(inferenceErrors.length).toBeGreaterThanOrEqual(1);
    const record = inferenceErrors[0];
    if (record === undefined) throw new Error("expected inference error");
    expect(record.fatal).toBe(false);
  });

  test("errors survive a commitErrors failure", async () => {
    const transport = makeMockTransport();
    const committedErrors: ErrorRecord[][] = [];
    let shouldFail = true;
    const auditStore: AuditStore & { getCommittedErrors(): ErrorRecord[][] } = {
      async commitAudit() {
        /* noop */
      },
      async loadAudit() {
        return [];
      },
      async commitErrors(records: ErrorRecord[]) {
        if (shouldFail) {
          shouldFail = false;
          throw new Error("simulated storage failure");
        }
        committedErrors.push([...records]);
      },
      getCommittedErrors() {
        return committedErrors;
      },
    };
    const events: InferenceEvent[] = [];

    // Director that triggers inference (fails due to bad URL), then
    // checkpoints (commitErrors throws on first call), then completes
    // (commitErrors succeeds on shutdown flush with the retained records).
    const director: ReactorDirector = {
      async decide(
        event: { type: string },
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          return caps.infer("claude-test");
        }
        if (event.type === "inference.error") {
          return [caps.checkpoint("will-fail"), caps.done()];
        }
        return caps.done();
      },
    };

    const harness = createHarness(
      makeConfig(transport, {
        provider: {
          provider: "anthropic",
          baseURL: "http://localhost:1",
          apiKey: "test-key",
          model: "claude-test",
        },
        auditStore,
        authorize: () => allowAll(),
        onEvent: (e) => events.push(e),
        director,
      }),
    );

    harness.start();
    harness.deliver(makeInboundMessage());
    await waitForDone(events);

    // The first flush failed but the records should have been retained
    // and flushed on shutdown.
    expect(committedErrors.length).toBe(1);
    const record = committedErrors[0]?.[0];
    if (record === undefined) throw new Error("expected error record");
    expect(record.source).toBe("inference");
  });

  test("errors are flushed at shutdown when no checkpoint occurred", async () => {
    const transport = makeMockTransport();
    const auditStore = makeErrorAuditStore();
    const events: InferenceEvent[] = [];

    // Director that throws — reactor.error is emitted and then shutdown
    // happens (no explicit checkpoint). Errors must be flushed via
    // onShutdown.
    const director: ReactorDirector = {
      async decide(event: { type: string }, _state: ReactorState) {
        if (event.type === "message.received") {
          throw new Error("shutdown flush test");
        }
        return { type: "done" as const };
      },
    };

    const harness = createHarness(
      makeConfig(transport, {
        auditStore,
        authorize: () => allowAll(),
        onEvent: (e) => events.push(e),
        director,
      }),
    );

    harness.start();
    harness.deliver(makeInboundMessage());
    await waitForDone(events);

    const batches = auditStore.getCommittedErrors();
    expect(batches.length).toBe(1);
    expect(batches[0]?.[0]?.source).toBe("reactor");
  });
});

// ---------------------------------------------------------------------------
// 8. BlobReader
// ---------------------------------------------------------------------------

describe("Harness blobReader", () => {
  test("resolves a tool-output URI through the wrapped context store", async () => {
    const blobs = new Map<string, Uint8Array>();
    blobs.set("abc123", new TextEncoder().encode("spilled bytes"));

    const transport = makeMockTransport();
    const harness = createHarness(
      makeConfig(transport, {
        storage: makeContextStore({ blobs }),
      }),
    );

    const bytes = await harness.blobReader.read("tool-output:///abc123");
    expect(new TextDecoder().decode(bytes)).toBe("spilled bytes");
  });

  test("throws when the underlying store has no matching blob", async () => {
    const blobs = new Map<string, Uint8Array>();
    const transport = makeMockTransport();
    const harness = createHarness(
      makeConfig(transport, {
        storage: makeContextStore({ blobs }),
      }),
    );

    let thrown: Error | undefined;
    try {
      await harness.blobReader.read("tool-output:///missing");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("Blob not found");
  });

  test("throws on malformed tool-output URIs without reading from the store", async () => {
    let readCount = 0;
    const blobs = new Map<string, Uint8Array>();
    const wrapped = makeContextStore({ blobs });
    const originalReadBlob = wrapped.readBlob.bind(wrapped);
    wrapped.readBlob = async (key, signal) => {
      readCount++;
      return originalReadBlob(key, signal);
    };

    const transport = makeMockTransport();
    const harness = createHarness(makeConfig(transport, { storage: wrapped }));

    let thrown: Error | undefined;
    try {
      await harness.blobReader.read("file:///abc");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("invalid tool-output URI scheme");
    expect(readCount).toBe(0);
  });
});
