import { describe, test, expect } from "bun:test";

import type {
  MessageTransport,
  CryptoProvider,
  ContextStore,
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
  ConversationMessage,
  PendingOperation,
  TokenUsage,
  ContextCommit,
  ToolCall,
  ToolResult,
  InferenceEvent,
} from "@interchange/types/runtime";

import { createHarness } from "./harness";
import { buildMessageToolHandlers, buildCombinedRunner } from "./tools";
import { createDefaultPlugin } from "./plugin";
import type { HarnessConfig } from "./config";

// ---------------------------------------------------------------------------
// Mock factory helpers
// ---------------------------------------------------------------------------

function emptyUsage(): TokenUsage {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
}

function makeContextStore(): ContextStore {
  return {
    async load() {
      return {
        messages: [] as ConversationMessage[],
        pendingOperations: [] as PendingOperation[],
        tokenUsage: emptyUsage(),
      };
    },
    async commit(
      _msgs: ConversationMessage[],
      _ops: PendingOperation[],
      _usage: TokenUsage,
      message: string,
    ): Promise<ContextCommit> {
      return { hash: "mock-hash", message, timestamp: Date.now() };
    },
    async branch(): Promise<void> {
      /* noop */
    },
    async log(): Promise<ContextCommit[]> {
      return [];
    },
    async readAt(): Promise<ConversationMessage[]> {
      return [];
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
  return {
    ref: { uid: 1, mailbox: "INBOX" },
    headers: {
      from,
      to: ["agent@local.interchange"],
      date: new Date().toISOString(),
      messageId: `<${Math.random()}@test>`,
      subject: "Test conversation",
    },
    flags: [],
    content: "Hello, agent!",
    signatureStatus: "missing",
  };
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

    const inboundMsg = makeInboundMessage();
    transport.enqueueMessage(inboundMsg.ref, inboundMsg);

    const harness = createHarness(
      makeConfig(transport, { onEvent: (e) => events.push(e) }),
    );
    harness.start();

    // Fire a watch event simulating IMAP IDLE notification.
    transport.fireWatch({
      type: "exists",
      uid: inboundMsg.ref.uid,
      headers: inboundMsg.headers,
    });

    // Reactor should emit message.received after harness fetches.
    await waitForEvent(events, (e) => e.type === "message.received");

    harness.stop();
  });

  test("non-'exists' watch events are ignored", async () => {
    const transport = makeMockTransport();
    const events: InferenceEvent[] = [];

    const harness = createHarness(
      makeConfig(transport, { onEvent: (e) => events.push(e) }),
    );
    harness.start();

    transport.fireWatch({ type: "flagsChanged", uid: 1, flags: ["\\Seen"] });
    transport.fireWatch({ type: "expunged", uid: 1 });

    // Give a brief window for any erroneous delivery to appear.
    await new Promise<void>((r) => setTimeout(r, 50));

    const received = events.filter((e) => e.type === "message.received");
    expect(received.length).toBe(0);

    harness.stop();
  });

  test("deliver() injects a message directly into the reactor", async () => {
    const transport = makeMockTransport();
    const events: InferenceEvent[] = [];

    const harness = createHarness(
      makeConfig(transport, { onEvent: (e) => events.push(e) }),
    );
    harness.start();

    // Wait for reactor to start before delivering.
    await waitForEvent(events, (e) => e.type === "reactor.start");

    const msg = makeInboundMessage();
    harness.deliver(msg);

    await waitForEvent(events, (e) => e.type === "message.received");

    harness.stop();
  });
});

// ---------------------------------------------------------------------------
// 3. Tool name collision detection
// ---------------------------------------------------------------------------

describe("Tool name collision detection", () => {
  test("buildCombinedRunner throws when caller provides a message.* tool", () => {
    const transport = makeMockTransport();
    const messageHandlers = buildMessageToolHandlers(transport);
    const callerTools: ToolRunner = {
      async run(call) {
        return { callId: call.id, content: "ok" };
      },
    };

    expect(() =>
      buildCombinedRunner(messageHandlers, callerTools, ["message.send"]),
    ).toThrow('Tool name collision: "message.send"');
  });

  test("buildCombinedRunner succeeds when no name collisions", () => {
    const transport = makeMockTransport();
    const messageHandlers = buildMessageToolHandlers(transport);
    const callerTools: ToolRunner = {
      async run(call) {
        return { callId: call.id, content: "ok" };
      },
    };

    expect(() =>
      buildCombinedRunner(messageHandlers, callerTools, [
        "read_file",
        "write_file",
      ]),
    ).not.toThrow();
  });

  test("combined runner dispatches message tools to message handlers", async () => {
    const transport = makeMockTransport();
    const messageHandlers = buildMessageToolHandlers(transport);
    const callerTools: ToolRunner = {
      async run(call) {
        return { callId: call.id, content: "caller-result" };
      },
    };

    const runner = buildCombinedRunner(messageHandlers, callerTools, []);

    const call: ToolCall = {
      id: "c1",
      name: "message.send",
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

  test("combined runner dispatches non-message tools to caller runner", async () => {
    const transport = makeMockTransport();
    const messageHandlers = buildMessageToolHandlers(transport);
    const callerTools: ToolRunner = {
      async run(call) {
        return { callId: call.id, content: "caller-handled" };
      },
    };

    const runner = buildCombinedRunner(messageHandlers, callerTools, []);

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
// 4. Message tool: message.send
// ---------------------------------------------------------------------------

describe("message.send tool", () => {
  test("sends a conversation message and returns messageId", async () => {
    const transport = makeMockTransport();
    const handlers = buildMessageToolHandlers(transport);
    const sendHandler = handlers.get("message.send");
    if (sendHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "s1",
      name: "message.send",
      arguments: {
        to: "user@test",
        content: "Hello from agent",
        type: "conversation.message",
      },
    };

    const result = await sendHandler(call, new AbortController().signal);

    expect(result.isError).toBeUndefined();
    const content = result.content as Record<string, unknown>;
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
    const handlers = buildMessageToolHandlers(transport);
    const sendHandler = handlers.get("message.send");
    if (sendHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "s2",
      name: "message.send",
      arguments: { content: "No recipient" },
    };

    const result = await sendHandler(call, new AbortController().signal);
    expect(result.isError).toBe(true);
  });

  test("returns error when both content and payload are provided", async () => {
    const transport = makeMockTransport();
    const handlers = buildMessageToolHandlers(transport);
    const sendHandler = handlers.get("message.send");
    if (sendHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "s3",
      name: "message.send",
      arguments: {
        to: "user@test",
        content: "text",
        payload: { type: "offering.response", version: "1", body: {} },
      },
    };

    const result = await sendHandler(call, new AbortController().signal);
    expect(result.isError).toBe(true);
  });

  test("returns pending marker when correlationId is provided", async () => {
    const transport = makeMockTransport();
    const handlers = buildMessageToolHandlers(transport);
    const sendHandler = handlers.get("message.send");
    if (sendHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "s4",
      name: "message.send",
      arguments: {
        to: "peer@test",
        content: "invoke request",
        type: "offering.request",
        correlationId: "corr-abc123",
      },
    };

    const result = await sendHandler(call, new AbortController().signal);
    expect(result.pendingMarker).toBeDefined();
    if (result.pendingMarker === undefined) throw new Error("unreachable");
    expect(result.pendingMarker.correlationId).toBe("corr-abc123");
    expect(result.pendingMarker.expectedFrom).toBe("peer@test");
  });
});

// ---------------------------------------------------------------------------
// 5. Message tool: message.reply
// ---------------------------------------------------------------------------

describe("message.reply tool", () => {
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

    const handlers = buildMessageToolHandlers(transport);
    const replyHandler = handlers.get("message.reply");
    if (replyHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "r1",
      name: "message.reply",
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
    const handlers = buildMessageToolHandlers(transport);
    const replyHandler = handlers.get("message.reply");
    if (replyHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "r2",
      name: "message.reply",
      arguments: { content: "no ref" },
    };

    const result = await replyHandler(call, new AbortController().signal);
    expect(result.isError).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 6. Message tool: message.search
// ---------------------------------------------------------------------------

describe("message.search tool", () => {
  test("calls transport.search and returns summaries", async () => {
    const transport = makeMockTransport();
    const handlers = buildMessageToolHandlers(transport);
    const searchHandler = handlers.get("message.search");
    if (searchHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "q1",
      name: "message.search",
      arguments: {
        mailbox: "INBOX",
        query: { from: "user@test" },
        limit: 5,
      },
    };

    const result = await searchHandler(call, new AbortController().signal);
    expect(result.isError).toBeUndefined();
    const content = result.content as Record<string, unknown>;
    expect(Array.isArray(content["results"])).toBe(true);
  });

  test("defaults mailbox to INBOX when not specified", async () => {
    const transport = makeMockTransport();
    const handlers = buildMessageToolHandlers(transport);
    const searchHandler = handlers.get("message.search");
    if (searchHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "q2",
      name: "message.search",
      arguments: { query: {} },
    };

    const result = await searchHandler(call, new AbortController().signal);
    expect(result.isError).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// 7. Message tool: message.read
// ---------------------------------------------------------------------------

describe("message.read tool", () => {
  test("fetches full message when parts='full'", async () => {
    const transport = makeMockTransport();
    const ref: MessageRef = { uid: 5, mailbox: "INBOX" };
    const msg = makeInboundMessage();
    const storedMsg = { ...msg, ref };
    transport.enqueueMessage(ref, storedMsg);

    const handlers = buildMessageToolHandlers(transport);
    const readHandler = handlers.get("message.read");
    if (readHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "rd1",
      name: "message.read",
      arguments: { ref, parts: "full" },
    };

    const result = await readHandler(call, new AbortController().signal);
    expect(result.isError).toBeUndefined();
    const content = result.content as Record<string, unknown>;
    expect(content["headers"]).toBeDefined();
    expect(content["signatureStatus"]).toBe("missing");
  });

  test("fetches only headers when parts='headers'", async () => {
    const transport = makeMockTransport();
    const ref: MessageRef = { uid: 6, mailbox: "INBOX" };
    const msg = makeInboundMessage();
    transport.enqueueMessage(ref, { ...msg, ref });

    const handlers = buildMessageToolHandlers(transport);
    const readHandler = handlers.get("message.read");
    if (readHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "rd2",
      name: "message.read",
      arguments: { ref, parts: "headers" },
    };

    const result = await readHandler(call, new AbortController().signal);
    expect(result.isError).toBeUndefined();
    const content = result.content as Record<string, unknown>;
    expect(content["headers"]).toBeDefined();
  });

  test("returns error when ref is missing", async () => {
    const transport = makeMockTransport();
    const handlers = buildMessageToolHandlers(transport);
    const readHandler = handlers.get("message.read");
    if (readHandler === undefined) throw new Error("handler not found");

    const call: ToolCall = {
      id: "rd3",
      name: "message.read",
      arguments: { parts: "full" },
    };

    const result = await readHandler(call, new AbortController().signal);
    expect(result.isError).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 8. Default plugin decision logic
// ---------------------------------------------------------------------------

describe("Default plugin", () => {
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
      emit(eventType: `custom.${string}`, data: Record<string, unknown>) {
        this.calls.push({ type: "emit", args: [eventType, data] });
        return { type: "emit" as const, eventType, data };
      },
      checkpoint() {
        this.calls.push({ type: "checkpoint", args: [] });
        return { type: "checkpoint" as const };
      },
      done() {
        this.calls.push({ type: "done", args: [] });
        return { type: "done" as const };
      },
    };
  }

  function makeState(): import("@interchange/types/runtime").ReactorState {
    return {
      messages: [],
      activeForks: [],
      pendingOperations: [],
      activeGates: [],
      tokenUsage: emptyUsage(),
      sessionId: "test-session",
    };
  }

  test("message.received triggers infer action", async () => {
    const plugin = createDefaultPlugin("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const event: import("@interchange/types/runtime").ReactorInboundEvent = {
      type: "message.received",
      message: makeInboundMessage(),
    };

    const actions = await plugin.decide(event, state, caps);

    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "infer")).toBe(true);
  });

  test("inference.done with tool calls triggers execute_tools", async () => {
    const plugin = createDefaultPlugin("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const event: import("@interchange/types/runtime").ReactorInboundEvent = {
      type: "inference.done",
      message: {
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
      },
      usage: emptyUsage(),
    };

    const actions = await plugin.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "execute_tools")).toBe(true);
  });

  test("inference.done without tool calls returns done", async () => {
    const plugin = createDefaultPlugin("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    // First deliver a message so pendingReplyTo is set.
    const receiveEvent: import("@interchange/types/runtime").ReactorInboundEvent =
      {
        type: "message.received",
        message: makeInboundMessage("user@test"),
      };
    await plugin.decide(receiveEvent, state, caps);

    const doneEvent: import("@interchange/types/runtime").ReactorInboundEvent =
      {
        type: "inference.done",
        message: {
          role: "assistant",
          model: "claude-test",
          content: [{ type: "text", text: "Here is my response." }],
        },
        usage: emptyUsage(),
      };

    const actions = await plugin.decide(doneEvent, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];

    // Should send reply via executeTools; done comes after tool.done.
    expect(normalized.some((a) => a.type === "execute_tools")).toBe(true);
    expect(normalized.some((a) => a.type === "done")).toBe(false);
  });

  test("tool.done after reply returns done (not re-infer)", async () => {
    const plugin = createDefaultPlugin("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    // 1. Receive message to set pendingReplyTo.
    const receiveEvent: import("@interchange/types/runtime").ReactorInboundEvent =
      {
        type: "message.received",
        message: makeInboundMessage("user@test"),
      };
    await plugin.decide(receiveEvent, state, caps);

    // 2. Inference produces text (no tools) — plugin dispatches message.send.
    const doneEvent: import("@interchange/types/runtime").ReactorInboundEvent =
      {
        type: "inference.done",
        message: {
          role: "assistant",
          model: "claude-test",
          content: [{ type: "text", text: "My reply." }],
        },
        usage: emptyUsage(),
      };
    await plugin.decide(doneEvent, state, caps);

    // 3. tool.done for the reply — should wait (empty), not re-infer or done.
    const toolDone: import("@interchange/types/runtime").ReactorInboundEvent = {
      type: "tool.done",
      result: { callId: "harness-reply", content: "sent" },
    };
    const actions = await plugin.decide(toolDone, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized).toEqual([]);
    expect(normalized.some((a) => a.type === "infer")).toBe(false);
    expect(normalized.some((a) => a.type === "done")).toBe(false);
  });

  test("tool.done triggers re-infer", async () => {
    const plugin = createDefaultPlugin("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const event: import("@interchange/types/runtime").ReactorInboundEvent = {
      type: "tool.done",
      result: { callId: "tc1", content: "file contents" },
    };

    const actions = await plugin.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "infer")).toBe(true);
  });

  test("inference.error returns done (not crash)", async () => {
    const plugin = createDefaultPlugin("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const event: import("@interchange/types/runtime").ReactorInboundEvent = {
      type: "inference.error",
      error: {
        category: "retryable",
        message: "rate limited",
      },
      partial: { text: "" },
    };

    const actions = await plugin.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "done")).toBe(true);
  });

  test("abort returns done", async () => {
    const plugin = createDefaultPlugin("claude-test", "You are helpful.");
    const caps = makeCapabilities();
    const state = makeState();

    const event: import("@interchange/types/runtime").ReactorInboundEvent = {
      type: "abort",
      reason: "user_disconnect",
    };

    const actions = await plugin.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "done")).toBe(true);
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
});
