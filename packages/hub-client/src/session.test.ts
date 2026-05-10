/* eslint-disable @typescript-eslint/no-non-null-assertion -- index after toHaveLength check */
/* eslint-disable @typescript-eslint/no-unsafe-type-assertion -- Transport.fetch<T> is a generic interface method; mock implementations must use `as T` to satisfy the return type contract */
import { beforeEach, describe, expect, test } from "bun:test";

import type { InferenceTurnResponse, MailResponse } from "@interchange/types";

import { createInstanceSession, type InstanceSession } from "./session";
import type { Transport } from "./transport";

const TENANT_ID = "ten_1";
const INSTANCE_ID = "ins_abc123";
const BASE_PATH = `/api/tenants/${TENANT_ID}/agents/instances/${INSTANCE_ID}`;

const AGENT_ADDR = `${INSTANCE_ID}@tenant.example`;
const HUMAN_ADDR = "user@tenant.example";

function noop(): void {
  // intentional no-op for onChange callbacks
}

// Mock transport

type FetchHandler = (method: string, path: string, body?: unknown) => unknown;

function createMockTransport(fetchHandler?: FetchHandler) {
  let subscriber: ((event: unknown) => void) | null = null;

  const transport: Transport = {
    async fetch<T>(method: string, path: string, body?: unknown): Promise<T> {
      if (!fetchHandler) {
        throw new Error(`Unexpected fetch: ${method} ${path}`);
      }
      return fetchHandler(method, path, body) as T;
    },
    subscribe(
      _path: string,
      onEvent: (event: unknown) => void,
      _opts?: { eventName?: string },
    ): () => void {
      subscriber = onEvent;
      return () => {
        subscriber = null;
      };
    },
  };

  return {
    transport,
    emit(event: unknown) {
      subscriber?.(event);
    },
    get hasSubscriber() {
      return subscriber !== null;
    },
  };
}

// Canned response builders

function makeMail(overrides: Partial<MailResponse> = {}): MailResponse {
  return {
    id: "mail_1",
    sessionId: "sess_1",
    instanceId: INSTANCE_ID,
    direction: "inbound",
    status: "delivered",
    receivedAt: "2024-01-01T00:00:00Z",
    from: [{ name: "Alice", email: HUMAN_ADDR }],
    to: [{ name: null, email: AGENT_ADDR }],
    subject: null,
    sentAt: null,
    bodyValues: { p1: { value: "Hello" } },
    textBody: [{ partId: "p1", type: "text/plain" }],
    htmlBody: [],
    attachments: [],
    headers: {},
    ...overrides,
  };
}

function makeTurn(
  overrides: Partial<InferenceTurnResponse> = {},
): InferenceTurnResponse {
  return {
    id: "turn_1",
    sessionId: "sess_1",
    instanceId: INSTANCE_ID,
    model: "gpt-4",
    status: "completed",
    startedAt: "2024-01-01T01:00:00Z",
    endedAt: "2024-01-01T01:00:01Z",
    parts: [
      {
        id: "part_1",
        type: "text",
        content: "Hello from assistant",
        metadata: null,
        ordinal: 0,
      },
    ],
    ...overrides,
  };
}

function makeHydrationHandler(
  mails: MailResponse[] = [],
  turns: InferenceTurnResponse[] = [],
): FetchHandler {
  return (_method, path) => {
    if (path.includes("/mail")) return { data: mails };
    if (path.includes("/turns")) return { data: turns };
    throw new Error(`Unexpected path: ${path}`);
  };
}

// Session lifecycle

describe("session lifecycle", () => {
  test("start opens SSE subscription", async () => {
    const mock = createMockTransport(makeHydrationHandler());
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });

    expect(mock.hasSubscriber).toBe(false);
    const cleanup = session.start();
    expect(mock.hasSubscriber).toBe(true);

    // Allow hydration to complete
    await Promise.resolve();

    cleanup();
  });

  test("double start throws", async () => {
    const mock = createMockTransport(makeHydrationHandler());
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });

    session.start();
    expect(() => session.start()).toThrow(
      "start() called on an already-started session",
    );
  });

  test("destroy closes SSE subscription", async () => {
    const mock = createMockTransport(makeHydrationHandler());
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });

    session.start();
    expect(mock.hasSubscriber).toBe(true);

    session.destroy();
    expect(mock.hasSubscriber).toBe(false);
  });

  test("destroy is idempotent", async () => {
    const mock = createMockTransport(makeHydrationHandler());
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });

    session.start();
    session.destroy();
    // Second destroy must not throw
    expect(() => session.destroy()).not.toThrow();
  });

  test("SSE events after destroy are ignored", async () => {
    let changes = 0;
    const mock = createMockTransport(makeHydrationHandler());
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: () => {
        changes++;
      },
    });

    session.start();
    session.destroy();

    // After destroy the subscriber is cleared, so emit is a no-op.
    mock.emit({ type: "inference.start", seq: 1, data: { model: "gpt-4" } });
    expect(changes).toBe(0);
  });

  test("hydrated starts false and becomes true after hydration", async () => {
    const mock = createMockTransport(
      makeHydrationHandler([makeMail()], [makeTurn()]),
    );
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });

    session.start();
    expect(session.hydrated).toBe(false);

    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(session.hydrated).toBe(true);
  });
});

// Hydration

describe("hydration", () => {
  test("populates events from mail and turns after start", async () => {
    const mail = makeMail({ id: "mail_1", receivedAt: "2024-01-01T00:00:00Z" });
    const turn = makeTurn({
      id: "turn_1",
      startedAt: "2024-01-01T01:00:00Z",
    });

    const mock = createMockTransport(makeHydrationHandler([mail], [turn]));
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });

    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(session.events).toHaveLength(2);
    expect(session.events[0]!.kind).toBe("mail");
    expect(session.events[1]!.kind).toBe("turn");
  });

  test("filters mail through shouldShowMail", async () => {
    // Outbound connector reply — should be suppressed
    const suppressedMail = makeMail({
      id: "mail_suppress",
      direction: "outbound",
      from: [{ name: null, email: AGENT_ADDR }],
      to: [{ name: "Alice", email: HUMAN_ADDR }],
      headers: { "interchange-type": "conversation.message" },
    });
    // Inbound — should be shown
    const visibleMail = makeMail({ id: "mail_visible", direction: "inbound" });

    const mock = createMockTransport(
      makeHydrationHandler([suppressedMail, visibleMail]),
    );
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });

    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(session.events).toHaveLength(1);
    const first = session.events[0]!;
    expect(first.kind).toBe("mail");
    if (first.kind === "mail") {
      expect(first.id).toBe("mail_visible");
    }
  });

  test("skips turns that produce no displayable event", async () => {
    const reasoningOnlyTurn = makeTurn({
      parts: [
        {
          id: "part_1",
          type: "reasoning",
          content: "internal",
          metadata: null,
          ordinal: 0,
        },
      ],
    });

    const mock = createMockTransport(
      makeHydrationHandler([], [reasoningOnlyTurn]),
    );
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });

    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(session.events).toHaveLength(0);
  });

  test("sorts events by timestamp", async () => {
    const earlierMail = makeMail({
      id: "mail_early",
      receivedAt: "2024-01-01T00:00:00Z",
    });
    const laterTurn = makeTurn({
      id: "turn_later",
      startedAt: "2024-01-02T00:00:00Z",
    });
    const betweenMail = makeMail({
      id: "mail_mid",
      receivedAt: "2024-01-01T12:00:00Z",
    });

    const mock = createMockTransport(
      makeHydrationHandler([betweenMail, earlierMail], [laterTurn]),
    );
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });

    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));

    const ids = session.events.map((e) =>
      e.kind === "mail" ? e.id : e.turnId,
    );
    expect(ids).toEqual(["mail_early", "mail_mid", "turn_later"]);
  });

  test("calls onChange when hydration completes", async () => {
    let changeCount = 0;
    const mock = createMockTransport(makeHydrationHandler([makeMail()]));
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: () => {
        changeCount++;
      },
    });

    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(changeCount).toBeGreaterThan(0);
  });

  test("SSE subscription is opened before hydration fetch", () => {
    const fetchOrder: string[] = [];
    let subscribeCalled = false;

    const transport: Transport = {
      async fetch<T>(_method: string, path: string): Promise<T> {
        if (!subscribeCalled) {
          fetchOrder.push("fetch-before-subscribe");
        } else {
          fetchOrder.push("fetch-after-subscribe");
        }
        if (path.includes("/mail")) return { data: [] } as T;
        if (path.includes("/turns")) return { data: [] } as T;
        throw new Error(`Unexpected path: ${path}`);
      },
      subscribe(
        _path: string,
        _onEvent: (event: unknown) => void,
        _opts?: { eventName?: string },
      ): () => void {
        subscribeCalled = true;
        fetchOrder.push("subscribe");
        return noop;
      },
    };

    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport,
      onChange: noop,
    });

    session.start();

    // subscribe must appear before any fetch
    expect(fetchOrder[0]).toBe("subscribe");
    expect(fetchOrder.every((v) => v !== "fetch-before-subscribe")).toBe(true);
  });
});

// SSE buffer deduplication

describe("SSE buffer deduplication", () => {
  test("events buffered during hydration are merged without duplicates", async () => {
    let resolveFetch!: () => void;
    const fetchPending = new Promise<void>((r) => {
      resolveFetch = r;
    });

    const mail = makeMail({ id: "mail_1", receivedAt: "2024-01-01T00:00:00Z" });

    // Use a mutable object so the subscriber reference survives without
    // triggering TypeScript's nullable narrowing on the variable.
    const bus = { emit: noop as (event: unknown) => void };
    const transport: Transport = {
      async fetch<T>(_method: string, path: string): Promise<T> {
        await fetchPending;
        if (path.includes("/mail")) return { data: [mail] } as T;
        if (path.includes("/turns")) return { data: [] } as T;
        throw new Error(`Unexpected: ${path}`);
      },
      subscribe(_path: string, onEvent: (event: unknown) => void): () => void {
        bus.emit = onEvent;
        return () => {
          bus.emit = noop;
        };
      },
    };

    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport,
      onChange: noop,
    });

    session.start();

    // Emit mail.delivered for mail_1 while hydration is in-flight.
    bus.emit({
      type: "mail.delivered",
      data: {
        id: "mail_1",
        direction: "inbound",
        from: [{ name: "Alice", email: HUMAN_ADDR }],
        to: [{ name: null, email: AGENT_ADDR }],
        bodyValues: { p1: { value: "Hello" } },
        textBody: [{ partId: "p1", type: "text/plain" }],
        headers: {},
        receivedAt: "2024-01-01T00:00:00Z",
      },
    });

    // Unblock hydration fetch
    resolveFetch();
    await new Promise((resolve) => setTimeout(resolve, 0));

    // mail_1 should appear exactly once
    const mailEvents = session.events.filter((e) => e.kind === "mail");
    expect(mailEvents).toHaveLength(1);
  });

  test("new SSE events after hydration completes are added directly", async () => {
    const mock = createMockTransport(makeHydrationHandler());
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });

    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(session.hydrated).toBe(true);

    mock.emit({
      type: "mail.delivered",
      data: {
        id: "mail_new",
        direction: "inbound",
        from: [{ name: "Bob", email: HUMAN_ADDR }],
        to: [{ name: null, email: AGENT_ADDR }],
        bodyValues: { p1: { value: "New message" } },
        textBody: [{ partId: "p1", type: "text/plain" }],
        headers: {},
        receivedAt: "2024-01-02T00:00:00Z",
      },
    });

    expect(session.events).toHaveLength(1);
    const first = session.events[0]!;
    expect(first.kind).toBe("mail");
    if (first.kind === "mail") {
      expect(first.id).toBe("mail_new");
    }
  });

  test("duplicate mail.delivered events are ignored", async () => {
    const mock = createMockTransport(makeHydrationHandler());
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });

    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));

    const sseMailEvent = {
      type: "mail.delivered",
      data: {
        id: "mail_dup",
        direction: "inbound",
        from: [{ name: "Alice", email: HUMAN_ADDR }],
        to: [{ name: null, email: AGENT_ADDR }],
        bodyValues: { p1: { value: "Hello" } },
        textBody: [{ partId: "p1", type: "text/plain" }],
        headers: {},
        receivedAt: "2024-01-01T00:00:00Z",
      },
    };

    mock.emit(sseMailEvent);
    mock.emit(sseMailEvent);

    expect(session.events.filter((e) => e.kind === "mail")).toHaveLength(1);
  });
});

// SSE event processing

describe("SSE event processing", () => {
  let session: InstanceSession;
  let mock: ReturnType<typeof createMockTransport>;

  beforeEach(async () => {
    mock = createMockTransport(makeHydrationHandler());
    session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });
    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));
  });

  test("session.ended calls onSessionEnded", () => {
    let ended = false;
    const m2 = createMockTransport(makeHydrationHandler());
    const s2 = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: m2.transport,
      onChange: noop,
      onSessionEnded: () => {
        ended = true;
      },
    });
    s2.start();
    m2.emit({ type: "session.ended" });
    expect(ended).toBe(true);
  });

  test("mail.delivered pushes a mail event", async () => {
    mock.emit({
      type: "mail.delivered",
      data: {
        id: "mail_2",
        direction: "inbound",
        from: [{ name: "Alice", email: HUMAN_ADDR }],
        to: [{ name: null, email: AGENT_ADDR }],
        bodyValues: { p1: { value: "Hi" } },
        textBody: [{ partId: "p1", type: "text/plain" }],
        headers: {},
        receivedAt: "2024-01-01T00:00:00Z",
      },
    });

    const mailEvents = session.events.filter((e) => e.kind === "mail");
    expect(mailEvents).toHaveLength(1);
    const first = mailEvents[0]!;
    if (first.kind === "mail") {
      expect(first.id).toBe("mail_2");
    }
  });

  test("mail.delivered for connector reply is suppressed", async () => {
    mock.emit({
      type: "mail.delivered",
      data: {
        id: "mail_outbound",
        direction: "outbound",
        from: [{ name: null, email: AGENT_ADDR }],
        to: [{ name: "Alice", email: HUMAN_ADDR }],
        bodyValues: { p1: { value: "Reply" } },
        textBody: [{ partId: "p1", type: "text/plain" }],
        headers: { "interchange-type": "conversation.message" },
        receivedAt: "2024-01-01T00:00:00Z",
      },
    });

    expect(session.events).toHaveLength(0);
  });

  test("mail.delivered for non-connector outbound mail is shown", async () => {
    mock.emit({
      type: "mail.delivered",
      data: {
        id: "mail_tool_send",
        direction: "outbound",
        from: [{ name: null, email: AGENT_ADDR }],
        to: [{ name: "Other Agent", email: "ins_other@tenant.example" }],
        bodyValues: { p1: { value: "Hello agent" } },
        textBody: [{ partId: "p1", type: "text/plain" }],
        headers: {},
        receivedAt: "2024-01-01T00:00:00Z",
      },
    });

    expect(session.events).toHaveLength(1);
    const event = session.events[0]!;
    expect(event.kind).toBe("mail");
    if (event.kind === "mail") {
      expect(event.id).toBe("mail_tool_send");
    }
  });

  test("turn.committed pushes a turn event", () => {
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_2",
        status: "completed",
        text: "Hello from assistant",
        hadReply: false,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    const turnEvents = session.events.filter((e) => e.kind === "turn");
    expect(turnEvents).toHaveLength(1);
    const first = turnEvents[0]!;
    if (first.kind === "turn") {
      expect(first.turnId).toBe("turn_2");
      expect(first.content).toBe("Hello from assistant");
    }
  });

  test("turn.committed with errors sets isError", () => {
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_err",
        status: "failed",
        text: "",
        hadReply: false,
        hadError: true,
        errors: [{ category: "timeout", message: "Timed out" }],
        toolErrors: [],
      },
    });

    const turnEvents = session.events.filter((e) => e.kind === "turn");
    expect(turnEvents).toHaveLength(1);
    const first = turnEvents[0]!;
    if (first.kind === "turn") {
      expect(first.isError).toBe(true);
      expect(first.errors).toEqual([
        { category: "timeout", message: "Timed out" },
      ]);
    }
  });

  test("duplicate turn.committed events are ignored", () => {
    const turnEvent = {
      type: "turn.committed",
      data: {
        turnId: "turn_dup",
        status: "completed",
        text: "Hello",
        hadReply: false,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    };

    mock.emit(turnEvent);
    mock.emit(turnEvent);

    expect(session.events.filter((e) => e.kind === "turn")).toHaveLength(1);
  });

  test("unknown/invalid SSE events are silently ignored", () => {
    expect(() => {
      mock.emit({ type: "unknown.event", data: {} });
      mock.emit("not an object");
      mock.emit(null);
    }).not.toThrow();

    expect(session.events).toHaveLength(0);
  });
});

// Streaming buffer

describe("streaming buffer", () => {
  let session: InstanceSession;
  let mock: ReturnType<typeof createMockTransport>;

  beforeEach(async () => {
    mock = createMockTransport(makeHydrationHandler());
    session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });
    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));
  });

  test("streaming starts empty", () => {
    expect(session.streaming).toBe("");
  });

  test("inference.text.delta tokens accumulate", () => {
    mock.emit({
      type: "inference.text.delta",
      seq: 1,
      data: { token: "Hello", partial: { text: "Hello" } },
    });
    mock.emit({
      type: "inference.text.delta",
      seq: 2,
      data: { token: " world", partial: { text: "Hello world" } },
    });

    expect(session.streaming).toBe("Hello world");
  });

  test("turn.committed clears streaming when text matches", () => {
    mock.emit({
      type: "inference.text.delta",
      seq: 1,
      data: { token: "Hello", partial: { text: "Hello" } },
    });

    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_1",
        status: "completed",
        text: "Hello",
        hadReply: false,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    expect(session.streaming).toBe("");
  });

  test("turn.committed clears streaming when streaming is empty", () => {
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_1",
        status: "completed",
        text: "Some text",
        hadReply: false,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    expect(session.streaming).toBe("");
  });

  test("multi-turn guard: turn.committed does not clear streaming when it contains newer content", () => {
    // Simulate turn N+1 deltas already in the buffer
    mock.emit({
      type: "inference.text.delta",
      seq: 1,
      data: {
        token: "Turn N+1 content",
        partial: { text: "Turn N+1 content" },
      },
    });

    // turn.committed for turn N arrives late; streaming holds turn N+1 content
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_n",
        status: "completed",
        text: "Turn N text",
        hadReply: false,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    // Streaming buffer should NOT be cleared because it contains newer content
    expect(session.streaming).toBe("Turn N+1 content");
  });

  test("inference.error clears streaming", () => {
    mock.emit({
      type: "inference.text.delta",
      seq: 1,
      data: { token: "Partial", partial: { text: "Partial" } },
    });
    mock.emit({
      type: "inference.error",
      seq: 2,
      data: {
        error: { category: "retryable", message: "Retry" },
        partial: { text: "Partial" },
      },
    });

    expect(session.streaming).toBe("");
  });

  test("reactor.done clears streaming", () => {
    mock.emit({
      type: "inference.text.delta",
      seq: 1,
      data: { token: "Some text", partial: { text: "Some text" } },
    });
    mock.emit({
      type: "reactor.done",
      seq: 2,
      data: {},
    });

    expect(session.streaming).toBe("");
  });

  test("inference.text.replay sets streaming when streaming is empty", () => {
    mock.emit({ type: "inference.text.replay", data: { text: "hello world" } });

    expect(session.streaming).toBe("hello world");
  });

  test("inference.text.replay does not overwrite streaming when already populated", () => {
    mock.emit({
      type: "inference.text.delta",
      seq: 1,
      data: { token: "already here", partial: { text: "already here" } },
    });

    mock.emit({ type: "inference.text.replay", data: { text: "hello world" } });

    expect(session.streaming).toBe("already here");
  });

  test("replay-only streaming is cleared after hydration completes", async () => {
    let resolveFetch!: () => void;
    const fetchPending = new Promise<void>((r) => {
      resolveFetch = r;
    });

    const turn = makeTurn({ id: "turn_1", startedAt: "2024-01-01T00:00:00Z" });
    const bus = { emit: noop as (event: unknown) => void };
    const transport: Transport = {
      async fetch<T>(_method: string, path: string): Promise<T> {
        await fetchPending;
        if (path.includes("/mail")) return { data: [] } as T;
        if (path.includes("/turns")) return { data: [turn] } as T;
        throw new Error(`Unexpected: ${path}`);
      },
      subscribe(_path: string, onEvent: (event: unknown) => void): () => void {
        bus.emit = onEvent;
        return () => {
          bus.emit = noop;
        };
      },
    };

    const s = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport,
      onChange: noop,
    });

    s.start();

    // Replay arrives before hydration completes
    bus.emit({ type: "inference.text.replay", data: { text: "stale text" } });
    expect(s.streaming).toBe("stale text");

    // Hydration completes — the turn is already finished, so replay is stale
    resolveFetch();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(s.streaming).toBe("");
  });

  test("replay streaming survives hydration when live deltas have arrived", async () => {
    let resolveFetch!: () => void;
    const fetchPending = new Promise<void>((r) => {
      resolveFetch = r;
    });

    const bus = { emit: noop as (event: unknown) => void };
    const transport: Transport = {
      async fetch<T>(_method: string, path: string): Promise<T> {
        await fetchPending;
        if (path.includes("/mail")) return { data: [] } as T;
        if (path.includes("/turns")) return { data: [] } as T;
        throw new Error(`Unexpected: ${path}`);
      },
      subscribe(_path: string, onEvent: (event: unknown) => void): () => void {
        bus.emit = onEvent;
        return () => {
          bus.emit = noop;
        };
      },
    };

    const s = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport,
      onChange: noop,
    });

    s.start();

    // Replay arrives, then live deltas continue
    bus.emit({ type: "inference.text.replay", data: { text: "replay " } });
    bus.emit({
      type: "inference.text.delta",
      seq: 1,
      data: { token: "live", partial: { text: "replay live" } },
    });
    expect(s.streaming).toBe("replay live");

    // Hydration completes — streaming should NOT be cleared because
    // live deltas prove inference is still active
    resolveFetch();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(s.streaming).toBe("replay live");
  });

  test("inference.error after replay clears streamingFromReplay so hydration does not double-clear", async () => {
    let resolveFetch!: () => void;
    const fetchPending = new Promise<void>((r) => {
      resolveFetch = r;
    });

    const bus = { emit: noop as (event: unknown) => void };
    const transport: Transport = {
      async fetch<T>(_method: string, path: string): Promise<T> {
        await fetchPending;
        if (path.includes("/mail")) return { data: [] } as T;
        if (path.includes("/turns")) return { data: [] } as T;
        throw new Error(`Unexpected: ${path}`);
      },
      subscribe(_path: string, onEvent: (event: unknown) => void): () => void {
        bus.emit = onEvent;
        return () => {
          bus.emit = noop;
        };
      },
    };

    const s = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport,
      onChange: noop,
    });

    s.start();

    // Replay arrives, then inference fails
    bus.emit({ type: "inference.text.replay", data: { text: "partial" } });
    expect(s.streaming).toBe("partial");

    bus.emit({
      type: "inference.error",
      seq: 1,
      data: {
        error: { category: "retryable", message: "fail" },
        partial: { text: "partial" },
      },
    });
    expect(s.streaming).toBe("");

    // Hydration completes — streaming should remain "" (not be re-cleared
    // by a stale streamingFromReplay flag)
    resolveFetch();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(s.streaming).toBe("");
  });

  test("reactor.done after replay clears streamingFromReplay so hydration does not double-clear", async () => {
    let resolveFetch!: () => void;
    const fetchPending = new Promise<void>((r) => {
      resolveFetch = r;
    });

    const bus = { emit: noop as (event: unknown) => void };
    const transport: Transport = {
      async fetch<T>(_method: string, path: string): Promise<T> {
        await fetchPending;
        if (path.includes("/mail")) return { data: [] } as T;
        if (path.includes("/turns")) return { data: [] } as T;
        throw new Error(`Unexpected: ${path}`);
      },
      subscribe(_path: string, onEvent: (event: unknown) => void): () => void {
        bus.emit = onEvent;
        return () => {
          bus.emit = noop;
        };
      },
    };

    const s = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport,
      onChange: noop,
    });

    s.start();

    bus.emit({ type: "inference.text.replay", data: { text: "done text" } });
    expect(s.streaming).toBe("done text");

    bus.emit({ type: "reactor.done", seq: 1, data: {} });
    expect(s.streaming).toBe("");

    resolveFetch();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(s.streaming).toBe("");
  });
});

// Activity state machine

describe("activity state machine", () => {
  let session: InstanceSession;
  let mock: ReturnType<typeof createMockTransport>;

  beforeEach(async () => {
    mock = createMockTransport(makeHydrationHandler());
    session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });
    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));
  });

  test("activity starts null", () => {
    expect(session.activity).toBeNull();
  });

  test("inference.start sets inferring activity", () => {
    mock.emit({ type: "inference.start", seq: 1, data: { model: "gpt-4" } });
    expect(session.activity).toEqual({ type: "inferring" });
  });

  test("inference.text.delta clears activity", () => {
    mock.emit({ type: "inference.start", seq: 1, data: { model: "gpt-4" } });
    mock.emit({
      type: "inference.text.delta",
      seq: 2,
      data: { token: "Hi", partial: { text: "Hi" } },
    });
    expect(session.activity).toBeNull();
  });

  test("inference.tool_call.start sets tool_call activity with name", () => {
    mock.emit({
      type: "inference.tool_call.start",
      seq: 1,
      data: { callId: "call_1", name: "search", partial: { text: "" } },
    });
    expect(session.activity).toEqual({ type: "tool_call", name: "search" });
  });

  test("tool.start sets tool_running activity with name", () => {
    mock.emit({
      type: "tool.start",
      seq: 1,
      data: {
        call: {
          id: "call_1",
          name: "search",
          arguments: {},
        },
      },
    });
    expect(session.activity).toEqual({ type: "tool_running", name: "search" });
  });

  test("tool.done clears activity", () => {
    mock.emit({
      type: "tool.start",
      seq: 1,
      data: { call: { id: "call_1", name: "search", arguments: {} } },
    });
    mock.emit({
      type: "tool.done",
      seq: 2,
      data: {
        result: {
          callId: "call_1",
          content: "result",
        },
      },
    });
    expect(session.activity).toBeNull();
  });

  test("inference.done clears activity", () => {
    mock.emit({ type: "inference.start", seq: 1, data: { model: "gpt-4" } });
    mock.emit({
      type: "inference.done",
      seq: 2,
      data: {
        turn: {
          role: "assistant",
          content: [],
          model: "gpt-4",
          timestamp: 1000,
        },
        usage: {
          input: 10,
          output: 5,
          cacheRead: 0,
          cacheWrite: 0,
          thinking: 0,
        },
      },
    });
    expect(session.activity).toBeNull();
  });

  test("inference.error clears activity", () => {
    mock.emit({ type: "inference.start", seq: 1, data: { model: "gpt-4" } });
    mock.emit({
      type: "inference.error",
      seq: 2,
      data: {
        error: { category: "retryable", message: "Retry" },
        partial: { text: "" },
      },
    });
    expect(session.activity).toBeNull();
  });

  test("reactor.done clears activity", () => {
    mock.emit({ type: "inference.start", seq: 1, data: { model: "gpt-4" } });
    mock.emit({ type: "reactor.done", seq: 2, data: {} });
    expect(session.activity).toBeNull();
  });

  test("turn.committed clears activity", () => {
    mock.emit({ type: "inference.start", seq: 1, data: { model: "gpt-4" } });
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_1",
        status: "completed",
        text: "Hello",
        hadReply: false,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });
    expect(session.activity).toBeNull();
  });

  test("destroy clears activity", async () => {
    mock.emit({ type: "inference.start", seq: 1, data: { model: "gpt-4" } });
    expect(session.activity).toEqual({ type: "inferring" });

    session.destroy();
    expect(session.activity).toBeNull();
  });
});

// sendMail

describe("sendMail", () => {
  test("sends POST with content body", async () => {
    const fetches: { method: string; path: string; body: unknown }[] = [];

    const transport: Transport = {
      async fetch<T>(method: string, path: string, body?: unknown): Promise<T> {
        fetches.push({ method, path, body });
        if (path.includes("/mail") && method === "GET")
          return { data: [] } as T;
        if (path.includes("/turns")) return { data: [] } as T;
        if (path.includes("/mail") && method === "POST") return undefined as T;
        throw new Error(`Unexpected: ${method} ${path}`);
      },
      subscribe(_path: string, _onEvent: (event: unknown) => void): () => void {
        return noop;
      },
    };

    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport,
      onChange: noop,
    });

    session.start();
    await session.sendMail("Hello agent");

    const postFetch = fetches.find((f) => f.method === "POST");
    expect(postFetch).toBeDefined();
    expect(postFetch?.path).toBe(`${BASE_PATH}/mail`);
    expect(postFetch?.body).toEqual({ content: "Hello agent" });
  });
});

// onChange callback

describe("onChange callback", () => {
  test("turn.committed fires onChange exactly once", async () => {
    let changes = 0;
    const mock = createMockTransport(makeHydrationHandler());
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: () => {
        changes++;
      },
    });
    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));

    const before = changes;

    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_single",
        status: "completed",
        text: "Hello",
        hadReply: false,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    expect(changes).toBe(before + 1);
  });

  test("onChange is called on each state-changing SSE event", async () => {
    let changes = 0;
    const mock = createMockTransport(makeHydrationHandler());
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: () => {
        changes++;
      },
    });
    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));

    const before = changes;

    mock.emit({ type: "inference.start", seq: 1, data: { model: "gpt-4" } });
    expect(changes).toBe(before + 1);

    mock.emit({
      type: "inference.text.delta",
      seq: 2,
      data: { token: "Hi", partial: { text: "Hi" } },
    });
    expect(changes).toBe(before + 2);
  });
});

// Agent reply lifecycle (end-to-end)
//
// Exercises the full sequence a real user sees when an agent replies to a
// human: streaming tokens arrive, the turn commits with hadReply: true, and
// an outbound mail.delivered event follows. The agent's reply must remain
// visible in the timeline at every stage.

describe("agent reply lifecycle", () => {
  let session: InstanceSession;
  let mock: ReturnType<typeof createMockTransport>;

  beforeEach(async () => {
    mock = createMockTransport(makeHydrationHandler());
    session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });
    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));
  });

  test("agent reply is visible after turn.committed with hadReply: true", () => {
    // 1. User sees streaming tokens as the agent generates its reply
    mock.emit({
      type: "inference.text.delta",
      seq: 1,
      data: { token: "I can help", partial: { text: "I can help" } },
    });
    mock.emit({
      type: "inference.text.delta",
      seq: 2,
      data: {
        token: " with that.",
        partial: { text: "I can help with that." },
      },
    });
    expect(session.streaming).toBe("I can help with that.");

    // 2. Turn commits — hadReply: true because the agent sent an outbound reply
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_reply",
        status: "completed",
        text: "I can help with that.",
        hadReply: true,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    // Streaming is cleared (expected)
    expect(session.streaming).toBe("");

    // The turn MUST appear in events — the user's reply cannot vanish
    const turnEvents = session.events.filter((e) => e.kind === "turn");
    expect(turnEvents).toHaveLength(1);
    expect(turnEvents[0]!.content).toBe("I can help with that.");
  });

  test("agent reply remains visible after outbound mail.delivered is suppressed", () => {
    // Full sequence: stream → commit → outbound mail
    mock.emit({
      type: "inference.text.delta",
      seq: 1,
      data: { token: "Sure thing.", partial: { text: "Sure thing." } },
    });

    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_reply_2",
        status: "completed",
        text: "Sure thing.",
        hadReply: true,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    // Outbound connector reply mail arrives — suppressed by shouldShowMail
    mock.emit({
      type: "mail.delivered",
      data: {
        id: "mail_reply",
        direction: "outbound",
        from: [{ name: null, email: AGENT_ADDR }],
        to: [{ name: "Alice", email: HUMAN_ADDR }],
        bodyValues: { p1: { value: "Sure thing." } },
        textBody: [{ partId: "p1", type: "text/plain" }],
        headers: { "interchange-type": "conversation.message" },
        receivedAt: "2024-01-01T00:01:00Z",
      },
    });

    // After the full sequence, the reply must still be visible
    expect(session.streaming).toBe("");
    const allEvents = session.events;
    expect(allEvents.length).toBeGreaterThanOrEqual(1);

    const turnEvents = allEvents.filter((e) => e.kind === "turn");
    expect(turnEvents).toHaveLength(1);
    expect(turnEvents[0]!.content).toBe("Sure thing.");
  });
});

// Multi-turn conversation
//
// Exercises a realistic back-and-forth: human sends mail, agent thinks and
// replies, human sends another mail, agent replies again. Verifies the full
// timeline accumulates correctly.

describe("multi-turn conversation", () => {
  let session: InstanceSession;
  let mock: ReturnType<typeof createMockTransport>;

  beforeEach(async () => {
    mock = createMockTransport(makeHydrationHandler());
    session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });
    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));
  });

  test("two full request-reply cycles produce four timeline events", () => {
    // Turn 1: human mail arrives
    mock.emit({
      type: "mail.delivered",
      data: {
        id: "mail_human_1",
        direction: "inbound",
        from: [{ name: "Alice", email: HUMAN_ADDR }],
        to: [{ name: null, email: AGENT_ADDR }],
        bodyValues: { p1: { value: "What is 2+2?" } },
        textBody: [{ partId: "p1", type: "text/plain" }],
        headers: {},
        receivedAt: "2024-01-01T00:00:00Z",
      },
    });

    // Agent starts thinking, streams, commits
    mock.emit({ type: "inference.start", seq: 1, data: { model: "gpt-4" } });
    mock.emit({
      type: "inference.text.delta",
      seq: 2,
      data: { token: "4", partial: { text: "4" } },
    });
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_1",
        status: "completed",
        text: "4",
        hadReply: true,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    // Turn 2: human follows up
    mock.emit({
      type: "mail.delivered",
      data: {
        id: "mail_human_2",
        direction: "inbound",
        from: [{ name: "Alice", email: HUMAN_ADDR }],
        to: [{ name: null, email: AGENT_ADDR }],
        bodyValues: { p1: { value: "And 3+3?" } },
        textBody: [{ partId: "p1", type: "text/plain" }],
        headers: {},
        receivedAt: "2024-01-01T00:01:00Z",
      },
    });

    // Agent replies again
    mock.emit({ type: "inference.start", seq: 3, data: { model: "gpt-4" } });
    mock.emit({
      type: "inference.text.delta",
      seq: 4,
      data: { token: "6", partial: { text: "6" } },
    });
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_2",
        status: "completed",
        text: "6",
        hadReply: true,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    expect(session.streaming).toBe("");
    expect(session.activity).toBeNull();
    expect(session.events).toHaveLength(4);

    const mails = session.events.filter((e) => e.kind === "mail");
    const turns = session.events.filter((e) => e.kind === "turn");
    expect(mails).toHaveLength(2);
    expect(turns).toHaveLength(2);
    expect(turns[0]!.content).toBe("4");
    expect(turns[1]!.content).toBe("6");
  });

  test("agent uses tools then replies", () => {
    // Human sends a question that requires tool use
    mock.emit({
      type: "mail.delivered",
      data: {
        id: "mail_tools",
        direction: "inbound",
        from: [{ name: "Alice", email: HUMAN_ADDR }],
        to: [{ name: null, email: AGENT_ADDR }],
        bodyValues: { p1: { value: "Search for X" } },
        textBody: [{ partId: "p1", type: "text/plain" }],
        headers: {},
        receivedAt: "2024-01-01T00:00:00Z",
      },
    });

    // Agent starts inference, calls a tool
    mock.emit({ type: "inference.start", seq: 1, data: { model: "gpt-4" } });
    mock.emit({
      type: "inference.tool_call.start",
      seq: 2,
      data: { callId: "call_1", name: "search", partial: { text: "" } },
    });
    expect(session.activity).toEqual({ type: "tool_call", name: "search" });

    mock.emit({
      type: "tool.start",
      seq: 3,
      data: { call: { id: "call_1", name: "search", arguments: {} } },
    });
    expect(session.activity).toEqual({ type: "tool_running", name: "search" });

    mock.emit({
      type: "tool.done",
      seq: 4,
      data: { result: { callId: "call_1", content: "found it" } },
    });
    expect(session.activity).toBeNull();

    // Agent produces final text after tool use
    mock.emit({ type: "inference.start", seq: 5, data: { model: "gpt-4" } });
    mock.emit({
      type: "inference.text.delta",
      seq: 6,
      data: { token: "Found it.", partial: { text: "Found it." } },
    });
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_tools",
        status: "completed",
        text: "Found it.",
        hadReply: true,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    expect(session.streaming).toBe("");
    expect(session.activity).toBeNull();

    const turns = session.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(1);
    expect(turns[0]!.content).toBe("Found it.");
  });
});

// Hydration edge cases

describe("hydration edge cases", () => {
  test("turn arriving via SSE during hydration is deduplicated against fetched turns", async () => {
    let resolveFetch!: () => void;
    const fetchPending = new Promise<void>((r) => {
      resolveFetch = r;
    });

    const turn = makeTurn({
      id: "turn_race",
      startedAt: "2024-01-01T01:00:00Z",
    });

    const bus = { emit: noop as (event: unknown) => void };
    const transport: Transport = {
      async fetch<T>(_method: string, path: string): Promise<T> {
        await fetchPending;
        if (path.includes("/mail")) return { data: [] } as T;
        if (path.includes("/turns")) return { data: [turn] } as T;
        throw new Error(`Unexpected: ${path}`);
      },
      subscribe(_path: string, onEvent: (event: unknown) => void): () => void {
        bus.emit = onEvent;
        return () => {
          bus.emit = noop;
        };
      },
    };

    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport,
      onChange: noop,
    });

    session.start();

    // Same turn arrives via SSE while fetch is in-flight
    bus.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_race",
        status: "completed",
        text: "Hello from assistant",
        hadReply: false,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    resolveFetch();
    await new Promise((resolve) => setTimeout(resolve, 0));

    // Turn should appear exactly once
    const turns = session.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(1);
  });

  test("destroy during hydration does not call onChange or set hydrated", async () => {
    let resolveFetch!: () => void;
    const fetchPending = new Promise<void>((r) => {
      resolveFetch = r;
    });

    let changes = 0;
    const bus = { emit: noop as (event: unknown) => void };
    const transport: Transport = {
      async fetch<T>(_method: string, path: string): Promise<T> {
        await fetchPending;
        if (path.includes("/mail")) return { data: [makeMail()] } as T;
        if (path.includes("/turns")) return { data: [makeTurn()] } as T;
        throw new Error(`Unexpected: ${path}`);
      },
      subscribe(_path: string, onEvent: (event: unknown) => void): () => void {
        bus.emit = onEvent;
        return () => {
          bus.emit = noop;
        };
      },
    };

    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport,
      onChange: () => {
        changes++;
      },
    });

    session.start();
    session.destroy();

    // Unblock fetch — but session is already destroyed
    resolveFetch();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(session.hydrated).toBe(false);
    expect(session.events).toHaveLength(0);
    expect(changes).toBe(0);
  });

  test("start cleanup cancels hydration without destroying session", async () => {
    let resolveFetch!: () => void;
    const fetchPending = new Promise<void>((r) => {
      resolveFetch = r;
    });

    let changes = 0;
    const transport: Transport = {
      async fetch<T>(_method: string, path: string): Promise<T> {
        await fetchPending;
        if (path.includes("/mail")) return { data: [makeMail()] } as T;
        if (path.includes("/turns")) return { data: [makeTurn()] } as T;
        throw new Error(`Unexpected: ${path}`);
      },
      subscribe(_path: string, _onEvent: (event: unknown) => void): () => void {
        return noop;
      },
    };

    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport,
      onChange: () => {
        changes++;
      },
    });

    const cleanup = session.start();
    cleanup();

    // Unblock fetch — but cleanup already cancelled
    resolveFetch();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(session.hydrated).toBe(false);
    expect(session.events).toHaveLength(0);
    expect(changes).toBe(0);
  });

  test("hydration fetch failure calls onError and still drains SSE buffer", async () => {
    const bus = { emit: noop as (event: unknown) => void };
    const transport: Transport = {
      async fetch<T>(_method: string, _path: string): Promise<T> {
        throw new Error("network failure");
      },
      subscribe(_path: string, onEvent: (event: unknown) => void): () => void {
        bus.emit = onEvent;
        return () => {
          bus.emit = noop;
        };
      },
    };

    let reportedError: Error | null = null;
    const session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport,
      onChange: noop,
      onError: (err) => {
        reportedError = err;
      },
    });

    session.start();

    // SSE event arrives during the (failing) fetch
    bus.emit({
      type: "mail.delivered",
      data: {
        id: "mail_during_fail",
        direction: "inbound",
        from: [{ name: "Alice", email: HUMAN_ADDR }],
        to: [{ name: null, email: AGENT_ADDR }],
        bodyValues: { p1: { value: "Hello" } },
        textBody: [{ partId: "p1", type: "text/plain" }],
        headers: {},
        receivedAt: "2024-01-01T00:00:00Z",
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 0));

    // Session is hydrated with SSE buffer contents despite the fetch failure
    expect(session.hydrated).toBe(true);
    expect(session.events).toHaveLength(1);
    expect(session.events[0]!.kind).toBe("mail");

    // Error was reported through onError callback
    expect(reportedError).toBeInstanceOf(Error);
    expect(reportedError!.message).toBe("Failed to hydrate chat history");
    expect(reportedError!.cause).toBeInstanceOf(Error);
    expect((reportedError!.cause as Error).message).toBe("network failure");
  });
});

// Turn edge cases

describe("turn edge cases", () => {
  let session: InstanceSession;
  let mock: ReturnType<typeof createMockTransport>;

  beforeEach(async () => {
    mock = createMockTransport(makeHydrationHandler());
    session = createInstanceSession({
      tenantId: TENANT_ID,
      instanceId: INSTANCE_ID,
      transport: mock.transport,
      onChange: noop,
    });
    session.start();
    await new Promise((resolve) => setTimeout(resolve, 0));
  });

  test("turn with only toolErrors and no text is shown", () => {
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_toolerr",
        status: "completed",
        text: "",
        hadReply: false,
        hadError: false,
        errors: [],
        toolErrors: [{ name: "search", content: "API rate limited" }],
      },
    });

    const turns = session.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(1);
    const turn = turns[0]!;
    expect(turn.content).toBe("An error occurred during inference.");
    if (turn.kind === "turn") {
      expect(turn.toolErrors).toEqual([
        { name: "search", content: "API rate limited" },
      ]);
    }
  });

  test("turn with no text, no error, and no toolErrors is dropped", () => {
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_empty",
        status: "completed",
        text: "",
        hadReply: false,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    expect(session.events).toHaveLength(0);
  });

  test("turn with failed status but no hadError still shows as error", () => {
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_failed",
        status: "failed",
        text: "",
        hadReply: false,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    const turns = session.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(1);
    if (turns[0]!.kind === "turn") {
      expect(turns[0]!.isError).toBe(true);
    }
  });

  test("turn committed during multi-step tool loop preserves newer streaming", () => {
    // Agent is already streaming turn N+1 content
    mock.emit({
      type: "inference.text.delta",
      seq: 1,
      data: { token: "New reply", partial: { text: "New reply" } },
    });

    // Turn N commits (late arrival) — should not clobber streaming
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_old",
        status: "completed",
        text: "Old reply",
        hadReply: true,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    expect(session.streaming).toBe("New reply");

    // Turn N+1 commits — streaming matches, so it clears
    mock.emit({
      type: "turn.committed",
      data: {
        turnId: "turn_new",
        status: "completed",
        text: "New reply",
        hadReply: true,
        hadError: false,
        errors: [],
        toolErrors: [],
      },
    });

    expect(session.streaming).toBe("");

    // Both turns should be in the timeline
    const turns = session.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(2);
    expect(turns[0]!.content).toBe("Old reply");
    expect(turns[1]!.content).toBe("New reply");
  });
});
