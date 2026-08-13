/* eslint-disable @typescript-eslint/no-unsafe-type-assertion -- Transport.fetch<T> is a generic interface method; mock implementations must use `as T` to satisfy the return type contract */
import { describe, expect, test } from "bun:test";

import { createRunSession, type RunSession } from "./session";
import type { Transport } from "./transport";
import type { WorkflowRunEvent } from "./validators";

const TENANT_ID = "tnt_1";
const RUN_ID = "run_test";
const BASE_PATH = `/api/tenants/${TENANT_ID}/workflows/runs/${RUN_ID}`;
const POLL_MS = 10;

function noop(): void {
  // intentional no-op for onChange callbacks
}

function tick(ms = 0): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function event(seq: number, type: string, body: Record<string, unknown> = {}) {
  return { seq, type, body };
}

// A mock transport whose /events response is supplied per call, so a test can
// grow the log across polls or fail a specific read.
type Responder = (call: number, path: string) => unknown;

function createMockTransport(responder: Responder) {
  const paths: string[] = [];
  let calls = 0;
  const transport: Transport = {
    async fetch<T>(_method: string, path: string): Promise<T> {
      calls += 1;
      paths.push(path);
      return responder(calls, path) as T;
    },
    subscribe(): () => void {
      throw new Error("createRunSession must not open an SSE subscription");
    },
  };
  return {
    transport,
    paths,
    get calls() {
      return calls;
    },
  };
}

function eventsResponse(events: WorkflowRunEvent[]) {
  return { runId: RUN_ID, events };
}

describe("run session lifecycle", () => {
  test("start polls the events endpoint and hydrates the timeline", async () => {
    const mock = createMockTransport(() =>
      eventsResponse([event(0, "RunStarted"), event(1, "StepStarted")]),
    );
    const session = createRunSession({
      tenantId: TENANT_ID,
      runId: RUN_ID,
      transport: mock.transport,
      onChange: noop,
      pollIntervalMs: POLL_MS,
    });

    expect(session.hydrated).toBe(false);
    const stop = session.start();
    await tick();

    expect(session.hydrated).toBe(true);
    expect(mock.paths[0]).toBe(`${BASE_PATH}/events`);
    expect(session.events.map((e) => e.seq)).toEqual([0, 1]);
    expect(session.terminal).toBe(false);

    stop();
    session.destroy();
  });

  test("an empty log hydrates as an empty, non-terminal timeline", async () => {
    const mock = createMockTransport(() => eventsResponse([]));
    const session = createRunSession({
      tenantId: TENANT_ID,
      runId: RUN_ID,
      transport: mock.transport,
      onChange: noop,
      pollIntervalMs: POLL_MS,
    });

    session.start();
    await tick();

    expect(session.hydrated).toBe(true);
    expect(session.events).toHaveLength(0);
    expect(session.terminal).toBe(false);
    session.destroy();
  });

  test("double start throws", () => {
    const mock = createMockTransport(() => eventsResponse([]));
    const session = createRunSession({
      tenantId: TENANT_ID,
      runId: RUN_ID,
      transport: mock.transport,
      onChange: noop,
      pollIntervalMs: POLL_MS,
    });

    session.start();
    expect(() => session.start()).toThrow(
      "start() called on an already-started session",
    );
    session.destroy();
  });

  test("destroy is idempotent", async () => {
    const mock = createMockTransport(() => eventsResponse([]));
    const session = createRunSession({
      tenantId: TENANT_ID,
      runId: RUN_ID,
      transport: mock.transport,
      onChange: noop,
      pollIntervalMs: POLL_MS,
    });

    session.start();
    session.destroy();
    expect(() => session.destroy()).not.toThrow();
  });
});

describe("run session polling", () => {
  test("later reads replace the timeline with the fuller log", async () => {
    const mock = createMockTransport((call) =>
      eventsResponse(
        call === 1
          ? [event(0, "RunStarted")]
          : [event(0, "RunStarted"), event(1, "StepStarted")],
      ),
    );
    const session = createRunSession({
      tenantId: TENANT_ID,
      runId: RUN_ID,
      transport: mock.transport,
      onChange: noop,
      pollIntervalMs: POLL_MS,
    });

    session.start();
    await tick();
    expect(session.events.map((e) => e.seq)).toEqual([0]);

    // Wait past the poll interval for the second read.
    await tick(POLL_MS * 2);
    expect(session.events.map((e) => e.seq)).toEqual([0, 1]);
    session.destroy();
  });

  test("a terminal event stops polling", async () => {
    const mock = createMockTransport(() =>
      eventsResponse([event(0, "RunStarted"), event(1, "RunCompleted")]),
    );
    const session = createRunSession({
      tenantId: TENANT_ID,
      runId: RUN_ID,
      transport: mock.transport,
      onChange: noop,
      pollIntervalMs: POLL_MS,
    });

    session.start();
    await tick();
    expect(session.terminal).toBe(true);
    const callsAtTerminal = mock.calls;

    // No further reads once the run has settled.
    await tick(POLL_MS * 3);
    expect(mock.calls).toBe(callsAtTerminal);
    session.destroy();
  });

  test("onChange fires once per successful read", async () => {
    let changes = 0;
    const mock = createMockTransport(() =>
      eventsResponse([event(0, "RunCompleted")]),
    );
    const session = createRunSession({
      tenantId: TENANT_ID,
      runId: RUN_ID,
      transport: mock.transport,
      onChange: () => {
        changes += 1;
      },
      pollIntervalMs: POLL_MS,
    });

    session.start();
    await tick();
    // Terminal on the first read, so exactly one change fired.
    expect(changes).toBe(1);
    session.destroy();
  });

  test("destroy halts further polling", async () => {
    const mock = createMockTransport(() =>
      eventsResponse([event(0, "RunStarted")]),
    );
    const session = createRunSession({
      tenantId: TENANT_ID,
      runId: RUN_ID,
      transport: mock.transport,
      onChange: noop,
      pollIntervalMs: POLL_MS,
    });

    session.start();
    await tick();
    session.destroy();
    const callsAtDestroy = mock.calls;

    await tick(POLL_MS * 3);
    expect(mock.calls).toBe(callsAtDestroy);
  });

  test("start cleanup cancels the in-flight read without hydrating", async () => {
    let resolveFetch!: () => void;
    const fetchPending = new Promise<void>((r) => {
      resolveFetch = r;
    });
    const transport: Transport = {
      async fetch<T>(): Promise<T> {
        await fetchPending;
        return eventsResponse([event(0, "RunStarted")]) as T;
      },
      subscribe(): () => void {
        throw new Error("unexpected subscribe");
      },
    };

    const session = createRunSession({
      tenantId: TENANT_ID,
      runId: RUN_ID,
      transport,
      onChange: noop,
      pollIntervalMs: POLL_MS,
    });

    const cleanup = session.start();
    cleanup();
    resolveFetch();
    await tick();

    expect(session.hydrated).toBe(false);
    expect(session.events).toHaveLength(0);
    session.destroy();
  });
});

describe("run session error handling", () => {
  test("a failed read is reported through onError", async () => {
    // A holder object avoids control-flow narrowing of a `let` that is only
    // assigned inside the onError callback.
    const reported: { error: Error | null } = { error: null };
    const transport: Transport = {
      async fetch<T>(): Promise<T> {
        throw new Error("network down");
      },
      subscribe(): () => void {
        throw new Error("unexpected subscribe");
      },
    };

    const session = createRunSession({
      tenantId: TENANT_ID,
      runId: RUN_ID,
      transport,
      onChange: noop,
      onError: (err) => {
        reported.error = err;
      },
      pollIntervalMs: POLL_MS,
    });

    session.start();
    await tick();

    if (reported.error === null) {
      throw new Error("expected an error to be reported");
    }
    expect(reported.error.message).toBe("Failed to read run events");
    expect(reported.error.cause).toBeInstanceOf(Error);
    expect(session.hydrated).toBe(false);
    session.destroy();
  });

  test("an invalid response shape is reported through onError", async () => {
    const reported: { error: Error | null } = { error: null };
    const transport: Transport = {
      async fetch<T>(): Promise<T> {
        return { notRunId: true } as T;
      },
      subscribe(): () => void {
        throw new Error("unexpected subscribe");
      },
    };

    const session = createRunSession({
      tenantId: TENANT_ID,
      runId: RUN_ID,
      transport,
      onChange: noop,
      onError: (err) => {
        reported.error = err;
      },
      pollIntervalMs: POLL_MS,
    });

    session.start();
    await tick();

    if (reported.error === null) {
      throw new Error("expected an error to be reported");
    }
    expect(reported.error.message).toMatch(/Invalid run events response/);
    expect(session.hydrated).toBe(false);
    session.destroy();
  });
});

// Type-level guard: the session exposes a read-only timeline surface.
describe("run session shape", () => {
  test("exposes events, hydrated, and terminal", async () => {
    const mock = createMockTransport(() => eventsResponse([]));
    const session: RunSession = createRunSession({
      tenantId: TENANT_ID,
      runId: RUN_ID,
      transport: mock.transport,
      onChange: noop,
      pollIntervalMs: POLL_MS,
    });
    expect(Array.isArray(session.events)).toBe(true);
    expect(session.hydrated).toBe(false);
    expect(session.terminal).toBe(false);
    session.destroy();
  });
});
