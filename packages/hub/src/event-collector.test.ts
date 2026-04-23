import { describe, test, expect, beforeEach } from "bun:test";
import { sessionMessage, messagePart } from "@interchange/db/schema";
import type { InferenceEvent } from "@interchange/types/runtime";

import { createEventCollector, type EventCollector } from "./event-collector";

// ---------------------------------------------------------------------------
// Test helpers: fake DB that records insert/update calls
// ---------------------------------------------------------------------------

type InsertCall = {
  table: "session_message" | "message_part";
  values: Record<string, unknown>;
};

type UpdateCall = {
  table: "session_message" | "message_part";
  set: Record<string, unknown>;
};

function createFakeDB() {
  const inserts: InsertCall[] = [];
  const updates: UpdateCall[] = [];

  function tableName(table: unknown): "session_message" | "message_part" {
    if (table === sessionMessage) return "session_message";
    if (table === messagePart) return "message_part";
    throw new Error(`Unexpected table: ${String(table)}`);
  }

  const db = {
    insert(table: unknown) {
      const name = tableName(table);
      return {
        values(vals: Record<string, unknown>) {
          inserts.push({ table: name, values: vals });
          return Promise.resolve();
        },
      };
    },
    update(table: unknown) {
      const name = tableName(table);
      return {
        set(vals: Record<string, unknown>) {
          return {
            where(_condition: unknown) {
              updates.push({ table: name, set: vals });
              return Promise.resolve();
            },
          };
        },
      };
    },
  };

  return { db: db as never, inserts, updates };
}

function event(type: string, seq: number, data: unknown): InferenceEvent {
  return { type, seq, data } as InferenceEvent;
}

function at<T>(arr: T[], index: number): T {
  const item = arr[index];
  if (item === undefined) {
    throw new Error(`Expected element at index ${index}`);
  }
  return item;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("EventCollector", () => {
  let fakeDB: ReturnType<typeof createFakeDB>;
  let collector: EventCollector;

  beforeEach(() => {
    fakeDB = createFakeDB();
    collector = createEventCollector({
      db: fakeDB.db,
      sessionId: "ses_test",
      instanceId: "ins_test",
      tenantId: "tnt_test",
      agentAddress: "agt_test@test.localhost",
    });
  });

  test("inference.start creates an assistant message row and step-start part", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));

    const messages = fakeDB.inserts.filter(
      (i) => i.table === "session_message",
    );
    expect(messages).toHaveLength(1);
    const insert = at(messages, 0);
    expect(insert.values.role).toBe("assistant");
    expect(insert.values.status).toBe("pending");
    expect(insert.values.sessionId).toBe("ses_test");
    expect(insert.values.tenantId).toBe("tnt_test");

    const parts = fakeDB.inserts.filter((i) => i.table === "message_part");
    expect(parts).toHaveLength(1);
    expect(at(parts, 0).values.type).toBe("step-start");
    expect(at(parts, 0).values.metadata).toEqual({ model: "gpt-4" });
  });

  test("reactor.start alone does not create a message", async () => {
    await collector.onEvent(event("reactor.start", 1, {}));

    expect(fakeDB.inserts).toHaveLength(0);
  });

  test("inference.done inserts text, reasoning, and tool-call parts", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(
      event("inference.done", 5, {
        message: {
          role: "assistant",
          content: [
            { type: "text", text: "Hello world" },
            { type: "thinking", thinking: "Let me think..." },
            {
              type: "tool_call",
              id: "call_1",
              name: "search",
              arguments: { query: "test" },
            },
          ],
          model: "gpt-4",
        },
        usage: { input: 10, output: 20 },
      }),
    );

    const parts = fakeDB.inserts.filter((i) => i.table === "message_part");
    // step-start + text + reasoning + tool_call + step-finish = 5 parts
    expect(parts).toHaveLength(5);

    expect(at(parts, 0).values.type).toBe("step-start");

    expect(at(parts, 1).values.type).toBe("text");
    expect(at(parts, 1).values.content).toBe("Hello world");

    expect(at(parts, 2).values.type).toBe("reasoning");
    expect(at(parts, 2).values.content).toBe("Let me think...");

    expect(at(parts, 3).values.type).toBe("tool");
    expect(at(parts, 3).values.metadata).toEqual({
      kind: "call",
      callId: "call_1",
      name: "search",
      arguments: { query: "test" },
    });

    expect(at(parts, 4).values.type).toBe("step-finish");
  });

  test("tool.done inserts a tool result part", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(
      event("tool.done", 3, {
        result: {
          callId: "call_1",
          content: "Result text",
          isError: false,
        },
      }),
    );

    const parts = fakeDB.inserts.filter((i) => i.table === "message_part");
    // step-start from inference.start + tool result = 2 parts
    expect(parts).toHaveLength(2);
    expect(at(parts, 0).values.type).toBe("step-start");
    expect(at(parts, 1).values.type).toBe("tool");
    expect(at(parts, 1).values.metadata).toEqual({
      kind: "result",
      callId: "call_1",
      content: "Result text",
      isError: false,
    });
  });

  test("reactor.done marks message as delivered", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(event("reactor.done", 10, {}));

    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).table).toBe("session_message");
    expect(at(fakeDB.updates, 0).set).toEqual({ status: "delivered" });
  });

  test("reactor.error with fatal=true marks message as failed", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(
      event("reactor.error", 10, { error: "boom", fatal: true }),
    );

    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).table).toBe("session_message");
    expect(at(fakeDB.updates, 0).set).toEqual({ status: "failed" });
  });

  test("reactor.error with fatal=false does not update status", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(
      event("reactor.error", 10, { error: "transient", fatal: false }),
    );

    expect(fakeDB.updates).toHaveLength(0);
  });

  test("parts before inference.start are dropped", async () => {
    await collector.onEvent(
      event("inference.done", 5, {
        message: {
          role: "assistant",
          content: [{ type: "text", text: "orphan" }],
          model: "gpt-4",
        },
        usage: { input: 1, output: 1 },
      }),
    );

    const parts = fakeDB.inserts.filter((i) => i.table === "message_part");
    expect(parts).toHaveLength(0);
  });

  test("ordinals increment correctly across parts", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(
      event("inference.done", 5, {
        message: {
          role: "assistant",
          content: [
            { type: "text", text: "hello" },
            { type: "thinking", thinking: "hmm" },
          ],
          model: "gpt-4",
        },
        usage: { input: 1, output: 1 },
      }),
    );

    const parts = fakeDB.inserts.filter((i) => i.table === "message_part");
    const ordinals = parts.map((p) => p.values.ordinal);
    // step-start (0), text (1), reasoning (2), step-finish (3)
    expect(ordinals).toEqual([0, 1, 2, 3]);
  });

  test("abandon marks pending message as failed", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.abandon();

    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).table).toBe("session_message");
    expect(at(fakeDB.updates, 0).set).toEqual({ status: "failed" });
  });

  test("abandon with no active message is a no-op", async () => {
    await collector.abandon();
    expect(fakeDB.updates).toHaveLength(0);
  });

  test("tool_result content blocks are skipped to avoid duplication", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(
      event("inference.done", 5, {
        message: {
          role: "assistant",
          content: [
            {
              type: "tool_result",
              callId: "call_1",
              content: "already persisted",
            },
            { type: "text", text: "final answer" },
          ],
          model: "gpt-4",
        },
        usage: { input: 1, output: 1 },
      }),
    );

    const parts = fakeDB.inserts.filter((i) => i.table === "message_part");
    const types = parts.map((p) => p.values.type);
    expect(types).toEqual(["step-start", "text", "step-finish"]);
  });

  test("streaming deltas are not persisted", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(
      event("inference.text.delta", 2, {
        token: "hi",
        partial: { text: "hi" },
      }),
    );
    await collector.onEvent(
      event("inference.thinking.delta", 3, {
        token: "hmm",
        partial: { text: "hi", thinking: "hmm" },
      }),
    );
    await collector.onEvent(
      event("inference.usage", 4, {
        usage: { input: 10, output: 5 },
      }),
    );

    const parts = fakeDB.inserts.filter((i) => i.table === "message_part");
    // Only the step-start from inference.start; deltas are not persisted
    expect(parts).toHaveLength(1);
    expect(at(parts, 0).values.type).toBe("step-start");
  });

  test("full reactor cycle produces correct sequence with per-turn messages", async () => {
    // Simulate: inference turn 1 (text + tool call) ->
    // tool result -> inference turn 2 (text) -> reactor.done
    //
    // Each inference.start creates a new assistant message. The second
    // inference.start finalizes the first message as failed (via the
    // orphan-guard path) and starts a fresh one.

    // Turn 1
    await collector.onEvent(event("inference.start", 2, { model: "claude-3" }));
    await collector.onEvent(
      event("inference.done", 5, {
        message: {
          role: "assistant",
          content: [
            { type: "text", text: "Let me search" },
            {
              type: "tool_call",
              id: "call_1",
              name: "search",
              arguments: { q: "test" },
            },
          ],
          model: "claude-3",
        },
        usage: { input: 10, output: 20 },
      }),
    );

    // Tool execution
    await collector.onEvent(
      event("tool.done", 7, {
        result: { callId: "call_1", content: "found: test data" },
      }),
    );

    // Turn 2 — inference.start finalizes turn 1's message as failed (orphan)
    await collector.onEvent(event("inference.start", 8, { model: "claude-3" }));
    await collector.onEvent(
      event("inference.done", 12, {
        message: {
          role: "assistant",
          content: [{ type: "text", text: "Here are the results" }],
          model: "claude-3",
        },
        usage: { input: 30, output: 15 },
      }),
    );

    await collector.onEvent(event("reactor.done", 13, {}));

    // Two message rows created (one per inference.start)
    const messages = fakeDB.inserts.filter(
      (i) => i.table === "session_message",
    );
    expect(messages).toHaveLength(2);

    const parts = fakeDB.inserts.filter((i) => i.table === "message_part");
    const types = parts.map((p) => p.values.type);
    expect(types).toEqual([
      "step-start", // inference turn 1 start
      "text", // "Let me search"
      "tool", // tool call
      "step-finish", // inference turn 1 end
      "tool", // tool result
      "step-start", // inference turn 2 start
      "text", // "Here are the results"
      "step-finish", // inference turn 2 end
    ]);

    // First message finalized as failed (orphan), second as delivered
    expect(fakeDB.updates).toHaveLength(2);
    expect(at(fakeDB.updates, 0).set).toEqual({ status: "failed" });
    expect(at(fakeDB.updates, 1).set).toEqual({ status: "delivered" });
  });

  test("second inference.start finalizes first message as failed", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(event("inference.start", 5, { model: "gpt-4" }));

    // Two message rows created
    const messages = fakeDB.inserts.filter(
      (i) => i.table === "session_message",
    );
    expect(messages).toHaveLength(2);

    // First message finalized as failed
    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).set).toEqual({ status: "failed" });
  });

  test("abandon after reactor.done is a no-op", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(event("reactor.done", 5, {}));
    await collector.abandon();

    // Only one update from reactor.done, not two
    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).set).toEqual({ status: "delivered" });
  });
});
