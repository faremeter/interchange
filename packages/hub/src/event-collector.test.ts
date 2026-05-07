import { describe, test, expect, beforeEach } from "bun:test";
import { inferenceTurn, turnPart } from "@interchange/db/schema";
import type { InferenceEvent } from "@interchange/types/runtime";

import {
  createEventCollector,
  type EventCollector,
  type TurnFinalized,
} from "./event-collector";
import { createEventCollectorRegistry } from "./event-collector-registry";

// ---------------------------------------------------------------------------
// Test helpers: fake DB that records insert/update calls
// ---------------------------------------------------------------------------

type InsertCall = {
  table: "inference_turn" | "turn_part";
  values: Record<string, unknown>;
};

type UpdateCall = {
  table: "inference_turn" | "turn_part";
  set: Record<string, unknown>;
};

function createFakeDB() {
  const inserts: InsertCall[] = [];
  const updates: UpdateCall[] = [];

  function tableName(table: unknown): "inference_turn" | "turn_part" {
    if (table === inferenceTurn) return "inference_turn";
    if (table === turnPart) return "turn_part";
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
    });
  });

  test("inference.start creates an inference_turn row and step-start part", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));

    const turns = fakeDB.inserts.filter((i) => i.table === "inference_turn");
    expect(turns).toHaveLength(1);
    const insert = at(turns, 0);
    expect(insert.values.model).toBe("gpt-4");
    expect(insert.values.status).toBe("running");
    expect(insert.values.sessionId).toBe("ses_test");
    expect(insert.values.tenantId).toBe("tnt_test");
    expect(insert.values.startedAt).toBeInstanceOf(Date);

    const parts = fakeDB.inserts.filter((i) => i.table === "turn_part");
    expect(parts).toHaveLength(1);
    expect(at(parts, 0).values.type).toBe("step-start");
    expect(at(parts, 0).values.metadata).toEqual({ model: "gpt-4" });
  });

  test("reactor.start alone does not create a turn", async () => {
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

    const parts = fakeDB.inserts.filter((i) => i.table === "turn_part");
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

    const parts = fakeDB.inserts.filter((i) => i.table === "turn_part");
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

  test("reactor.done marks turn as completed with endedAt", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(event("reactor.done", 10, {}));

    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).table).toBe("inference_turn");
    expect(at(fakeDB.updates, 0).set.status).toBe("completed");
    expect(at(fakeDB.updates, 0).set.endedAt).toBeInstanceOf(Date);
  });

  test("reactor.error with fatal=true marks turn as failed", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(
      event("reactor.error", 10, { error: "boom", fatal: true }),
    );

    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).table).toBe("inference_turn");
    expect(at(fakeDB.updates, 0).set.status).toBe("failed");
    expect(at(fakeDB.updates, 0).set.endedAt).toBeInstanceOf(Date);
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

    const parts = fakeDB.inserts.filter((i) => i.table === "turn_part");
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

    const parts = fakeDB.inserts.filter((i) => i.table === "turn_part");
    const ordinals = parts.map((p) => p.values.ordinal);
    // step-start (0), text (1), reasoning (2), step-finish (3)
    expect(ordinals).toEqual([0, 1, 2, 3]);
  });

  test("abandon marks pending turn as failed", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.abandon();

    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).table).toBe("inference_turn");
    expect(at(fakeDB.updates, 0).set.status).toBe("failed");
    expect(at(fakeDB.updates, 0).set.endedAt).toBeInstanceOf(Date);
  });

  test("abandon with no active turn is a no-op", async () => {
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

    const parts = fakeDB.inserts.filter((i) => i.table === "turn_part");
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

    const parts = fakeDB.inserts.filter((i) => i.table === "turn_part");
    // Only the step-start from inference.start; deltas are not persisted
    expect(parts).toHaveLength(1);
    expect(at(parts, 0).values.type).toBe("step-start");
  });

  test("full reactor cycle produces correct sequence with per-turn rows", async () => {
    // Simulate: inference turn 1 (text + tool call) ->
    // tool result -> inference turn 2 (text) -> reactor.done
    //
    // Each inference.start creates a new turn. The second inference.start
    // finalizes the first turn as failed (via the orphan-guard path) and
    // starts a fresh one.

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

    // Turn 2 — inference.start finalizes turn 1 as completed
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

    // Two turn rows created (one per inference.start)
    const turns = fakeDB.inserts.filter((i) => i.table === "inference_turn");
    expect(turns).toHaveLength(2);

    const parts = fakeDB.inserts.filter((i) => i.table === "turn_part");
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

    // Both turns finalized as completed
    expect(fakeDB.updates).toHaveLength(2);
    expect(at(fakeDB.updates, 0).set.status).toBe("completed");
    expect(at(fakeDB.updates, 1).set.status).toBe("completed");
  });

  test("second inference.start finalizes first turn as completed", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(event("inference.start", 5, { model: "gpt-4" }));

    // Two turn rows created
    const turns = fakeDB.inserts.filter((i) => i.table === "inference_turn");
    expect(turns).toHaveLength(2);

    // First turn finalized as completed (superseded by next inference step)
    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).set.status).toBe("completed");
  });

  test("abandon after reactor.done is a no-op", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(event("reactor.done", 5, {}));
    await collector.abandon();

    // Only one update from reactor.done, not two
    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).set.status).toBe("completed");
  });

  test("fatal reactor.error with no active turn creates a turn and inserts error part", async () => {
    await collector.onEvent(
      event("reactor.error", 1, {
        error: "context store unavailable",
        fatal: true,
      }),
    );

    const turns = fakeDB.inserts.filter((i) => i.table === "inference_turn");
    expect(turns).toHaveLength(1);
    expect(at(turns, 0).values.model).toBe("unknown");
    expect(at(turns, 0).values.status).toBe("running");

    const parts = fakeDB.inserts.filter((i) => i.table === "turn_part");
    expect(parts).toHaveLength(1);
    expect(at(parts, 0).values.type).toBe("error");
    expect(at(parts, 0).values.content).toBe("context store unavailable");
    expect(at(parts, 0).values.metadata).toEqual({ category: "reactor_error" });

    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).table).toBe("inference_turn");
    expect(at(fakeDB.updates, 0).set.status).toBe("failed");
  });

  test("fatal reactor.error inserts error part before finalizing", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(
      event("reactor.error", 5, { error: "fatal failure", fatal: true }),
    );

    const parts = fakeDB.inserts.filter((i) => i.table === "turn_part");
    const types = parts.map((p) => p.values.type);
    expect(types).toContain("error");

    const errorPart = parts.find((p) => p.values.type === "error");
    if (errorPart === undefined) throw new Error("Expected an error part");
    expect(errorPart.values.content).toBe("fatal failure");
    expect(errorPart.values.metadata).toEqual({ category: "reactor_error" });

    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).set.status).toBe("failed");
  });

  test("inference.error persists error part and connector.reply persists text", async () => {
    await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
    await collector.onEvent(
      event("inference.error", 3, {
        error: {
          message: "rate limit exceeded",
          category: "rate_limit",
          statusCode: 429,
        },
      }),
    );
    await collector.onEvent(
      event("connector.reply", 4, { content: "I encountered an error." }),
    );

    const parts = fakeDB.inserts.filter((i) => i.table === "turn_part");
    const types = parts.map((p) => p.values.type);
    expect(types).toEqual(["step-start", "error", "text"]);

    expect(at(parts, 1).values.content).toBe("rate limit exceeded");
    expect(at(parts, 1).values.metadata).toEqual({
      category: "rate_limit",
      statusCode: 429,
    });
    expect(at(parts, 2).values.content).toBe("I encountered an error.");

    expect(fakeDB.updates).toHaveLength(1);
    expect(at(fakeDB.updates, 0).set.status).toBe("completed");
  });

  describe("onTurnFinalized callback", () => {
    let notifications: TurnFinalized[];
    let notifyCollector: EventCollector;

    beforeEach(() => {
      notifications = [];
      notifyCollector = createEventCollector({
        db: fakeDB.db,
        sessionId: "ses_test",
        instanceId: "ins_test",
        tenantId: "tnt_test",
        onTurnFinalized: (turn) => notifications.push(turn),
      });
    });

    test("reactor.done fires callback with accumulated text", async () => {
      await notifyCollector.onEvent(
        event("inference.start", 1, { model: "gpt-4" }),
      );
      await notifyCollector.onEvent(
        event("inference.done", 5, {
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Hello world" }],
            model: "gpt-4",
          },
          usage: { input: 10, output: 20 },
        }),
      );
      await notifyCollector.onEvent(event("reactor.done", 10, {}));

      expect(notifications).toHaveLength(1);
      expect(at(notifications, 0).status).toBe("completed");
      expect(at(notifications, 0).text).toBe("Hello world");
      expect(at(notifications, 0).hadError).toBe(false);
    });

    test("callback excludes thinking blocks from text", async () => {
      await notifyCollector.onEvent(
        event("inference.start", 1, { model: "gpt-4" }),
      );
      await notifyCollector.onEvent(
        event("inference.done", 5, {
          message: {
            role: "assistant",
            content: [
              { type: "thinking", thinking: "Let me think..." },
              { type: "text", text: "The answer" },
            ],
            model: "gpt-4",
          },
          usage: { input: 10, output: 20 },
        }),
      );
      await notifyCollector.onEvent(event("reactor.done", 10, {}));

      expect(notifications).toHaveLength(1);
      expect(at(notifications, 0).text).toBe("The answer");
    });

    test("multi-step tool loop fires callback for each turn", async () => {
      await notifyCollector.onEvent(
        event("inference.start", 1, { model: "gpt-4" }),
      );
      await notifyCollector.onEvent(
        event("inference.done", 3, {
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Searching" }],
            model: "gpt-4",
          },
          usage: { input: 10, output: 5 },
        }),
      );
      // Second inference.start finalizes the first turn
      await notifyCollector.onEvent(
        event("inference.start", 5, { model: "gpt-4" }),
      );
      await notifyCollector.onEvent(
        event("inference.done", 8, {
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Results" }],
            model: "gpt-4",
          },
          usage: { input: 20, output: 10 },
        }),
      );
      await notifyCollector.onEvent(event("reactor.done", 10, {}));

      expect(notifications).toHaveLength(2);
      expect(at(notifications, 0).text).toBe("Searching");
      expect(at(notifications, 1).text).toBe("Results");
    });

    test("abandon does not fire callback", async () => {
      await notifyCollector.onEvent(
        event("inference.start", 1, { model: "gpt-4" }),
      );
      await notifyCollector.onEvent(
        event("inference.done", 5, {
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Some text" }],
            model: "gpt-4",
          },
          usage: { input: 10, output: 20 },
        }),
      );
      await notifyCollector.abandon();

      expect(notifications).toHaveLength(0);
    });

    test("error path includes connector.reply content and sets hadError", async () => {
      await notifyCollector.onEvent(
        event("inference.start", 1, { model: "gpt-4" }),
      );
      await notifyCollector.onEvent(
        event("inference.error", 3, {
          error: { message: "rate limit", category: "rate_limit" },
        }),
      );
      await notifyCollector.onEvent(
        event("connector.reply", 4, { content: "I encountered an error." }),
      );

      expect(notifications).toHaveLength(1);
      expect(at(notifications, 0).status).toBe("completed");
      expect(at(notifications, 0).text).toBe("I encountered an error.");
      expect(at(notifications, 0).hadError).toBe(true);
    });

    test("tool-only turn with no text fires callback with empty text", async () => {
      await notifyCollector.onEvent(
        event("inference.start", 1, { model: "gpt-4" }),
      );
      await notifyCollector.onEvent(
        event("inference.done", 3, {
          message: {
            role: "assistant",
            content: [
              {
                type: "tool_call",
                id: "call_1",
                name: "search",
                arguments: { q: "test" },
              },
            ],
            model: "gpt-4",
          },
          usage: { input: 10, output: 5 },
        }),
      );
      await notifyCollector.onEvent(event("reactor.done", 10, {}));

      expect(notifications).toHaveLength(1);
      expect(at(notifications, 0).text).toBe("");
    });

    test("fatal reactor.error fires callback with failed status", async () => {
      await notifyCollector.onEvent(
        event("inference.start", 1, { model: "gpt-4" }),
      );
      await notifyCollector.onEvent(
        event("reactor.error", 5, { error: "boom", fatal: true }),
      );

      expect(notifications).toHaveLength(1);
      expect(at(notifications, 0).status).toBe("failed");
    });

    test("fatal reactor.error with no active turn includes error in TurnFinalized", async () => {
      await notifyCollector.onEvent(
        event("reactor.error", 1, {
          error: "context store unavailable",
          fatal: true,
        }),
      );

      expect(notifications).toHaveLength(1);
      expect(at(notifications, 0).status).toBe("failed");
      expect(at(notifications, 0).errors).toEqual([
        { category: "reactor_error", message: "context store unavailable" },
      ]);
    });

    test("TurnFinalized includes accumulated errors", async () => {
      await notifyCollector.onEvent(
        event("inference.start", 1, { model: "gpt-4" }),
      );
      await notifyCollector.onEvent(
        event("inference.error", 3, {
          error: { message: "rate limit exceeded", category: "rate_limit" },
        }),
      );
      await notifyCollector.onEvent(
        event("connector.reply", 4, { content: "I hit a rate limit." }),
      );

      expect(notifications).toHaveLength(1);
      expect(at(notifications, 0).errors).toEqual([
        { category: "rate_limit", message: "rate limit exceeded" },
      ]);
    });

    test("TurnFinalized includes tool errors from failed tool results", async () => {
      await notifyCollector.onEvent(
        event("inference.start", 1, { model: "gpt-4" }),
      );
      await notifyCollector.onEvent(
        event("inference.done", 3, {
          message: {
            role: "assistant",
            content: [
              {
                type: "tool_call",
                id: "call_1",
                name: "file_read",
                arguments: { path: "/etc/shadow" },
              },
            ],
            model: "gpt-4",
          },
          usage: { input: 10, output: 5 },
        }),
      );
      await notifyCollector.onEvent(
        event("tool.done", 5, {
          result: {
            callId: "call_1",
            content: "Permission denied",
            isError: true,
          },
        }),
      );
      await notifyCollector.onEvent(event("reactor.done", 10, {}));

      expect(notifications).toHaveLength(1);
      expect(at(notifications, 0).toolErrors).toEqual([
        { name: "file_read", content: "Permission denied" },
      ]);
    });

    test("TurnFinalized includes non-fatal reactor errors", async () => {
      await notifyCollector.onEvent(
        event("inference.start", 1, { model: "gpt-4" }),
      );
      await notifyCollector.onEvent(
        event("reactor.error", 3, {
          error: "checkpoint failed",
          fatal: false,
        }),
      );
      await notifyCollector.onEvent(
        event("inference.done", 5, {
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Continuing" }],
            model: "gpt-4",
          },
          usage: { input: 5, output: 5 },
        }),
      );
      await notifyCollector.onEvent(event("reactor.done", 10, {}));

      expect(notifications).toHaveLength(1);
      expect(at(notifications, 0).errors).toEqual([
        { category: "reactor_error", message: "checkpoint failed" },
      ]);
      expect(at(notifications, 0).hadError).toBe(true);
    });

    test("non-fatal reactor.error with no active turn creates a turn and surfaces error", async () => {
      await notifyCollector.onEvent(
        event("reactor.error", 1, {
          error: "checkpoint hook failed",
          fatal: false,
        }),
      );
      // The non-fatal error created a synthetic turn. The next
      // inference.start auto-finalizes it before starting a new turn.
      await notifyCollector.onEvent(
        event("inference.start", 5, { model: "gpt-4" }),
      );

      expect(notifications).toHaveLength(1);
      expect(at(notifications, 0).status).toBe("completed");
      expect(at(notifications, 0).hadError).toBe(true);
      expect(at(notifications, 0).errors).toEqual([
        { category: "reactor_error", message: "checkpoint hook failed" },
      ]);
    });

    test("TurnFinalized errors array is empty when no errors occurred", async () => {
      await notifyCollector.onEvent(
        event("inference.start", 1, { model: "gpt-4" }),
      );
      await notifyCollector.onEvent(
        event("inference.done", 5, {
          message: {
            role: "assistant",
            content: [{ type: "text", text: "All good" }],
            model: "gpt-4",
          },
          usage: { input: 5, output: 5 },
        }),
      );
      await notifyCollector.onEvent(event("reactor.done", 10, {}));

      expect(notifications).toHaveLength(1);
      expect(at(notifications, 0).errors).toEqual([]);
    });
  });

  describe("getAccumulatedText", () => {
    test("returns empty string before any events", () => {
      expect(collector.getAccumulatedText()).toBe("");
    });

    test("returns accumulated text after inference.done events with text blocks", async () => {
      await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
      await collector.onEvent(
        event("inference.done", 5, {
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Hello " }],
            model: "gpt-4",
          },
          usage: { input: 10, output: 5 },
        }),
      );
      await collector.onEvent(
        event("inference.done", 6, {
          message: {
            role: "assistant",
            content: [{ type: "text", text: "world" }],
            model: "gpt-4",
          },
          usage: { input: 10, output: 5 },
        }),
      );

      expect(collector.getAccumulatedText()).toBe("Hello world");
    });

    test("resets to empty string after a new inference.start event", async () => {
      await collector.onEvent(event("inference.start", 1, { model: "gpt-4" }));
      await collector.onEvent(
        event("inference.done", 5, {
          message: {
            role: "assistant",
            content: [{ type: "text", text: "First turn text" }],
            model: "gpt-4",
          },
          usage: { input: 10, output: 5 },
        }),
      );
      expect(collector.getAccumulatedText()).toBe("First turn text");

      await collector.onEvent(event("inference.start", 8, { model: "gpt-4" }));
      expect(collector.getAccumulatedText()).toBe("");
    });
  });
});

describe("EventCollectorRegistry getAccumulatedText", () => {
  function createFakeDBForRegistry() {
    const db = {
      insert(_table: unknown) {
        return {
          values(_vals: Record<string, unknown>) {
            return Promise.resolve();
          },
        };
      },
      update(_table: unknown) {
        return {
          set(_vals: Record<string, unknown>) {
            return {
              where(_condition: unknown) {
                return Promise.resolve();
              },
            };
          },
        };
      },
    };
    return { db: db as never };
  }

  test("returns undefined for unknown agent address", () => {
    const { db } = createFakeDBForRegistry();
    const registry = createEventCollectorRegistry({ db });
    expect(registry.getAccumulatedText("agent://unknown")).toBeUndefined();
  });

  test("returns accumulated text from the collector for a known address", async () => {
    const { db } = createFakeDBForRegistry();
    const registry = createEventCollectorRegistry({ db });
    const address = "agent://test-instance";

    registry.create(address, "tnt_test", "ses_test", "ins_test");

    registry.dispatch(address, event("inference.start", 1, { model: "gpt-4" }));
    // dispatch is fire-and-forget; wait for the microtask queue to flush
    await Promise.resolve();

    registry.dispatch(
      address,
      event("inference.done", 5, {
        message: {
          role: "assistant",
          content: [{ type: "text", text: "streaming text" }],
          model: "gpt-4",
        },
        usage: { input: 10, output: 5 },
      }),
    );
    await Promise.resolve();

    expect(registry.getAccumulatedText(address)).toBe("streaming text");
  });
});
