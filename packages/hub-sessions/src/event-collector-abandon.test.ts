// The hub collector fed by a real reactor through an abandon: the
// inference_turn row of the abandoned turn must be finalized when the turn
// ends, not left running for the next turn's inference.start to complete.

import { describe, expect, test } from "bun:test";

import { inferenceTurn, turnPart } from "@intx/db/schema";
import {
  createReactor,
  type InferenceHarnessOptions,
  type ReactorEmittedEvent,
} from "@intx/inference";
import { createDefaultDependencies } from "@intx/inference/providers";
import { createInboundMessage } from "@intx/mime";
import type { InferenceEvent, ToolCall } from "@intx/types/runtime";
import { waitUntil } from "@intx/types/testing";

import { createEventCollector } from "./event-collector";
import {
  emptyStore,
  emptyUsage,
  testSource,
} from "./event-collector.test-helpers";

// A database that records the status of each inference_turn update.
function recordingDb() {
  const statuses: unknown[] = [];
  const db = {
    insert() {
      return { values: () => Promise.resolve() };
    },
    update(table: unknown) {
      return {
        set(values: Record<string, unknown>) {
          return {
            where() {
              if (table === inferenceTurn) statuses.push(values.status);
              else if (table !== turnPart) throw new Error("unexpected table");
              return Promise.resolve();
            },
          };
        },
      };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the fake satisfies the shape the collector uses at runtime
  return { db: db as never, statuses };
}

describe("hub collector and an abandoned turn", () => {
  test("the row of an abandoned turn is finalized failed, and the next turn does not complete it", async () => {
    const { db, statuses } = recordingDb();
    const collector = createEventCollector({
      db,
      sessionId: "ses_test",
      runId: "run_test",
      tenantId: "tnt_test",
    });
    const events: ReactorEmittedEvent[] = [];
    let persisted: Promise<void> = Promise.resolve();
    const reactor = createReactor({
      sessionId: "abandon-collector",
      director: {
        async decide(e, _s, caps) {
          if (e.type === "message.received") return caps.infer();
          if (e.type === "inference.done") return caps.reply("hi");
          return caps.wait();
        },
      },
      source: testSource,
      toolRunner: {
        async run(call) {
          return { callId: call.id, content: "ok" };
        },
      },
      contextStore: emptyStore(),
      deps: createDefaultDependencies(),
      shutdownTimeoutMs: 100,
      onEvent: (e) => {
        events.push(e);
        if (e.type !== "message.received") {
          persisted = persisted.then(() => collector.onEvent(e));
        }
      },
      // A "slow" message infers until its signal aborts; any other message
      // answers at once.
      inferenceRunner: async function* (
        opts: InferenceHarnessOptions,
      ): AsyncGenerator<InferenceEvent> {
        yield {
          type: "inference.start",
          seq: opts.nextSeq(),
          data: { model: "m" },
        };
        const signal = opts.signal;
        if (signal === undefined) throw new Error("expected a signal");
        const block = opts.turns.at(-1)?.content[0];
        const text = block?.type === "text" ? block.text : "";
        if (text.endsWith("slow")) {
          await new Promise<boolean>((resolve) => {
            signal.addEventListener("abort", () => resolve(true), {
              once: true,
            });
          });
          yield {
            type: "inference.error",
            seq: opts.nextSeq(),
            data: {
              error: { category: "aborted", message: "aborted" },
              partial: { text: "" },
            },
          };
          return;
        }
        yield {
          type: "inference.done",
          seq: opts.nextSeq(),
          data: {
            turn: {
              role: "assistant",
              content: [{ type: "text", text: "hi" }],
              model: "m",
              timestamp: Date.now(),
            },
            usage: emptyUsage(),
            source: { sourceId: "s", provider: "anthropic", model: "m" },
          },
        };
      },
    });
    const message = (content: string) =>
      createInboundMessage({ from: "a@local", to: "b@local", content });
    const ended = (turnId: string) =>
      events.some(
        (e) => e.type === "reactor.turn.ended" && e.data.turnId === turnId,
      );

    reactor.start();
    reactor.deliver(message("slow"), { turnId: "turn-a" });
    await waitUntil(() => events.some((e) => e.type === "inference.start"));
    reactor.abandonTurn("turn-a");
    await waitUntil(() => ended("turn-a"));
    await persisted;
    expect(statuses).toEqual(["failed"]);

    reactor.deliver(message("fast"), { turnId: "turn-b" });
    await waitUntil(() => ended("turn-b"));
    await persisted;
    expect(statuses).toEqual(["failed", "completed"]);

    reactor.abort("admin_kill");
    await waitUntil(() => events.some((e) => e.type === "reactor.done"));
  });

  test("a parked turn's row stays open through another turn and is finalized failed when the turn is abandoned", async () => {
    // Turn A parks its ask call. Turn B infers, replies and ends. A's row
    // stays open through B, and A's abandon finalizes it.
    const { db, statuses } = recordingDb();
    const collector = createEventCollector({
      db,
      sessionId: "ses_test",
      runId: "run_test",
      tenantId: "tnt_test",
    });
    const events: ReactorEmittedEvent[] = [];
    let persisted: Promise<void> = Promise.resolve();
    const releaseB = Promise.withResolvers<boolean>();
    let inferences = 0;
    const askCall: ToolCall = { id: "c-ask", name: "ask", arguments: {} };
    const reactor = createReactor({
      sessionId: "interleave-collector",
      director: {
        async decide(e, _s, caps) {
          if (e.type === "message.received") return caps.infer();
          if (e.type === "inference.done") {
            const calls: ToolCall[] = [];
            for (const block of e.turn.content) {
              if (block.type === "tool_call") {
                calls.push({ id: block.id, name: block.name, arguments: {} });
              }
            }
            return calls.length > 0
              ? caps.executeTools(calls)
              : caps.reply("done");
          }
          return caps.wait();
        },
      },
      beforeToolExtensions: [
        {
          async beforeTool(call, _state, _signal, ctx) {
            if (call.name !== "ask" || ctx.approved) return { type: "allow" };
            return {
              type: "suspend",
              gate: {
                type: "approval",
                gateId: "g-ask",
                correlationId: "corr-ask",
                timeoutAt: Date.now() + 60_000,
              },
              pendingOp: {
                correlationId: "corr-ask",
                kind: "approval",
                registeredAt: Date.now(),
                gateId: "g-ask",
                suspendedCall: call,
              },
            };
          },
        },
      ],
      source: testSource,
      toolRunner: {
        async run(call) {
          return { callId: call.id, content: "ok" };
        },
      },
      contextStore: emptyStore(),
      deps: createDefaultDependencies(),
      shutdownTimeoutMs: 100,
      onEvent: (e) => {
        events.push(e);
        if (e.type !== "message.received") {
          persisted = persisted.then(() => collector.onEvent(e));
        }
      },
      inferenceRunner: async function* (
        opts: InferenceHarnessOptions,
      ): AsyncGenerator<InferenceEvent> {
        inferences += 1;
        const current = inferences;
        yield {
          type: "inference.start",
          seq: opts.nextSeq(),
          data: { model: "m" },
        };
        if (current === 2) await releaseB.promise;
        yield {
          type: "inference.done",
          seq: opts.nextSeq(),
          data: {
            turn: {
              role: "assistant",
              content:
                current === 1
                  ? [{ type: "tool_call", ...askCall }]
                  : [{ type: "text", text: "b" }],
              model: "m",
              timestamp: Date.now(),
            },
            usage: emptyUsage(),
            source: { sourceId: "s", provider: "anthropic", model: "m" },
          },
        };
      },
    });
    const message = (content: string) =>
      createInboundMessage({ from: "a@local", to: "b@local", content });
    const ended = (turnId: string) =>
      events.some(
        (e) => e.type === "reactor.turn.ended" && e.data.turnId === turnId,
      );

    reactor.start();
    reactor.deliver(message("go"), { turnId: "turn-a" });
    await waitUntil(() =>
      events.some((e) => e.type === "reactor.gate.blocked"),
    );
    reactor.deliver(message("B"), { turnId: "turn-b" });
    await waitUntil(() => inferences === 2);
    releaseB.resolve(true);
    await waitUntil(() => ended("turn-b"));
    await persisted;
    expect(statuses).toEqual(["completed"]);

    reactor.abandonTurn("turn-a");
    await waitUntil(() => ended("turn-a"));
    await persisted;
    expect(statuses).toEqual(["completed", "failed"]);

    reactor.abort("admin_kill");
    await waitUntil(() => events.some((e) => e.type === "reactor.done"));
  });
});
