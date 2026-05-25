// End-to-end replay of the committed example session captures.
//
// These tests are the integration proof that recording → replay
// round-trips through production `runInference`. Each test loads a
// committed session, drives every captured turn through production
// `runInference` (using the replay harness's body-aware matchers and
// captured dispatch results), and asserts orchestration invariants
// hold: the conversation length grows as the capture grew, dispatch
// results land in the correct subsequent exchange request bodies, the
// terminal event sequence for each turn validates against the shape
// invariants declared by INTR-79's compat-replay layer.

import { afterEach, describe, expect, test } from "bun:test";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  INVARIANTS,
  createReplayHarness,
  type CapturedDispatch,
  type ReplayHarness,
} from "@intx/inference-testing";
import type { ConversationTurn, InferenceEvent } from "@intx/types/runtime";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const SESSIONS_ROOT = path.resolve(
  __dirname,
  "..",
  "..",
  "packages",
  "inference-testing",
  "sessions",
);

let activeReplay: ReplayHarness | null = null;
afterEach(() => {
  if (activeReplay !== null) {
    activeReplay.dispose();
    activeReplay = null;
  }
});

function userTurn(text: string): ConversationTurn {
  return {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: 0,
  };
}

// Build the tool_result content array that should be threaded into the
// next user turn. Each captured dispatch's args carry the call_id-like
// identity (the matched `tool_call` block's id), and the result is
// served verbatim — but `tool_result.content` only accepts the narrow
// media block set, so we render the result to a JSON string text block.
function toolResultBlocks(
  callIds: readonly string[],
  results: readonly unknown[],
): ConversationTurn["content"] {
  if (callIds.length !== results.length) {
    throw new Error(
      `tool_result mismatch: ${String(callIds.length)} call ids vs ${String(results.length)} results`,
    );
  }
  return callIds.map((callId, i) => ({
    type: "tool_result",
    callId,
    content: [{ type: "text", text: JSON.stringify(results[i]) }],
  }));
}

function extractToolCallIds(turn: ConversationTurn): string[] {
  const ids: string[] = [];
  for (const block of turn.content) {
    if (block.type === "tool_call") ids.push(block.id);
  }
  return ids;
}

function dispatchesUsedByTurn(
  remaining: CapturedDispatch[],
  count: number,
): { used: CapturedDispatch[]; rest: CapturedDispatch[] } {
  const used = remaining.slice(0, count);
  if (used.length !== count) {
    throw new Error(
      `expected ${String(count)} captured dispatches, only ${String(used.length)} remain`,
    );
  }
  return { used, rest: remaining.slice(count) };
}

function validateInvariants(events: InferenceEvent[]): void {
  for (const invariant of INVARIANTS) {
    const violations = invariant.check(events);
    if (violations.length > 0) {
      throw new Error(
        `invariant "${invariant.name}" violated: ${violations
          .map((v) => v.message)
          .join("; ")}`,
      );
    }
  }
}

describe("session replay integration", () => {
  test("anthropic-tool-roundtrip replays end-to-end", async () => {
    const sessionDir = path.join(SESSIONS_ROOT, "anthropic-tool-roundtrip");
    const replay = await createReplayHarness({ sessionDir });
    activeReplay = replay;

    expect(replay.capturedExchanges).toHaveLength(2);
    expect(replay.capturedDispatches).toHaveLength(1);

    const initialUser = userTurn("What is the weather in SF?");
    let conversation: ConversationTurn[] = [initialUser];
    let remainingDispatches: CapturedDispatch[] = [
      ...replay.capturedDispatches,
    ];
    const perTurnEvents: InferenceEvent[][] = [];

    // Turn 1: model emits tool_use.
    const turn1Events = await replay.runTurn({ turns: conversation });
    perTurnEvents.push(turn1Events);
    validateInvariants(turn1Events);
    const turn1Done = turn1Events.find((e) => e.type === "inference.done");
    if (turn1Done === undefined || turn1Done.type !== "inference.done") {
      throw new Error("expected inference.done in turn 1");
    }
    const turn1CallIds = extractToolCallIds(turn1Done.data.turn);
    expect(turn1CallIds).toHaveLength(1);
    const { used: usedDispatches1, rest } = dispatchesUsedByTurn(
      remainingDispatches,
      turn1CallIds.length,
    );
    remainingDispatches = rest;
    conversation = [
      ...conversation,
      turn1Done.data.turn,
      {
        role: "user",
        content: toolResultBlocks(
          turn1CallIds,
          usedDispatches1.map((d) => d.result),
        ),
        timestamp: 0,
      },
    ];

    // Turn 2: model integrates the tool result into final text.
    const turn2Events = await replay.runTurn({ turns: conversation });
    perTurnEvents.push(turn2Events);
    validateInvariants(turn2Events);
    const turn2Done = turn2Events.find((e) => e.type === "inference.done");
    if (turn2Done === undefined || turn2Done.type !== "inference.done") {
      throw new Error("expected inference.done in turn 2");
    }
    expect(extractToolCallIds(turn2Done.data.turn)).toHaveLength(0);

    replay.assertFullyConsumed();
    expect(perTurnEvents).toHaveLength(replay.capturedExchanges.length);
    expect(remainingDispatches).toHaveLength(0);
  });

  test("anthropic-multi-tool-multi-turn replays end-to-end with cross-turn context", async () => {
    const sessionDir = path.join(
      SESSIONS_ROOT,
      "anthropic-multi-tool-multi-turn",
    );
    const replay = await createReplayHarness({ sessionDir });
    activeReplay = replay;

    expect(replay.capturedExchanges).toHaveLength(4);
    expect(replay.capturedDispatches).toHaveLength(3);

    let conversation: ConversationTurn[] = [
      userTurn("Weather in SF and the time in UTC?"),
    ];
    let remainingDispatches: CapturedDispatch[] = [
      ...replay.capturedDispatches,
    ];
    const perTurnEvents: InferenceEvent[][] = [];

    // Turn 1: two parallel tool_calls.
    const t1 = await replay.runTurn({ turns: conversation });
    perTurnEvents.push(t1);
    validateInvariants(t1);
    const t1Done = t1.find((e) => e.type === "inference.done");
    if (t1Done === undefined || t1Done.type !== "inference.done") {
      throw new Error("expected inference.done in turn 1");
    }
    const t1CallIds = extractToolCallIds(t1Done.data.turn);
    expect(t1CallIds).toHaveLength(2);
    const t1Use = dispatchesUsedByTurn(remainingDispatches, t1CallIds.length);
    remainingDispatches = t1Use.rest;
    conversation = [
      ...conversation,
      t1Done.data.turn,
      {
        role: "user",
        content: toolResultBlocks(
          t1CallIds,
          t1Use.used.map((d) => d.result),
        ),
        timestamp: 0,
      },
    ];

    // Turn 2: model summary.
    const t2 = await replay.runTurn({ turns: conversation });
    perTurnEvents.push(t2);
    validateInvariants(t2);
    const t2Done = t2.find((e) => e.type === "inference.done");
    if (t2Done === undefined || t2Done.type !== "inference.done") {
      throw new Error("expected inference.done in turn 2");
    }
    expect(extractToolCallIds(t2Done.data.turn)).toHaveLength(0);
    conversation = [
      ...conversation,
      t2Done.data.turn,
      userTurn("What about NYC?"),
    ];

    // Turn 3: model emits the third tool call.
    const t3 = await replay.runTurn({ turns: conversation });
    perTurnEvents.push(t3);
    validateInvariants(t3);
    const t3Done = t3.find((e) => e.type === "inference.done");
    if (t3Done === undefined || t3Done.type !== "inference.done") {
      throw new Error("expected inference.done in turn 3");
    }
    const t3CallIds = extractToolCallIds(t3Done.data.turn);
    expect(t3CallIds).toHaveLength(1);
    const t3Use = dispatchesUsedByTurn(remainingDispatches, t3CallIds.length);
    remainingDispatches = t3Use.rest;
    conversation = [
      ...conversation,
      t3Done.data.turn,
      {
        role: "user",
        content: toolResultBlocks(
          t3CallIds,
          t3Use.used.map((d) => d.result),
        ),
        timestamp: 0,
      },
    ];

    // Turn 4: final text.
    const t4 = await replay.runTurn({ turns: conversation });
    perTurnEvents.push(t4);
    validateInvariants(t4);
    const t4Done = t4.find((e) => e.type === "inference.done");
    if (t4Done === undefined || t4Done.type !== "inference.done") {
      throw new Error("expected inference.done in turn 4");
    }
    expect(extractToolCallIds(t4Done.data.turn)).toHaveLength(0);

    replay.assertFullyConsumed();
    expect(perTurnEvents).toHaveLength(replay.capturedExchanges.length);
    expect(remainingDispatches).toHaveLength(0);

    // Cross-turn invariant: conversation length grew monotonically.
    // The replay's body-aware matchers already enforce per-turn body
    // equality with capture, so if any turn's reconstructed request
    // body had failed to thread the previous tool_results, the matcher
    // would have refused to fire and `SessionReplayMismatchError`
    // would have surfaced from `runTurn`.
    expect(conversation.length).toBeGreaterThan(1);
  });
});
