// End-to-end multi-turn proof against the captured Anthropic
// `function-calling-with-thinking-streaming` corpus. The test
// chains turn-1's response through the harness, takes the
// finalized assistant turn, builds turn-2's request from it via
// the production request builder, and asserts the rebuilt body
// carries every wire-required field byte-identical to the
// captured turn-2 request.json — most importantly the thinking
// block's cryptographic signature, which Anthropic rejects with
// `messages.N.content.M.thinking.signature: Field required` if it
// drifts.
//
// The corpus has thinking@0, text@1, tool_use@2 in the assistant
// response. The post-task-22 per-index harness preserves that
// ordering through the final turn. The post-task-17 request
// builder echoes the thinking signature back. This test is the
// integration proof that those two pieces line up against real
// captured wire bytes.

import { promises as fs } from "node:fs";
import * as path from "node:path";

import { describe, expect, test } from "bun:test";

import {
  createAnthropicAdapter,
  runInference,
  type Dependencies,
  type Scheduler,
} from "@intx/inference";
import type {
  AssistantTurn,
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

const WORKSPACE_ROOT = path.resolve(__dirname, "../..");
const CORPUS_ROOT = path.join(
  WORKSPACE_ROOT,
  "packages/inference-testing/wire/anthropic/claude-haiku-4-5-20251001/function-calling-with-thinking-streaming",
);

const SOURCE: InferenceSource = {
  id: "anthropic:claude-haiku-4-5-20251001",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "test",
  model: "claude-haiku-4-5-20251001",
};

const inertScheduler: Scheduler = {
  setTimeout: () => () => {
    /* tests do not exercise timer firing */
  },
  now: () => 0,
};

async function drain(
  stream: AsyncIterable<InferenceEvent>,
): Promise<InferenceEvent[]> {
  const out: InferenceEvent[] = [];
  for await (const event of stream) {
    out.push(event);
  }
  return out;
}

function streamingFetchFromBytes(sseBytes: Uint8Array): Dependencies["fetch"] {
  return () => {
    return Promise.resolve(
      new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(sseBytes);
            controller.close();
          },
        }),
        { status: 200, headers: { "content-type": "text/event-stream" } },
      ),
    );
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

describe("multi-turn integration: function-calling-with-thinking-streaming", () => {
  test("turn-1 → turn-2 round-trip carries the thinking signature byte-identical", async () => {
    // 1. Load turn-1's captured response.sse and turn-1's request.json
    //    (to seed the initial user turn for the harness's request
    //    building — we won't actually rebuild turn-1's request here,
    //    just consume its response).
    const turn1Sse = await fs.readFile(
      path.join(CORPUS_ROOT, "turn-1/response.sse"),
    );
    const turn1ReqRaw = await fs.readFile(
      path.join(CORPUS_ROOT, "turn-1/request.json"),
      "utf-8",
    );
    const turn2ReqRaw = await fs.readFile(
      path.join(CORPUS_ROOT, "turn-2/request.json"),
      "utf-8",
    );
    const turn1ReqUnknown: unknown = JSON.parse(turn1ReqRaw);
    const turn2ReqUnknown: unknown = JSON.parse(turn2ReqRaw);
    if (!isRecord(turn1ReqUnknown) || !isRecord(turn2ReqUnknown)) {
      throw new Error("expected request.json files to parse as objects");
    }

    // 2. Replay turn-1 through the harness.
    const deps: Dependencies = {
      fetch: streamingFetchFromBytes(turn1Sse),
      scheduler: inertScheduler,
    };
    let seq = 0;
    const events = await drain(
      runInference({
        turns: [
          {
            role: "user",
            content: [
              { type: "text", text: "What's the weather in Boston, MA?" },
            ],
            timestamp: 0,
          },
        ],
        source: SOURCE,
        nextSeq: () => seq++,
        deps,
      }),
    );

    // 3. Extract the final assistant turn from inference.done.
    const done = events.find((e) => e.type === "inference.done");
    if (done?.type !== "inference.done") {
      const err = events.find((e) => e.type === "inference.error");
      throw new Error(
        `turn-1 replay did not reach inference.done${err?.type === "inference.error" ? `: ${JSON.stringify(err.data.error).slice(0, 300)}` : ""}`,
      );
    }
    const reconstructed: AssistantTurn = done.data.turn;

    // 4. Sanity: the reconstructed turn has the expected block
    //    structure (thinking → text → tool_call). This is the
    //    function-calling-with-thinking-streaming corpus shape.
    const kinds = reconstructed.content.map((b) => b.type);
    expect(kinds).toEqual(["thinking", "text", "tool_call"]);

    const thinkingBlock = reconstructed.content[0];
    const textBlock = reconstructed.content[1];
    const toolCallBlock = reconstructed.content[2];
    if (
      thinkingBlock?.type !== "thinking" ||
      textBlock?.type !== "text" ||
      toolCallBlock?.type !== "tool_call"
    ) {
      throw new Error("unexpected reconstructed block kinds");
    }
    expect(thinkingBlock.signature).toBeDefined();
    expect(typeof thinkingBlock.signature).toBe("string");
    expect((thinkingBlock.signature ?? "").length).toBeGreaterThan(0);

    // 5. Build turn-2's request body via the production builder.
    //    The wire's turn-2 request has [user, assistant, user-with-
    //    tool_result]. Mirror that with the reconstructed assistant
    //    turn in the middle slot.
    const adapter = createAnthropicAdapter();
    const turns: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "What's the weather in Boston, MA?" }],
        timestamp: 0,
      },
      reconstructed,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: toolCallBlock.id,
            content: [{ type: "text", text: "{}" }],
          },
        ],
        timestamp: 1,
      },
    ];
    const req = adapter.buildRequest(
      turns,
      "claude-haiku-4-5-20251001",
      // The captured turn-2 request carries `thinking: { type: enabled,
      // budget_tokens: ... }` from the original request options;
      // reproduce that so the rebuilt body matches structurally.
      {
        thinking: { enabled: true, budgetTokens: 4096 },
        tools: [
          {
            name: "get_weather",
            description: "Get the current weather for a location.",
            inputSchema: {
              type: "object",
              properties: {
                location: { type: "string", description: "City and state" },
              },
              required: ["location"],
            },
          },
        ],
        maxTokens: 4096,
      },
    );

    // 6. Decode the rebuilt body and walk into the assistant
    //    message's thinking block. Assert the signature matches the
    //    captured turn-2 wire's signature byte-identical — this is
    //    the load-bearing invariant Anthropic enforces on every
    //    follow-up turn that includes a prior thinking block.
    const rebuilt: unknown = JSON.parse(req.body);
    if (!isRecord(rebuilt)) throw new Error("rebuilt body is not an object");
    const rebuiltMessages = rebuilt["messages"];
    if (!Array.isArray(rebuiltMessages)) {
      throw new Error("rebuilt body.messages is not an array");
    }
    const rebuiltAssistant = rebuiltMessages
      .filter(isRecord)
      .find((m) => m["role"] === "assistant");
    if (rebuiltAssistant === undefined) {
      throw new Error("rebuilt body has no assistant message");
    }
    const rebuiltContent = rebuiltAssistant["content"];
    if (!Array.isArray(rebuiltContent)) {
      throw new Error("rebuilt assistant content is not an array");
    }
    const rebuiltThinking = rebuiltContent
      .filter(isRecord)
      .find((b) => b["type"] === "thinking");
    if (rebuiltThinking === undefined) {
      throw new Error("rebuilt assistant has no thinking block");
    }

    // Now pull the captured turn-2 thinking signature for
    // byte-identical comparison.
    const capturedMessages = turn2ReqUnknown["messages"];
    if (!Array.isArray(capturedMessages)) {
      throw new Error("captured turn-2 messages is not an array");
    }
    const capturedAssistant = capturedMessages
      .filter(isRecord)
      .find((m) => m["role"] === "assistant");
    if (capturedAssistant === undefined) {
      throw new Error("captured turn-2 has no assistant message");
    }
    const capturedContent = capturedAssistant["content"];
    if (!Array.isArray(capturedContent)) {
      throw new Error("captured turn-2 assistant content is not an array");
    }
    const capturedThinking = capturedContent
      .filter(isRecord)
      .find((b) => b["type"] === "thinking");
    if (capturedThinking === undefined) {
      throw new Error("captured turn-2 has no thinking block");
    }

    expect(rebuiltThinking["signature"]).toBe(capturedThinking["signature"]);
    expect(rebuiltThinking["thinking"]).toBe(capturedThinking["thinking"]);

    // 7. Tool_use round-trip — id, name, and input must match
    //    byte-identical too. The id is what links the assistant
    //    turn's tool_use to the next user turn's tool_result.
    const rebuiltToolUse = rebuiltContent
      .filter(isRecord)
      .find((b) => b["type"] === "tool_use");
    const capturedToolUse = capturedContent
      .filter(isRecord)
      .find((b) => b["type"] === "tool_use");
    if (rebuiltToolUse === undefined || capturedToolUse === undefined) {
      throw new Error("missing tool_use block in rebuilt or captured turn-2");
    }
    expect(rebuiltToolUse["id"]).toBe(capturedToolUse["id"]);
    expect(rebuiltToolUse["name"]).toBe(capturedToolUse["name"]);
    expect(rebuiltToolUse["input"]).toEqual(capturedToolUse["input"]);

    // Reference turn1ReqUnknown to keep it from being dead — we
    // loaded it as a sanity check that the corpus is structurally
    // consistent with what we feed the harness above.
    expect(turn1ReqUnknown["messages"]).toBeDefined();
  });
});
