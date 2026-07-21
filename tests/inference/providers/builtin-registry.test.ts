import { type } from "arktype";
import { describe, test, expect } from "bun:test";
import { createBuiltinRegistry } from "@intx/inference/providers";
import type { ConversationTurn, LastCycleSource } from "@intx/types/runtime";

const OPENAI_SOURCE: LastCycleSource = {
  sourceId: "test-openai",
  provider: "openai",
  model: "test-openai-model",
};

// A focused view of the built request: the only field these tests assert on
// is `reasoning_content` on the first assistant message. Validating through
// arktype keeps the parsed body typed without an unsafe access; arktype
// retains the message's other keys, so `"reasoning_content" in message`
// reflects the wire shape faithfully.
const AssistantMessageView = type({
  messages: type({ role: "string", "reasoning_content?": "string" }).array(),
});

const ASSISTANT_TURN: ConversationTurn[] = [
  { role: "assistant", content: [{ type: "text", text: "hi" }], timestamp: 1 },
];

function firstMessageThroughResolve(quirks?: unknown) {
  const adapter = createBuiltinRegistry().resolve(OPENAI_SOURCE, quirks);
  const body = AssistantMessageView.assert(
    JSON.parse(
      adapter.buildRequest(ASSISTANT_TURN, "test-openai-model", {}).body,
    ),
  );
  const message = body.messages[0];
  if (message === undefined) throw new Error("expected an assistant message");
  return message;
}

describe("createBuiltinRegistry resolves quirks into the real provider factory", () => {
  test("applies an openai quirk passed through resolve", () => {
    // forceAssistantReasoningContent:false omits reasoning_content on a turn
    // with no thinking. Observing that omission on the adapter produced by
    // resolve proves the quirk bag reaches the real factory through the
    // registry — the production path the harness takes.
    const message = firstMessageThroughResolve({
      forceAssistantReasoningContent: false,
    });
    expect("reasoning_content" in message).toBe(false);
  });

  test("surfaces a factory quirks rejection through resolve", () => {
    expect(() =>
      createBuiltinRegistry().resolve(OPENAI_SOURCE, {
        forceAssistantReasoningContnt: false,
      }),
    ).toThrow(/invalid quirks/);
  });
});
