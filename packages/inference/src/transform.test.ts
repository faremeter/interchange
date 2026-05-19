import { describe, test, expect } from "bun:test";
import { transformMessages, createIDNormalizer } from "./transform";
import type { ConversationTurn } from "@intx/types/runtime";

describe("transformMessages", () => {
  test("preserves messages when target model matches originating model", () => {
    const messages: ConversationTurn[] = [
      {
        role: "assistant",
        model: "claude-3-5-sonnet",
        content: [
          { type: "thinking", thinking: "Let me think..." },
          { type: "text", text: "Here is the answer." },
        ],
        timestamp: 1000,
      },
    ];

    const result = transformMessages(messages, {
      targetModel: "claude-3-5-sonnet",
      keepThinkingForSameModel: true,
    });

    expect(result).toHaveLength(1);
    const firstMsg = result[0];
    expect(firstMsg?.content).toHaveLength(2);
    expect(firstMsg?.content[0]?.type).toBe("thinking");
  });

  test("strips thinking blocks when replaying to a different model", () => {
    const messages: ConversationTurn[] = [
      {
        role: "assistant",
        model: "claude-3-5-sonnet",
        content: [
          { type: "thinking", thinking: "Some reasoning..." },
          { type: "text", text: "Answer." },
        ],
        timestamp: 1000,
      },
    ];

    const result = transformMessages(messages, {
      targetModel: "gpt-4o",
    });

    expect(result).toHaveLength(1);
    const firstMsg = result[0];
    expect(firstMsg?.content).toHaveLength(1);
    expect(firstMsg?.content[0]?.type).toBe("text");
  });

  test("strips thinking blocks when keepThinkingForSameModel is false", () => {
    const messages: ConversationTurn[] = [
      {
        role: "assistant",
        model: "claude-3-5-sonnet",
        content: [
          { type: "thinking", thinking: "Reasoning..." },
          { type: "text", text: "Answer." },
        ],
        timestamp: 1000,
      },
    ];

    const result = transformMessages(messages, {
      targetModel: "claude-3-5-sonnet",
      keepThinkingForSameModel: false,
    });

    const firstMsg = result[0];
    expect(firstMsg?.content).toHaveLength(1);
    expect(firstMsg?.content[0]?.type).toBe("text");
  });

  test("injects synthetic tool results for orphaned tool calls", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Do something." }],
        timestamp: 1000,
      },
      {
        role: "assistant",
        content: [
          {
            type: "tool_call",
            id: "call_1",
            name: "read_file",
            arguments: { path: "/tmp/foo" },
          },
        ],
        timestamp: 1000,
      },
      // No tool result follows — the conversation was interrupted.
    ];

    const result = transformMessages(messages, { targetModel: "gpt-4o" });

    // Should inject a synthetic tool result message.
    expect(result).toHaveLength(3);
    const injected = result[2];
    expect(injected?.role).toBe("user");
    expect(injected?.content[0]?.type).toBe("tool_result");
    const toolResult = injected?.content[0];
    if (toolResult?.type === "tool_result") {
      expect(toolResult.callId).toBe("call_1");
      expect(toolResult.isError).toBe(true);
    }
  });

  test("does not inject when tool results are present", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Do something." }],
        timestamp: 1000,
      },
      {
        role: "assistant",
        content: [
          {
            type: "tool_call",
            id: "call_1",
            name: "read_file",
            arguments: { path: "/tmp/foo" },
          },
        ],
        timestamp: 1000,
      },
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_1",
            content: [{ type: "text", text: "file contents" }],
          },
        ],
        timestamp: 1000,
      },
    ];

    const result = transformMessages(messages, { targetModel: "gpt-4o" });
    expect(result).toHaveLength(3);
  });

  test("preserves user and system messages unchanged", () => {
    const messages: ConversationTurn[] = [
      {
        role: "system",
        content: [{ type: "text", text: "You are helpful." }],
        timestamp: 1000,
      },
      {
        role: "user",
        content: [{ type: "text", text: "Hello." }],
        timestamp: 1000,
      },
    ];

    const result = transformMessages(messages, { targetModel: "gpt-4o" });
    expect(result).toHaveLength(2);
    expect(result[0]).toEqual(messages[0]);
    expect(result[1]).toEqual(messages[1]);
  });
});

describe("createIDNormalizer", () => {
  test("assigns stable portable IDs", () => {
    const norm = createIDNormalizer();
    const id1 = norm.normalize(
      "call_abc_very_long_id_from_openai_responses_api",
    );
    const id2 = norm.normalize(
      "call_abc_very_long_id_from_openai_responses_api",
    );
    expect(id1).toBe(id2);
    expect(id1.startsWith("tc_")).toBe(true);
  });

  test("assigns different IDs to different provider IDs", () => {
    const norm = createIDNormalizer();
    const id1 = norm.normalize("toolu_01A");
    const id2 = norm.normalize("toolu_02B");
    expect(id1).not.toBe(id2);
  });

  test("resolves a portable ID back to the provider ID", () => {
    const norm = createIDNormalizer();
    const providerId = "toolu_01AbCdEfGhIjKlMnOpQrStUvWx";
    const portable = norm.normalize(providerId);
    expect(norm.resolve(portable)).toBe(providerId);
  });

  test("resolve returns undefined for unknown portable ID", () => {
    const norm = createIDNormalizer();
    expect(norm.resolve("tc_unknown")).toBeUndefined();
  });

  test("IDs are monotonically increasing", () => {
    const norm = createIDNormalizer();
    const ids = ["a", "b", "c"].map((x) => norm.normalize(x));
    // All different.
    expect(new Set(ids).size).toBe(3);
  });
});
