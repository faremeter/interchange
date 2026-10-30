import { describe, test, expect } from "bun:test";
import { extractDispatches } from "./dispatch-reconstruction";

function responsesBody(): Record<string, unknown> {
  return {
    model: "gpt-5.5",
    input: [
      {
        type: "message",
        role: "user",
        content: "What is the weather in Boston, MA? Use the provided tool.",
      },
      {
        type: "function_call",
        call_id: "call_test",
        name: "get_weather",
        arguments: '{"location":"Boston, MA"}',
      },
      {
        type: "function_call_output",
        call_id: "call_test",
        output:
          '{"location":"Boston, MA","temperatureF":68,"conditions":"clear"}',
      },
    ],
    tools: [
      {
        type: "function",
        name: "get_weather",
        description: "Look up the current weather for a city.",
        parameters: { type: "object" },
      },
    ],
    tool_choice: "auto",
  };
}

describe("extractDispatches openai-responses", () => {
  test("pairs function_call items with their outputs in call order", () => {
    expect(extractDispatches("openai-responses", responsesBody())).toEqual([
      {
        toolName: "get_weather",
        args: { location: "Boston, MA" },
        result:
          '{"location":"Boston, MA","temperatureF":68,"conditions":"clear"}',
      },
    ]);
  });

  test("a body with no input array throws", () => {
    expect(() =>
      extractDispatches("openai-responses", { model: "gpt-5.5" }),
    ).toThrow(/no input array/);
  });

  test("a duplicate function_call_output throws", () => {
    const body = responsesBody();
    const input = body["input"];
    if (!Array.isArray(input)) throw new Error("expected input array");
    input.push({
      type: "function_call_output",
      call_id: "call_test",
      output: "again",
    });
    expect(() => extractDispatches("openai-responses", body)).toThrow(
      /duplicate tool result/,
    );
  });

  test("an unpaired call throws", () => {
    const body = responsesBody();
    const input = body["input"];
    if (!Array.isArray(input)) throw new Error("expected input array");
    input.pop();
    expect(() => extractDispatches("openai-responses", body)).toThrow(
      /has no matching tool_result/,
    );
  });
});
