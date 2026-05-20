import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { type } from "arktype";

import { getLogger } from "@intx/log";

import {
  runNonStreamingStepCapture,
  writeMultiStepMetadata,
} from "../../gemini-discover/capture.ts";
import {
  OPENCODE_REDACT_HEADERS,
  buildAuthHeaders,
  buildChatCompletionsRequest,
  buildChatCompletionsURL,
  type ChatMessage,
} from "../capture.ts";
import type { Capability } from "./index.ts";
import {
  SYNTHETIC_TOOL_RESPONSE,
  TOOL_DEFINITION,
  USER_PROMPT,
} from "./function-calling-shared.ts";

const logger = getLogger(["opencode-discover", "function-calling-multi-turn"]);

const NAME = "function-calling-multi-turn";
const ENDPOINT = "chat/completions";

const HERE = fileURLToPath(new URL(".", import.meta.url));
const FIXTURE_ROOT = join(
  HERE,
  "..",
  "..",
  "..",
  "packages",
  "inference-testing",
  "wire",
  "opencode-zen",
);

const ToolCall = type({
  id: "string",
  "type?": "string",
  function: type({
    name: "string",
    "arguments?": "string",
  }),
});

const AssistantMessage = type({
  "role?": "string",
  "tool_calls?": ToolCall.array(),
}).and("Record<string, unknown>");

const Choice = type({
  "message?": AssistantMessage,
});

const ChatCompletionsResponse = type({
  "choices?": Choice.array(),
});

type ToolCall = typeof ToolCall.infer;

function extractAssistantTurn(
  response: unknown,
  context: string,
): { message: Record<string, unknown>; toolCalls: ToolCall[] } {
  const parsed = ChatCompletionsResponse(response);
  if (parsed instanceof type.errors) {
    throw new Error(`${context}: malformed response: ${parsed.summary}`);
  }
  const message = parsed.choices?.[0]?.message;
  if (!message) {
    throw new Error(`${context}: response has no choices[0].message`);
  }
  const toolCalls = message.tool_calls;
  if (!toolCalls || toolCalls.length === 0) {
    throw new Error(
      `${context}: assistant message did not contain tool_calls; got ${JSON.stringify(message)}`,
    );
  }
  return { message, toolCalls };
}

export const capability: Capability = {
  name: NAME,
  endpoint: ENDPOINT,
  build: async ({ apiKey, baseUrl, model, scriptVersion }) => {
    const destination = join(FIXTURE_ROOT, model, NAME);
    const url = buildChatCompletionsURL(baseUrl);
    const requestHeaders = buildAuthHeaders(apiKey);

    const userMessage: ChatMessage = { role: "user", content: USER_PROMPT };

    const turn1Body = buildChatCompletionsRequest({
      model,
      messages: [userMessage],
      overrides: { tools: [TOOL_DEFINITION] },
    });

    const turn1 = await runNonStreamingStepCapture({
      capability: NAME,
      stepName: "turn-1",
      model,
      endpoint: ENDPOINT,
      url,
      requestHeaders,
      redactHeaderNames: OPENCODE_REDACT_HEADERS,
      body: turn1Body,
      destinationOverride: destination,
    });

    const { message: assistantMessage, toolCalls } = extractAssistantTurn(
      turn1.responseJson,
      `${NAME} turn-1 (model=${model})`,
    );
    logger.info`turn-1 returned ${String(toolCalls.length)} tool_call(s) for model=${model}`;

    const toolResponses: Record<string, unknown>[] = toolCalls.map((call) => ({
      role: "tool",
      tool_call_id: call.id,
      content: JSON.stringify(SYNTHETIC_TOOL_RESPONSE),
    }));

    const turn2Body = buildChatCompletionsRequest({
      model,
      messages: [userMessage],
      overrides: { tools: [TOOL_DEFINITION] },
    });
    const turn2BodyWithHistory: Record<string, unknown> = {
      ...turn2Body,
      messages: [userMessage, assistantMessage, ...toolResponses],
    };

    await runNonStreamingStepCapture({
      capability: NAME,
      stepName: "turn-2",
      model,
      endpoint: ENDPOINT,
      url,
      requestHeaders,
      redactHeaderNames: OPENCODE_REDACT_HEADERS,
      body: turn2BodyWithHistory,
      destinationOverride: destination,
    });

    await writeMultiStepMetadata({
      capability: NAME,
      model,
      endpoint: ENDPOINT,
      scriptVersion,
      sequence: ["turn-1", "turn-2"],
      destinationOverride: destination,
    });
  },
};
