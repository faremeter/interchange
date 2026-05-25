import { readFileSync } from "node:fs";
import {
  type Capability,
  type CapabilityIntent,
  type MediaRef,
  resolveMediaPath,
} from "@intx/inference-discovery/catalog";

export interface ChatMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: unknown;
  name?: string;
  tool_call_id?: string;
  tool_calls?: ToolCall[];
}

export interface ToolCall {
  id: string;
  type: "function";
  function: {
    name: string;
    arguments: string;
  };
}

export interface ChatCompletionsRequest {
  model: string;
  messages: unknown[];
  stream?: boolean;
  tools?: unknown[];
  response_format?: unknown;
}

function mimeTypeFor(ref: MediaRef): string {
  if (ref.kind === "image") return "image/jpeg";
  if (ref.kind === "audio") return "audio/wav";
  if (ref.kind === "video") return "video/mp4";
  if (ref.kind === "document") return "application/pdf";
  throw new Error(`unsupported media kind: ${String(ref.kind)}`);
}

function readMediaDataUri(ref: MediaRef): string {
  const abs = resolveMediaPath(ref);
  const bytes = readFileSync(abs);
  const mime = mimeTypeFor(ref);
  return `data:${mime};base64,${bytes.toString("base64")}`;
}

function buildToolDecl(intent: CapabilityIntent): unknown[] {
  if (!intent.tools || intent.tools.length === 0) {
    throw new Error("intent has no tools but capability requires them");
  }
  return intent.tools.map((tool) => ({
    type: "function",
    function: {
      name: tool.name,
      description: tool.description,
      parameters: tool.parameters,
    },
  }));
}

function buildPlainTextBody(
  model: string,
  intent: CapabilityIntent,
  stream: boolean,
): ChatCompletionsRequest {
  const body: ChatCompletionsRequest = {
    model,
    messages: [{ role: "user", content: intent.prompt }],
  };
  if (stream) body.stream = true;
  return body;
}

function buildFunctionCallingBody(
  model: string,
  intent: CapabilityIntent,
): ChatCompletionsRequest {
  return {
    model,
    messages: [{ role: "user", content: intent.prompt }],
    tools: buildToolDecl(intent),
  };
}

export function buildMultiTurnTurn1Body(opts: {
  model: string;
  intent: CapabilityIntent;
}): ChatCompletionsRequest {
  return {
    model: opts.model,
    messages: [{ role: "user", content: opts.intent.prompt }],
    tools: buildToolDecl(opts.intent),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function findToolFollowUp(intent: CapabilityIntent): {
  toolName: string;
  content: string;
} {
  const followUp = intent.followUp;
  if (followUp === undefined || followUp.length === 0) {
    throw new Error("multi-turn: intent has no followUp entries");
  }
  for (const step of followUp) {
    if (step.role === "tool") {
      return { toolName: step.toolName, content: step.content };
    }
  }
  throw new Error("multi-turn: intent.followUp has no role:'tool' entry");
}

function extractAssistantMessage(parsed: unknown): Record<string, unknown> {
  if (!isRecord(parsed)) {
    throw new Error("multi-turn: turn-1 response is not a JSON object");
  }
  const choices = parsed.choices;
  if (!Array.isArray(choices) || choices.length === 0) {
    throw new Error("multi-turn: turn-1 response has no choices array");
  }
  const first = choices[0];
  if (!isRecord(first)) {
    throw new Error("multi-turn: turn-1 response choices[0] is not an object");
  }
  const message = first.message;
  if (!isRecord(message)) {
    throw new Error(
      "multi-turn: turn-1 response choices[0].message is not an object",
    );
  }
  return message;
}

function extractFirstToolCallId(message: Record<string, unknown>): string {
  const toolCalls = message.tool_calls;
  if (!Array.isArray(toolCalls) || toolCalls.length === 0) {
    throw new Error("multi-turn: turn-1 assistant message has no tool_calls");
  }
  const first = toolCalls[0];
  if (!isRecord(first)) {
    throw new Error("multi-turn: turn-1 tool_calls[0] is not an object");
  }
  const id = first.id;
  if (typeof id !== "string" || id.length === 0) {
    throw new Error("multi-turn: turn-1 tool_calls[0].id is not a string");
  }
  return id;
}

export function buildMultiTurnTurn2Body(opts: {
  model: string;
  intent: CapabilityIntent;
  turn1Body: ChatCompletionsRequest;
  turn1Response: unknown;
}): ChatCompletionsRequest {
  const assistantMessage = extractAssistantMessage(opts.turn1Response);
  const toolCallId = extractFirstToolCallId(assistantMessage);
  const tool = findToolFollowUp(opts.intent);

  const messages: unknown[] = [
    ...opts.turn1Body.messages,
    assistantMessage,
    {
      role: "tool",
      tool_call_id: toolCallId,
      content: tool.content,
    },
  ];

  const tools = opts.turn1Body.tools;
  const body: ChatCompletionsRequest = {
    model: opts.model,
    messages,
  };
  if (tools !== undefined) {
    body.tools = tools;
  }
  return body;
}

function buildReasoningBody(
  model: string,
  intent: CapabilityIntent,
  stream: boolean,
): ChatCompletionsRequest {
  const body: ChatCompletionsRequest = {
    model,
    messages: [{ role: "user", content: intent.prompt }],
  };
  if (stream) body.stream = true;
  return body;
}

// Translate the intent's responseFormat to OpenAI's response_format
// wire field. Mirrors toOpenAIResponseFormat in the inference adapter
// (packages/inference/src/providers/openai.ts); duplicated here
// because the discovery plug-in builds wire requests directly without
// running through the adapter. Kept in sync by the shared
// CapabilityIntent shape.
function toOpenAIResponseFormat(
  format: NonNullable<CapabilityIntent["responseFormat"]>,
): Record<string, unknown> {
  switch (format.kind) {
    case "text":
      return { type: "text" };
    case "json":
      return { type: "json_object" };
    case "json-schema": {
      const jsonSchema: Record<string, unknown> = {
        name: format.name,
        schema: format.schema,
      };
      if (format.strict !== undefined) jsonSchema["strict"] = format.strict;
      return { type: "json_schema", json_schema: jsonSchema };
    }
  }
}

function buildStructuredOutputBody(
  model: string,
  intent: CapabilityIntent,
  stream: boolean,
): ChatCompletionsRequest {
  if (intent.responseFormat === undefined) {
    throw new Error(
      "OpenAI protocol: structured-output intent has no responseFormat",
    );
  }
  const body: ChatCompletionsRequest = {
    model,
    messages: [{ role: "user", content: intent.prompt }],
    response_format: toOpenAIResponseFormat(intent.responseFormat),
  };
  if (stream) body.stream = true;
  return body;
}

function buildVisionBody(
  model: string,
  intent: CapabilityIntent,
): ChatCompletionsRequest {
  if (!intent.media || intent.media.length === 0) {
    throw new Error("intent has no media but vision-input requires it");
  }
  const parts: unknown[] = [{ type: "text", text: intent.prompt }];
  for (const ref of intent.media) {
    if (ref.kind !== "image") {
      throw new Error(
        `vision-input only accepts image media, got: ${ref.kind}`,
      );
    }
    parts.push({
      type: "image_url",
      image_url: { url: readMediaDataUri(ref) },
    });
  }
  return {
    model,
    messages: [{ role: "user", content: parts }],
  };
}

export interface BuildRequestBodyArgs {
  model: string;
  capability: Capability;
  intent: CapabilityIntent;
}

export function buildRequestBody(args: BuildRequestBodyArgs): unknown {
  const { model, capability, intent } = args;
  switch (capability) {
    case "plain-text":
      return buildPlainTextBody(model, intent, false);
    case "plain-text-streaming":
      return buildPlainTextBody(model, intent, true);
    case "function-calling":
      return buildFunctionCallingBody(model, intent);
    case "function-calling-multi-turn":
      throw new Error(
        "function-calling-multi-turn is a multi-step capability; use buildMultiTurnTurn1Body / buildMultiTurnTurn2Body via iterateCaptureSteps",
      );
    case "reasoning-content":
      return buildReasoningBody(model, intent, false);
    case "reasoning-content-streaming":
      return buildReasoningBody(model, intent, true);
    case "vision-input":
      return buildVisionBody(model, intent);
    case "structured-output":
      return buildStructuredOutputBody(model, intent, false);
    case "structured-output-streaming":
      return buildStructuredOutputBody(model, intent, true);
    default:
      throw new Error(
        `OpenAI protocol: capability "${capability}" not implemented`,
      );
  }
}
