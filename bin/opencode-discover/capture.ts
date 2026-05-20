import type { HeadersMap } from "../gemini-discover/capture.ts";

export const OPENCODE_REDACT_HEADERS = new Set(["authorization"]);

export function buildAuthHeaders(apiKey: string): HeadersMap {
  return {
    "Content-Type": "application/json",
    Authorization: `Bearer ${apiKey}`,
  };
}

export function buildChatCompletionsURL(baseUrl: string): string {
  return `${baseUrl}/chat/completions`;
}

export type ChatMessage = {
  role: "system" | "user" | "assistant" | "tool";
  content: unknown;
  name?: string;
  tool_call_id?: string;
};

export type ChatCompletionsRequest = {
  model: string;
  messages: ChatMessage[];
  stream?: boolean;
  tools?: unknown[];
  tool_choice?: unknown;
  max_tokens?: number;
  temperature?: number;
};

export type BuildChatCompletionsRequestArgs = {
  model: string;
  messages: ChatMessage[];
  stream?: boolean;
  overrides?: Partial<Omit<ChatCompletionsRequest, "model" | "messages">>;
};

export function buildChatCompletionsRequest(
  args: BuildChatCompletionsRequestArgs,
): ChatCompletionsRequest {
  const overrides = args.overrides ?? {};
  const definedOverrides: Partial<ChatCompletionsRequest> = {};
  if (overrides.tools !== undefined) definedOverrides.tools = overrides.tools;
  if (overrides.tool_choice !== undefined)
    definedOverrides.tool_choice = overrides.tool_choice;
  if (overrides.max_tokens !== undefined)
    definedOverrides.max_tokens = overrides.max_tokens;
  if (overrides.temperature !== undefined)
    definedOverrides.temperature = overrides.temperature;
  if (overrides.stream !== undefined)
    definedOverrides.stream = overrides.stream;

  const body: ChatCompletionsRequest = {
    model: args.model,
    messages: args.messages,
    ...definedOverrides,
  };
  if (args.stream !== undefined) {
    body.stream = args.stream;
  }
  return body;
}

export type ProbeCapabilityFlags = {
  text: boolean;
  functionCalling: boolean;
  reasoning: boolean;
  vision: boolean;
};

export type ProbeReasoningEvidence = {
  fieldPath: string;
  sample: unknown;
};

export type ProbeResult = {
  model: string;
  flags: ProbeCapabilityFlags;
  reasoningEvidence: ProbeReasoningEvidence | null;
  textNonStreaming: ProbeStepRecord;
  textStreaming: ProbeStepRecord;
  functionCalling: ProbeStepRecord;
  reasoningNonStreaming: ProbeStepRecord;
  reasoningStreaming: ProbeStepRecord;
  vision: ProbeStepRecord;
};

export type ProbeStepRecord = {
  request: ChatCompletionsRequest;
  httpStatus: number;
  httpStatusText: string;
  responseBody: unknown;
  responseSseText?: string;
  errorMessage?: string;
};

const REASONING_FIELD_PATHS = [
  ["choices", 0, "message", "reasoning_content"],
  ["choices", 0, "message", "reasoning"],
  ["choices", 0, "message", "reasoning_details"],
] as const;

const REASONING_DELTA_FIELDS = ["reasoning", "reasoning_content"] as const;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function lookupPath(
  value: unknown,
  path: readonly (string | number)[],
): unknown {
  let cursor: unknown = value;
  for (const segment of path) {
    if (cursor === null || cursor === undefined) return undefined;
    if (typeof segment === "number") {
      if (!Array.isArray(cursor)) return undefined;
      cursor = cursor[segment];
    } else {
      if (!isRecord(cursor)) return undefined;
      cursor = cursor[segment];
    }
  }
  return cursor;
}

function isNonEmpty(value: unknown): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === "string") return value.length > 0;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === "object") return Object.keys(value).length > 0;
  return true;
}

export function detectReasoningFromJson(
  responseJson: unknown,
): ProbeReasoningEvidence | null {
  for (const path of REASONING_FIELD_PATHS) {
    const value = lookupPath(responseJson, path);
    if (isNonEmpty(value)) {
      return { fieldPath: path.join("."), sample: value };
    }
  }
  return null;
}

export function detectReasoningFromSseText(
  sseText: string,
): ProbeReasoningEvidence | null {
  const lines = sseText.split(/\r?\n/);
  for (const line of lines) {
    if (!line.startsWith("data: ")) continue;
    const payload = line.slice("data: ".length);
    if (payload.length === 0 || payload === "[DONE]") continue;
    let event: unknown;
    try {
      event = JSON.parse(payload);
    } catch {
      continue;
    }
    for (const field of REASONING_DELTA_FIELDS) {
      const value = lookupPath(event, ["choices", 0, "delta", field]);
      if (isNonEmpty(value)) {
        return {
          fieldPath: `choices.0.delta.${field}`,
          sample: value,
        };
      }
    }
  }
  return null;
}

export type FunctionCallEvidence = {
  toolCalls: unknown;
  finishReason: string | null;
};

export function detectFunctionCallingFromJson(
  responseJson: unknown,
): FunctionCallEvidence | null {
  const toolCalls = lookupPath(responseJson, [
    "choices",
    0,
    "message",
    "tool_calls",
  ]);
  if (!isNonEmpty(toolCalls)) return null;
  const finish = lookupPath(responseJson, ["choices", 0, "finish_reason"]);
  return {
    toolCalls,
    finishReason: typeof finish === "string" ? finish : null,
  };
}
