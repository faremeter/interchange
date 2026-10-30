import { basename } from "node:path";
import {
  CapabilityNotBuildableError,
  type Capability,
  type CapabilityIntent,
  type MediaRef,
} from "@intx/inference-discovery/catalog";
import { findToolFollowUp, readMediaDataUri } from "./body";

export interface ResponsesRequest {
  model: string;
  input: unknown[];
  stream?: boolean;
  tools?: unknown[];
  tool_choice?: "auto";
  reasoning?: { effort: string; summary?: "auto" | "detailed" };
  store?: boolean;
  include?: string[];
}

function buildTextInput(intent: CapabilityIntent): unknown[] {
  return [{ type: "message", role: "user", content: intent.prompt }];
}

function buildToolDecl(intent: CapabilityIntent): unknown[] {
  if (!intent.tools || intent.tools.length === 0) {
    throw new Error("intent has no tools but capability requires them");
  }
  // Responses function tools are flat — name/description/parameters sit
  // beside `type`, not nested under a `function` key (unlike Chat
  // Completions).
  return intent.tools.map((tool) => ({
    type: "function",
    name: tool.name,
    description: tool.description,
    parameters: tool.parameters,
  }));
}

function buildPlainTextBody(
  model: string,
  intent: CapabilityIntent,
  stream: boolean,
): ResponsesRequest {
  const body: ResponsesRequest = { model, input: buildTextInput(intent) };
  if (stream) body.stream = true;
  return body;
}

function buildFunctionCallingBody(
  model: string,
  intent: CapabilityIntent,
  stream: boolean,
): ResponsesRequest {
  const body: ResponsesRequest = {
    model,
    input: buildTextInput(intent),
    tools: buildToolDecl(intent),
    tool_choice: "auto",
  };
  if (stream) body.stream = true;
  return body;
}

export function buildResponsesMultiTurnTurn1Body(opts: {
  model: string;
  intent: CapabilityIntent;
}): ResponsesRequest {
  return buildFunctionCallingBody(opts.model, opts.intent, false);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function extractOutputItems(parsed: unknown): unknown[] {
  if (!isRecord(parsed)) {
    throw new Error("multi-turn: turn-1 response is not a JSON object");
  }
  const output = parsed.output;
  if (!Array.isArray(output)) {
    throw new Error("multi-turn: turn-1 response has no output array");
  }
  return output;
}

function extractFirstFunctionCall(output: unknown[]): {
  callId: string;
  name: string;
  args: string;
} {
  for (const item of output) {
    if (!isRecord(item) || item.type !== "function_call") continue;
    const { call_id, name, arguments: args } = item;
    if (
      typeof call_id !== "string" ||
      call_id.length === 0 ||
      typeof name !== "string" ||
      name.length === 0 ||
      typeof args !== "string"
    ) {
      throw new Error(
        "multi-turn: turn-1 function_call item has no usable call_id/name/arguments",
      );
    }
    return { callId: call_id, name, args };
  }
  throw new Error("multi-turn: turn-1 response output has no function_call");
}

export function buildResponsesMultiTurnTurn2Body(opts: {
  model: string;
  intent: CapabilityIntent;
  turn1Body: ResponsesRequest;
  turn1Response: unknown;
}): ResponsesRequest {
  const output = extractOutputItems(opts.turn1Response);
  const call = extractFirstFunctionCall(output);
  const tool = findToolFollowUp(opts.intent);
  // The adapter runs with `store: false` and replays the encrypted reasoning
  // item, so turn-2 exercises exactly that path: unstored, with the
  // reasoning item replayed in the adapter's shape (no `id` — the adapter
  // sends only type, summary, and encrypted_content). Replaying the
  // server's `rs_...` id instead would prove a stored-item lookup, not the
  // encrypted replay the adapter depends on.
  const reasoningItems = output
    .filter(
      (item): item is Record<string, unknown> =>
        isRecord(item) && item.type === "reasoning",
    )
    .map((item) => ({
      type: "reasoning",
      summary: item["summary"] ?? [],
      encrypted_content: item["encrypted_content"],
    }));
  const input: unknown[] = [
    ...opts.turn1Body.input,
    ...reasoningItems,
    {
      type: "function_call",
      call_id: call.callId,
      name: call.name,
      arguments: call.args,
    },
    {
      type: "function_call_output",
      call_id: call.callId,
      output: tool.content,
    },
  ];
  const body: ResponsesRequest = {
    model: opts.model,
    input,
    store: false,
    include: ["reasoning.encrypted_content"],
  };
  if (opts.turn1Body.tools !== undefined) {
    body.tools = opts.turn1Body.tools;
    body.tool_choice = "auto";
  }
  return body;
}

function buildServerToolBody(
  model: string,
  intent: CapabilityIntent,
  stream: boolean,
  tool: unknown,
): ResponsesRequest {
  const body: ResponsesRequest = {
    model,
    input: buildTextInput(intent),
    tools: [tool],
  };
  if (stream) body.stream = true;
  return body;
}

function buildReasoningBody(
  model: string,
  intent: CapabilityIntent,
  stream: boolean,
): ResponsesRequest {
  // `summary: "detailed"` forces the backend to return summary text: without
  // it the server reports `"summary": null` and the fixtures cannot prove
  // summary text reaches `inference.thinking`.
  const body: ResponsesRequest = {
    model,
    input: buildTextInput(intent),
    reasoning: { effort: "medium", summary: "detailed" },
  };
  if (stream) body.stream = true;
  return body;
}

// Shared skeleton for Responses multimodal user turns: text prompt plus one
// typed part per media ref. Vision and document differ only in the expected
// kind and the part shape.
function buildMediaInputBody(
  model: string,
  intent: CapabilityIntent,
  capability: string,
  expectedKind: MediaRef["kind"],
  toPart: (ref: MediaRef) => unknown,
): ResponsesRequest {
  if (!intent.media || intent.media.length === 0) {
    throw new Error(`intent has no media but ${capability} requires it`);
  }
  const parts: unknown[] = [{ type: "input_text", text: intent.prompt }];
  for (const ref of intent.media) {
    if (ref.kind !== expectedKind) {
      throw new Error(
        `${capability} only accepts ${expectedKind} media, got: ${ref.kind}`,
      );
    }
    parts.push(toPart(ref));
  }
  return {
    model,
    input: [{ type: "message", role: "user", content: parts }],
  };
}

function imagePart(ref: MediaRef): unknown {
  return { type: "input_image", image_url: readMediaDataUri(ref) };
}

function documentPart(ref: MediaRef): unknown {
  const filename = basename(ref.path);
  if (filename.length === 0) {
    throw new Error(
      "document-input media path has an empty basename; the Responses input_file part requires a filename",
    );
  }
  return {
    type: "input_file",
    filename,
    file_data: readMediaDataUri(ref),
  };
}

export interface BuildResponsesRequestBodyArgs {
  model: string;
  capability: Capability;
  intent: CapabilityIntent;
}

export function buildResponsesRequestBody(
  args: BuildResponsesRequestBodyArgs,
): unknown {
  const { model, capability, intent } = args;
  switch (capability) {
    case "plain-text":
      return buildPlainTextBody(model, intent, false);
    case "plain-text-streaming":
      return buildPlainTextBody(model, intent, true);
    case "function-calling":
      return buildFunctionCallingBody(model, intent, false);
    case "function-calling-multi-turn":
      throw new Error(
        "function-calling-multi-turn is a multi-step capability; use buildResponsesMultiTurnTurn1Body / buildResponsesMultiTurnTurn2Body via iterateCaptureSteps",
      );
    // Single-step streamed tool call (mirrors the Anthropic plug-in): the
    // live SSE proves arguments arrive as `function_call_arguments.delta`
    // rather than only in the terminal `.done` envelope.
    case "function-calling-multi-turn-streaming":
      return buildFunctionCallingBody(model, intent, true);
    case "reasoning-content":
      return buildReasoningBody(model, intent, false);
    case "reasoning-content-streaming":
      return buildReasoningBody(model, intent, true);
    case "code-execution":
      return buildServerToolBody(model, intent, false, {
        type: "code_interpreter",
        container: { type: "auto", memory_limit: "4g" },
      });
    case "code-execution-streaming":
      return buildServerToolBody(model, intent, true, {
        type: "code_interpreter",
        container: { type: "auto", memory_limit: "4g" },
      });
    case "grounding":
      return buildServerToolBody(model, intent, false, {
        type: "web_search",
      });
    case "grounding-streaming":
      return buildServerToolBody(model, intent, true, {
        type: "web_search",
      });
    case "vision-input":
      return buildMediaInputBody(
        model,
        intent,
        "vision-input",
        "image",
        imagePart,
      );
    case "document-input":
      return buildMediaInputBody(
        model,
        intent,
        "document-input",
        "document",
        documentPart,
      );
    default:
      throw new CapabilityNotBuildableError(
        capability,
        `Responses protocol: capability "${capability}" not implemented`,
      );
  }
}
