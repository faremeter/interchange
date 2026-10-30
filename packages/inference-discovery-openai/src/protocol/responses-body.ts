import { readFileSync } from "node:fs";
import { basename } from "node:path";
import {
  CapabilityNotBuildableError,
  type Capability,
  type CapabilityIntent,
  type MediaRef,
  resolveMediaPath,
} from "@intx/inference-discovery/catalog";

export interface ResponsesRequest {
  model: string;
  input: unknown[];
  stream?: boolean;
  tools?: unknown[];
  tool_choice?: "auto";
  reasoning?: { effort: string };
}

function mimeTypeFor(ref: MediaRef): string {
  if (ref.kind === "image") return "image/jpeg";
  if (ref.kind === "document") return "application/pdf";
  throw new Error(`unsupported media kind: ${String(ref.kind)}`);
}

function readMediaDataUri(ref: MediaRef): string {
  const abs = resolveMediaPath(ref);
  const bytes = readFileSync(abs);
  const mime = mimeTypeFor(ref);
  return `data:${mime};base64,${bytes.toString("base64")}`;
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
): ResponsesRequest {
  return {
    model,
    input: buildTextInput(intent),
    tools: buildToolDecl(intent),
    tool_choice: "auto",
  };
}

export function buildResponsesMultiTurnTurn1Body(opts: {
  model: string;
  intent: CapabilityIntent;
}): ResponsesRequest {
  return buildFunctionCallingBody(opts.model, opts.intent);
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

export function buildResponsesMultiTurnTurn2Body(opts: {
  model: string;
  intent: CapabilityIntent;
  turn1Body: ResponsesRequest;
  turn1Response: unknown;
}): ResponsesRequest {
  const output = extractOutputItems(opts.turn1Response);
  const call = extractFirstFunctionCall(output);
  const tool = findToolFollowUp(opts.intent);
  const input: unknown[] = [
    ...opts.turn1Body.input,
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
  const body: ResponsesRequest = { model: opts.model, input };
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
  const body: ResponsesRequest = {
    model,
    input: buildTextInput(intent),
    reasoning: { effort: "medium" },
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
      return buildFunctionCallingBody(model, intent);
    case "function-calling-multi-turn":
      throw new Error(
        "function-calling-multi-turn is a multi-step capability; use buildResponsesMultiTurnTurn1Body / buildResponsesMultiTurnTurn2Body via iterateCaptureSteps",
      );
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
