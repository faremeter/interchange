import { readFileSync } from "node:fs";
import {
  resolveMediaPath,
  type Capability,
  type CapabilityIntent,
  type MediaRef,
  type ToolDecl,
} from "@intx/inference-discovery/catalog";
import { mediaTypeFor } from "./media";

const PLAIN_MAX_TOKENS = 512;
const THINKING_BUDGET_TOKENS = 1024;
const THINKING_MAX_TOKENS = THINKING_BUDGET_TOKENS + 1024;

// Tool versions for Anthropic's server-side tools. These are wire-shape
// markers — bumping them is a deliberate decision that should be
// reflected in regenerated fixtures.
const CODE_EXECUTION_TOOL_TYPE = "code_execution_20250522";
const WEB_SEARCH_TOOL_TYPE = "web_search_20250305";

export interface AnthropicTextBlock {
  type: "text";
  text: string;
}

export interface AnthropicImageBlock {
  type: "image";
  source: {
    type: "base64";
    media_type: string;
    data: string;
  };
}

export interface AnthropicDocumentBlock {
  type: "document";
  source:
    | { type: "base64"; media_type: "application/pdf"; data: string }
    | { type: "file"; file_id: string };
}

export interface AnthropicToolUseBlock {
  type: "tool_use";
  id: string;
  name: string;
  input: Record<string, unknown>;
}

export interface AnthropicToolResultBlock {
  type: "tool_result";
  tool_use_id: string;
  content: string;
}

export interface AnthropicThinkingBlock {
  type: "thinking";
  thinking: string;
  signature?: string;
}

export interface AnthropicRedactedThinkingBlock {
  type: "redacted_thinking";
  data: string;
  signature?: string;
}

// Blocks this plug-in constructs for outgoing request bodies. Also the
// type assistant-echo paths produce: blocks Anthropic returns that
// aren't enumerated here (server_tool_use, web_search_tool_result,
// code_execution_tool_use, citation blocks inside text, …) are
// forwarded verbatim because the wire round-trip is what matters; the
// runtime values just don't match the static union. The single
// quarantined cast lives in extractAssistantContentBlocks.
export type AnthropicContentBlock =
  | AnthropicTextBlock
  | AnthropicImageBlock
  | AnthropicDocumentBlock
  | AnthropicToolUseBlock
  | AnthropicToolResultBlock
  | AnthropicThinkingBlock
  | AnthropicRedactedThinkingBlock;

export interface AnthropicMessage {
  role: "user" | "assistant";
  content: string | AnthropicContentBlock[];
}

export interface AnthropicToolDecl {
  name: string;
  description: string;
  input_schema: ToolDecl["parameters"];
}

export interface AnthropicServerTool {
  type: string;
  name: string;
}

export type AnthropicTool = AnthropicToolDecl | AnthropicServerTool;

export type AnthropicThinkingConfig =
  | { type: "enabled"; budget_tokens: number }
  | { type: "adaptive" };

export type AnthropicEffort = "low" | "medium" | "high" | "xhigh" | "max";

export interface AnthropicOutputConfig {
  effort: AnthropicEffort;
}

export interface AnthropicRequestBody {
  model: string;
  max_tokens: number;
  messages: AnthropicMessage[];
  tools?: AnthropicTool[];
  thinking?: AnthropicThinkingConfig;
  output_config?: AnthropicOutputConfig;
  stream?: true;
}

// claude-sonnet-5 rejects the classic thinking:{type:"enabled",budget_tokens}
// shape with invalid_request_error and requires thinking:{type:"adaptive"}
// paired with output_config.effort. This is the only model-keyed branch in
// this file: the adaptive requirement can be selected only by model identity,
// because the API surfaces it solely as a runtime 400 with no build-time
// signal. Add a model here when its API rejects the classic shape.
const ADAPTIVE_THINKING_MODELS: ReadonlySet<string> = new Set([
  "claude-sonnet-5",
]);

// Adaptive thinking is the model's own per-request choice; empirically only
// effort "max" reliably elicits a thinking block to capture (low/medium/high/
// xhigh are non-deterministic).
const ADAPTIVE_THINKING_EFFORT: AnthropicEffort = "max";

// The adaptive path carries no budget_tokens, so it cannot reuse the
// budget-derived THINKING_MAX_TOKENS. A flat ceiling sized to let effort:max
// emit a capturable thinking block; the exact value is not load-bearing, and
// truncation (stop_reason max_tokens) is acceptable for a capability probe.
const ADAPTIVE_THINKING_MAX_TOKENS = 4096;

// Sets the model-appropriate extended-thinking request shape in place: adaptive
// models get thinking:{type:"adaptive"} + output_config.effort; all others get
// the classic thinking:{type:"enabled",budget_tokens}. Owns max_tokens for the
// thinking path because the two shapes size it differently.
function applyThinking(body: AnthropicRequestBody, model: string): void {
  if (ADAPTIVE_THINKING_MODELS.has(model)) {
    body.thinking = { type: "adaptive" };
    body.output_config = { effort: ADAPTIVE_THINKING_EFFORT };
    body.max_tokens = ADAPTIVE_THINKING_MAX_TOKENS;
    return;
  }
  body.thinking = { type: "enabled", budget_tokens: THINKING_BUDGET_TOKENS };
  body.max_tokens = THINKING_MAX_TOKENS;
}

function readMediaBase64(ref: MediaRef): string {
  return readFileSync(resolveMediaPath(ref)).toString("base64");
}

function expectSingleMedia(intent: CapabilityIntent): MediaRef {
  if (intent.media === undefined || intent.media.length === 0) {
    throw new Error(
      "anthropic: media-input capability requires intent.media to be non-empty",
    );
  }
  if (intent.media.length !== 1) {
    throw new Error(
      `anthropic: media-input capability expects exactly one media reference, got ${String(intent.media.length)}`,
    );
  }
  const [media] = intent.media;
  if (media === undefined) {
    throw new Error("anthropic: media-input capability: media[0] is undefined");
  }
  return media;
}

function expectSingleTool(intent: CapabilityIntent): ToolDecl {
  if (intent.tools === undefined || intent.tools.length === 0) {
    throw new Error(
      "anthropic: function-calling capability requires intent.tools to be non-empty",
    );
  }
  if (intent.tools.length !== 1) {
    throw new Error(
      `anthropic: function-calling capability expects exactly one tool, got ${String(intent.tools.length)}`,
    );
  }
  const [tool] = intent.tools;
  if (tool === undefined) {
    throw new Error(
      "anthropic: function-calling capability: tools[0] is undefined",
    );
  }
  return tool;
}

function userTextMessage(prompt: string): AnthropicMessage {
  return { role: "user", content: prompt };
}

function plainTextBody(
  model: string,
  intent: CapabilityIntent,
  opts: { stream: boolean },
): AnthropicRequestBody {
  const body: AnthropicRequestBody = {
    model,
    max_tokens: PLAIN_MAX_TOKENS,
    messages: [userTextMessage(intent.prompt)],
  };
  if (opts.stream) body.stream = true;
  return body;
}

function functionToolDecl(decl: ToolDecl): AnthropicToolDecl {
  return {
    name: decl.name,
    description: decl.description,
    input_schema: decl.parameters,
  };
}

function functionCallingBody(
  model: string,
  intent: CapabilityIntent,
  opts: { stream: boolean; thinking: boolean },
): AnthropicRequestBody {
  const decl = expectSingleTool(intent);
  const body: AnthropicRequestBody = {
    model,
    max_tokens: PLAIN_MAX_TOKENS,
    messages: [userTextMessage(intent.prompt)],
    tools: [functionToolDecl(decl)],
  };
  if (opts.thinking) applyThinking(body, model);
  if (opts.stream) body.stream = true;
  return body;
}

function visionBody(
  model: string,
  intent: CapabilityIntent,
  opts: { stream: boolean },
): AnthropicRequestBody {
  const media = expectSingleMedia(intent);
  if (media.kind !== "image") {
    throw new Error(
      `anthropic vision-input: expected media.kind=image, got ${media.kind}`,
    );
  }
  const body: AnthropicRequestBody = {
    model,
    max_tokens: PLAIN_MAX_TOKENS,
    messages: [
      {
        role: "user",
        content: [
          {
            type: "image",
            source: {
              type: "base64",
              media_type: mediaTypeFor(media),
              data: readMediaBase64(media),
            },
          },
          { type: "text", text: intent.prompt },
        ],
      },
    ],
  };
  if (opts.stream) body.stream = true;
  return body;
}

function documentBody(
  model: string,
  intent: CapabilityIntent,
  opts: { stream: boolean },
): AnthropicRequestBody {
  const media = expectSingleMedia(intent);
  if (media.kind !== "document") {
    throw new Error(
      `anthropic document-input: expected media.kind=document, got ${media.kind}`,
    );
  }
  const mediaType = mediaTypeFor(media);
  if (mediaType !== "application/pdf") {
    throw new Error(
      `anthropic document-input: only application/pdf is supported, got ${mediaType}`,
    );
  }
  const body: AnthropicRequestBody = {
    model,
    max_tokens: PLAIN_MAX_TOKENS,
    messages: [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: {
              type: "base64",
              media_type: "application/pdf",
              data: readMediaBase64(media),
            },
          },
          { type: "text", text: intent.prompt },
        ],
      },
    ],
  };
  if (opts.stream) body.stream = true;
  return body;
}

function codeExecutionBody(
  model: string,
  intent: CapabilityIntent,
  opts: { stream: boolean },
): AnthropicRequestBody {
  const body: AnthropicRequestBody = {
    model,
    max_tokens: PLAIN_MAX_TOKENS,
    messages: [userTextMessage(intent.prompt)],
    tools: [{ type: CODE_EXECUTION_TOOL_TYPE, name: "code_execution" }],
  };
  if (opts.stream) body.stream = true;
  return body;
}

function reasoningBody(
  model: string,
  intent: CapabilityIntent,
  opts: { stream: boolean },
): AnthropicRequestBody {
  const body: AnthropicRequestBody = {
    model,
    max_tokens: PLAIN_MAX_TOKENS,
    messages: [userTextMessage(intent.prompt)],
  };
  applyThinking(body, model);
  if (opts.stream) body.stream = true;
  return body;
}

function groundingBody(
  model: string,
  intent: CapabilityIntent,
  opts: { stream: boolean },
): AnthropicRequestBody {
  const body: AnthropicRequestBody = {
    model,
    max_tokens: PLAIN_MAX_TOKENS,
    messages: [userTextMessage(intent.prompt)],
    tools: [{ type: WEB_SEARCH_TOOL_TYPE, name: "web_search" }],
  };
  if (opts.stream) body.stream = true;
  return body;
}

function redactedThinkingTurn1Body(
  model: string,
  intent: CapabilityIntent,
  opts: { stream: boolean },
): AnthropicRequestBody {
  const body: AnthropicRequestBody = {
    model,
    max_tokens: PLAIN_MAX_TOKENS,
    messages: [userTextMessage(intent.prompt)],
  };
  applyThinking(body, model);
  if (opts.stream) body.stream = true;
  return body;
}

export function buildRequestBody(opts: {
  model: string;
  capability: Capability;
  intent: CapabilityIntent;
}): AnthropicRequestBody {
  switch (opts.capability) {
    case "plain-text":
      return plainTextBody(opts.model, opts.intent, { stream: false });
    case "plain-text-streaming":
      return plainTextBody(opts.model, opts.intent, { stream: true });
    case "function-calling":
      return functionCallingBody(opts.model, opts.intent, {
        stream: false,
        thinking: false,
      });
    case "function-calling-multi-turn":
      return functionCallingBody(opts.model, opts.intent, {
        stream: false,
        thinking: false,
      });
    case "function-calling-multi-turn-streaming":
      return functionCallingBody(opts.model, opts.intent, {
        stream: true,
        thinking: false,
      });
    case "function-calling-with-thinking":
      return functionCallingBody(opts.model, opts.intent, {
        stream: false,
        thinking: true,
      });
    case "function-calling-with-thinking-streaming":
      return functionCallingBody(opts.model, opts.intent, {
        stream: true,
        thinking: true,
      });
    case "redacted-thinking":
      return redactedThinkingTurn1Body(opts.model, opts.intent, {
        stream: false,
      });
    case "redacted-thinking-streaming":
      return redactedThinkingTurn1Body(opts.model, opts.intent, {
        stream: true,
      });
    case "files-api-reference":
    case "files-api-reference-streaming":
      throw new Error(
        `anthropic: capability ${opts.capability} is multipart; ` +
          "use iterateCaptureSteps from the plug-in, not buildRequestBody.",
      );
    case "vision-input":
      return visionBody(opts.model, opts.intent, { stream: false });
    case "vision-input-streaming":
      return visionBody(opts.model, opts.intent, { stream: true });
    case "document-input":
      return documentBody(opts.model, opts.intent, { stream: false });
    case "document-input-streaming":
      return documentBody(opts.model, opts.intent, { stream: true });
    case "code-execution":
      return codeExecutionBody(opts.model, opts.intent, { stream: false });
    case "code-execution-streaming":
      return codeExecutionBody(opts.model, opts.intent, { stream: true });
    case "reasoning-content":
      return reasoningBody(opts.model, opts.intent, { stream: false });
    case "reasoning-content-streaming":
      return reasoningBody(opts.model, opts.intent, { stream: true });
    case "grounding":
      return groundingBody(opts.model, opts.intent, { stream: false });
    case "grounding-streaming":
      return groundingBody(opts.model, opts.intent, { stream: true });
    case "audio-input":
    case "audio-input-streaming":
    case "video-input":
    case "video-input-streaming":
    case "image-output":
    case "image-output-streaming":
    case "safety-classification":
    case "safety-classification-streaming":
    case "structured-output":
    case "structured-output-streaming":
      throw new Error(
        `anthropic: capability ${opts.capability} is not supported by any Anthropic model`,
      );
    default: {
      const exhaustive: never = opts.capability;
      throw new Error(`anthropic: unhandled capability ${String(exhaustive)}`);
    }
  }
}

// Multi-turn helpers.

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function extractAssistantContentBlocks(
  parsed: unknown,
): AnthropicContentBlock[] {
  if (!isRecord(parsed)) {
    throw new Error("anthropic multi-turn: turn-1 response is not an object");
  }
  const content = parsed.content;
  if (!Array.isArray(content)) {
    throw new Error(
      "anthropic multi-turn: turn-1 response has no content array",
    );
  }
  for (const block of content) {
    if (!isRecord(block) || typeof block.type !== "string") {
      throw new Error(
        "anthropic multi-turn: turn-1 response content[] entry is not a block",
      );
    }
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Anthropic may return assistant content blocks (server_tool_use, web_search_tool_result, code_execution_tool_use, citation blocks) that this plug-in does not enumerate. The wire round-trip is what matters; we forward verbatim. The runtime guard above only verifies each entry is an object with a string type field.
  return content as AnthropicContentBlock[];
}

function findFirstToolUse(blocks: readonly AnthropicContentBlock[]): {
  id: string;
  name: string;
} {
  for (const block of blocks) {
    if (block.type === "tool_use") {
      return { id: block.id, name: block.name };
    }
  }
  throw new Error(
    "anthropic multi-turn: turn-1 response had no tool_use content block",
  );
}

interface ToolFollowUp {
  toolName: string;
  content: string;
}

interface UserFollowUp {
  content: string;
}

function partitionFollowUps(intent: CapabilityIntent): {
  tool?: ToolFollowUp;
  user?: UserFollowUp;
} {
  const out: { tool?: ToolFollowUp; user?: UserFollowUp } = {};
  if (intent.followUp === undefined) return out;
  for (const step of intent.followUp) {
    if (step.role === "tool" && out.tool === undefined) {
      out.tool = { toolName: step.toolName, content: step.content };
    } else if (step.role === "user" && out.user === undefined) {
      out.user = { content: step.content };
    }
  }
  return out;
}

// When an intent has no tool-role followUp (true for
// function-calling-with-thinking, whose INTENTS record declares only a
// prompt and a tool decl), fall back to the tool name from intent.tools
// with an empty JSON object payload. Matches the deriveToolFollowUp
// fallback in @intx/inference-discovery-google-genai.
function deriveToolFollowUp(intent: CapabilityIntent): ToolFollowUp {
  const followUps = partitionFollowUps(intent);
  if (followUps.tool !== undefined) return followUps.tool;
  const tools = intent.tools;
  if (tools === undefined || tools.length === 0) {
    throw new Error(
      "anthropic multi-turn: intent has neither followUp.tool nor tools",
    );
  }
  const [tool] = tools;
  if (tool === undefined) {
    throw new Error("anthropic multi-turn: intent.tools[0] is undefined");
  }
  return { toolName: tool.name, content: "{}" };
}

export function buildFunctionCallingTurn2Body(opts: {
  model: string;
  capability: Capability;
  intent: CapabilityIntent;
  turn1Body: AnthropicRequestBody;
  turn1Response: unknown;
}): AnthropicRequestBody {
  const assistantBlocks = extractAssistantContentBlocks(opts.turn1Response);
  const toolUse = findFirstToolUse(assistantBlocks);
  const toolFollowUp = deriveToolFollowUp(opts.intent);
  const messages: AnthropicMessage[] = [
    ...opts.turn1Body.messages,
    { role: "assistant", content: assistantBlocks },
    {
      role: "user",
      content: [
        {
          type: "tool_result",
          tool_use_id: toolUse.id,
          content: toolFollowUp.content,
        },
      ],
    },
  ];
  const body: AnthropicRequestBody = {
    model: opts.model,
    max_tokens: opts.turn1Body.max_tokens,
    messages,
  };
  if (opts.turn1Body.tools !== undefined) body.tools = opts.turn1Body.tools;
  if (opts.turn1Body.thinking !== undefined)
    body.thinking = opts.turn1Body.thinking;
  if (opts.turn1Body.output_config !== undefined)
    body.output_config = opts.turn1Body.output_config;
  if (opts.turn1Body.stream === true) body.stream = true;
  return body;
}

export function buildRedactedThinkingTurn2Body(opts: {
  model: string;
  intent: CapabilityIntent;
  turn1Body: AnthropicRequestBody;
  turn1Response: unknown;
}): AnthropicRequestBody {
  const assistantBlocks = extractAssistantContentBlocks(opts.turn1Response);
  const followUps = partitionFollowUps(opts.intent);
  const userFollowUp = followUps.user;
  if (userFollowUp === undefined) {
    throw new Error(
      "anthropic redacted-thinking: intent.followUp must include a user entry",
    );
  }
  const messages: AnthropicMessage[] = [
    ...opts.turn1Body.messages,
    { role: "assistant", content: assistantBlocks },
    { role: "user", content: userFollowUp.content },
  ];
  const body: AnthropicRequestBody = {
    model: opts.model,
    max_tokens: opts.turn1Body.max_tokens,
    messages,
  };
  if (opts.turn1Body.thinking !== undefined)
    body.thinking = opts.turn1Body.thinking;
  if (opts.turn1Body.output_config !== undefined)
    body.output_config = opts.turn1Body.output_config;
  if (opts.turn1Body.stream === true) body.stream = true;
  return body;
}

export function buildFilesApiGenerateBody(opts: {
  model: string;
  fileId: string;
  intent: CapabilityIntent;
  stream: boolean;
}): AnthropicRequestBody {
  const body: AnthropicRequestBody = {
    model: opts.model,
    max_tokens: PLAIN_MAX_TOKENS,
    messages: [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: { type: "file", file_id: opts.fileId },
          },
          { type: "text", text: opts.intent.prompt },
        ],
      },
    ],
  };
  if (opts.stream) body.stream = true;
  return body;
}

// Capability-keyed model-supports check. Anthropic's three current
// models (Sonnet, Opus, Haiku) all expose the same surface; if that
// stops being true, this gate is where to encode the divergence.
const SUPPORTED_CAPABILITIES: ReadonlySet<Capability> = new Set<Capability>([
  "plain-text",
  "plain-text-streaming",
  "function-calling",
  "function-calling-multi-turn",
  "function-calling-multi-turn-streaming",
  "function-calling-with-thinking",
  "function-calling-with-thinking-streaming",
  "vision-input",
  "vision-input-streaming",
  "document-input",
  "document-input-streaming",
  "code-execution",
  "code-execution-streaming",
  "reasoning-content",
  "reasoning-content-streaming",
  "grounding",
  "grounding-streaming",
  "files-api-reference",
  "files-api-reference-streaming",
  "redacted-thinking",
  "redacted-thinking-streaming",
]);

export function isSupportedCapability(capability: Capability): boolean {
  return SUPPORTED_CAPABILITIES.has(capability);
}
