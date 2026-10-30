// OpenAI Responses API provider adapter.
//
// Ported from `@corbits/openai-responses` 0.2.3
// (https://github.com/corbitsdev/corbits-openai-responses, LGPL-2.1-only)
// and grounded in this repository's inference contracts. The port keeps the
// source's streaming and buffered parsing, tool calls, usage mapping,
// supported media, quirks validation, factory composition hooks, and
// provider- and model-scoped reasoning signature replay. Header extraction
// reuses the Chat Completions adapter's extractors (the Responses surface
// shares those headers byte-for-byte) instead of duplicating them.

import { type } from "arktype";

import type { InferenceEvent } from "@intx/types/inference-events";
import type {
  ContentBlock,
  ConversationTurn,
  InferenceEffort,
  InferenceOptions,
  LastCycleSource,
  TokenUsage,
} from "@intx/types/runtime-core";
import { formatSafetyRatingText } from "@intx/types/format-safety-rating";
import type { AdapterFactory, BuiltRequest, ProviderAdapter } from "../adapter";
import { BEARER_CREDENTIAL_SENTINEL } from "../auth";
import { warnDroppedOption } from "../effort";
import { ProtocolMismatchError } from "../errors";
import {
  decodeToolName,
  encodeToolName,
  type ToolNameLimit,
} from "../tool-name";
import { extractPacingDelayMs, extractRetryAfterMs } from "./openai";

// Wire-charset limit for function names on the Responses surface:
// `^[a-zA-Z0-9_-]{1,64}$` across every vendor that speaks this protocol.
const RESPONSES_TOOL_NAME_LIMIT: ToolNameLimit = {
  provider: "responses",
  maxLength: 64,
};

// ---------------------------------------------------------------------------
// Quirks
// ---------------------------------------------------------------------------
//
// Everything a vendor (Codex, xAI/Grok, plain OpenAI, ...) needs to bend the
// shared Responses wire protocol to its own backend, expressed as data
// instead of a fork of the adapter. Every field here must be JSON-safe: the
// quirks bag rides inside an `InferenceSource`, which is persisted and sent
// over the wire, so a function-valued field would silently fail to survive
// that round trip. Code-shaped accommodations live in `ResponsesHooks`
// instead — see `responsesAdapterFactory` below.

const HeaderFromOption = type({
  optionKey: "string",
  header: "string",
});

const ResponsesHeaders = type({
  "static?": { "[string]": "string" },
  "modelHeader?": "string",
  "fromOption?": HeaderFromOption.array(),
});

// `shape: "string"` sends plain string content (Grok/OpenAI); `"parts"`
// sends a single `input_text` content part (Codex, which addresses the item
// as a `developer` message).
const SystemPromptPlacement = type({
  role: "'system' | 'developer'",
  shape: "'string' | 'parts'",
});

const ReasoningQuirks = type({
  "summary?": "'auto' | 'detailed'",
  "effortOption?": "string",
  // The wire value Interchange's neutral `max` effort becomes. The highest
  // level differs per model, so only the source names it.
  "maxEffort?": "'xhigh' | 'max'",
});
type ReasoningQuirks = typeof ReasoningQuirks.infer;

// Per-source accommodations for the OpenAI Responses API wire protocol.
// Every field is optional; an absent field resolves to the strict protocol
// default, so a source that supplies no quirks gets no accommodation and
// must opt into lenient behavior explicitly.
export const ResponsesQuirks = type({
  "path?": "string",
  "headers?": ResponsesHeaders,
  "sessionIdOption?": "string",
  "sessionIdHeader?": "string",
  "systemPrompt?": SystemPromptPlacement,
  // `typed` is the protocol-native shape (`output_text` for assistant items,
  // `input_text` otherwise); `flat` collapses text-only content to a plain
  // string for backends that reject typed parts.
  "contentShape?": "'typed' | 'flat'",
  // Sent verbatim when present; absent omits the field so the backend's own
  // default applies. Some backends require an explicit `false`.
  "parallelToolCalls?": "boolean",
  // The Responses protocol forwards a caller-supplied maxTokens/temperature
  // verbatim; these default true. Set `false` only when a vendor's backend
  // rejects the field outright (the quirk opts a source OUT of protocol-
  // native forwarding, not into it).
  "maxOutputTokens?": "boolean",
  "temperature?": "boolean",
  "store?": "boolean",
  "stream?": "boolean",
  "reasoning?": ReasoningQuirks,
  "instructions?": "string",
  // Reject unknown keys so a mistyped quirk name fails loudly at
  // construction rather than being silently ignored and running with
  // default behavior.
  "+": "reject",
});
export type ResponsesQuirks = typeof ResponsesQuirks.infer;

// Quirks resolved to concrete values at the factory edge, so interior code
// never re-decides a default.
export type ResolvedResponsesQuirks = {
  path: string;
  headers: {
    static: Record<string, string>;
    modelHeader: string | undefined;
    fromOption: { optionKey: string; header: string }[];
  };
  sessionIdOption: string | undefined;
  sessionIdHeader: string | undefined;
  systemPrompt: { role: "system" | "developer"; shape: "string" | "parts" };
  contentShape: "typed" | "flat";
  parallelToolCalls: boolean | undefined;
  maxOutputTokens: boolean;
  temperature: boolean;
  store: boolean;
  stream: boolean;
  reasoning: ReasoningQuirks | undefined;
  instructions: string | undefined;
};

/**
 * TS-only composition hooks a caller can bake into a factory-built adapter.
 * Never JSON, never part of the quirks bag: quirks ride inside an
 * `InferenceSource`, which is persisted and sent over the wire, so a
 * function-valued field would not survive that round trip.
 */
export type ResponsesHooks = {
  wrapSystemPrompt?: (systemPrompt: string) => string;
  includeReasoningEffort?: (effort: string) => boolean;
};

// Parses an adapter's `quirks` argument (an `InferenceSource.quirks` bag)
// into a validated `ResponsesQuirks` and fills in defaults. An absent bag
// (`undefined`) resolves the same as `{}` — protocol-native defaults, not a
// thrown error. Throws on shape violations rather than silently ignoring an
// unrecognized or misspelled field.
function parseResponsesQuirks(raw: unknown): ResolvedResponsesQuirks {
  const validated = ResponsesQuirks(raw ?? {});
  if (validated instanceof type.errors) {
    throw new Error(
      `openai-responses adapter: invalid quirks: ${validated.summary}`,
    );
  }
  // Header names are case-insensitive; lowercasing lets a quirk header
  // override the stock lowercase ones instead of being sent alongside them.
  // No quirk may name the credential header, so neither a persisted bag nor
  // a caller's providerOptions can replace the sentinel the harness swaps
  // for the real credential.
  function headerName(field: string, name: string): string {
    const lower = name.toLowerCase();
    if (lower === "authorization") {
      throw new Error(
        `openai-responses adapter: invalid quirks: ${field} must not set authorization`,
      );
    }
    return lower;
  }
  const staticHeaders: Record<string, string> = {};
  for (const [name, value] of Object.entries(validated.headers?.static ?? {})) {
    staticHeaders[headerName("headers.static", name)] = value;
  }
  const modelHeader = validated.headers?.modelHeader;
  const sessionIdHeader = validated.sessionIdHeader;
  return {
    path: validated.path ?? "/responses",
    headers: {
      static: staticHeaders,
      modelHeader:
        modelHeader === undefined
          ? undefined
          : headerName("headers.modelHeader", modelHeader),
      fromOption: (validated.headers?.fromOption ?? []).map(
        ({ optionKey, header }) => ({
          optionKey,
          header: headerName("headers.fromOption", header),
        }),
      ),
    },
    sessionIdOption: validated.sessionIdOption,
    sessionIdHeader:
      sessionIdHeader === undefined
        ? undefined
        : headerName("sessionIdHeader", sessionIdHeader),
    // Protocol-native behaviour always sends the caller's system prompt as
    // a plain `system` message; a vendor whose backend addresses it
    // differently (Codex: `developer` role, parts shape) overrides this
    // quirk instead of opting out of sending it at all.
    systemPrompt: validated.systemPrompt ?? {
      role: "system",
      shape: "string",
    },
    contentShape: validated.contentShape ?? "typed",
    parallelToolCalls: validated.parallelToolCalls,
    maxOutputTokens: validated.maxOutputTokens ?? true,
    temperature: validated.temperature ?? true,
    store: validated.store ?? false,
    stream: validated.stream ?? true,
    reasoning: validated.reasoning,
    instructions: validated.instructions,
  };
}

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

// Tool results carry a content array; the Responses API wants a string. Join
// the text parts; non-text content is not representable here and is dropped
// with a marker so the model is not misled into thinking it went missing
// silently. The join and the `<error>` wrapping match the Chat Completions
// adapter's `role: "tool"` encoding, whose schema likewise has no `is_error`
// field, so the same conversation history feeds the model identical
// tool-output text on both wires.
function toolResultText(
  block: Extract<ContentBlock, { type: "tool_result" }>,
): string {
  const parts: string[] = [];
  for (const c of block.content) {
    if (c.type === "text") parts.push(c.text);
    else parts.push(`[unsupported ${c.type} content omitted]`);
  }
  const text = parts.join("\n");
  return block.isError ? `<error>\n${text}\n</error>` : text;
}

// Grounded on the OpenAI Responses API document input shape: a base64
// document synthesizes a deterministic filename from its mimeType, since
// MediaSource carries no filename field of its own. Only PDF is supported
// today, matching the Chat Completions adapter's own document support.
function filenameForDocumentMime(mimeType: string): string {
  if (mimeType === "application/pdf") return "document.pdf";
  throw new Error(
    `openai-responses adapter: document input currently supports application/pdf only; received mimeType: ${mimeType}`,
  );
}

// Converts one content block to zero or one Responses content part. Blocks
// handled as whole `ResponsesInputItem`s elsewhere (tool_call, tool_result,
// signed thinking) are not passed here.
function toResponsesContentPart(
  block: ContentBlock,
  textKind: "input_text" | "output_text",
): Record<string, unknown> | undefined {
  switch (block.type) {
    case "text":
      return { type: textKind, text: block.text };
    case "safety_rating":
      // No input wire shape for safety_rating; render it as text so a
      // safety_rating-only turn is not a silently empty message.
      return { type: textKind, text: formatSafetyRatingText(block) };
    case "image": {
      const source = block.source;
      if (source.kind === "base64") {
        return {
          type: "input_image",
          image_url: `data:${source.mimeType};base64,${source.data}`,
        };
      }
      if (source.kind === "url") {
        return { type: "input_image", image_url: source.url };
      }
      // Unlike Chat Completions' image_url-only surface, the Responses API
      // accepts an uploaded file_id directly on an input_image part.
      return { type: "input_image", file_id: source.reference };
    }
    case "document": {
      const source = block.source;
      if (source.kind === "base64") {
        return {
          type: "input_file",
          filename: filenameForDocumentMime(source.mimeType),
          file_data: `data:${source.mimeType};base64,${source.data}`,
        };
      }
      if (source.kind === "file-reference") {
        return { type: "input_file", file_id: source.reference };
      }
      throw new Error(
        `openai-responses adapter: input_file does not accept url document sources; the Responses API only takes base64 data (file_data) or an uploaded file_id. Received url: ${source.url}`,
      );
    }
    case "audio":
    case "video":
      throw new Error(
        `openai-responses adapter: adapter does not yet handle ${block.type} content blocks.`,
      );
    case "citation":
    case "redacted_thinking":
      // Server-emitted attribution/opaque metadata with no Responses input
      // wire shape; dropping it is a silent no-op, not data loss the caller
      // needs to see, matching the Chat Completions adapter's own policy.
      return undefined;
    case "code_execution_request":
    case "code_execution_result":
      throw new Error(
        `openai-responses adapter: adapter does not handle ${block.type} content blocks.`,
      );
    case "refusal":
      throw new Error(
        "openai-responses adapter: adapter does not handle refusal content blocks.",
      );
    case "thinking":
    case "tool_call":
    case "tool_result":
      // Handled as whole items in toResponsesItems, not as content parts.
      return undefined;
  }
}

// Map one internal turn to zero or more Responses items. Reasoning blocks
// are echoed back only when their signature was issued by this provider for
// the requested model; otherwise only the reasoning item is dropped, and the
// function_call items it preceded are kept so each function_call_output
// still has its call.
function toResponsesItems(
  turn: ConversationTurn,
  requestModel: string,
  requestProvider: string,
  contentShape: "typed" | "flat",
): Record<string, unknown>[] {
  const items: Record<string, unknown>[] = [];
  const role = turn.role;
  const textKind =
    contentShape === "typed" && role === "assistant"
      ? "output_text"
      : "input_text";
  const parts: Record<string, unknown>[] = [];
  let hasNonTextPart = false;
  const flush = (): void => {
    if (parts.length === 0) return;
    const content =
      contentShape === "flat" && !hasNonTextPart
        ? parts.map((part) => ("text" in part ? part.text : "")).join("")
        : [...parts];
    items.push({ type: "message", role, content });
    parts.length = 0;
    hasNonTextPart = false;
  };
  for (const block of turn.content) {
    if (block.type === "tool_call") {
      flush();
      items.push({
        type: "function_call",
        name: encodeToolName(block.name, RESPONSES_TOOL_NAME_LIMIT),
        arguments: JSON.stringify(block.arguments ?? {}),
        call_id: block.id,
      });
      continue;
    }
    if (block.type === "tool_result") {
      flush();
      items.push({
        type: "function_call_output",
        call_id: block.callId,
        output: toolResultText(block),
      });
      continue;
    }
    if (block.type === "thinking") {
      if (typeof block.signature !== "string" || block.signature.length === 0)
        continue;
      flush();
      const encryptedContent = signatureForModel(
        turn,
        requestModel,
        requestProvider,
        block.signature,
      );
      if (encryptedContent !== undefined)
        items.push({
          type: "reasoning",
          summary: [],
          encrypted_content: encryptedContent,
        });
      continue;
    }
    const part = toResponsesContentPart(block, textKind);
    if (part === undefined) continue;
    if (part.type !== "input_text" && part.type !== "output_text")
      hasNonTextPart = true;
    parts.push(part);
  }
  flush();
  return items;
}

// Keeps the LAST occurrence of each duplicate function_call /
// function_call_output call_id, not the first: a duplicate is most often a
// corrected retry, and discarding the retry in favor of the stale original
// silently replays the wrong tool result. Both item types are covered — a
// duplicated function_call is just as invalid on the wire as a duplicated
// output.
function dedupeToolItems(
  items: Record<string, unknown>[],
): Record<string, unknown>[] {
  const lastIndexForCall = new Map<string, number>();
  items.forEach((item, i) => {
    if (item.type === "function_call" || item.type === "function_call_output") {
      lastIndexForCall.set(`${String(item.type)}:${String(item.call_id)}`, i);
    }
  });
  return items.filter((item, i) => {
    if (item.type === "function_call" || item.type === "function_call_output") {
      return (
        lastIndexForCall.get(`${String(item.type)}:${String(item.call_id)}`) ===
        i
      );
    }
    return true;
  });
}

function toResponsesTools(
  options: InferenceOptions,
): Record<string, unknown>[] | undefined {
  if (options.tools === undefined || options.tools.length === 0) {
    return undefined;
  }
  // Responses function tools are FLAT — name/description/parameters sit
  // beside `type`, not nested under a `function` key (unlike Chat
  // Completions).
  return options.tools.map((t) => ({
    type: "function",
    name: encodeToolName(t.name, RESPONSES_TOOL_NAME_LIMIT),
    description: t.description,
    parameters: t.inputSchema,
  }));
}

function optionString(
  options: InferenceOptions,
  key: string,
): string | undefined {
  const value = options.providerOptions?.[key];
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function systemPromptItem(
  quirks: ResolvedResponsesQuirks,
  systemPrompt: string,
  hooks: ResponsesHooks | undefined,
): Record<string, unknown> {
  const placement = quirks.systemPrompt;
  const text =
    hooks?.wrapSystemPrompt !== undefined
      ? hooks.wrapSystemPrompt(systemPrompt)
      : systemPrompt;
  const content =
    placement.shape === "string" ? text : [{ type: "input_text", text }];
  return { type: "message", role: placement.role, content };
}

// Interchange's neutral effort: low, medium and high pass as given, and max
// is the source's `maxEffort`, else high. The Chat Completions adapter maps
// it the same way but warns on that fallback via `warnDroppedOption`.
function wireEffort(
  model: string,
  effort: InferenceEffort | undefined,
  maxEffort: "xhigh" | "max" | undefined,
): string | undefined {
  if (effort !== "max") return effort;
  if (maxEffort !== undefined) return maxEffort;
  warnDroppedOption(
    model,
    `effort "max"`,
    'no maxEffort quirk on this source, so it sends "high"',
  );
  return "high";
}

function reasoningField(
  model: string,
  quirks: ResolvedResponsesQuirks,
  options: InferenceOptions,
  hooks: ResponsesHooks | undefined,
): Record<string, unknown> | undefined {
  const reasoning = quirks.reasoning;
  const field: Record<string, unknown> = {};
  if (reasoning?.summary !== undefined) field["summary"] = reasoning.summary;
  const effortOption = reasoning?.effortOption;
  const fromOption =
    effortOption === undefined
      ? undefined
      : optionString(options, effortOption);
  const effort =
    fromOption ?? wireEffort(model, options.effort, reasoning?.maxEffort);
  if (
    effort !== undefined &&
    (hooks?.includeReasoningEffort === undefined ||
      hooks.includeReasoningEffort(effort))
  ) {
    field["effort"] = effort;
  }
  return Object.keys(field).length > 0 ? field : undefined;
}

export function buildResponsesRequest(
  quirks: ResolvedResponsesQuirks,
  messages: ConversationTurn[],
  model: string,
  options: InferenceOptions,
  requestProvider: string,
  hooks: ResponsesHooks | undefined,
): BuiltRequest {
  const conversation = dedupeToolItems(
    messages.flatMap((turn) =>
      toResponsesItems(turn, model, requestProvider, quirks.contentShape),
    ),
  );
  const leadingSystemItem =
    options.systemPrompt !== undefined
      ? systemPromptItem(quirks, options.systemPrompt, hooks)
      : undefined;
  const input =
    leadingSystemItem !== undefined
      ? [leadingSystemItem, ...conversation]
      : conversation;
  const tools = toResponsesTools(options);
  const body: Record<string, unknown> = {
    model,
    input,
    store: quirks.store,
    stream: quirks.stream,
    include: ["reasoning.encrypted_content"],
  };
  if (quirks.instructions !== undefined) {
    body["instructions"] = quirks.instructions;
  }
  if (quirks.parallelToolCalls !== undefined) {
    body["parallel_tool_calls"] = quirks.parallelToolCalls;
  }
  if (tools !== undefined) {
    body["tools"] = tools;
    body["tool_choice"] = "auto";
  }
  if (quirks.maxOutputTokens && options.maxTokens !== undefined) {
    body["max_output_tokens"] = options.maxTokens;
  }
  if (quirks.temperature && options.temperature !== undefined) {
    body["temperature"] = options.temperature;
  }
  const reasoning = reasoningField(model, quirks, options, hooks);
  if (reasoning !== undefined) body["reasoning"] = reasoning;
  const sessionId =
    quirks.sessionIdOption !== undefined
      ? optionString(options, quirks.sessionIdOption)
      : undefined;
  // With store:false this is the only cache-routing signal; keying it to the
  // inference thread's session id keeps every request on the same cache
  // shard.
  if (sessionId !== undefined) body["prompt_cache_key"] = sessionId;
  const headers: Record<string, string> = {
    "content-type": "application/json",
    // A non-streaming request must ask for a plain JSON body: sending
    // `text/event-stream` on `stream: false` gets some backends to 406, and
    // others to silently ignore the header and return JSON anyway — neither
    // is a wire contract to depend on.
    accept: quirks.stream ? "text/event-stream" : "application/json",
    authorization: BEARER_CREDENTIAL_SENTINEL,
  };
  for (const [name, value] of Object.entries(quirks.headers.static)) {
    headers[name] = value;
  }
  if (quirks.headers.modelHeader !== undefined) {
    headers[quirks.headers.modelHeader] = model;
  }
  for (const { optionKey, header } of quirks.headers.fromOption) {
    const value = optionString(options, optionKey);
    if (value !== undefined) headers[header] = value;
  }
  if (sessionId !== undefined && quirks.sessionIdHeader !== undefined) {
    headers[quirks.sessionIdHeader] = sessionId;
  }
  return { url: quirks.path, headers, body: JSON.stringify(body) };
}

// A thinking block's `signature` is opaque ciphertext a specific backend
// issued for a specific model; only that backend can decrypt it. `model` is
// arbitrary catalog/user-supplied text — nothing stops two distinct backends
// (proxy aliases, two OpenAI-compatible endpoints) from declaring the same
// literal model name, so comparing `turn.model` alone treats a foreign
// signature as safe to replay. `ConversationTurn` carries no field for which
// provider produced it, so provenance rides inside the signature string
// itself: capture tags it `<provider>:<ciphertext>` (see `tagSignature`),
// and replay only unwraps the ciphertext when both the tagged provider and
// the model match the current request.
//
// Provider, not the per-account source id, is the unit of decrypt
// capability — a backend shared across accounts can decrypt a signature
// issued to any of them, so keying on provider (rather than source id) is
// what lets an account switch keep reasoning continuity while a genuine
// cross-provider collision still gets dropped. A poisoned history self-heals
// on the next request instead of being replayed forever.
const SIGNATURE_TAG_SEPARATOR = ":";

export function tagSignature(
  provider: string,
  encryptedContent: string,
): string {
  return `${provider}${SIGNATURE_TAG_SEPARATOR}${encryptedContent}`;
}

function untagSignature(
  tagged: string,
): { provider: string; encryptedContent: string } | undefined {
  const idx = tagged.indexOf(SIGNATURE_TAG_SEPARATOR);
  if (idx === -1) return undefined;
  return {
    provider: tagged.slice(0, idx),
    encryptedContent: tagged.slice(idx + 1),
  };
}

// Returns the replayable `encrypted_content` for a thinking block's
// signature, or `undefined` when it must not be replayed on this request
// (persisted-turn model mismatch, or a signature tagged for a different
// provider).
function signatureForModel(
  turn: ConversationTurn,
  requestModel: string,
  requestProvider: string,
  signature: string,
): string | undefined {
  // `model` is optional on the persisted turn schema; a turn saved before
  // that field existed (or otherwise missing it) is not evidence of a model
  // switch — treat the absence as benign and fall through to the provider
  // check, rather than dropping reasoning that never actually crossed
  // models.
  if (turn.model !== undefined && turn.model !== requestModel) {
    return undefined;
  }
  const tagged = untagSignature(signature);
  if (tagged === undefined) return undefined;
  return tagged.provider === requestProvider
    ? tagged.encryptedContent
    : undefined;
}

// ---------------------------------------------------------------------------
// Streaming response parsing
// ---------------------------------------------------------------------------
//
// Every `response.*` event type this adapter acts on gets a schema here. An
// event whose `type` is not one of these is protocol-legal but uninteresting
// (a lifecycle envelope this adapter has nothing to do with) and is ignored;
// an event whose `type` IS one of these but whose shape fails validation is
// a genuine protocol mismatch and throws.

const EMPTY_PARTIAL = { text: "" };

const ResponsesEnvelope = type({ type: "string" });
const TextDeltaEvent = type({ item_id: "string > 0", delta: "string" });
// Every reasoning delta the Responses API emits carries the item_id of the
// reasoning item it belongs to and a delta string (possibly empty); an event
// of this type missing either field is not a lenient variant, it's a
// protocol mismatch this adapter cannot attribute to any block.
const ReasoningDeltaEvent = type({
  item_id: "string > 0",
  delta: "string",
});
const RefusalDeltaEvent = type({ item_id: "string > 0", delta: "string" });
const RefusalDoneEvent = type({
  item_id: "string > 0",
  refusal: "string",
});
const FunctionCallArgsDeltaEvent = type({
  item_id: "string > 0",
  delta: "string",
});
const OutputItemAddedEvent = type({
  item: {
    type: "string",
    "id?": "string > 0",
    "call_id?": "string > 0",
    "name?": "string > 0",
  },
});
const OutputItemDoneEvent = type({
  item: {
    type: "string",
    "id?": "string > 0",
    "encrypted_content?": "string",
  },
});
const ResponsesUsage = type({
  "input_tokens?": "number",
  "output_tokens?": "number",
  "input_tokens_details?": type({
    "cached_tokens?": "number",
    "cache_write_tokens?": "number",
    "cache_creation_tokens?": "number",
  }).or("null"),
  "output_tokens_details?": type({ "reasoning_tokens?": "number" }).or("null"),
}).or("null");
const CompletedEvent = type({ response: { "usage?": ResponsesUsage } });
const FailedEvent = type({
  "response?": { "error?": { "message?": "string" } },
});
const IncompleteEvent = type({
  response: {
    "incomplete_details?": { "reason?": "string" },
    "usage?": ResponsesUsage,
  },
});
const ErrorEvent = type({ "message?": "string" });

type ResponsesUsageShape = typeof ResponsesUsage.infer | undefined;

// Maps the Responses API's usage object onto the internal TokenUsage,
// splitting the cached-token subset out of input_tokens. Shared by the SSE
// `response.completed` handler and the non-streaming parseJSONResponse,
// whose usage objects carry the same field names.
function toInferenceUsage(usage: ResponsesUsageShape): TokenUsage {
  if (usage === null || usage === undefined) {
    return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
  }
  // Responses-API `input_tokens` counts the full prompt and `cached_tokens`
  // is a subset of it. Downstream consumers treat `TokenUsage` fields as
  // non-overlapping and sum them, so the cached subset must be split out of
  // input here — emitting the wire counts verbatim double-counts every
  // cached token and inflates context occupancy up to ~2x on high
  // cache-hit sessions.
  const totalInput = usage.input_tokens ?? 0;
  const details = usage.input_tokens_details;
  // Counters are non-negative quantities: a negative count off the wire is
  // garbage, clamped to zero rather than propagated into sums or allowed to
  // inflate input by subtracting a negative.
  const cachedTokens = Math.max(0, details?.cached_tokens ?? 0);
  // OpenAI (GPT-5.6+) reports cache writes as `cache_write_tokens` and
  // documents them as a subset of `input_tokens`, so they must be split out
  // of input exactly like `cached_tokens`. Gateways fronting OpenAI-shaped
  // endpoints report the Anthropic-shaped `cache_creation_tokens` instead;
  // its subset relationship to `input_tokens` is unobservable from here, so
  // that fallback keeps the historic behavior of not reducing input.
  const rawWriteTokens = details?.cache_write_tokens;
  const openaiWriteTokens =
    rawWriteTokens === undefined ? undefined : Math.max(0, rawWriteTokens);
  return {
    input: Math.max(0, totalInput - cachedTokens - (openaiWriteTokens ?? 0)),
    output: Math.max(0, usage.output_tokens ?? 0),
    cacheRead: cachedTokens,
    cacheWrite: Math.max(
      0,
      openaiWriteTokens ?? details?.cache_creation_tokens ?? 0,
    ),
    thinking: Math.max(0, usage.output_tokens_details?.reasoning_tokens ?? 0),
  };
}

export type ResponsesBlockIndexer = {
  nextIndex: number;
  items: Map<string, { index: number; kind: string }>;
};

export function createResponsesBlockIndexer(): ResponsesBlockIndexer {
  return { nextIndex: 0, items: new Map() };
}

function getOrAssignBlockIndex(
  state: ResponsesBlockIndexer,
  itemId: string,
  kind: string,
): number {
  const existing = state.items.get(itemId);
  if (existing !== undefined) return existing.index;
  const index = state.nextIndex;
  state.nextIndex += 1;
  state.items.set(itemId, { index, kind });
  return index;
}

function protocolMismatch(
  provider: string,
  message: string,
  raw: unknown,
): ProtocolMismatchError {
  return new ProtocolMismatchError(
    `${provider} parseResponse: ${message}`,
    raw,
  );
}

export function parseResponse(
  sseData: string,
  indexer: ResponsesBlockIndexer,
  source: LastCycleSource,
): InferenceEvent[] {
  const provider = source.provider;
  let parsed: unknown;
  try {
    parsed = JSON.parse(sseData);
  } catch (cause) {
    throw new ProtocolMismatchError(
      `${provider} parseResponse: malformed JSON in SSE data payload: ${cause instanceof Error ? cause.message : String(cause)}`,
      sseData,
    );
  }
  const envelope = ResponsesEnvelope(parsed);
  if (envelope instanceof type.errors) {
    throw protocolMismatch(
      provider,
      `SSE envelope failed schema validation: ${envelope.summary}`,
      parsed,
    );
  }
  const eventType: string = envelope.type;
  const seq = 0;
  const events: InferenceEvent[] = [];
  switch (eventType) {
    case "response.output_text.delta": {
      const validated = TextDeltaEvent(parsed);
      if (validated instanceof type.errors) {
        throw protocolMismatch(
          provider,
          `output_text.delta failed schema validation: ${validated.summary}`,
          parsed,
        );
      }
      if (validated.delta.length > 0) {
        events.push({
          type: "inference.text.delta",
          seq,
          data: {
            token: validated.delta,
            partial: EMPTY_PARTIAL,
            index: getOrAssignBlockIndex(indexer, validated.item_id, "text"),
          },
        });
      }
      return events;
    }
    case "response.reasoning_summary_text.delta":
    case "response.reasoning_text.delta": {
      const validated = ReasoningDeltaEvent(parsed);
      if (validated instanceof type.errors) {
        throw protocolMismatch(
          provider,
          `${eventType} failed schema validation: ${validated.summary}`,
          parsed,
        );
      }
      // Always register the block and emit a thinking delta (even for empty
      // tokens). This ensures a preceding thinking block exists for any
      // subsequent signature, supporting reasoning items whose visible
      // summary may be empty or delivered only via the done envelope.
      const index = getOrAssignBlockIndex(
        indexer,
        validated.item_id,
        "thinking",
      );
      events.push({
        type: "inference.thinking.delta",
        seq,
        data: { token: validated.delta, partial: EMPTY_PARTIAL, index },
      });
      return events;
    }
    case "response.refusal.delta": {
      const validated = RefusalDeltaEvent(parsed);
      if (validated instanceof type.errors) {
        throw protocolMismatch(
          provider,
          `refusal.delta failed schema validation: ${validated.summary}`,
          parsed,
        );
      }
      if (validated.delta.length > 0) {
        events.push({
          type: "inference.refusal.delta",
          seq,
          data: {
            token: validated.delta,
            partial: EMPTY_PARTIAL,
            index: getOrAssignBlockIndex(indexer, validated.item_id, "refusal"),
          },
        });
      }
      return events;
    }
    case "response.refusal.done": {
      const validated = RefusalDoneEvent(parsed);
      if (validated instanceof type.errors) {
        throw protocolMismatch(
          provider,
          `refusal.done failed schema validation: ${validated.summary}`,
          parsed,
        );
      }
      // The deltas already streamed the full text; the done envelope carries
      // no incremental payload this adapter needs.
      return events;
    }
    case "response.output_item.added": {
      const validated = OutputItemAddedEvent(parsed);
      if (validated instanceof type.errors) {
        throw protocolMismatch(
          provider,
          `output_item.added failed schema validation: ${validated.summary}`,
          parsed,
        );
      }
      const { item } = validated;
      if (item.type === "function_call") {
        // A function_call start missing id/call_id/name is not a lenient
        // partial to skip past — the harness would then see a
        // function_call_arguments.delta with no corresponding
        // inference.tool_call.start and silently drop the fragments.
        if (
          item.id === undefined ||
          item.call_id === undefined ||
          item.name === undefined
        ) {
          throw protocolMismatch(
            provider,
            "output_item.added function_call is missing id, call_id, or name",
            parsed,
          );
        }
        events.push({
          type: "inference.tool_call.start",
          seq,
          data: {
            callId: item.call_id,
            name: decodeToolName(item.name),
            partial: EMPTY_PARTIAL,
            index: getOrAssignBlockIndex(indexer, item.id, "tool_call"),
          },
        });
      } else if (item.type === "reasoning") {
        // A reasoning item with no id cannot be pre-registered, and the
        // signature it will later carry on output_item.done could never be
        // attached to a block — that's a protocol mismatch, not a skip.
        if (item.id === undefined) {
          throw protocolMismatch(
            provider,
            "output_item.added reasoning item is missing id",
            parsed,
          );
        }
        // Pre-register reasoning items on added so the index is stable even
        // if no text deltas follow (pure-encrypted case).
        const index = getOrAssignBlockIndex(indexer, item.id, "thinking");
        events.push({
          type: "inference.thinking.delta",
          seq,
          data: { token: "", partial: EMPTY_PARTIAL, index },
        });
      }
      return events;
    }
    case "response.output_item.done": {
      const validated = OutputItemDoneEvent(parsed);
      if (validated instanceof type.errors) {
        throw protocolMismatch(
          provider,
          `output_item.done failed schema validation: ${validated.summary}`,
          parsed,
        );
      }
      // Capture the encrypted reasoning blob (signature) so it can be echoed
      // back on the next turn. Required for multi-turn continuity when the
      // backend uses store:false + reasoning.encrypted_content. A thinking
      // block is ensured to exist (emitting an empty delta if this is the
      // first signal for the item) so the harness can attach the signature
      // without a protocol-mismatch error.
      const { item } = validated;
      if (item.type === "reasoning") {
        // Neither field present means the backend closed a reasoning item
        // this adapter can neither index (no id) nor carry forward (no
        // encrypted_content) — silently doing nothing here would drop a
        // reasoning turn's continuity with no signal anywhere.
        if (item.id === undefined && item.encrypted_content === undefined) {
          throw protocolMismatch(
            provider,
            "output_item.done reasoning item has neither id nor encrypted_content",
            parsed,
          );
        }
        if (item.encrypted_content !== undefined && item.id === undefined) {
          throw protocolMismatch(
            provider,
            "output_item.done reasoning item carries encrypted_content but is missing id",
            parsed,
          );
        }
        if (item.id !== undefined && item.encrypted_content !== undefined) {
          const itemId = item.id;
          const hadPrior = indexer.items.has(itemId);
          const index = getOrAssignBlockIndex(indexer, itemId, "thinking");
          if (!hadPrior) {
            events.push({
              type: "inference.thinking.delta",
              seq,
              data: { token: "", partial: EMPTY_PARTIAL, index },
            });
          }
          events.push({
            type: "inference.block.signature",
            seq,
            data: {
              signature: tagSignature(provider, item.encrypted_content),
              index,
            },
          });
        }
      }
      return events;
    }
    case "response.function_call_arguments.delta": {
      const validated = FunctionCallArgsDeltaEvent(parsed);
      if (validated instanceof type.errors) {
        throw protocolMismatch(
          provider,
          `function_call_arguments.delta failed schema validation: ${validated.summary}`,
          parsed,
        );
      }
      // The block must already have been registered by
      // `response.output_item.added`; an argument fragment for an item_id
      // this adapter never saw start is an orphan the harness has no
      // tool_call.start to route it against, not a fresh block to
      // synthesize silently.
      const existing = indexer.items.get(validated.item_id);
      if (existing === undefined) {
        throw protocolMismatch(
          provider,
          `function_call_arguments.delta for item_id ${validated.item_id} was never announced by output_item.added`,
          parsed,
        );
      }
      if (validated.delta.length > 0) {
        const blockIndex = existing.index;
        events.push({
          type: "inference.tool_call.delta",
          seq,
          data: {
            // The harness routes argument fragments by a per-stream
            // placeholder keyed to the block index registered on the start
            // event.
            callId: String(blockIndex),
            argumentFragment: validated.delta,
            partial: EMPTY_PARTIAL,
            index: blockIndex,
          },
        });
      }
      return events;
    }
    // `response.done` is an alias some backends emit for `response.completed`;
    // both carry the turn's usage on the terminal envelope, so they share
    // validation and usage handling here and in `isResponsesStreamTerminal`.
    case "response.completed":
    case "response.done": {
      const validated = CompletedEvent(parsed);
      if (validated instanceof type.errors) {
        throw protocolMismatch(
          provider,
          `${eventType} failed schema validation: ${validated.summary}`,
          parsed,
        );
      }
      if (validated.response.usage !== undefined) {
        events.push({
          type: "inference.usage",
          seq,
          data: {
            usage: toInferenceUsage(validated.response.usage),
            source,
          },
        });
      }
      return events;
    }
    case "response.failed": {
      const validated = FailedEvent(parsed);
      if (validated instanceof type.errors) {
        throw protocolMismatch(
          provider,
          `response.failed failed schema validation: ${validated.summary}`,
          parsed,
        );
      }
      const message = validated.response?.error?.message ?? "response failed";
      throw new ProtocolMismatchError(`${provider}: ${message}`, parsed);
    }
    // A truncated turn is still a turn: the backend stopped (often on
    // max_output_tokens) after emitting usable deltas, and those deltas were
    // already delivered as events by earlier envelopes. Terminal handling
    // lives in isResponsesStreamTerminal; the usage is what the backend
    // bills for the partial turn, so it is reported like a completed one.
    case "response.incomplete": {
      const validated = IncompleteEvent(parsed);
      if (validated instanceof type.errors) {
        throw protocolMismatch(
          provider,
          `response.incomplete failed schema validation: ${validated.summary}`,
          parsed,
        );
      }
      if (validated.response.usage !== undefined) {
        events.push({
          type: "inference.usage",
          seq,
          data: {
            usage: toInferenceUsage(validated.response.usage),
            source,
          },
        });
      }
      return events;
    }
    case "error": {
      const validated = ErrorEvent(parsed);
      if (validated instanceof type.errors) {
        throw protocolMismatch(
          provider,
          `error event failed schema validation: ${validated.summary}`,
          parsed,
        );
      }
      throw new ProtocolMismatchError(
        `${provider}: ${validated.message ?? "stream error"}`,
        parsed,
      );
    }
    default:
      // Lifecycle envelopes (response.created, response.in_progress,
      // content_part.*, *_text.done) carry no incremental payload a caller
      // needs; ignore them. Unknown event types are protocol-legal — the
      // vocabulary is expected to grow.
      return events;
  }
}

// The Responses stream ends on a semantic lifecycle event, not `[DONE]` or
// a socket close: `response.completed` on success, `response.incomplete`
// when the backend truncates, `response.done` as an alias some backends
// emit. Failure envelopes (`response.failed`, `error`) already throw in
// `parseResponse`.
const RESPONSES_TERMINAL_EVENTS = new Set([
  "response.completed",
  "response.incomplete",
  "response.done",
]);

/** True when an SSE event payload is a Responses stream's terminal event. */
export function isResponsesStreamTerminal(sseData: string): boolean {
  let parsed: unknown;
  try {
    parsed = JSON.parse(sseData);
  } catch {
    // parseResponse re-parses the same payload and raises the protocol
    // error; reporting "not terminal" here defers to that single throw
    // site.
    return false;
  }
  const envelope = ResponsesEnvelope(parsed);
  if (envelope instanceof type.errors) return false;
  return RESPONSES_TERMINAL_EVENTS.has(envelope.type);
}

// ---------------------------------------------------------------------------
// Non-streaming JSON parse
//
// A complete non-streaming Responses object returns the whole turn in one
// JSON body. parseJSONResponse re-expresses it as the same InferenceEvent
// vocabulary parseResponse emits from the stream, walking `output[]` in
// order and synthesizing indices the same way the streaming path assigns
// them on first observation.
// ---------------------------------------------------------------------------

const OutputTextPart = type({ type: "'output_text'", text: "string" });
const RefusalPart = type({ type: "'refusal'", refusal: "string" });
// Content parts are validated individually by kind, same rationale as
// output items below: an "unknown kind, ignore it" fallback branch defeats
// narrowing on the known branches.
const MessageItem = type({
  type: "'message'",
  content: type({ type: "string" }).array(),
});
const FunctionCallItem = type({
  type: "'function_call'",
  id: "string",
  call_id: "string",
  name: "string",
  arguments: "string",
});
const ReasoningSummaryPart = type({ type: "'summary_text'", text: "string" });
const ReasoningItem = type({
  type: "'reasoning'",
  id: "string",
  "summary?": ReasoningSummaryPart.array(),
  "encrypted_content?": "string",
});
// Output items are validated individually by kind rather than as one
// discriminated array schema: a fallback "unknown item kind, ignore it"
// branch has a non-literal `type: string` discriminant, which defeats
// TypeScript's narrowing on every known branch. Validating per-item lets an
// unrecognized `type` skip validation entirely (protocol-legal, like an
// unrecognized SSE event) while a recognized `type` with the wrong shape
// still throws.
const CompleteResponse = type({
  output: type({ type: "string" }).array(),
  "usage?": ResponsesUsage,
  "status?":
    "'completed' | 'failed' | 'incomplete' | 'in_progress' | 'cancelled' | 'queued'",
  "error?": { "message?": "string" },
  "incomplete_details?": { "reason?": "string" },
});

export function parseJSONResponse(
  body: string,
  source: LastCycleSource,
): InferenceEvent[] {
  const provider = source.provider;
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch (cause) {
    throw new ProtocolMismatchError(
      `${provider} parseJSONResponse: malformed JSON response body: ${cause instanceof Error ? cause.message : String(cause)}`,
      body,
    );
  }
  const response = CompleteResponse(parsed);
  if (response instanceof type.errors) {
    throw new ProtocolMismatchError(
      `${provider} parseJSONResponse: response failed schema validation: ${response.summary}`,
      parsed,
    );
  }
  // A non-streaming response can report failure inside a 200 body instead of
  // an HTTP error code or an SSE `response.failed` event; decoding `output`
  // (typically empty) as a successful empty turn would hide the failure from
  // the caller entirely.
  if (response.status === "failed") {
    throw new ProtocolMismatchError(
      `${provider} parseJSONResponse: response status is "failed": ${response.error?.message ?? "no error message"}`,
      parsed,
    );
  }
  // A truncated body still carries the partial turn the backend managed
  // to produce; throwing on it would discard usable content, so status
  // "incomplete" decodes like a completed one.
  const seq = 0;
  const events: InferenceEvent[] = [];
  // A fresh indexer per body, synthesizing indices in output[] arrival
  // order — the same convention the SSE path uses on first observation of
  // each item id.
  let nextIndex = 0;
  for (const rawItem of response.output) {
    if (rawItem.type === "message") {
      const item = MessageItem(rawItem);
      if (item instanceof type.errors) {
        throw new ProtocolMismatchError(
          `${provider} parseJSONResponse: message output item failed schema validation: ${item.summary}`,
          rawItem,
        );
      }
      for (const rawPart of item.content) {
        if (rawPart.type === "output_text") {
          const part = OutputTextPart(rawPart);
          if (part instanceof type.errors) {
            throw new ProtocolMismatchError(
              `${provider} parseJSONResponse: output_text content part failed schema validation: ${part.summary}`,
              rawPart,
            );
          }
          if (part.text.length > 0) {
            events.push({
              type: "inference.text.delta",
              seq,
              data: {
                token: part.text,
                partial: EMPTY_PARTIAL,
                index: nextIndex,
              },
            });
            nextIndex += 1;
          }
        } else if (rawPart.type === "refusal") {
          const part = RefusalPart(rawPart);
          if (part instanceof type.errors) {
            throw new ProtocolMismatchError(
              `${provider} parseJSONResponse: refusal content part failed schema validation: ${part.summary}`,
              rawPart,
            );
          }
          if (part.refusal.length > 0) {
            events.push({
              type: "inference.refusal.delta",
              seq,
              data: {
                token: part.refusal,
                partial: EMPTY_PARTIAL,
                index: nextIndex,
              },
            });
            nextIndex += 1;
          }
        }
      }
    } else if (rawItem.type === "reasoning") {
      const item = ReasoningItem(rawItem);
      if (item instanceof type.errors) {
        throw new ProtocolMismatchError(
          `${provider} parseJSONResponse: reasoning output item failed schema validation: ${item.summary}`,
          rawItem,
        );
      }
      const summaryText = (item.summary ?? []).map((s) => s.text).join("");
      if (summaryText.length > 0 || item.encrypted_content !== undefined) {
        const index = nextIndex;
        nextIndex += 1;
        events.push({
          type: "inference.thinking.delta",
          seq,
          data: { token: summaryText, partial: EMPTY_PARTIAL, index },
        });
        if (item.encrypted_content !== undefined) {
          events.push({
            type: "inference.block.signature",
            seq,
            data: {
              signature: tagSignature(provider, item.encrypted_content),
              index,
            },
          });
        }
      }
    } else if (rawItem.type === "function_call") {
      const item = FunctionCallItem(rawItem);
      if (item instanceof type.errors) {
        throw new ProtocolMismatchError(
          `${provider} parseJSONResponse: function_call output item failed schema validation: ${item.summary}`,
          rawItem,
        );
      }
      const index = nextIndex;
      nextIndex += 1;
      events.push({
        type: "inference.tool_call.start",
        seq,
        data: {
          callId: item.call_id,
          name: decodeToolName(item.name),
          partial: EMPTY_PARTIAL,
          index,
        },
      });
      if (item.arguments.length > 0) {
        events.push({
          type: "inference.tool_call.delta",
          seq,
          data: {
            callId: String(index),
            argumentFragment: item.arguments,
            partial: EMPTY_PARTIAL,
            index,
          },
        });
      }
    }
    // Unknown output item kinds are protocol-legal extensions this adapter
    // has nothing to do with; ignored, mirroring the SSE default case.
  }
  events.push({
    type: "inference.usage",
    seq,
    data: { usage: toInferenceUsage(response.usage), source },
  });
  return events;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

function buildAdapter(
  source: LastCycleSource,
  quirks: ResolvedResponsesQuirks,
  hooks: ResponsesHooks | undefined,
): ProviderAdapter {
  // Re-created per request in buildRequest, not just once here — otherwise
  // block indices accumulate across every request this adapter instance
  // ever serves, growing the map for the life of the conversation.
  let indexer = createResponsesBlockIndexer();
  const buildRequest = (
    messages: ConversationTurn[],
    model: string,
    options: InferenceOptions,
  ): BuiltRequest => {
    indexer = createResponsesBlockIndexer();
    return buildResponsesRequest(
      quirks,
      messages,
      model,
      options,
      source.provider,
      hooks,
    );
  };
  const parseResponseFromStream = (sseData: string): InferenceEvent[] =>
    parseResponse(sseData, indexer, source);
  const parseCompleteResponse = (body: string): InferenceEvent[] =>
    parseJSONResponse(body, source);
  return {
    buildRequest,
    parseResponse: parseResponseFromStream,
    parseJSONResponse: parseCompleteResponse,
    extractRetryAfterMs,
    extractPacingDelayMs,
    isStreamTerminal: isResponsesStreamTerminal,
  };
}

/**
 * `AdapterFactory` for the OpenAI Responses API wire protocol. `quirks` is
 * this adapter's own `ResponsesQuirks` (an `InferenceSource.quirks` bag)
 * describing exactly how this vendor's backend deviates from the shared
 * protocol — headers, system-prompt placement, session-id routing, and the
 * handful of request-body switches that differ between backends. An absent
 * `quirks` bag resolves the same as `{}`: protocol-native defaults, not an
 * error.
 */
export const createOpenAIResponsesAdapter: AdapterFactory = (source, quirks) =>
  buildAdapter(source, parseResponsesQuirks(quirks), undefined);

/**
 * Curries a fixed `ResponsesQuirks` bag into a plain `AdapterFactory`, so a
 * provider package can bake its vendor's config in once and hand Interchange
 * a factory that takes only `(source)` — no quirks bag needed at the call
 * site.
 *
 * `hooks` carries the handful of accommodations that must be code rather
 * than data (wrapping a system prompt in vendor-specific markup, deciding
 * whether a reasoning effort value is safe to forward for a given model).
 * They are TypeScript-only and deliberately never part of `quirks`: quirks
 * ride inside an `InferenceSource`, which is JSON — a function-valued field
 * would not survive being persisted or sent over the wire, and a caller with
 * a genuine code-shaped need reaches for `hooks` instead of smuggling a
 * closure into a bag that has to stay serializable.
 */
export function responsesAdapterFactory(
  quirks: ResponsesQuirks,
  hooks?: ResponsesHooks,
): AdapterFactory {
  const resolved = parseResponsesQuirks(quirks);
  return (source) => buildAdapter(source, resolved, hooks);
}

export const OPENAI_RESPONSES_PROVIDER = "openai-responses";
export const OPENAI_COMPATIBLE_RESPONSES_PROVIDER =
  "openai-compatible-responses";

/**
 * Provider id -> factory record for a host's adapter registry. Maps both
 * provider ids to `createOpenAIResponsesAdapter`.
 */
export const responsesAdapterFactories: Record<string, AdapterFactory> = {
  [OPENAI_RESPONSES_PROVIDER]: createOpenAIResponsesAdapter,
  [OPENAI_COMPATIBLE_RESPONSES_PROVIDER]: createOpenAIResponsesAdapter,
};
