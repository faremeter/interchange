import { readFileSync } from "node:fs";
import {
  resolveMediaPath,
  type Capability,
  type CapabilityIntent,
} from "@intx/inference-discovery/catalog";
import {
  resolveTurn1Response,
  type CaptureStep,
  type CapturedResponse,
  type IterateCaptureStepsOpts,
  type ProviderPlugin,
  type Turn1Reconstructor,
} from "@intx/inference-discovery";
import { buildAuthHeaders } from "./auth";
import {
  buildFilesURL,
  buildMessagesURL,
  isStreamingCapability,
} from "./endpoint";
import { mediaTypeFor } from "./media";
import {
  buildFilesApiGenerateBody,
  buildFunctionCallingTurn2Body,
  buildRedactedThinkingTurn2Body,
  buildRequestBody,
} from "./request-body";
import { extractReasoningTrace } from "./reasoning";
import { extractContentBlocksFromSSE } from "./sse";

const PROVIDER_NAME = "anthropic";

const MODELS = [
  "claude-sonnet-5",
  "claude-sonnet-4-5-20250929",
  "claude-opus-4-1-20250805",
  "claude-haiku-4-5-20251001",
] as const;

const REDACT_REQUEST_HEADERS = ["x-api-key"] as const;
const REDACT_RESPONSE_HEADERS: readonly string[] = [];

// Beta-flag header markers Anthropic requires for opt-in features.
// These live on per-step headers (not in buildAuthHeaders) because they
// apply per-request, not per-plug-in; auth headers are plug-in-wide.
const FILES_API_BETA = "files-api-2025-04-14";
const CODE_EXECUTION_BETA = "code-execution-2025-05-22";

const FUNCTION_CALLING_MULTI_TURN_CAPABILITIES: ReadonlySet<Capability> =
  new Set<Capability>([
    "function-calling-multi-turn",
    "function-calling-multi-turn-streaming",
    "function-calling-with-thinking",
    "function-calling-with-thinking-streaming",
  ]);

const REDACTED_THINKING_CAPABILITIES: ReadonlySet<Capability> =
  new Set<Capability>(["redacted-thinking", "redacted-thinking-streaming"]);

const FILES_API_CAPABILITIES: ReadonlySet<Capability> = new Set<Capability>([
  "files-api-reference",
  "files-api-reference-streaming",
]);

const CODE_EXECUTION_CAPABILITIES: ReadonlySet<Capability> =
  new Set<Capability>(["code-execution", "code-execution-streaming"]);

export interface AnthropicPluginOptions {
  apiKey: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function basename(p: string): string {
  const slash = p.lastIndexOf("/");
  return slash < 0 ? p : p.slice(slash + 1);
}

function perCapabilityHeaders(
  capability: Capability,
): Record<string, string> | undefined {
  if (FILES_API_CAPABILITIES.has(capability)) {
    return { "anthropic-beta": FILES_API_BETA };
  }
  if (CODE_EXECUTION_CAPABILITIES.has(capability)) {
    return { "anthropic-beta": CODE_EXECUTION_BETA };
  }
  return undefined;
}

function withPerCapabilityHeaders(
  capability: Capability,
  step: CaptureStep,
): CaptureStep {
  const extra = perCapabilityHeaders(capability);
  if (extra === undefined) return step;
  return { ...step, headers: { ...(step.headers ?? {}), ...extra } };
}

interface MultipartUpload {
  contentType: string;
  body: Uint8Array;
}

function buildMultipartUpload(opts: {
  fieldName: string;
  filename: string;
  contentType: string;
  bytes: Uint8Array;
}): MultipartUpload {
  // Boundary is regenerated on every call, which means a captured
  // request-headers.json carries a run-specific Content-Type header.
  // Byte-diffing regenerated fixtures against committed ones will
  // always differ on the boundary; structural comparison is the right
  // tool for files-api fixture equivalence.
  const boundary = `----intx-anthropic-${crypto.randomUUID().replace(/-/g, "")}`;
  const encoder = new TextEncoder();
  const preamble = encoder.encode(
    `--${boundary}\r\n` +
      `Content-Disposition: form-data; name="${opts.fieldName}"; filename="${opts.filename}"\r\n` +
      `Content-Type: ${opts.contentType}\r\n\r\n`,
  );
  const epilogue = encoder.encode(`\r\n--${boundary}--\r\n`);
  const body = new Uint8Array(
    preamble.length + opts.bytes.length + epilogue.length,
  );
  body.set(preamble, 0);
  body.set(opts.bytes, preamble.length);
  body.set(epilogue, preamble.length + opts.bytes.length);
  return {
    contentType: `multipart/form-data; boundary=${boundary}`,
    body,
  };
}

function buildFilesUploadStep(intent: CapabilityIntent): {
  step: CaptureStep;
  filename: string;
  mediaType: string;
} {
  const media = intent.media?.[0];
  if (media === undefined) {
    throw new Error(
      "anthropic files-api: intent.media[0] is required; the catalog's " +
        "files-api-reference intent must declare the document to upload.",
    );
  }
  const buf = readFileSync(resolveMediaPath(media));
  const bytes = new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
  const filename = basename(media.path);
  const mediaType = mediaTypeFor(media);
  const multipart = buildMultipartUpload({
    fieldName: "file",
    filename,
    contentType: mediaType,
    bytes,
  });
  return {
    step: {
      kind: "raw",
      subdir: "upload",
      url: buildFilesURL(),
      method: "POST",
      contentType: multipart.contentType,
      headers: { "anthropic-beta": FILES_API_BETA },
      body: multipart.body,
    },
    filename,
    mediaType,
  };
}

function extractFileId(parsed: unknown): string {
  if (!isRecord(parsed)) {
    throw new Error(
      "anthropic files-api: upload response is not a JSON object",
    );
  }
  const id = parsed.id;
  if (typeof id !== "string" || id.length === 0) {
    throw new Error(
      "anthropic files-api: upload response has no string 'id' field",
    );
  }
  return id;
}

function makeJsonStep(opts: {
  capability: Capability;
  subdir: string | null;
  body: unknown;
}): CaptureStep {
  return withPerCapabilityHeaders(opts.capability, {
    kind: "json",
    subdir: opts.subdir,
    url: buildMessagesURL(),
    body: opts.body,
  });
}

// Reconstructs the assistant response from turn-1's SSE bytes into the shape
// buildFunctionCallingTurn2Body and buildRedactedThinkingTurn2Body expect:
// the content blocks wrapped as { content: blocks }. The parsed/bytes/throw
// dispatch lives in the shared resolveTurn1Response; this callback owns only
// Anthropic's wire shape.
const reconstructTurn1Blocks: Turn1Reconstructor = (bytes) => ({
  content: extractContentBlocksFromSSE(bytes),
});

export function* iterateCaptureSteps(
  opts: IterateCaptureStepsOpts,
): Generator<CaptureStep, void, CapturedResponse> {
  const { model, capability, intent } = opts;

  if (FILES_API_CAPABILITIES.has(capability)) {
    const upload = buildFilesUploadStep(intent);
    const uploadResponse = yield upload.step;
    const fileId = extractFileId(uploadResponse.parsed);
    const generateBody = buildFilesApiGenerateBody({
      model,
      fileId,
      intent,
      stream: isStreamingCapability(capability),
    });
    yield makeJsonStep({
      capability,
      subdir: "generate",
      body: generateBody,
    });
    return;
  }

  if (FUNCTION_CALLING_MULTI_TURN_CAPABILITIES.has(capability)) {
    const turn1Body = buildRequestBody({ model, capability, intent });
    const turn1Response = yield makeJsonStep({
      capability,
      subdir: "turn-1",
      body: turn1Body,
    });
    const turn2Body = buildFunctionCallingTurn2Body({
      model,
      capability,
      intent,
      turn1Body,
      turn1Response: resolveTurn1Response(
        turn1Response,
        reconstructTurn1Blocks,
      ),
    });
    yield makeJsonStep({
      capability,
      subdir: "turn-2",
      body: turn2Body,
    });
    return;
  }

  if (REDACTED_THINKING_CAPABILITIES.has(capability)) {
    // Turn-1 carries the canary prompt and thinking enabled; Anthropic
    // returns either thinking or redacted_thinking blocks depending on
    // whether the safety classifier fires. Turn-2 echoes the assistant
    // content blocks verbatim and prompts a brief follow-up so the
    // round-trip is exercised on the wire.
    const turn1Body = buildRequestBody({ model, capability, intent });
    const turn1Response = yield makeJsonStep({
      capability,
      subdir: "turn-1",
      body: turn1Body,
    });
    const turn2Body = buildRedactedThinkingTurn2Body({
      model,
      intent,
      turn1Body,
      turn1Response: resolveTurn1Response(
        turn1Response,
        reconstructTurn1Blocks,
      ),
    });
    yield makeJsonStep({
      capability,
      subdir: "turn-2",
      body: turn2Body,
    });
    return;
  }

  yield makeJsonStep({
    capability,
    subdir: null,
    body: buildRequestBody({ model, capability, intent }),
  });
}

export function createAnthropicPlugin(
  opts: AnthropicPluginOptions,
): ProviderPlugin {
  const apiKey = opts.apiKey;
  return {
    name: PROVIDER_NAME,
    models: MODELS,
    redactRequestHeaders: REDACT_REQUEST_HEADERS,
    redactResponseHeaders: REDACT_RESPONSE_HEADERS,
    buildAuthHeaders: () => buildAuthHeaders(apiKey),
    extractReasoningTrace,
    iterateCaptureSteps,
  };
}
