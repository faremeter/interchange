import { readFileSync } from "node:fs";
import {
  resolveMediaPath,
  type Capability,
  type CapabilityIntent,
  type MediaRef,
} from "@intx/inference-discovery/catalog";
import {
  resolveTurn1Response,
  type CaptureStep,
  type CapturedResponse,
  type IterateCaptureStepsOpts,
  type ProviderPlugin,
} from "@intx/inference-discovery";
import { buildAuthHeaders } from "./auth";
import { buildEndpointURL } from "./endpoint";
import { buildRequestBody } from "./request-body";
import { reconstructResponseFromSSE } from "./sse";

const PROVIDER_NAME = "google-genai";
const MODELS = [
  "gemini-2.5-flash",
  "gemini-2.5-flash-image",
  "gemini-2.5-pro",
] as const;
const REDACT_REQUEST_HEADERS = ["x-goog-api-key"] as const;
const REDACT_RESPONSE_HEADERS: readonly string[] = [];

const FILES_API_UPLOAD_URL =
  "https://generativelanguage.googleapis.com/upload/v1beta/files";

const MULTI_TURN_CAPABILITIES: ReadonlySet<Capability> = new Set<Capability>([
  "function-calling-multi-turn",
  "function-calling-multi-turn-streaming",
  "function-calling-with-thinking",
  "function-calling-with-thinking-streaming",
]);

const FILES_API_CAPABILITIES: ReadonlySet<Capability> = new Set<Capability>([
  "files-api-reference",
  "files-api-reference-streaming",
]);

export interface GoogleGenaiPluginOptions {
  apiKey: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

interface UploadStepDescriptor {
  url: string;
  mimeType: string;
  displayName: string;
  bytes: Uint8Array;
}

function buildUploadDescriptor(intent: CapabilityIntent): UploadStepDescriptor {
  const media = intent.media?.[0];
  if (media === undefined) {
    throw new Error(
      "google-genai files-API: intent.media[0] is required; the catalog's " +
        "files-api-reference intent must declare the document to upload.",
    );
  }
  const bytes = readFileSync(resolveMediaPath(media));
  return {
    url: FILES_API_UPLOAD_URL,
    mimeType: mimeTypeForMedia(media),
    displayName: basename(media.path),
    bytes: new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength),
  };
}

function basename(path: string): string {
  const slash = path.lastIndexOf("/");
  return slash < 0 ? path : path.slice(slash + 1);
}

function mimeTypeForMedia(ref: MediaRef): string {
  if (ref.kind === "document") return "application/pdf";
  if (ref.kind === "image") return "image/jpeg";
  if (ref.kind === "audio") return "audio/wav";
  if (ref.kind === "video") return "video/mp4";
  throw new Error(`google-genai: unsupported media kind ${String(ref.kind)}`);
}

function extractFileUri(parsed: unknown): string {
  if (!isRecord(parsed)) {
    throw new Error(
      "google-genai files-API: upload response is not a JSON object",
    );
  }
  const file = parsed.file;
  if (!isRecord(file)) {
    throw new Error(
      "google-genai files-API: upload response missing 'file' object",
    );
  }
  const uri = file.uri;
  if (typeof uri !== "string" || uri.length === 0) {
    throw new Error(
      "google-genai files-API: upload response has no string 'file.uri'",
    );
  }
  return uri;
}

function extractMimeTypeFromUpload(parsed: unknown): string {
  if (!isRecord(parsed)) {
    throw new Error(
      "google-genai files-API: upload response is not a JSON object",
    );
  }
  const file = parsed.file;
  if (!isRecord(file)) {
    throw new Error(
      "google-genai files-API: upload response missing 'file' object",
    );
  }
  const mime = file.mimeType;
  if (typeof mime !== "string" || mime.length === 0) {
    throw new Error(
      "google-genai files-API: upload response has no string 'file.mimeType'",
    );
  }
  return mime;
}

function buildFilesApiGenerateBody(opts: {
  intent: CapabilityIntent;
  fileUri: string;
  mimeType: string;
}): unknown {
  return {
    contents: [
      {
        role: "user",
        parts: [
          { text: opts.intent.prompt },
          {
            fileData: {
              mimeType: opts.mimeType,
              fileUri: opts.fileUri,
            },
          },
        ],
      },
    ],
  };
}

function extractAssistantContent(parsed: unknown): unknown {
  if (!isRecord(parsed)) {
    throw new Error(
      "google-genai multi-turn: turn-1 response is not a JSON object",
    );
  }
  const candidates = parsed.candidates;
  if (!Array.isArray(candidates) || candidates.length === 0) {
    throw new Error(
      "google-genai multi-turn: turn-1 response has no candidates array",
    );
  }
  const first = candidates[0];
  if (!isRecord(first)) {
    throw new Error(
      "google-genai multi-turn: turn-1 response candidates[0] is not an object",
    );
  }
  const content = first.content;
  if (!isRecord(content)) {
    throw new Error(
      "google-genai multi-turn: turn-1 response candidates[0].content is not an object",
    );
  }
  return content;
}

function deriveToolFollowUp(intent: CapabilityIntent): {
  toolName: string;
  content: string;
} {
  const followUp = intent.followUp;
  if (followUp !== undefined) {
    for (const step of followUp) {
      if (step.role === "tool") {
        return { toolName: step.toolName, content: step.content };
      }
    }
  }
  const tools = intent.tools;
  if (tools === undefined || tools.length === 0) {
    throw new Error(
      "google-genai multi-turn: intent has neither followUp nor tools",
    );
  }
  const [tool] = tools;
  if (tool === undefined) {
    throw new Error("google-genai multi-turn: intent.tools[0] is undefined");
  }
  return { toolName: tool.name, content: "{}" };
}

function buildMultiTurnTurn2Body(opts: {
  capability: Capability;
  intent: CapabilityIntent;
  turn1Body: unknown;
  turn1Response: unknown;
}): unknown {
  if (!isRecord(opts.turn1Body)) {
    throw new Error("google-genai multi-turn: turn-1 body is not an object");
  }
  const turn1Contents = opts.turn1Body.contents;
  if (!Array.isArray(turn1Contents)) {
    throw new Error(
      "google-genai multi-turn: turn-1 body.contents is not an array",
    );
  }
  const assistantContent = extractAssistantContent(opts.turn1Response);
  const tool = deriveToolFollowUp(opts.intent);
  const toolResponseObject: unknown = JSON.parse(tool.content);

  const turn1Tools = opts.turn1Body.tools;
  const turn1GenerationConfig = opts.turn1Body.generationConfig;

  const body: Record<string, unknown> = {
    contents: [
      ...turn1Contents,
      assistantContent,
      {
        role: "user",
        parts: [
          {
            functionResponse: {
              name: tool.toolName,
              response: toolResponseObject,
            },
          },
        ],
      },
    ],
  };
  if (turn1Tools !== undefined) {
    body.tools = turn1Tools;
  }
  if (turn1GenerationConfig !== undefined) {
    body.generationConfig = turn1GenerationConfig;
  }
  return body;
}

export function* iterateCaptureSteps(
  opts: IterateCaptureStepsOpts,
): Generator<CaptureStep, void, CapturedResponse> {
  const { model, capability, intent } = opts;

  if (FILES_API_CAPABILITIES.has(capability)) {
    const upload = buildUploadDescriptor(intent);
    const uploadResponse = yield {
      kind: "raw",
      subdir: "upload",
      url: upload.url,
      method: "POST",
      contentType: upload.mimeType,
      headers: {
        "X-Goog-Upload-Protocol": "raw",
        "X-Goog-Upload-File-Name": upload.displayName,
      },
      body: upload.bytes,
    };
    const fileUri = extractFileUri(uploadResponse.parsed);
    const mimeType = extractMimeTypeFromUpload(uploadResponse.parsed);
    const generateBody = buildFilesApiGenerateBody({
      intent,
      fileUri,
      mimeType,
    });
    yield {
      kind: "json",
      subdir: "generate",
      url: buildEndpointURL({ model, capability }),
      body: generateBody,
    };
    return;
  }

  if (MULTI_TURN_CAPABILITIES.has(capability)) {
    const turn1Body = buildRequestBody({ model, capability, intent });
    const url = buildEndpointURL({ model, capability });
    const turn1Response = yield {
      kind: "json",
      subdir: "turn-1",
      url,
      body: turn1Body,
    };
    const turn2Body = buildMultiTurnTurn2Body({
      capability,
      intent,
      turn1Body,
      turn1Response: resolveTurn1Response(
        turn1Response,
        reconstructResponseFromSSE,
      ),
    });
    yield {
      kind: "json",
      subdir: "turn-2",
      url,
      body: turn2Body,
    };
    return;
  }

  yield {
    kind: "json",
    subdir: null,
    url: buildEndpointURL({ model, capability }),
    body: buildRequestBody({ model, capability, intent }),
  };
}

export function createGoogleGenaiPlugin(
  opts: GoogleGenaiPluginOptions,
): ProviderPlugin {
  const apiKey = opts.apiKey;
  return {
    name: PROVIDER_NAME,
    models: MODELS,
    redactRequestHeaders: REDACT_REQUEST_HEADERS,
    redactResponseHeaders: REDACT_RESPONSE_HEADERS,
    buildAuthHeaders: () => buildAuthHeaders(apiKey),
    iterateCaptureSteps,
  };
}
