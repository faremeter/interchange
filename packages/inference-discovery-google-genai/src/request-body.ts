import { readFileSync } from "node:fs";
import {
  resolveMediaPath,
  type Capability,
  type CapabilityIntent,
  type MediaRef,
  type ToolDecl,
} from "@intx/inference-discovery/catalog";

// gemini-2.5-pro shares gemini-2.5-flash's text capability surface, so both are
// recognized as text models; only their thinking-budget handling differs (pro
// cannot disable thinking — see minimalThinkingBudget).
const TEXT_MODELS: ReadonlySet<string> = new Set([
  "gemini-2.5-flash",
  "gemini-2.5-pro",
]);
const IMAGE_MODEL = "gemini-2.5-flash-image";

const TEXT_MODEL_CAPABILITIES: ReadonlySet<Capability> = new Set<Capability>([
  "plain-text",
  "plain-text-streaming",
  "function-calling-multi-turn",
  "function-calling-multi-turn-streaming",
  "function-calling-with-thinking",
  "function-calling-with-thinking-streaming",
  "vision-input",
  "vision-input-streaming",
  "audio-input",
  "audio-input-streaming",
  "video-input",
  "video-input-streaming",
  "document-input",
  "document-input-streaming",
  "code-execution",
  "code-execution-streaming",
  "grounding",
  "grounding-streaming",
  "files-api-reference",
  "files-api-reference-streaming",
  "safety-classification",
  "safety-classification-streaming",
  "structured-output",
  "structured-output-streaming",
]);

const IMAGE_MODEL_CAPABILITIES: ReadonlySet<Capability> = new Set<Capability>([
  "image-output",
  "image-output-streaming",
]);

const EXTENSION_TO_MIME_TYPE: Readonly<Record<string, string>> = {
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  png: "image/png",
  gif: "image/gif",
  webp: "image/webp",
  wav: "audio/wav",
  mp3: "audio/mpeg",
  ogg: "audio/ogg",
  flac: "audio/flac",
  mp4: "video/mp4",
  mov: "video/quicktime",
  webm: "video/webm",
  pdf: "application/pdf",
};

interface GeminiTextPart {
  text: string;
}

interface GeminiInlineDataPart {
  inlineData: {
    mimeType: string;
    data: string;
  };
}

interface GeminiFileDataPart {
  fileData: {
    mimeType: string;
    fileUri: string;
  };
}

type GeminiPart = GeminiTextPart | GeminiInlineDataPart | GeminiFileDataPart;

interface GeminiContent {
  role: "user" | "model";
  parts: GeminiPart[];
}

interface GeminiFunctionDeclaration {
  name: string;
  description: string;
  parameters: ToolDecl["parameters"];
}

interface GeminiFunctionTool {
  functionDeclarations: GeminiFunctionDeclaration[];
}

interface GeminiCodeExecutionTool {
  codeExecution: Record<string, never>;
}

interface GeminiGoogleSearchTool {
  googleSearch: Record<string, never>;
}

type GeminiTool =
  | GeminiFunctionTool
  | GeminiCodeExecutionTool
  | GeminiGoogleSearchTool;

interface GeminiThinkingConfig {
  thinkingBudget: number;
  includeThoughts?: true;
}

interface GeminiGenerationConfig {
  maxOutputTokens?: number;
  thinkingConfig?: GeminiThinkingConfig;
  responseModalities?: readonly ["TEXT", "IMAGE"];
  responseMimeType?: string;
  responseSchema?: unknown;
}

interface GeminiToolConfig {
  functionCallingConfig: {
    mode: "ANY";
    allowedFunctionNames: string[];
  };
}

interface GeminiRequestBody {
  contents: GeminiContent[];
  tools?: GeminiTool[];
  toolConfig?: GeminiToolConfig;
  generationConfig?: GeminiGenerationConfig;
}

function modelSupportsCapability(model: string, capability: Capability): void {
  if (TEXT_MODELS.has(model)) {
    if (!TEXT_MODEL_CAPABILITIES.has(capability)) {
      throw new Error(
        `google-genai: model ${model} does not support capability ${capability}`,
      );
    }
    return;
  }
  if (model === IMAGE_MODEL) {
    if (!IMAGE_MODEL_CAPABILITIES.has(capability)) {
      throw new Error(
        `google-genai: model ${model} does not support capability ${capability}`,
      );
    }
    return;
  }
  throw new Error(`google-genai: unknown model ${model}`);
}

function extensionFor(path: string): string {
  const dot = path.lastIndexOf(".");
  if (dot < 0 || dot === path.length - 1) {
    throw new Error(
      `google-genai: cannot infer media MIME type, no extension in path: ${path}`,
    );
  }
  return path.slice(dot + 1).toLowerCase();
}

function mimeTypeFor(ref: MediaRef): string {
  const ext = extensionFor(ref.path);
  const mime = EXTENSION_TO_MIME_TYPE[ext];
  if (mime === undefined) {
    throw new Error(
      `google-genai: no MIME type mapping for extension .${ext} (path ${ref.path})`,
    );
  }
  return mime;
}

function readMediaBase64(ref: MediaRef): string {
  const absolute = resolveMediaPath(ref);
  const bytes = readFileSync(absolute);
  return bytes.toString("base64");
}

function expectSingleMedia(intent: CapabilityIntent): MediaRef {
  if (intent.media === undefined || intent.media.length === 0) {
    throw new Error(
      "google-genai: media-input capability requires intent.media to be non-empty",
    );
  }
  if (intent.media.length !== 1) {
    throw new Error(
      `google-genai: media-input capability expects exactly one media reference, got ${String(intent.media.length)}`,
    );
  }
  const [media] = intent.media;
  if (media === undefined) {
    throw new Error(
      "google-genai: media-input capability: media[0] is unexpectedly undefined",
    );
  }
  return media;
}

function expectSingleTool(intent: CapabilityIntent): ToolDecl {
  if (intent.tools === undefined || intent.tools.length === 0) {
    throw new Error(
      "google-genai: function-calling capability requires intent.tools to be non-empty",
    );
  }
  if (intent.tools.length !== 1) {
    throw new Error(
      `google-genai: function-calling capability expects exactly one tool declaration, got ${String(intent.tools.length)}`,
    );
  }
  const [tool] = intent.tools;
  if (tool === undefined) {
    throw new Error(
      "google-genai: function-calling capability: tools[0] is unexpectedly undefined",
    );
  }
  return tool;
}

function userTextContent(prompt: string): GeminiContent {
  return {
    role: "user",
    parts: [{ text: prompt }],
  };
}

// Gemini's "dynamic" thinking budget sentinel: the model decides how much to
// think and no cap is imposed.
const DYNAMIC_THINKING_BUDGET = -1;

// gemini-2.5-pro rejects thinkingConfig.thinkingBudget: 0 with HTTP 400
// "Budget 0 is invalid. This model only works in thinking mode." The budget can
// only be chosen by model identity, because the API surfaces the constraint
// solely as a runtime 400 with no build-time signal. Add a model here when its
// API rejects a zero thinking budget.
const THINKING_MANDATORY_MODELS: ReadonlySet<string> = new Set([
  "gemini-2.5-pro",
]);

// The thinking budget to request when a probe wants thinking suppressed: 0
// (fully off) for models that allow it, or the dynamic budget for models that
// cannot be set to 0. includeThoughts stays false in both cases, so no thought
// parts are returned either way.
function minimalThinkingBudget(model: string): number {
  return THINKING_MANDATORY_MODELS.has(model) ? DYNAMIC_THINKING_BUDGET : 0;
}

function plainTextBody(intent: CapabilityIntent): GeminiRequestBody {
  return {
    contents: [userTextContent(intent.prompt)],
  };
}

function plainTextStreamingBody(
  intent: CapabilityIntent,
  model: string,
): GeminiRequestBody {
  return {
    contents: [userTextContent(intent.prompt)],
    generationConfig: {
      maxOutputTokens: 400,
      thinkingConfig: {
        thinkingBudget: minimalThinkingBudget(model),
      },
    },
  };
}

function functionToolFromDecl(decl: ToolDecl): GeminiFunctionTool {
  return {
    functionDeclarations: [
      {
        name: decl.name,
        description: decl.description,
        parameters: decl.parameters,
      },
    ],
  };
}

function functionCallingBody(
  intent: CapabilityIntent,
  thinking: { budget: number; includeThoughts: boolean },
): GeminiRequestBody {
  const decl = expectSingleTool(intent);
  const thinkingConfig: GeminiThinkingConfig = thinking.includeThoughts
    ? { thinkingBudget: thinking.budget, includeThoughts: true }
    : { thinkingBudget: thinking.budget };
  return {
    contents: [userTextContent(intent.prompt)],
    tools: [functionToolFromDecl(decl)],
    toolConfig: {
      functionCallingConfig: {
        mode: "ANY",
        allowedFunctionNames: [decl.name],
      },
    },
    generationConfig: {
      thinkingConfig,
    },
  };
}

function inlineMediaBody(intent: CapabilityIntent): GeminiRequestBody {
  const media = expectSingleMedia(intent);
  return {
    contents: [
      {
        role: "user",
        parts: [
          { text: intent.prompt },
          {
            inlineData: {
              mimeType: mimeTypeFor(media),
              data: readMediaBase64(media),
            },
          },
        ],
      },
    ],
  };
}

function imageOutputBody(intent: CapabilityIntent): GeminiRequestBody {
  return {
    contents: [userTextContent(intent.prompt)],
    generationConfig: {
      responseModalities: ["TEXT", "IMAGE"],
    },
  };
}

function codeExecutionBody(intent: CapabilityIntent): GeminiRequestBody {
  return {
    contents: [userTextContent(intent.prompt)],
    tools: [{ codeExecution: {} }],
  };
}

function groundingBody(intent: CapabilityIntent): GeminiRequestBody {
  return {
    contents: [userTextContent(intent.prompt)],
    tools: [{ googleSearch: {} }],
  };
}

// Shape-identical between streaming and non-streaming variants — only the
// endpoint differs (handled by buildEndpointURL). The streaming variant
// deliberately does NOT clamp `thinkingBudget: 0` the way plain-text
// streaming does: the safety classifier's engagement may depend on
// whether the model goes through a thinking phase, and the probe's job
// is to observe natural classifier behavior at default generation
// settings, not constrained ones.
function safetyClassificationBody(intent: CapabilityIntent): GeminiRequestBody {
  return {
    contents: [userTextContent(intent.prompt)],
  };
}

function structuredOutputBody(intent: CapabilityIntent): GeminiRequestBody {
  const format = intent.responseFormat;
  if (format === undefined) {
    throw new Error(
      "google-genai: structured-output intent has no responseFormat",
    );
  }
  const generationConfig: GeminiGenerationConfig = {};
  switch (format.kind) {
    case "text":
      // Free-form text is Gemini's default; emit no responseMimeType.
      break;
    case "json":
      generationConfig.responseMimeType = "application/json";
      break;
    case "json-schema":
      generationConfig.responseMimeType = "application/json";
      generationConfig.responseSchema = format.schema;
      break;
  }
  const body: GeminiRequestBody = {
    contents: [userTextContent(intent.prompt)],
  };
  if (Object.keys(generationConfig).length > 0) {
    body.generationConfig = generationConfig;
  }
  return body;
}

export function buildRequestBody(opts: {
  model: string;
  capability: Capability;
  intent: CapabilityIntent;
}): GeminiRequestBody {
  modelSupportsCapability(opts.model, opts.capability);

  switch (opts.capability) {
    case "plain-text":
      return plainTextBody(opts.intent);
    case "plain-text-streaming":
      return plainTextStreamingBody(opts.intent, opts.model);
    case "function-calling-multi-turn":
    case "function-calling-multi-turn-streaming":
      return functionCallingBody(opts.intent, {
        budget: minimalThinkingBudget(opts.model),
        includeThoughts: false,
      });
    case "function-calling-with-thinking":
    case "function-calling-with-thinking-streaming":
      return functionCallingBody(opts.intent, {
        budget: 1024,
        includeThoughts: true,
      });
    case "vision-input":
    case "vision-input-streaming":
    case "audio-input":
    case "audio-input-streaming":
    case "video-input":
    case "video-input-streaming":
    case "document-input":
    case "document-input-streaming":
      return inlineMediaBody(opts.intent);
    case "image-output":
    case "image-output-streaming":
      return imageOutputBody(opts.intent);
    case "code-execution":
    case "code-execution-streaming":
      return codeExecutionBody(opts.intent);
    case "grounding":
    case "grounding-streaming":
      return groundingBody(opts.intent);
    case "safety-classification":
    case "safety-classification-streaming":
      return safetyClassificationBody(opts.intent);
    case "structured-output":
    case "structured-output-streaming":
      return structuredOutputBody(opts.intent);
    case "files-api-reference":
    case "files-api-reference-streaming":
      throw new Error(
        `google-genai: capability ${opts.capability} is multi-step; use iterateCaptureSteps from the plug-in, not buildRequestBody.`,
      );
    case "function-calling":
    case "reasoning-content":
    case "reasoning-content-streaming":
    case "redacted-thinking":
    case "redacted-thinking-streaming":
      throw new Error(
        `google-genai: capability ${opts.capability} is not supported by any google-genai model`,
      );
    default: {
      const exhaustive: never = opts.capability;
      throw new Error(
        `google-genai: unhandled capability ${String(exhaustive)}`,
      );
    }
  }
}
