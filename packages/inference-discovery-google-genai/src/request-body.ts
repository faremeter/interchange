import { readFileSync } from "node:fs";
import {
  resolveMediaPath,
  type Capability,
  type CapabilityIntent,
  type MediaRef,
  type ToolDecl,
} from "@intx/inference-discovery/catalog";

const TEXT_MODEL = "gemini-2.5-flash";
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
  if (model === TEXT_MODEL) {
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

function plainTextBody(intent: CapabilityIntent): GeminiRequestBody {
  return {
    contents: [userTextContent(intent.prompt)],
  };
}

function plainTextStreamingBody(intent: CapabilityIntent): GeminiRequestBody {
  return {
    contents: [userTextContent(intent.prompt)],
    generationConfig: {
      maxOutputTokens: 400,
      thinkingConfig: {
        thinkingBudget: 0,
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
      return plainTextStreamingBody(opts.intent);
    case "function-calling-multi-turn":
    case "function-calling-multi-turn-streaming":
      return functionCallingBody(opts.intent, {
        budget: 0,
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
    case "files-api-reference":
    case "files-api-reference-streaming":
      throw new Error(
        `google-genai: capability ${opts.capability} is multi-step; use iterateCaptureSteps from the plug-in, not buildRequestBody.`,
      );
    case "function-calling":
    case "reasoning-content":
    case "reasoning-content-streaming":
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
