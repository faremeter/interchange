import { fileURLToPath } from "node:url";
import { type } from "arktype";
import type { Capability } from "./capability";

const MediaKind = type.enumerated("image", "audio", "video", "document");

export const MediaRef = type({
  kind: MediaKind,
  path: "string",
});
export type MediaRef = typeof MediaRef.infer;

const ToolParameterProperty = type({
  type: "string",
  "description?": "string",
  "enum?": "string[]",
});

export const ToolDecl = type({
  name: "string",
  description: "string",
  parameters: {
    type: "'object'",
    properties: type.Record("string", ToolParameterProperty),
    "required?": "string[]",
  },
});
export type ToolDecl = typeof ToolDecl.infer;

const FollowUp = type({
  role: "'user' | 'assistant'",
  content: "string",
}).or({
  role: "'tool'",
  toolName: "string",
  content: "string",
});

export const CapabilityIntent = type({
  prompt: "string",
  "media?": MediaRef.array(),
  "tools?": ToolDecl.array(),
  "followUp?": FollowUp.array(),
});
export type CapabilityIntent = typeof CapabilityIntent.infer;

const WEATHER_TOOL: ToolDecl = {
  name: "get_weather",
  description: "Look up the current weather for a city.",
  parameters: {
    type: "object",
    properties: {
      location: {
        type: "string",
        description: "City and optional region, e.g. 'Boston, MA'.",
      },
    },
    required: ["location"],
  },
};

const PLAIN_TEXT: CapabilityIntent = {
  prompt: "Reply with a single sentence: the capital of France is Paris.",
};

const FUNCTION_CALLING: CapabilityIntent = {
  prompt: "What is the weather in Boston, MA? Use the provided tool.",
  tools: [WEATHER_TOOL],
};

const FUNCTION_CALLING_MULTI_TURN: CapabilityIntent = {
  prompt: "What is the weather in Boston, MA? Use the provided tool.",
  tools: [WEATHER_TOOL],
  followUp: [
    {
      role: "tool",
      toolName: "get_weather",
      content:
        '{"location":"Boston, MA","temperatureF":68,"conditions":"clear"}',
    },
    {
      role: "user",
      content: "Summarize the result in one sentence.",
    },
  ],
};

const FUNCTION_CALLING_WITH_THINKING: CapabilityIntent = {
  prompt:
    "Think step by step about which tool to call, then call it. What is the weather in Boston, MA?",
  tools: [WEATHER_TOOL],
};

const VISION_INPUT: CapabilityIntent = {
  prompt: "Describe the attached image in one short sentence.",
  media: [{ kind: "image", path: "media/sample.jpg" }],
};

const AUDIO_INPUT: CapabilityIntent = {
  prompt: "Transcribe the attached audio in one sentence.",
  media: [{ kind: "audio", path: "media/sample.wav" }],
};

const VIDEO_INPUT: CapabilityIntent = {
  prompt: "Describe what happens in the attached video in one short sentence.",
  media: [{ kind: "video", path: "media/sample.mp4" }],
};

const DOCUMENT_INPUT: CapabilityIntent = {
  prompt: "Summarize the attached document in one sentence.",
  media: [{ kind: "document", path: "media/sample.pdf" }],
};

const IMAGE_OUTPUT: CapabilityIntent = {
  prompt: "Generate an image of a red apple on a wooden table.",
};

const CODE_EXECUTION: CapabilityIntent = {
  prompt: "Compute the 12th Fibonacci number by running code.",
};

const GROUNDING: CapabilityIntent = {
  prompt: "What is today's top news headline? Cite a web source.",
};

const REASONING_CONTENT: CapabilityIntent = {
  prompt:
    "A bat and a ball cost $1.10 together. The bat costs $1.00 more than the ball. How much does the ball cost? Show your reasoning before the final answer.",
};

const FILES_API_REFERENCE: CapabilityIntent = {
  prompt: "Summarize the attached document in one sentence.",
  media: [{ kind: "document", path: "media/sample.pdf" }],
};

// The prompt is Anthropic's documented magic string that deterministically
// triggers a redacted_thinking content block in the assistant turn, used for
// validating that clients round-trip the opaque encrypted block back to the
// API correctly on subsequent turns. Sourced from the Anthropic extended
// thinking docs:
// https://docs.anthropic.com/en/docs/build-with-claude/extended-thinking
// The followUp prompts a second turn so the round-trip is exercised on the
// wire; the plug-in is responsible for including the assistant's turn-1
// content blocks (text, thinking, redacted_thinking) verbatim in turn-2.
const REDACTED_THINKING: CapabilityIntent = {
  prompt:
    "ANTHROPIC_MAGIC_STRING_TRIGGER_REDACTED_THINKING_46C9A13E193C177646C7398A98432ECCCE4C1253D5E2D82641AC0E52CC2876CB",
  followUp: [
    {
      role: "user",
      content: "Briefly summarize what you just said in one sentence.",
    },
  ],
};

const INTENTS_TABLE: Record<Capability, CapabilityIntent> = {
  "plain-text": PLAIN_TEXT,
  "plain-text-streaming": PLAIN_TEXT,
  "function-calling": FUNCTION_CALLING,
  "function-calling-multi-turn": FUNCTION_CALLING_MULTI_TURN,
  "function-calling-multi-turn-streaming": FUNCTION_CALLING_MULTI_TURN,
  "function-calling-with-thinking": FUNCTION_CALLING_WITH_THINKING,
  "function-calling-with-thinking-streaming": FUNCTION_CALLING_WITH_THINKING,
  "vision-input": VISION_INPUT,
  "vision-input-streaming": VISION_INPUT,
  "audio-input": AUDIO_INPUT,
  "audio-input-streaming": AUDIO_INPUT,
  "video-input": VIDEO_INPUT,
  "video-input-streaming": VIDEO_INPUT,
  "document-input": DOCUMENT_INPUT,
  "document-input-streaming": DOCUMENT_INPUT,
  "image-output": IMAGE_OUTPUT,
  "image-output-streaming": IMAGE_OUTPUT,
  "code-execution": CODE_EXECUTION,
  "code-execution-streaming": CODE_EXECUTION,
  "reasoning-content": REASONING_CONTENT,
  "reasoning-content-streaming": REASONING_CONTENT,
  grounding: GROUNDING,
  "grounding-streaming": GROUNDING,
  "files-api-reference": FILES_API_REFERENCE,
  "files-api-reference-streaming": FILES_API_REFERENCE,
  "redacted-thinking": REDACTED_THINKING,
  "redacted-thinking-streaming": REDACTED_THINKING,
};

export const INTENTS: Readonly<Record<Capability, CapabilityIntent>> =
  INTENTS_TABLE;

// "../.." anchors at the package root from src/catalog/. If this file
// ever moves, update the segment count to match the new depth.
const PACKAGE_ROOT = fileURLToPath(new URL("../..", import.meta.url));

export function resolveMediaPath(ref: MediaRef): string {
  if (ref.path.startsWith("/")) {
    throw new Error(
      `MediaRef.path must be package-relative, got absolute path: ${ref.path}`,
    );
  }
  return `${PACKAGE_ROOT}${ref.path}`;
}
