import { type } from "arktype";

export const CAPABILITIES = [
  "plain-text",
  "plain-text-streaming",
  "function-calling",
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
  "image-output",
  "image-output-streaming",
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
  "safety-classification",
  "safety-classification-streaming",
  "structured-output",
  "structured-output-streaming",
] as const;

export const Capability = type.enumerated(...CAPABILITIES);
export type Capability = typeof Capability.infer;
