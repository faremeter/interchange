// The capability vocabulary, duplicated from `@intx/types`' `CAPABILITIES`
// because importing it would leak an `import("@intx/types")` reference into
// the published `.d.ts` (types is a build-only devDependency). The
// catalog.test.ts guard pins the two lists equal; order is cosmetic because
// both the union type and the guard compare as sets.
//
// Not exported from the package barrel; the array stays private for the guard.
export const CATALOG_CAPABILITIES = [
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
  "structured-output",
  "structured-output-streaming",
  "long-context",
  "prompt-caching",
] as const;

export type Capability = (typeof CATALOG_CAPABILITIES)[number];
