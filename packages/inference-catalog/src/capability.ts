// The capability vocabulary, owned locally as a value array so the package's
// public type surface stays self-contained. `@intx/types` is the canonical
// source of this list (its `CAPABILITIES`), but re-exporting the type from
// there would put an `import("@intx/types")` reference into this package's
// emitted `.d.ts`, and `@intx/types` is only a build/test-time devDependency
// here — a published consumer would then carry a dangling reference to a
// package it never installed. So the list is duplicated and the guard test
// (catalog.test.ts) pins it: it asserts CATALOG_CAPABILITIES set-equals
// `@intx/types`' `CAPABILITIES`, failing loudly the moment the vocabularies
// drift. That guard is why re-exporting from `@intx/types` is the wrong fix
// even though it looks like the obvious deduplication. Order here is cosmetic
// (the type is an order-independent union and the guard is set-equality), so
// this list need not track `@intx/types`' array order.
//
// Not re-exported from the package barrel: `index.ts` exposes only the
// `Capability` type. The array stays package-private for the guard test's use.
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
