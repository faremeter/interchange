import { type } from "arktype";

// The capabilities the production inference runtime demonstrates on the wire,
// and that the discovery rig probes for. This is the single source of truth for
// the shared capability vocabulary: @intx/inference-discovery imports this list
// and extends it, so production code never has to depend on the discovery
// package. Each capability that has a streaming wire flow distinct from its
// buffered one carries a paired `-streaming` variant; `function-calling` is the
// sole base with no streaming pair (a bare tool call has no delta flow to
// capture).
export const WIRE_CAPABILITIES = [
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
] as const;

// Capabilities a model has that are not observable on the wire and cannot be
// proven by a discovery fixture. `long-context` denotes a model advertising a
// context window of at least ~200k tokens (a curation criterion, not a stored
// limit); `prompt-caching` denotes provider-side prompt caching. The discovery
// rig has no probe that could prove either, so operators curate them by hand.
export const CURATED_CAPABILITIES = ["long-context", "prompt-caching"] as const;

export const CAPABILITIES = [
  ...WIRE_CAPABILITIES,
  ...CURATED_CAPABILITIES,
] as const;
export type Capability = (typeof CAPABILITIES)[number];
export const Capability = type
  .enumerated(...CAPABILITIES)
  .describe(
    "A capability a provider advertises for a model: a wire capability the inference runtime supports, or one of the curated tags `long-context` and `prompt-caching`.",
  );
