// First-party OpenAI quirk: gpt-5.x rejects `max_tokens` and requires
// `max_completion_tokens`. The bag is per-deployment, so it is valid only
// while every first-party OpenAI model is gpt-5.x; a non-gpt-5 model would
// need the field split at model granularity.
export const OPENAI_FIRSTPARTY_QUIRKS: Record<string, unknown> = {
  maxTokensField: "max_completion_tokens",
};

// Reasoning relay quirk: the opencode-zen kimi/qwen/deepseek/glm/mimo models
// advertise reasoning_content, which the OpenAI adapter no longer forces by
// default. Verified for the Kimi backends; the rest assume the same wire.
// reasoningFieldNames restates the adapter default (reasoning_content then
// reasoning) as explicit documentation.
//
// Exported for providers.ts to import. The package `exports` map exposes only
// `.` and `./models`, so the constant is unreachable outside the package.
export const OPENAI_REASONING_QUIRKS: Record<string, unknown> = {
  forceAssistantReasoningContent: true,
  reasoningFieldNames: ["reasoning_content", "reasoning"],
};
