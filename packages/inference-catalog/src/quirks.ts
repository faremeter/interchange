// Quirk bag for the first-party OpenAI deployment. gpt-5.x rejects `max_tokens`
// and requires `max_completion_tokens`, so the deployment opts its adapter into
// that field. Part of the package's public surface: the `openai` offerings
// carry it, and exposing it as a named export lets a consumer reference the one
// definition rather than re-declaring the field.
//
// This is correct only because every first-party OpenAI deployment serves
// gpt-5.x models. `max_tokens` vs `max_completion_tokens` is really a per-model
// property; the quirk is per-deployment and collapses cleanly only while the
// deployment is single-family. A future non-gpt-5 first-party model would need
// the split expressed at a finer granularity.
export const OPENAI_FIRSTPARTY_QUIRKS: Record<string, unknown> = {
  maxTokensField: "max_completion_tokens",
};

// kimi-serving backends require reasoning_content on every assistant turn. The
// OpenAI adapter no longer forces that by default, so
// forceAssistantReasoningContent is a required override here: drop it and these
// deployments regress. reasoningFieldNames instead restates the adapter's
// still-lenient default (read reasoning_content, then reasoning); it is
// redundant with that default but kept as explicit catalog documentation of the
// reasoning fields these backends emit.
//
// Consumed only by the kimi offerings in providers.ts. It is exported so that
// sibling module can import it, but the package's `exports` map declares only
// `.` and `./models`, so Node's exports encapsulation blocks any external deep
// import of this module — the constant is genuinely unreachable outside the
// package, not merely undocumented.
export const OPENAI_REASONING_QUIRKS: Record<string, unknown> = {
  forceAssistantReasoningContent: true,
  reasoningFieldNames: ["reasoning_content", "reasoning"],
};
