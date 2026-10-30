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

// First-party offerings whose highest reasoning_effort is above "high". The
// adapter sends an effort of "max" as the maxEffort value, so these name the
// highest value each model accepts: gpt-5.2, gpt-5.4, and gpt-5.5 top out at
// "xhigh", and gpt-5.6-sol accepts "max". The mini, nano, terra, and luna
// variants are not verified to accept either value, so they carry no
// maxEffort, and "max" sends "high" on them.
export const OPENAI_FIRSTPARTY_XHIGH_QUIRKS: Record<string, unknown> = {
  ...OPENAI_FIRSTPARTY_QUIRKS,
  maxEffort: "xhigh",
};
export const OPENAI_FIRSTPARTY_MAX_QUIRKS: Record<string, unknown> = {
  ...OPENAI_FIRSTPARTY_QUIRKS,
  maxEffort: "max",
};

// Responses-wire counterparts of the maxEffort quirks above: the Responses
// adapter reads the value from `reasoning.maxEffort`, not the top level, and
// rejects the Chat keys. The values are carried from the Chat entries for
// the same models — no Responses-side capture has yet sent `xhigh` or `max`
// on this wire (INTR-720 tracks per-capture rig options to prove them).
// Every Responses offering also requests a `detailed` reasoning summary:
// the fixtures prove the server returns summary text when asked, and the
// adapter only forwards `summary` when this quirk is set — without it,
// `reasoning-content` calls would yield empty thinking blocks.
export const OPENAI_RESPONSES_REASONING_QUIRKS: Record<string, unknown> = {
  reasoning: { summary: "detailed" },
};
export const OPENAI_RESPONSES_XHIGH_QUIRKS: Record<string, unknown> = {
  reasoning: { maxEffort: "xhigh", summary: "detailed" },
};
export const OPENAI_RESPONSES_MAX_QUIRKS: Record<string, unknown> = {
  reasoning: { maxEffort: "max", summary: "detailed" },
};

// The reasoning-bearing opencode-zen relay offerings — the kimi models plus
// qwen, deepseek, glm, and mimo — advertise reasoning_content on the wire. The
// OpenAI adapter no longer forces reasoning content by default, so
// forceAssistantReasoningContent is an override here. This requirement is
// verified only for the Kimi backends; it is extended to the other reasoning
// models on the same relay wire on the assumption they behave alike, which the
// discovery matrix does not independently prove. reasoningFieldNames restates
// the adapter's still-lenient default (read reasoning_content, then reasoning);
// it is redundant with that default but kept as explicit documentation of the
// reasoning fields these backends emit.
//
// Consumed by the opencode-zen reasoning offerings in providers.ts. It is
// exported so that sibling module can import it, but the package's `exports`
// map declares only `.` and `./models`, so Node's exports encapsulation blocks
// any external deep import of this module — the constant is genuinely
// unreachable outside the package, not merely undocumented.
export const OPENAI_REASONING_QUIRKS: Record<string, unknown> = {
  forceAssistantReasoningContent: true,
  reasoningFieldNames: ["reasoning_content", "reasoning"],
};
