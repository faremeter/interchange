// Quirk bag for the first-party OpenAI deployment. gpt-5.x rejects
// `max_tokens` and requires `max_completion_tokens`, so the deployment opts
// its adapter into that field. Shared by the catalog seed data (production
// source config) and the live-session recorder so the two cannot drift.
//
// This is correct only because every first-party OpenAI deployment serves
// gpt-5.x models. `max_tokens` vs `max_completion_tokens` is really a
// per-model property; the quirk is per-deployment and collapses cleanly only
// while the deployment is single-family. A future non-gpt-5 first-party model
// would need the split expressed at a finer granularity.
export const OPENAI_FIRSTPARTY_QUIRKS: Record<string, unknown> = {
  maxTokensField: "max_completion_tokens",
};
