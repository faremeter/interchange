// Dev-only fixtures layered onto @intx/inference-catalog when seeding the local
// database. The published catalog deliberately excludes credential material and
// pricing — today's prices are dev-fakes — so the seed owns them here and joins
// them to the catalog's providers and offerings by name. Keeping them out of
// the package is what lets the package be a clean public contract.

export type CredentialFixture = {
  credentialName: string;
  credentialSecret: string;
};

// Keyed by catalog provider name. Fake dev secrets; never real keys.
export const credentialFixtures: Record<string, CredentialFixture> = {
  "Anthropic Direct": {
    credentialName: "Anthropic Direct Key",
    credentialSecret: "sk-ant-fake-key-for-seed-data",
  },
  "OpenAI Direct": {
    credentialName: "OpenAI Direct Key",
    credentialSecret: "sk-openai-fake-key-for-seed-data",
  },
  "Gemini Direct": {
    credentialName: "Gemini Direct Key",
    credentialSecret: "AIza-fake-key-for-seed-data",
  },
  "Fireworks Kimi": {
    credentialName: "Fireworks Kimi Key",
    credentialSecret: "fw-fake-key-for-seed-data",
  },
  "Moonshot Kimi": {
    credentialName: "Moonshot Kimi Key",
    credentialSecret: "sk-moonshot-fake-key-for-seed-data",
  },
  "OpenRouter Kimi": {
    credentialName: "OpenRouter Kimi Key",
    credentialSecret: "sk-or-fake-key-for-seed-data",
  },
  "OpenCode Zen v1": {
    credentialName: "OpenCode Zen v1 Key",
    credentialSecret: "ocz-fake-key-for-seed-data",
  },
  "OpenCode Zen Go v1": {
    credentialName: "OpenCode Zen Go v1 Key",
    credentialSecret: "ocz-go-fake-key-for-seed-data",
  },
};

// Dev pricing as decimal strings, matching the API's string money fields.
export type PriceFixture = { input: string; output: string };

// Keyed by catalog provider name, then model canonical name — the same
// (provider, model) identity the catalog offering and the seeded offering row
// carry.
export const priceFixtures: Record<string, Record<string, PriceFixture>> = {
  "Anthropic Direct": {
    "claude-sonnet-5": { input: "0.000003", output: "0.000015" },
    "claude-opus-5": { input: "0.000005", output: "0.000025" },
    "claude-haiku-4-5-20251001": { input: "0.0000008", output: "0.000004" },
  },
  "OpenAI Direct": {
    "gpt-5.5": { input: "0.0000025", output: "0.00001" },
    "gpt-5.6-sol": { input: "0.000003", output: "0.000012" },
  },
  "Gemini Direct": {
    "gemini-2.5-pro": { input: "0.00000125", output: "0.00001" },
    "gemini-3.6-flash": { input: "0.0000005", output: "0.000003" },
  },
  "Fireworks Kimi": {
    "kimi-k3": { input: "0.0000006", output: "0.0000025" },
  },
  "Moonshot Kimi": {
    "kimi-k3": { input: "0.0000006", output: "0.0000025" },
  },
  "OpenRouter Kimi": {
    "kimi-k3": { input: "0.0000006", output: "0.0000025" },
  },
  "OpenCode Zen v1": {
    "kimi-k2.7-code": { input: "0.0000006", output: "0.0000025" },
  },
  "OpenCode Zen Go v1": {
    "kimi-k2.7-code": { input: "0.0000006", output: "0.0000025" },
  },
};

// Offerings intentionally seeded without pricing, keyed by provider name to the
// model canonical names. Dev pricing is a fake fixture, so a model can ship
// without one — but absence must be declared here, not inferred: the seed
// requires every offering to be either priced or explicitly listed here (never
// neither, never both), so a dropped or mistyped price on a priced model still
// aborts the seed rather than silently skipping its pricing row.
export const unpricedOfferings: Record<string, readonly string[]> = {
  "Anthropic Direct": [
    "claude-fable-5",
    "claude-opus-4-8",
    "claude-opus-4-5-20251101",
    "claude-opus-4-6",
    "claude-opus-4-7",
    "claude-sonnet-4-5-20250929",
    "claude-sonnet-4-6",
  ],
  "OpenAI Direct": ["gpt-5.6-terra", "gpt-5.6-luna"],
  "Gemini Direct": [
    "gemini-2.5-flash",
    "gemini-3.5-flash",
    "gemini-2.5-flash-image",
    "gemini-3.1-flash-image",
  ],
  "Fireworks Kimi": ["kimi-k2.6"],
  "Moonshot Kimi": ["kimi-k2.6"],
  "OpenRouter Kimi": ["kimi-k2.6"],
  "OpenCode Zen v1": [
    "kimi-k2.6",
    "qwen3.7-plus",
    "mimo-v2.5",
    "glm-5.2",
    "gpt-5.4-mini",
    "glm-5",
    "glm-5.1",
    "hy3",
    "kimi-k2.5",
    "mimo-v2.5-pro",
    "minimax-m2.5",
    "minimax-m2.7",
    "minimax-m3",
    "qwen3.5-plus",
    "qwen3.6-plus",
    "qwen3.7-max",
    "qwen3.8-max",
  ],
  "OpenCode Zen Go v1": ["kimi-k2.6", "deepseek-v4-pro", "deepseek-v4-flash"],
};
