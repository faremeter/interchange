// Declarative catalog seed data for the local development database.
//
// bin/seed.ts drives these specs through the catalog HTTP API. The colocated
// catalog-seed-data.test.ts is the CI guard. It asserts every offering's quirks
// bag is a plain object that validates against its adapter's quirk schema; that
// every hand-authored curated capability is genuinely non-probeable, so a wire
// capability can only enter a row through the discovery matrix and never the
// curated list; and that a declared discovery source expands to a non-empty
// matrix capability set rather than a stale or mistyped tuple. A seeded
// deployment therefore cannot ship an accommodation the adapter would reject or
// claim a wire capability the matrix has not proven.
//
// This module is pure data and types — no HTTP, no side effects — so the
// guard test can import it without pulling in the seed's network machinery.

import type { Capability } from "@intx/types";

export type CatalogPlugin =
  | "anthropic"
  | "openai"
  | "openai-compatible"
  | "google-genai";

export type CatalogModelSpec = {
  canonicalName: string;
  displayName: string;
};

export type DiscoverySource = {
  // The discovery support-matrix provider name (e.g. "opencode-zen"), not the
  // catalog provider name or the plugin.
  provider: string;
  // The exact wire model id the matrix probed (e.g. "kimi-k2.6").
  model: string;
};

export type CatalogOfferingSpec = {
  // References a CatalogModelSpec.canonicalName.
  model: string;
  // Lower is preferred first when several deployments serve one model.
  priority: number;
  // The (provider, model) the offering's wire capabilities are drawn from in the
  // discovery support matrix, or null when this exact tuple has not been probed.
  // Wire capabilities are never hand-authored: the seed derives them from this
  // key through the discovery helper (see offeringCapabilities), so a row cannot
  // claim a capability the matrix has not proven.
  discoverySource: DiscoverySource | null;
  // Model capabilities the matrix cannot prove (long-context, prompt-caching),
  // curated by hand. Explicit on every offering even when empty, matching quirks.
  curatedCapabilities: Capability[];
  // Per-deployment adapter accommodations, explicit on every offering (the
  // guard enforces it) even when empty. See OPENAI_REASONING_QUIRKS for why
  // the openai-plugin deployments carry explicit reasoning quirks.
  quirks: Record<string, unknown>;
  // Dev pricing as decimal strings, matching the API's string money fields.
  price: { input: string; output: string };
};

export type CatalogProviderSpec = {
  // Names both the catalog provider and the backing old-system provider row
  // that owns its credential.
  name: string;
  plugin: CatalogPlugin;
  baseURL: string;
  credentialName: string;
  // Fake dev secret; never a real key.
  credentialSecret: string;
  offerings: CatalogOfferingSpec[];
};

// kimi-serving backends require reasoning_content on every assistant turn. The
// OpenAI adapter no longer forces that by default, so
// forceAssistantReasoningContent is a required override here: drop it and these
// deployments regress. reasoningFieldNames instead restates the adapter's
// still-lenient default (read reasoning_content, then reasoning); it is
// redundant with that default but kept as explicit catalog documentation of the
// reasoning fields these backends emit.
const OPENAI_REASONING_QUIRKS: Record<string, unknown> = {
  forceAssistantReasoningContent: true,
  reasoningFieldNames: ["reasoning_content", "reasoning"],
};

export const catalogModels: CatalogModelSpec[] = [
  { canonicalName: "claude-sonnet-5", displayName: "Claude Sonnet 5" },
  {
    canonicalName: "claude-haiku-4-5-20251001",
    displayName: "Claude Haiku 4.5",
  },
  { canonicalName: "gpt-5.5", displayName: "GPT-5.5" },
  { canonicalName: "gemini-2.5-pro", displayName: "Gemini 2.5 Pro" },
  { canonicalName: "kimi-k3", displayName: "Kimi K3" },
  { canonicalName: "kimi-k2.7-code", displayName: "Kimi K2.7 Code" },
];

// The Fireworks / Moonshot / OpenRouter providers all offer the one `kimi-k3`
// model, and the two OpenCode Zen providers both offer `kimi-k2.7-code`.
// Distinct priorities give source resolution a deterministic order across the
// deployments of a shared model. The anthropic and google-genai adapters carry
// no accommodations, and gpt-5.5 emits no reasoning_content on api.openai.com
// (its discovery source marks reasoning-content unsupported), so the Anthropic,
// Gemini, and OpenAI Direct offerings all ship an empty quirks bag; only the
// kimi-serving openai-compatible deployments need OPENAI_REASONING_QUIRKS.
export const catalogProviders: CatalogProviderSpec[] = [
  {
    name: "Anthropic Direct",
    plugin: "anthropic",
    baseURL: "https://api.anthropic.com",
    credentialName: "Anthropic Direct Key",
    credentialSecret: "sk-ant-fake-key-for-seed-data",
    offerings: [
      {
        model: "claude-sonnet-5",
        priority: 0,
        discoverySource: { provider: "anthropic", model: "claude-sonnet-5" },
        curatedCapabilities: ["long-context"],
        quirks: {},
        price: { input: "0.000003", output: "0.000015" },
      },
      {
        model: "claude-haiku-4-5-20251001",
        priority: 10,
        discoverySource: {
          provider: "anthropic",
          model: "claude-haiku-4-5-20251001",
        },
        curatedCapabilities: [],
        quirks: {},
        price: { input: "0.0000008", output: "0.000004" },
      },
    ],
  },
  {
    name: "OpenAI Direct",
    plugin: "openai",
    baseURL: "https://api.openai.com/v1",
    credentialName: "OpenAI Direct Key",
    credentialSecret: "sk-openai-fake-key-for-seed-data",
    offerings: [
      {
        model: "gpt-5.5",
        priority: 0,
        discoverySource: { provider: "openai", model: "gpt-5.5" },
        curatedCapabilities: [],
        quirks: {},
        price: { input: "0.0000025", output: "0.00001" },
      },
    ],
  },
  {
    name: "Gemini Direct",
    plugin: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    credentialName: "Gemini Direct Key",
    credentialSecret: "AIza-fake-key-for-seed-data",
    offerings: [
      {
        model: "gemini-2.5-pro",
        priority: 0,
        discoverySource: { provider: "google-genai", model: "gemini-2.5-pro" },
        curatedCapabilities: ["long-context"],
        quirks: {},
        price: { input: "0.00000125", output: "0.00001" },
      },
    ],
  },
  {
    name: "Fireworks Kimi",
    plugin: "openai-compatible",
    baseURL: "https://api.fireworks.ai/inference/v1",
    credentialName: "Fireworks Kimi Key",
    credentialSecret: "fw-fake-key-for-seed-data",
    offerings: [
      {
        model: "kimi-k3",
        priority: 0,
        discoverySource: { provider: "opencode-zen", model: "kimi-k3" },
        curatedCapabilities: [],
        quirks: OPENAI_REASONING_QUIRKS,
        price: { input: "0.0000006", output: "0.0000025" },
      },
    ],
  },
  {
    name: "Moonshot Kimi",
    plugin: "openai-compatible",
    baseURL: "https://api.moonshot.ai/v1",
    credentialName: "Moonshot Kimi Key",
    credentialSecret: "sk-moonshot-fake-key-for-seed-data",
    offerings: [
      {
        model: "kimi-k3",
        priority: 10,
        discoverySource: { provider: "opencode-zen", model: "kimi-k3" },
        curatedCapabilities: [],
        quirks: OPENAI_REASONING_QUIRKS,
        price: { input: "0.0000006", output: "0.0000025" },
      },
    ],
  },
  {
    name: "OpenRouter Kimi",
    plugin: "openai-compatible",
    baseURL: "https://openrouter.ai/api/v1",
    credentialName: "OpenRouter Kimi Key",
    credentialSecret: "sk-or-fake-key-for-seed-data",
    offerings: [
      {
        model: "kimi-k3",
        priority: 20,
        discoverySource: { provider: "opencode-zen", model: "kimi-k3" },
        curatedCapabilities: [],
        quirks: OPENAI_REASONING_QUIRKS,
        price: { input: "0.0000006", output: "0.0000025" },
      },
    ],
  },
  {
    name: "OpenCode Zen v1",
    plugin: "openai-compatible",
    baseURL: "https://opencode.ai/zen/v1",
    credentialName: "OpenCode Zen v1 Key",
    credentialSecret: "ocz-fake-key-for-seed-data",
    offerings: [
      {
        model: "kimi-k2.7-code",
        priority: 0,
        discoverySource: { provider: "opencode-zen", model: "kimi-k2.7-code" },
        curatedCapabilities: [],
        quirks: OPENAI_REASONING_QUIRKS,
        price: { input: "0.0000006", output: "0.0000025" },
      },
    ],
  },
  {
    name: "OpenCode Zen Go v1",
    plugin: "openai-compatible",
    baseURL: "https://opencode.ai/zen/go/v1",
    credentialName: "OpenCode Zen Go v1 Key",
    credentialSecret: "ocz-go-fake-key-for-seed-data",
    offerings: [
      {
        model: "kimi-k2.7-code",
        priority: 10,
        discoverySource: { provider: "opencode-zen", model: "kimi-k2.7-code" },
        curatedCapabilities: [],
        quirks: OPENAI_REASONING_QUIRKS,
        price: { input: "0.0000006", output: "0.0000025" },
      },
    ],
  },
];
