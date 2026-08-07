// The flat model list, exposed self-contained on the `./models` subpath for
// consumers that want only canonical and display names. This module imports
// nothing so the subpath stays a dependency-free leaf.

export type CatalogModelSpec = {
  canonicalName: string;
  displayName: string;
};

export const catalogModels: CatalogModelSpec[] = [
  { canonicalName: "claude-sonnet-5", displayName: "Claude Sonnet 5" },
  { canonicalName: "claude-opus-5", displayName: "Claude Opus 5" },
  {
    canonicalName: "claude-haiku-4-5-20251001",
    displayName: "Claude Haiku 4.5",
  },
  { canonicalName: "gpt-5.5", displayName: "GPT-5.5" },
  { canonicalName: "gpt-5.6-sol", displayName: "GPT-5.6 Sol" },
  { canonicalName: "gemini-2.5-pro", displayName: "Gemini 2.5 Pro" },
  { canonicalName: "gemini-3.6-flash", displayName: "Gemini 3.6 Flash" },
  { canonicalName: "kimi-k3", displayName: "Kimi K3" },
  { canonicalName: "kimi-k2.7-code", displayName: "Kimi K2.7 Code" },
];
