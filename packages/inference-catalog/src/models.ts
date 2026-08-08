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
  { canonicalName: "claude-fable-5", displayName: "Claude Fable 5" },
  { canonicalName: "claude-opus-4-8", displayName: "Claude Opus 4.8" },
  { canonicalName: "gpt-5.6-terra", displayName: "GPT-5.6 Terra" },
  { canonicalName: "gpt-5.6-luna", displayName: "GPT-5.6 Luna" },
  { canonicalName: "gemini-2.5-flash", displayName: "Gemini 2.5 Flash" },
  { canonicalName: "gemini-3.5-flash", displayName: "Gemini 3.5 Flash" },
  {
    canonicalName: "gemini-2.5-flash-image",
    displayName: "Gemini 2.5 Flash Image",
  },
  {
    canonicalName: "gemini-3.1-flash-image",
    displayName: "Gemini 3.1 Flash Image",
  },
  { canonicalName: "kimi-k2.6", displayName: "Kimi K2.6" },
  { canonicalName: "qwen3.7-plus", displayName: "Qwen 3.7 Plus" },
  { canonicalName: "mimo-v2.5", displayName: "MiMo v2.5" },
  { canonicalName: "glm-5.2", displayName: "GLM 5.2" },
  { canonicalName: "gpt-5.4-mini", displayName: "GPT-5.4 Mini" },
  { canonicalName: "deepseek-v4-pro", displayName: "DeepSeek V4 Pro" },
  { canonicalName: "deepseek-v4-flash", displayName: "DeepSeek V4 Flash" },
];
