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
  {
    canonicalName: "claude-opus-4-5-20251101",
    displayName: "Claude Opus 4.5",
  },
  { canonicalName: "claude-opus-4-6", displayName: "Claude Opus 4.6" },
  { canonicalName: "claude-opus-4-7", displayName: "Claude Opus 4.7" },
  {
    canonicalName: "claude-sonnet-4-5-20250929",
    displayName: "Claude Sonnet 4.5",
  },
  { canonicalName: "claude-sonnet-4-6", displayName: "Claude Sonnet 4.6" },
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
  {
    canonicalName: "gemini-3-flash-preview",
    displayName: "Gemini 3 Flash Preview",
  },
  {
    canonicalName: "gemini-3.1-pro-preview",
    displayName: "Gemini 3.1 Pro Preview",
  },
  { canonicalName: "kimi-k2.6", displayName: "Kimi K2.6" },
  { canonicalName: "qwen3.7-plus", displayName: "Qwen 3.7 Plus" },
  { canonicalName: "mimo-v2.5", displayName: "MiMo v2.5" },
  { canonicalName: "glm-5.2", displayName: "GLM 5.2" },
  { canonicalName: "gpt-5.4-mini", displayName: "GPT-5.4 Mini" },
  { canonicalName: "deepseek-v4-pro", displayName: "DeepSeek V4 Pro" },
  { canonicalName: "deepseek-v4-flash", displayName: "DeepSeek V4 Flash" },
  { canonicalName: "glm-5", displayName: "GLM 5" },
  { canonicalName: "glm-5.1", displayName: "GLM 5.1" },
  { canonicalName: "hy3", displayName: "HY3" },
  { canonicalName: "kimi-k2.5", displayName: "Kimi K2.5" },
  { canonicalName: "mimo-v2.5-pro", displayName: "MiMo v2.5 Pro" },
  { canonicalName: "minimax-m2.5", displayName: "MiniMax M2.5" },
  { canonicalName: "minimax-m2.7", displayName: "MiniMax M2.7" },
  { canonicalName: "minimax-m3", displayName: "MiniMax M3" },
  { canonicalName: "qwen3.5-plus", displayName: "Qwen 3.5 Plus" },
  { canonicalName: "qwen3.6-plus", displayName: "Qwen 3.6 Plus" },
  { canonicalName: "qwen3.7-max", displayName: "Qwen 3.7 Max" },
  { canonicalName: "qwen3.8-max", displayName: "Qwen 3.8 Max" },
  { canonicalName: "gpt-5", displayName: "GPT-5" },
  { canonicalName: "gpt-5-mini", displayName: "GPT-5 Mini" },
  { canonicalName: "gpt-5-nano", displayName: "GPT-5 Nano" },
  { canonicalName: "gpt-5.1", displayName: "GPT-5.1" },
  { canonicalName: "gpt-5.2", displayName: "GPT-5.2" },
  { canonicalName: "gpt-5.4", displayName: "GPT-5.4" },
  { canonicalName: "gpt-5.4-nano", displayName: "GPT-5.4 Nano" },
  { canonicalName: "o1", displayName: "o1" },
  { canonicalName: "o3", displayName: "o3" },
  { canonicalName: "o3-mini", displayName: "o3-mini" },
  { canonicalName: "o4-mini", displayName: "o4-mini" },
  { canonicalName: "gpt-4.1", displayName: "GPT-4.1" },
  { canonicalName: "gpt-4.1-mini", displayName: "GPT-4.1 Mini" },
  { canonicalName: "gpt-4.1-nano", displayName: "GPT-4.1 Nano" },
  { canonicalName: "gpt-4o", displayName: "GPT-4o" },
  { canonicalName: "gpt-4o-mini", displayName: "GPT-4o Mini" },
  { canonicalName: "gpt-4-turbo", displayName: "GPT-4 Turbo" },
  { canonicalName: "gpt-4", displayName: "GPT-4" },
];
