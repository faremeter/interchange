import type { Capability } from "@intx/inference-discovery/catalog";

export const GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta";

export function isStreamingCapability(capability: Capability): boolean {
  return capability.endsWith("-streaming");
}

export function buildEndpointURL(opts: {
  model: string;
  capability: Capability;
}): string {
  if (opts.model.length === 0) {
    throw new Error("google-genai: model must be a non-empty string");
  }
  if (isStreamingCapability(opts.capability)) {
    return `${GEMINI_BASE}/models/${opts.model}:streamGenerateContent?alt=sse`;
  }
  return `${GEMINI_BASE}/models/${opts.model}:generateContent`;
}
