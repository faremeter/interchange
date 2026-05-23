import type { Capability } from "@intx/inference-discovery/catalog";

export const ANTHROPIC_BASE = "https://api.anthropic.com";
export const MESSAGES_PATH = "/v1/messages";
export const FILES_PATH = "/v1/files";

// Both streaming and non-streaming requests target /v1/messages; the
// `stream: true` field in the request body is what distinguishes them.
export function buildMessagesURL(): string {
  return `${ANTHROPIC_BASE}${MESSAGES_PATH}`;
}

export function buildFilesURL(): string {
  return `${ANTHROPIC_BASE}${FILES_PATH}`;
}

export function isStreamingCapability(capability: Capability): boolean {
  return capability.endsWith("-streaming");
}
