// Fixtures for tests that feed a real reactor's events into the hub's
// collector and status tracker.

import type {
  ContextStore,
  InferenceSource,
  TokenUsage,
} from "@intx/types/runtime";

export function emptyUsage(): TokenUsage {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
}

// A context store that starts empty and keeps nothing.
export function emptyStore(): ContextStore {
  const noop = async () => {
    /* noop */
  };
  return {
    async load() {
      return {
        turns: [],
        pendingOperations: [],
        tokenUsage: emptyUsage(),
        connectorState: null,
      };
    },
    setConnectorState() {
      /* noop */
    },
    async commit(options) {
      return { hash: "h", message: options.message, timestamp: Date.now() };
    },
    branch: noop,
    async log() {
      return [];
    },
    async readAt() {
      return [];
    },
    writeBlob: noop,
    async readBlob() {
      throw new Error("not implemented");
    },
    writePrompt: noop,
    writeResponse: noop,
    writeManifest: noop,
    writeTurns: noop,
    writeMetadata: noop,
    async readManifestHistory() {
      throw new Error("not implemented");
    },
  };
}

export const testSource: InferenceSource = {
  id: "anthropic:m",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "c",
  model: "m",
};
