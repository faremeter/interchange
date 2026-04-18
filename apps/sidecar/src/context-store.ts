// In-memory ContextStore for agent sessions. Conversation history persists
// across websocket reconnects but not across sidecar restarts.

import type {
  ContextStore,
  ContextCommit,
  ConversationMessage,
  PendingOperation,
  TokenUsage,
} from "@interchange/types/runtime";

const ZERO_USAGE: TokenUsage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

export function createMemoryContextStore(): ContextStore {
  let messages: ConversationMessage[] = [];
  let pendingOperations: PendingOperation[] = [];
  let tokenUsage: TokenUsage = { ...ZERO_USAGE };
  let commitCount = 0;

  const commits: ContextCommit[] = [];
  const snapshots = new Map<string, ConversationMessage[]>();

  return {
    async load() {
      return {
        messages: [...messages],
        pendingOperations: [...pendingOperations],
        tokenUsage: { ...tokenUsage },
      };
    },

    async commit(msgs, ops, usage, message) {
      messages = [...msgs];
      pendingOperations = [...ops];
      tokenUsage = { ...usage };

      const hash = `mem-${++commitCount}`;
      const commit: ContextCommit = {
        hash,
        message,
        timestamp: Date.now(),
        ...(commits.length > 0
          ? // Length check above guarantees this element exists.
            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            { parentHash: commits[commits.length - 1]!.hash }
          : {}),
      };
      commits.push(commit);
      snapshots.set(hash, [...messages]);

      return commit;
    },

    async branch() {
      // No-op for in-memory store.
    },

    async log(limit) {
      const n = limit ?? commits.length;
      return commits.slice(-n).reverse();
    },

    async readAt(hash) {
      const snap = snapshots.get(hash);
      if (snap === undefined) {
        throw new Error(`No snapshot at hash "${hash}"`);
      }
      return [...snap];
    },
  };
}
