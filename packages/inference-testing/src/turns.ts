// Shared ConversationTurn constructors for tests that build user turns.

import type { ConversationTurn } from "@intx/types/runtime";

export function userTurn(text: string): ConversationTurn {
  return {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: 0,
  };
}
