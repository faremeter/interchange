import { type } from "arktype";

import {
  ConnectorThreadState,
  ConversationTurn,
  TokenUsage,
} from "./runtime-core";

// An agent step's state as exported from a workflow run. Pending operations
// are deliberately absent -- their correlation ids only mean something inside
// the run that registered them.
export const StepStateSnapshot = type({
  version: type("1").describe("Snapshot format version."),
  turns: ConversationTurn.array().describe(
    "The step agent's conversation history, oldest first.",
  ),
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null").describe(
    "Mail thread the agent replies on, or null when no thread is active.",
  ),
});
export type StepStateSnapshot = typeof StepStateSnapshot.infer;
