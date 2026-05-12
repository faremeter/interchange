// Correlation registry and validator interface for the agent reactor.
//
// Correlation connects outbound async tool calls to inbound responses. The
// reactor owns the matching; the director does not participate.
//
// (INFERENCE.md § Correlation)

import type {
  InboundMessage,
  PendingOperation,
} from "@interchange/types/runtime";

/**
 * Validates whether an inbound message is an authentic response to a
 * registered pending operation. Consumers provide this at reactor construction
 * time to enforce sender identity and signature checks.
 */
export interface CorrelationValidator {
  /**
   * Return true if `message` is a valid resolution for `pending`.
   * False causes the message to be delivered as a regular uncorrelated event.
   */
  validate(
    pending: PendingOperation,
    message: InboundMessage,
  ): Promise<boolean>;
}

/**
 * Tracks pending async operations. Each entry maps a correlation ID to the
 * operation metadata and the gate that is waiting for it.
 */
export function createCorrelationRegistry() {
  const operations = new Map<string, PendingOperation>();

  function register(op: PendingOperation): void {
    if (operations.has(op.correlationId)) {
      throw new Error(
        `Correlation ID "${op.correlationId}" is already registered`,
      );
    }
    operations.set(op.correlationId, op);
  }

  function lookup(correlationId: string): PendingOperation | undefined {
    return operations.get(correlationId);
  }

  function remove(correlationId: string): boolean {
    return operations.delete(correlationId);
  }

  function all(): PendingOperation[] {
    return Array.from(operations.values());
  }

  function hasAny(): boolean {
    return operations.size > 0;
  }

  return { register, lookup, remove, all, hasAny };
}

export type CorrelationRegistry = ReturnType<typeof createCorrelationRegistry>;
