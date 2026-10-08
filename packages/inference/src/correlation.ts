// Correlation connects outbound async tool calls to inbound responses. The
// reactor owns the matching; the director does not participate.

import type { InboundMessage, PendingOperation } from "@intx/types/runtime";

/**
 * Validate that an inbound message is an authentic response to a registered
 * pending operation. The reactor performs no sender check of its own;
 * without a validator, any message bearing the registered correlation ID
 * resolves the operation.
 */
export interface CorrelationValidator {
  /** True if `message` resolves `pending`; false delivers it as a regular
   *  uncorrelated event. */
  validate(
    pending: PendingOperation,
    message: InboundMessage,
  ): Promise<boolean>;
}

/**
 * Tracks pending async operations, mapping a correlation ID to the operation
 * metadata and the gate waiting for it.
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

  function findByGateId(gateId: string): PendingOperation | undefined {
    for (const op of operations.values()) {
      if (op.gateId === gateId) return op;
    }
    return undefined;
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

  return { register, lookup, findByGateId, remove, all, hasAny };
}

export type CorrelationRegistry = ReturnType<typeof createCorrelationRegistry>;
