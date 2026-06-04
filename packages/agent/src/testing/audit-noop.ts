// No-op AuditStore for tests and examples.
//
// Returns immediately for every commit; returns empty arrays for every
// load. Useful when the agent is exercised in tests that do not assert
// audit content, or in examples whose purpose is the agent surface
// rather than the audit ledger. Production callers must supply a real
// audit store.

import type { AuditRecord, ErrorRecord } from "@intx/types/audit";
import type { AuditStore } from "@intx/types/runtime";

/**
 * Construct a no-op AuditStore. Each call returns a fresh object so
 * tests that introspect the store identity (e.g. asserting two agents
 * received different stores) can do so.
 */
export function noopAuditStore(): AuditStore {
  return {
    async commitAudit(_records: AuditRecord[]): Promise<void> {
      // No-op.
    },
    async commitErrors(_errors: ErrorRecord[]): Promise<void> {
      // No-op.
    },
    async loadAudit(_sessionId: string): Promise<AuditRecord[]> {
      return [];
    },
  };
}
