// No-op AuditStore for tests and examples: returns immediately for
// every commit and empty arrays for every load. Production callers
// must supply a real audit store.

import type { AuditRecord, ErrorRecord } from "@intx/types/audit";
import type { AuditStore } from "@intx/types/runtime";

/**
 * Construct a no-op AuditStore. Each call returns a fresh object so
 * tests that introspect the store identity can do so.
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
