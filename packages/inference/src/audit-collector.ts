// Audit collector: accumulates tool invocation records for persistence.
//
// The collector correlates three data sources into complete AuditRecord
// objects:
//   1. tool.start events — tool name and arguments (allowed calls only)
//   2. AuthzDecision via onDecision — governance decision
//   3. tool.done events — result and completion metadata
//
// Correlation is by callId. For blocked calls, no tool.start is emitted;
// the collector creates the record from the buffered decision and the
// tool.done event alone.
//
// Wiring: the caller must connect onDecision to the authz extension's
// onDecision callback, and onEvent to the reactor's event stream. The
// types alone do not enforce this — it is a composition-layer concern.

import type { AuditRecord, AuditAuthz } from "@intx/types/audit";
import type { InferenceEvent } from "@intx/types/runtime";
import { getLogger } from "@intx/log";
import type { AuthzDecision } from "./authz-extension";

const logger = getLogger(["interchange", "audit-collector"]);

type PendingRecord = {
  callId: string;
  tool: string;
  arguments: Record<string, unknown>;
  authz: AuditAuthz | null;
};

export type AuditCollector = {
  onEvent(event: InferenceEvent): void;
  onDecision(decision: AuthzDecision): void;
  flush(): AuditRecord[];
  pending(): number;
};

function coerceContent(content: unknown): string | Record<string, unknown> {
  if (typeof content === "string") return content;
  if (typeof content === "object" && content !== null) {
    // content is a non-null object — compatible with Record<string, unknown>
    // but TypeScript can't verify the index signature without a cast.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- non-null object is structurally compatible with Record<string, unknown> but TS won't widen
    return content as Record<string, unknown>;
  }
  throw new Error(`Unexpected tool result content type: ${typeof content}`);
}

function mapGrant(g: AuthzDecision["matchingGrants"][number]) {
  return {
    id: g.id,
    resource: g.resource,
    action: g.action,
    effect: g.effect,
    origin: g.origin,
    specificity: g.specificity,
  };
}

function decisionToAuthz(d: AuthzDecision): AuditAuthz {
  return {
    effect: d.effect,
    resolvedBy: d.resolvedBy ? mapGrant(d.resolvedBy) : null,
    matchingGrants: d.matchingGrants.map(mapGrant),
    blocked: d.blocked,
    ...(d.blockReason !== undefined ? { blockReason: d.blockReason } : {}),
  };
}

export function createAuditCollector(sessionId: string): AuditCollector {
  const decisions = new Map<string, AuthzDecision>();
  const pendingRecords = new Map<string, PendingRecord>();
  const completed: AuditRecord[] = [];

  function onDecision(decision: AuthzDecision): void {
    decisions.set(decision.callId, decision);
  }

  function onEvent(event: InferenceEvent): void {
    if (event.type === "tool.start") {
      const call = event.data.call;
      const decision = decisions.get(call.id);
      decisions.delete(call.id);

      pendingRecords.set(call.id, {
        callId: call.id,
        tool: call.name,
        arguments: call.arguments,
        authz: decision ? decisionToAuthz(decision) : null,
      });
      return;
    }

    if (event.type === "tool.done") {
      const result = event.data.result;
      const pending = pendingRecords.get(result.callId);

      if (pending) {
        pendingRecords.delete(result.callId);
        completed.push({
          callId: pending.callId,
          tool: pending.tool,
          arguments: pending.arguments,
          authz: pending.authz,
          result: {
            content: coerceContent(result.content),
            isError: result.isError === true,
          },
          timestamp: new Date().toISOString(),
          sessionId,
          seq: event.seq,
        });
        return;
      }

      // Blocked call: no tool.start was emitted. Build the record from
      // the buffered decision and the tool.done event.
      const decision = decisions.get(result.callId);
      if (decision === undefined) {
        // Orphaned tool.done: no tool.start or authz decision was recorded.
        // Emit a degraded record rather than crashing the session — the audit
        // system is observational infrastructure and must not veto execution.

        logger.warn`Orphaned tool.done for callId "${result.callId}": no tool.start or authz decision was recorded`;
        completed.push({
          callId: result.callId,
          tool: "$orphaned",
          arguments: {},
          authz: null,
          result: {
            content: coerceContent(result.content),
            isError: result.isError === true,
          },
          timestamp: new Date().toISOString(),
          sessionId,
          seq: event.seq,
        });
        return;
      }
      decisions.delete(result.callId);

      completed.push({
        callId: result.callId,
        tool: decision.tool,
        arguments: {},
        authz: decisionToAuthz(decision),
        result: {
          content: coerceContent(result.content),
          isError: result.isError === true,
        },
        timestamp: new Date().toISOString(),
        sessionId,
        seq: event.seq,
      });
    }
  }

  function flush(): AuditRecord[] {
    return completed.splice(0);
  }

  function pendingCount(): number {
    return pendingRecords.size + decisions.size;
  }

  return {
    onEvent,
    onDecision,
    flush,
    pending: pendingCount,
  };
}
