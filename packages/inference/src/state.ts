// Reactor state management: turn history, async operations, usage tracking.
//
// The state object is the authoritative view the plugin receives on every
// decision. It is mutable by the reactor only — the plugin receives a
// snapshot so it cannot corrupt the reactor's internal state.
//
// (INFERENCE.md § Agent Reactor › Plugin Decision Function)

import type {
  ConversationTurn,
  PendingOperation,
  TokenUsage,
  ReactorState,
} from "@interchange/types/runtime";
import type { GateSnapshot } from "./gates";

export type ReactorStateManager = ReturnType<typeof createStateManager>;

/**
 * Creates a mutable state container. All mutations go through explicit methods;
 * the `snapshot()` method produces an immutable view for the plugin.
 */
export function createStateManager(
  sessionId: string,
  initialTurns: ConversationTurn[],
  initialOps: PendingOperation[],
  initialUsage: TokenUsage,
) {
  const turns: ConversationTurn[] = [...initialTurns];
  const pendingOperations = new Map<string, PendingOperation>(
    initialOps.map((op) => [op.correlationId, op]),
  );
  const tokenUsage: TokenUsage = { ...initialUsage };
  let activeGatesSnapshot: GateSnapshot[] = [];
  const activeForks: { forkId: string; mode: "independent" | "child" }[] = [];

  function appendTurn(msg: ConversationTurn): void {
    turns.push(msg);
  }

  function addPendingOperation(op: PendingOperation): void {
    pendingOperations.set(op.correlationId, op);
  }

  function removePendingOperation(correlationId: string): void {
    pendingOperations.delete(correlationId);
  }

  function accumUsage(usage: TokenUsage): void {
    tokenUsage.input += usage.input;
    tokenUsage.output += usage.output;
    tokenUsage.cacheRead += usage.cacheRead;
    tokenUsage.cacheWrite += usage.cacheWrite;
    tokenUsage.thinking += usage.thinking;
  }

  function setGatesSnapshot(gates: GateSnapshot[]): void {
    activeGatesSnapshot = gates;
  }

  function addFork(forkId: string, mode: "independent" | "child"): void {
    activeForks.push({ forkId, mode });
  }

  function removeFork(forkId: string): void {
    const idx = activeForks.findIndex((f) => f.forkId === forkId);
    if (idx !== -1) activeForks.splice(idx, 1);
  }

  function getTurns(): ConversationTurn[] {
    return turns;
  }

  function getPendingOperations(): PendingOperation[] {
    return Array.from(pendingOperations.values());
  }

  function getTokenUsage(): TokenUsage {
    return { ...tokenUsage };
  }

  function snapshot(): ReactorState {
    return {
      sessionId,
      turns: turns.map((m) => ({
        ...m,
        content: m.content.map((b) => structuredClone(b)),
      })),
      pendingOperations: Array.from(pendingOperations.values()).map((op) =>
        structuredClone(op),
      ),
      activeGates: activeGatesSnapshot.map((g) => ({
        gateId: g.gateId,
        type: g.type,
        timeoutAt: g.timeoutAt,
      })),
      activeForks: activeForks.map((f) => ({ ...f })),
      tokenUsage: { ...tokenUsage },
    };
  }

  return {
    appendTurn,
    addPendingOperation,
    removePendingOperation,
    accumUsage,
    setGatesSnapshot,
    addFork,
    removeFork,
    getTurns,
    getPendingOperations,
    getTokenUsage,
    snapshot,
  };
}
