// Gate management for the agent reactor.
//
// Gates block the reactor until an external condition resolves. Each gate has
// a type, an ID, and a mandatory timeout. The gate manager owns all active
// gates and exposes methods to register, clear, and time out gates.
//
// (INFERENCE.md § Gates, Gate Timeouts, Gate Behavior During Suspension)

import type { GateType } from "@intx/types/runtime";

import { createDefaultScheduler, type Scheduler } from "./harness";

export type GateRecord = {
  gateId: string;
  type: GateType;
  timeoutAt: number;
  correlationId: string | undefined;
  /** The turn that holds this gate. */
  turnId: string;
  resolve: (reason: "resolved" | "timeout" | "shutdown") => void;
  onCleared: GateClearedHandler;
  /** Cancels the gate's timeout timer. */
  cancelTimer: () => void;
};

// The manager deletes a gate before it calls this handler, so the handler
// receives the gate's turn id instead of looking the gate up.
export type GateClearedHandler = (
  gateId: string,
  reason: "resolved" | "timeout" | "shutdown",
  turnId: string,
) => void;

export type GateSnapshot = {
  gateId: string;
  type: GateType;
  timeoutAt: number;
};

/**
 * Manages active gates. All gates must have a positive timeout. The timeout
 * timers run on `scheduler`; the reactor passes its injected one, so a test
 * can fire a gate's timeout when it chooses.
 */
export function createGateManager(
  scheduler: Scheduler = createDefaultScheduler(),
) {
  const gates = new Map<string, GateRecord>();

  function register(
    gateId: string,
    type: GateType,
    timeoutMs: number,
    correlationId: string | undefined,
    onCleared: GateClearedHandler,
    turnId: string,
  ): Promise<"resolved" | "timeout" | "shutdown"> {
    if (timeoutMs <= 0) {
      throw new Error(
        `Gate "${gateId}" must have a positive timeout (got ${timeoutMs})`,
      );
    }

    if (gates.has(gateId)) {
      throw new Error(`Gate "${gateId}" is already registered`);
    }

    const timeoutAt = Date.now() + timeoutMs;
    let resolveGate!: (reason: "resolved" | "timeout" | "shutdown") => void;

    const promise = new Promise<"resolved" | "timeout" | "shutdown">(
      (resolve) => {
        resolveGate = resolve;
      },
    );

    const cancelTimer = scheduler.setTimeout(() => {
      if (gates.has(gateId)) {
        gates.delete(gateId);
        resolveGate("timeout");
        onCleared(gateId, "timeout", turnId);
      }
    }, timeoutMs);

    gates.set(gateId, {
      gateId,
      type,
      timeoutAt,
      correlationId,
      turnId,
      resolve: resolveGate,
      onCleared,
      cancelTimer,
    });

    return promise;
  }

  function clear(gateId: string): boolean {
    const gate = gates.get(gateId);
    if (gate === undefined) return false;
    gate.cancelTimer();
    gates.delete(gateId);
    gate.resolve("resolved");
    gate.onCleared(gateId, "resolved", gate.turnId);
    return true;
  }

  // Clear a gate without invoking its onCleared callback. The caller has
  // already decided how the reactor resumes and does not want the standard
  // cleared-event enqueue that onCleared drives. Used by the approval
  // re-dispatch path, which resumes by re-running the parked tool call rather
  // than by re-inferring off a gate-cleared event: firing onCleared there
  // would enqueue a second, spurious continuation.
  function clearSilently(gateId: string): boolean {
    const gate = gates.get(gateId);
    if (gate === undefined) return false;
    gate.cancelTimer();
    gates.delete(gateId);
    gate.resolve("resolved");
    return true;
  }

  function shutdown(): void {
    const entries = Array.from(gates.values());
    gates.clear();
    for (const gate of entries) {
      gate.cancelTimer();
      gate.resolve("shutdown");
      gate.onCleared(gate.gateId, "shutdown", gate.turnId);
    }
  }

  function findByCorrelationId(correlationId: string): GateRecord | undefined {
    for (const gate of gates.values()) {
      if (gate.correlationId === correlationId) return gate;
    }
    return undefined;
  }

  function snapshot(): GateSnapshot[] {
    return Array.from(gates.values()).map((g) => ({
      gateId: g.gateId,
      type: g.type,
      timeoutAt: g.timeoutAt,
    }));
  }

  function has(gateId: string): boolean {
    return gates.has(gateId);
  }

  // A gate counts from its registration. The reactor registers a suspend's
  // gate before it commits the suspend, so the gate counts during the commit.
  function hasTurn(turnId: string): boolean {
    for (const gate of gates.values()) {
      if (gate.turnId === turnId) return true;
    }
    return false;
  }

  return {
    register,
    clear,
    clearSilently,
    shutdown,
    findByCorrelationId,
    snapshot,
    has,
    hasTurn,
  };
}

export type GateManager = ReturnType<typeof createGateManager>;
