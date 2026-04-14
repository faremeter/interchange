// Gate management for the agent reactor.
//
// Gates block the reactor until an external condition resolves. Each gate has
// a type, an ID, and a mandatory timeout. The gate manager owns all active
// gates and exposes methods to register, clear, and time out gates.
//
// (INFERENCE.md § Gates, Gate Timeouts, Gate Behavior During Suspension)

import type { GateType } from "@interchange/types/runtime";

export type GateRecord = {
  gateId: string;
  type: GateType;
  timeoutAt: number;
  correlationId: string | undefined;
  resolve: (reason: "resolved" | "timeout" | "shutdown") => void;
  onCleared: (
    gateId: string,
    reason: "resolved" | "timeout" | "shutdown",
  ) => void;
  timer: ReturnType<typeof setTimeout>;
};

export type GateSnapshot = {
  gateId: string;
  type: GateType;
  timeoutAt: number;
};

/**
 * Manages active gates. All gates must have a positive timeout.
 */
export function createGateManager() {
  const gates = new Map<string, GateRecord>();

  function register(
    gateId: string,
    type: GateType,
    timeoutMs: number,
    correlationId: string | undefined,
    onCleared: (
      gateId: string,
      reason: "resolved" | "timeout" | "shutdown",
    ) => void,
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

    const timer = setTimeout(() => {
      if (gates.has(gateId)) {
        gates.delete(gateId);
        resolveGate("timeout");
        onCleared(gateId, "timeout");
      }
    }, timeoutMs);

    gates.set(gateId, {
      gateId,
      type,
      timeoutAt,
      correlationId,
      resolve: resolveGate,
      onCleared,
      timer,
    });

    return promise;
  }

  function clear(gateId: string): boolean {
    const gate = gates.get(gateId);
    if (gate === undefined) return false;
    clearTimeout(gate.timer);
    gates.delete(gateId);
    gate.resolve("resolved");
    gate.onCleared(gateId, "resolved");
    return true;
  }

  function shutdown(): void {
    const entries = Array.from(gates.values());
    gates.clear();
    for (const gate of entries) {
      clearTimeout(gate.timer);
      gate.resolve("shutdown");
      gate.onCleared(gate.gateId, "shutdown");
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

  return { register, clear, shutdown, findByCorrelationId, snapshot, has };
}

export type GateManager = ReturnType<typeof createGateManager>;
