import type { HarnessConfig } from "@intx/types/runtime";

import {
  createSidecarRouter,
  type AllocatedSidecarTarget,
  type SidecarAuthIdentity,
  type SidecarRouterConfig,
  type WsHandle,
} from "./sidecar-handler";

export const TEST_IDENTITY: Extract<
  SidecarAuthIdentity,
  { kind: "allocated" }
> = {
  kind: "allocated",
  sidecarId: "sc-allocated",
  allocationId: "alloc-1",
  tenantId: "tenant-1",
  anchorRunId: "run_anchor",
  workflowRunAddress: "run_anchor@tenant.example",
  generation: 1,
};

export const TEST_TARGET: AllocatedSidecarTarget = {
  allocationId: TEST_IDENTITY.allocationId,
  generation: TEST_IDENTITY.generation,
};

export const TEST_CONFIG: HarnessConfig = {
  sessionId: "ses-router-test",
  agentId: "workflow",
  tenantId: TEST_IDENTITY.tenantId,
  principalId: "principal-1",
  agentAddress: TEST_IDENTITY.workflowRunAddress,
  systemPrompt: "test",
  tools: [],
  grants: [],
  sources: [
    {
      id: "test-source",
      provider: "test",
      baseURL: "https://api.example.test",
      credentialId: "test-credential",
      model: "test",
    },
  ],
  defaultSource: "test-source",
};

export function createMockWs(): WsHandle & {
  sent: string[];
  closed: boolean;
  /**
   * Resolve once `predicate` holds over the frames sent so far, re-checking
   * on each send.
   *
   * Several of the router's paths write to the socket from a fire-and-forget
   * continuation -- the mail redelivery retry is one -- so a test that
   * triggers one has no return value to await. The send is the event.
   */
  awaitSent(predicate: (sent: readonly string[]) => boolean): Promise<void>;
} {
  let waiters: (() => void)[] = [];
  return {
    sent: [],
    closed: false,
    send(data: string) {
      this.sent.push(data);
      const waking = waiters;
      waiters = [];
      for (const wake of waking) wake();
    },
    close() {
      this.closed = true;
    },
    async awaitSent(predicate) {
      for (;;) {
        // Re-checked on every pass, so a frame sent before this call resolves
        // it rather than leaving it waiting for another send.
        const sent = new Promise<void>((resolve) => {
          waiters.push(resolve);
        });
        if (predicate(this.sent)) return;
        await sent;
      }
    },
  };
}

export function createAllocatedRouter(
  config: Partial<SidecarRouterConfig> = {},
) {
  const router = createSidecarRouter({
    authenticateSidecar: async () => TEST_IDENTITY,
    validateSidecarIdentity: async () => true,
    hubPublicKey: "a".repeat(64),
    requestTimeoutMs: 500,
    ...config,
  });
  router.fenceAllocation(TEST_TARGET.allocationId, TEST_TARGET.generation);
  return router;
}

export async function connectAllocated(
  router: ReturnType<typeof createSidecarRouter>,
  agentAddresses: string[] = [],
  frameType: "register" | "reconnect" = "register",
) {
  const ws = createMockWs();
  router.handleOpen(ws);
  router.handleMessage(
    ws,
    JSON.stringify({
      type: frameType,
      sidecarId: TEST_IDENTITY.sidecarId,
      token: "token",
      agentAddresses,
    }),
  );
  await tick();
  return ws;
}

export function parsedFrames(ws: { sent: string[] }): unknown[] {
  return ws.sent.map((raw) => {
    const parsed: unknown = JSON.parse(raw);
    return parsed;
  });
}

export function tick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

/**
 * The redelivery retry timer, driven by the test.
 *
 * The retry interval was already injectable, but arming was not, so a test
 * wanting N redeliveries had to shorten the interval and sleep long enough
 * for N of them to fit -- making the assertion a bet on how much the machine
 * got through. Firing the retries explicitly makes the count exact.
 */
export function createManualRetries(retryIntervalMs: number): {
  scheduleTimeout: (handler: () => void, ms: number) => () => void;
  fireNext: () => void;
  armedCount: () => number;
} {
  const armed: { ms: number; fire: () => void; cancelled: boolean }[] = [];
  return {
    // The router arms its connection-liveness deadline through this same
    // seam, so the delay is what tells the two apart. Firing indiscriminately
    // closes the socket instead of redelivering.
    scheduleTimeout(handler, ms) {
      const entry = { ms, fire: handler, cancelled: false };
      armed.push(entry);
      return () => {
        entry.cancelled = true;
      };
    },
    fireNext() {
      const next = armed.find((e) => !e.cancelled && e.ms === retryIntervalMs);
      if (next === undefined) {
        throw new Error("no armed redelivery retry to fire");
      }
      next.cancelled = true;
      next.fire();
    },
    armedCount: () =>
      armed.filter((e) => !e.cancelled && e.ms === retryIntervalMs).length,
  };
}
