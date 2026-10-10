import type { HarnessConfig } from "@intx/types/runtime";
import type { HostedIncarnation } from "@intx/types/sidecar";

import type { SidecarCredentials } from "../sidecar-allocation/contracts";

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

export const TEST_CREDENTIALS: SidecarCredentials = {
  sidecarId: TEST_IDENTITY.sidecarId,
};

/**
 * Router authentication that verifies each claimed sidecar id as hosting the
 * bindings `lookup` returns for it. Like the production resolver, it accepts
 * a sidecar that hosts nothing, and the router turns that sidecar away.
 */
export function sidecarAuth(
  lookup: (sidecarId: string) => readonly SidecarAuthIdentity[],
): Pick<SidecarRouterConfig, "authenticateSidecar" | "resolveSidecarBindings"> {
  return {
    authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
    resolveSidecarBindings: async (sidecarId) => lookup(sidecarId),
  };
}

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

/** The ref tips a stopped test worker reports, which the test Hub holds. */
export const TEST_REF_TIPS = {
  "refs/heads/main": "c".repeat(40),
  "refs/heads/events": null,
};

export function createAllocatedRouter(
  config: Partial<SidecarRouterConfig> = {},
) {
  const router = createSidecarRouter({
    withExecutableWorkflowRun: async (_target, send) => send(),
    authenticateSidecar: async () => TEST_CREDENTIALS,
    validateSidecarIdentity: async () => true,
    resolveSidecarBindings: async () => [TEST_IDENTITY],
    hubPublicKey: "a".repeat(64),
    requestTimeoutMs: 500,
    ...config,
    lookups: {
      readWorkflowRunRefTips: async () => TEST_REF_TIPS,
      ...config.lookups,
    },
  });
  router.fenceAllocation(TEST_TARGET.allocationId, TEST_TARGET.generation);
  return router;
}

/** Live incarnations of `addresses` at `generation`, as a hello reports them. */
export function liveIncarnations(
  addresses: readonly string[],
  generation = TEST_IDENTITY.generation,
): HostedIncarnation[] {
  return addresses.map((address) => ({ address, generation, state: "live" }));
}

/** The `hello` a sidecar sends first, reporting what it holds. */
export function helloFrame(
  sidecarId: string,
  incarnations: readonly HostedIncarnation[] = [],
  token = "token",
): string {
  return JSON.stringify({ type: "hello", sidecarId, token, incarnations });
}

/**
 * Connect the test sidecar, reporting a live incarnation of each address at
 * the test allocation's generation.
 */
export async function connectAllocated(
  router: ReturnType<typeof createSidecarRouter>,
  agentAddresses: string[] = [],
) {
  const ws = createMockWs();
  router.handleOpen(ws);
  router.handleMessage(
    ws,
    helloFrame(TEST_IDENTITY.sidecarId, liveIncarnations(agentAddresses)),
  );
  await tick();
  return ws;
}

type SentLifecycleRequest = {
  type: string;
  requestId: string;
  agentAddress: string;
  generation: number;
};

/** The last `agent.deploy` or `agent.undeploy` the router sent on `ws`. */
export function lastRequest(
  ws: { sent: string[] },
  type: "agent.deploy" | "agent.undeploy",
  agentAddress?: string,
): SentLifecycleRequest {
  const found = ws.sent
    .map((raw): SentLifecycleRequest => JSON.parse(raw))
    .filter(
      (frame) =>
        frame.type === type &&
        (agentAddress === undefined || frame.agentAddress === agentAddress),
    )
    .at(-1);
  if (found === undefined) {
    throw new Error(
      `No ${type} was sent${agentAddress === undefined ? "" : ` for ${agentAddress}`}`,
    );
  }
  return found;
}

/**
 * The sidecar's answer to the last `agent.deploy` sent on `ws`, naming its
 * request id and incarnation.
 */
export function deployReply(
  ws: { sent: string[] },
  answer: { publicKey: string } | { error: string },
  agentAddress?: string,
): string {
  const {
    requestId,
    agentAddress: address,
    generation,
  } = lastRequest(ws, "agent.deploy", agentAddress);
  return JSON.stringify(
    "publicKey" in answer
      ? {
          type: "agent.deploy.ack",
          requestId,
          agentAddress: address,
          generation,
          publicKey: answer.publicKey,
        }
      : {
          type: "agent.deploy.error",
          requestId,
          agentAddress: address,
          generation,
          error: { code: "deployment_failed", message: answer.error },
        },
  );
}

/** The sidecar's acknowledgement of the last `agent.undeploy` sent on `ws`. */
export function undeployAck(
  ws: { sent: string[] },
  agentAddress?: string,
): string {
  const {
    requestId,
    agentAddress: address,
    generation,
  } = lastRequest(ws, "agent.undeploy", agentAddress);
  return JSON.stringify({
    type: "agent.undeploy.ack",
    requestId,
    agentAddress: address,
    generation,
  });
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
