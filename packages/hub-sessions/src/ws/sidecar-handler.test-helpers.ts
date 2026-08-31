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
} {
  return {
    sent: [],
    closed: false,
    send(data: string) {
      this.sent.push(data);
    },
    close() {
      this.closed = true;
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
