import { afterAll, describe, expect, test } from "bun:test";
import { type } from "arktype";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";

import {
  createSidecarRouter,
  type SidecarAuthIdentity,
  type WsHandle,
} from "@intx/hub-sessions";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { HarnessConfig } from "@intx/types/runtime";
import type { HostedIncarnation } from "@intx/types/sidecar";
import { waitUntil } from "@intx/types/testing";

import type { AgentKeyStore } from "../agent-key-store";
import type { SessionManager } from "../session-manager";
import { createHubLink, type DeployRouter } from "./hub-link";
import { resolveInboundMailPolicy } from "./inbound-signature";

// A deploy is still running on the sidecar when its link drops, and the Hub
// replaces or releases the allocation before the sidecar reconnects. The
// sidecar outlives that generation because it hosts another allocation.

type Allocated = Extract<SidecarAuthIdentity, { kind: "allocated" }>;

const ADDRESS = "run_across@tenant.example";

function binding(
  sidecarId: string,
  allocationId: string,
  generation: number,
  workflowRunAddress: string,
): Allocated {
  return {
    kind: "allocated",
    sidecarId,
    allocationId,
    tenantId: "tenant-1",
    anchorRunId: `anchor-${allocationId}`,
    workflowRunAddress,
    generation,
  };
}

function config(agentAddress: string): HarnessConfig {
  return {
    sessionId: "ses-across",
    agentId: "workflow",
    tenantId: "tenant-1",
    principalId: "principal-1",
    agentAddress,
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
}

const hosted = new Map<string, Allocated[]>();

const router = createSidecarRouter({
  withExecutableWorkflowRun: async (_target, send) => send(),
  authenticateSidecar: async ({ sidecarId }) =>
    hosted.has(sidecarId) ? { sidecarId } : null,
  resolveSidecarBindings: async (sidecarId) => hosted.get(sidecarId) ?? [],
  // No generation here finished initializing, so a reconnect takes back no
  // reported route and the Hub undeploys what the sidecar reports.
  validateSidecarIdentity: async (_identity, use) => use !== "reclaim",
  hubPublicKey: "a".repeat(64),
  requestTimeoutMs: 10_000,
});

const Ping = type({ type: "'ping'" });

// The link drops a connection whose pings go unanswered, as it would one to a
// Hub it can no longer reach. Silencing the current socket drops it from the
// sidecar's side, the way a network failure would.
let silenceCurrent: (() => void) | undefined;
const app = new Hono();
app.get(
  "/ws",
  upgradeWebSocket(() => {
    let handle: WsHandle;
    let silenced = false;
    return {
      onOpen(_evt, ws) {
        handle = {
          send(data: string) {
            ws.send(data);
          },
          close() {
            ws.close();
          },
        };
        silenceCurrent = () => {
          silenced = true;
        };
        router.handleOpen(handle);
      },
      onMessage(evt) {
        if (typeof evt.data !== "string") return;
        if (silenced && !(Ping(JSON.parse(evt.data)) instanceof type.errors))
          return;
        router.handleMessage(handle, evt.data);
      },
      onClose() {
        router.handleClose(handle);
      },
    };
  }),
);
const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });

afterAll(async () => {
  await server.stop(true);
});

function keyStore(): AgentKeyStore {
  return {
    async loadOrGenerateKey() {
      throw new Error("unused");
    },
    recordHubKey() {
      // These deployments verify no deploy commits.
    },
    async verifyDeployCommit() {
      return false;
    },
    forgetAgent() {
      // No key was ever cached for an address.
    },
  };
}

function sessions(): SessionManager {
  return {
    initRepo: () => Promise.resolve(),
    applyDeployPack: () => Promise.resolve(),
    applyAssetPack: () => Promise.resolve(),
    deleteAgentDir: () => Promise.resolve(),
    getSessionId: () => undefined,
  };
}

// The first deploy the sidecar receives runs until released; each deploy
// answers with a key naming its turn, so a reply cannot pass for another's.
// The router reports what it holds the way the sidecar's does: an
// incarnation is deploying from the moment its deploy starts.
function sidecarDeployments() {
  const events: string[] = [];
  const held = new Map<string, HostedIncarnation>();
  const started = Promise.withResolvers<boolean>();
  const release = Promise.withResolvers<boolean>();
  const undeployed = Promise.withResolvers<boolean>();
  let deploys = 0;
  const deployRouter: DeployRouter = {
    async deploy(frame) {
      deploys += 1;
      const turn = deploys;
      events.push(`deploy ${String(turn)}`);
      const incarnation = {
        address: frame.agentAddress,
        generation: frame.generation,
      };
      held.set(frame.agentAddress, { ...incarnation, state: "deploying" });
      if (turn === 1) {
        started.resolve(true);
        await release.promise;
      }
      events.push(`deployed ${String(turn)}`);
      held.set(frame.agentAddress, { ...incarnation, state: "live" });
      return { publicKey: String(turn).repeat(64) };
    },
    async undeploy(frame) {
      events.push(`undeploy ${String(frame.generation)}`);
      held.delete(frame.agentAddress);
      undeployed.resolve(true);
    },
  };
  return {
    events,
    deployRouter,
    incarnations: () => [...held.values()],
    started: started.promise,
    release: () => release.resolve(true),
    undeployed: undeployed.promise,
  };
}

function connectLink(
  sidecarId: string,
  sidecar: ReturnType<typeof sidecarDeployments>,
) {
  const routable: string[] = [];
  let reconnect: (() => void) | undefined;
  const link = createHubLink({
    hubURL: `ws://localhost:${String(server.port)}/ws`,
    sidecarId,
    token: "token",
    transport: createInMemoryTransport(),
    sessions: sessions(),
    keyStore: keyStore(),
    resolveSenderCrypto: () => undefined,
    lookupInboundMailPolicy: () => resolveInboundMailPolicy(undefined),
    cacheSenderKey: async () => undefined,
    evictSenderKey: async () => undefined,
    deployRouter: sidecar.deployRouter,
    getIncarnations: sidecar.incarnations,
    pingIntervalMs: 50,
    scheduleReconnect: (callback) => {
      reconnect = callback;
      return () => {
        reconnect = undefined;
      };
    },
    onWorkflowAddressesRoutable: (addresses) => routable.push(...addresses),
  });
  link.connect();

  // Drops the link and holds it down until the returned resume is called, so
  // the Hub can change what the sidecar hosts while it is away.
  async function drop(): Promise<() => void> {
    silenceCurrent?.();
    await waitUntil(
      () =>
        reconnect !== undefined &&
        !router.getConnectedSidecars().includes(sidecarId),
    );
    return () => {
      const resume = reconnect;
      reconnect = undefined;
      resume?.();
    };
  }

  return { link, routable, drop };
}

describe("a deploy still running when the sidecar's link drops", () => {
  test("is undeployed on the sidecar before the newer generation it lets attach deploys", async () => {
    const sidecarId = "sc-across-replaced";
    const neighbour = binding(
      sidecarId,
      "neighbour-r",
      1,
      "run_n@tenant.example",
    );
    hosted.set(sidecarId, [
      binding(sidecarId, "replaced", 1, ADDRESS),
      neighbour,
    ]);
    router.fenceAllocation("replaced", 1);
    router.fenceAllocation(neighbour.allocationId, 1);
    const sidecar = sidecarDeployments();
    const { link, routable, drop } = connectLink(sidecarId, sidecar);
    try {
      await router.waitForAllocatedSidecar(
        { allocationId: neighbour.allocationId, generation: 1 },
        5_000,
      );
      const generationOne = router
        .sendAgentDeployToAllocation(
          { allocationId: "replaced", generation: 1 },
          ADDRESS,
          config(ADDRESS),
        )
        .then(
          () => "deployed",
          (error: unknown) => error,
        );
      await sidecar.started;

      const resume = await drop();
      expect(await generationOne).toBeInstanceOf(Error);
      hosted.set(sidecarId, [
        binding(sidecarId, "replaced", 2, ADDRESS),
        neighbour,
      ]);
      router.fenceAllocation("replaced", 2);
      resume();

      // The hello reports generation 1 still deploying. The Hub undeploys it
      // and attaches generation 2 at once: whatever generation 1 still sends
      // names its generation and is never credited to generation 2.
      await router.waitForAllocatedSidecar(
        { allocationId: "replaced", generation: 2 },
        5_000,
      );
      const generationTwo = router.sendAgentDeployToAllocation(
        { allocationId: "replaced", generation: 2 },
        ADDRESS,
        config(ADDRESS),
      );

      // On the sidecar the undeploy and the newer deploy wait behind the
      // deploy still running for the address.
      sidecar.release();
      expect((await generationTwo).publicKey).toBe("2".repeat(64));
      expect(sidecar.events).toEqual([
        "deploy 1",
        "deployed 1",
        "undeploy 1",
        "deploy 2",
        "deployed 2",
      ]);
      expect(routable).toEqual([]);
    } finally {
      link.close();
    }
  });

  test("is undeployed once the sidecar reconnects after its allocation was released", async () => {
    const sidecarId = "sc-across-released";
    const neighbour = binding(
      sidecarId,
      "neighbour-x",
      1,
      "run_x@tenant.example",
    );
    hosted.set(sidecarId, [
      binding(sidecarId, "released", 1, ADDRESS),
      neighbour,
    ]);
    router.fenceAllocation("released", 1);
    router.fenceAllocation(neighbour.allocationId, 1);
    const sidecar = sidecarDeployments();
    const { link, routable, drop } = connectLink(sidecarId, sidecar);
    try {
      await router.waitForAllocatedSidecar(
        { allocationId: neighbour.allocationId, generation: 1 },
        5_000,
      );
      const deployed = router
        .sendAgentDeployToAllocation(
          { allocationId: "released", generation: 1 },
          ADDRESS,
          config(ADDRESS),
        )
        .then(
          () => "deployed",
          (error: unknown) => error,
        );
      await sidecar.started;

      const resume = await drop();
      expect(await deployed).toBeInstanceOf(Error);
      hosted.set(sidecarId, [neighbour]);
      router.retireAllocation({ allocationId: "released", generation: 1 });
      resume();

      await router.waitForAllocatedSidecar(
        { allocationId: neighbour.allocationId, generation: 1 },
        5_000,
      );
      sidecar.release();
      await sidecar.undeployed;
      expect(sidecar.events).toEqual(["deploy 1", "deployed 1", "undeploy 1"]);
      expect(routable).toEqual([]);
    } finally {
      link.close();
    }
  });
});
