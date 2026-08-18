// Pins the orchestrator's workflow-probe threading: a
// `workflowProbeExecutor` handed to `createSidecarOrchestrator` must reach
// the hub-link seam so an inbound `workflow.probe.request` is answered by
// the injected executor rather than the link's rejecting placeholder.
//
// The test drives a real `workflow.probe.request` end to end: it stands up
// the hub-side `createSidecarRouter` behind a WS server, constructs the
// orchestrator with a fake executor, lets the sidecar connect, and calls the
// router's `sendProbe`. The fake executor recording the frame (and the probe
// resolving with the fake's inert result) proves the executor was wired
// through the orchestrator into the link.

import { describe, test, expect, afterAll } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import {
  createSidecarRouter,
  type SidecarAuthenticator,
  type SidecarRouter,
  type WsHandle,
} from "@intx/hub-sessions";
import { createInMemoryTransport } from "@intx/mail-memory";
import { generateKeyPair, signEd25519, verifySSHSignature } from "@intx/crypto";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import type { WorkflowProbeRequestFrame } from "@intx/types/sidecar";

import { createSidecarOrchestrator } from "./sidecar-orchestrator";
import type { WorkflowProbeExecutor, WorkflowProbeResult } from "./ws/hub-link";

const acceptAnySidecar: SidecarAuthenticator = async ({ sidecarId }) => ({
  kind: "shared",
  sidecarId,
});

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 2000,
): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, 20));
  }
}

function startTestServer(): {
  server: ReturnType<typeof Bun.serve>;
  router: SidecarRouter;
} {
  const router = createSidecarRouter({
    authenticateSidecar: acceptAnySidecar,
    requestTimeoutMs: 5000,
    probeTimeoutMs: 5000,
  });

  const app = new Hono();
  app.get(
    "/ws",
    upgradeWebSocket((_c) => {
      let handle: WsHandle;
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
          router.handleOpen(handle);
        },
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") {
            router.handleMessage(handle, evt.data);
          }
        },
        onClose(_evt, _ws) {
          router.handleClose(handle);
        },
      };
    }),
  );

  const server = Bun.serve({ fetch: app.fetch, websocket, port: 0 });
  return { server, router };
}

const env = startTestServer();
const tempDirs: string[] = [];

afterAll(async () => {
  await env.server.stop(true);
  for (const dir of tempDirs) {
    await fs.rm(dir, { recursive: true, force: true });
  }
});

// A minimal projection that satisfies the closed `WorkflowProjectionDefinition`
// wire schema: a single `step`-kind step covered by stepOrder.
const PROJECTION = {
  id: "wf-probe",
  triggers: [],
  stepOrder: ["s1"],
  steps: { s1: { kind: "step", id: "s1" } },
};

const CLOSURE: ToolPackageManifest = {
  schemaVersion: "1",
  topLevel: [{ name: "@acme/workflow", version: "1.0.0" }],
  entries: [
    {
      name: "@acme/workflow",
      version: "1.0.0",
      source: {
        kind: "registry",
        registry: "npmjs",
        integrity: "sha512-deadbeef",
      },
    },
  ],
};

describe("createSidecarOrchestrator workflow-probe threading", () => {
  test("an injected workflowProbeExecutor answers an inbound probe", async () => {
    const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), "orch-probe-"));
    tempDirs.push(dataDir);

    const fakeResult: WorkflowProbeResult = {
      projection: PROJECTION,
      grants: ["capability:test/inspect"],
      grantWalkSnapshot: {
        perStep: [
          {
            stepId: "s1",
            grants: ["capability:test/inspect"],
            grantEffects: {},
          },
        ],
        grantRequirements: [],
      },
      wireHash: "hash-from-injected-executor",
    };
    const seenFrames: WorkflowProbeRequestFrame[] = [];
    const workflowProbeExecutor: WorkflowProbeExecutor = {
      async probe(frame) {
        seenFrames.push(frame);
        return fakeResult;
      },
    };

    const sidecarId = "sc-probe-thread";
    const orchestrator = createSidecarOrchestrator({
      hubURL: `ws://localhost:${String(env.server.port)}/ws`,
      sidecarId,
      token: "test-token",
      dataDir,
      transport: createInMemoryTransport(),
      cryptoOps: {
        generateKeyPair,
        signEd25519,
        verifySSHSig: verifySSHSignature,
      },
      createDeployRouter: () => ({
        deploy: () => Promise.resolve({ publicKey: "aa".repeat(32) }),
      }),
      applyWorkflowRunPack: async () => {
        /* no-op: this probe test never drives a workflow-run pack restore */
      },
      workflowProbeExecutor,
    });

    orchestrator.start();
    try {
      await waitFor(() =>
        env.router.getConnectedSidecars().includes(sidecarId),
      );

      const sendProbe = env.router.sendProbe;
      if (sendProbe === undefined) {
        throw new Error("createSidecarRouter did not expose sendProbe");
      }
      const result = await sendProbe({
        source: { kind: "registry", registry: "npmjs" },
        closure: CLOSURE,
        entry: "./workflow.js",
      });

      // The injected executor -- not the rejecting placeholder -- handled the
      // probe: it saw the frame and its inert result flowed back to the hub.
      expect(seenFrames).toHaveLength(1);
      const observed = seenFrames[0];
      expect(observed?.entry).toBe("./workflow.js");
      expect(observed?.source.kind).toBe("registry");
      if (observed?.source.kind === "registry") {
        expect(observed.source.registry).toBe("npmjs");
      }
      expect(result.wireHash).toBe("hash-from-injected-executor");
      expect(result.grants).toEqual(["capability:test/inspect"]);
      // The un-flattened walk snapshot threads back through the seam intact.
      expect(result.grantWalkSnapshot).toEqual(fakeResult.grantWalkSnapshot);
    } finally {
      orchestrator.close();
      await waitFor(
        () => !env.router.getConnectedSidecars().includes(sidecarId),
      );
    }
  });
});
