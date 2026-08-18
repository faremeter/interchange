import { describe, test, expect } from "bun:test";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import {
  createSidecarRouter,
  type SendProbeArgs,
  type SidecarAuthenticator,
  type SidecarRouter,
  type WsHandle,
} from "./sidecar-handler";

// Accept-any authenticator: probe selection is address-independent, so the
// handshake just needs to succeed to land the sidecar in `connections`.
const acceptAnySidecar: SidecarAuthenticator = async ({ sidecarId }) => ({
  kind: "shared",
  sidecarId,
});

function createMockWs(): WsHandle & { sent: string[]; closed: boolean } {
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

// Let the async register key-existence gate settle before reading routing.
async function tick(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

// `sendProbe` is optional on `SidecarRouter` (a capability not every consumer
// implements); the concrete router always provides it. Capture it behind a
// guard so the tests call a definite function without a non-null assertion.
function sendProbe(router: SidecarRouter, args: SendProbeArgs) {
  const fn = router.sendProbe;
  if (fn === undefined) {
    throw new Error("createSidecarRouter did not expose sendProbe");
  }
  return fn(args);
}

// Register a token-authed sidecar with ZERO addresses -- exactly the
// pre-deploy state a probe selects. Returns the mock socket so the test can
// read the outbound probe frame and drive replies back through it.
async function registerBareSidecar(
  router: SidecarRouter,
  sidecarId: string,
): Promise<ReturnType<typeof createMockWs>> {
  const ws = createMockWs();
  router.handleOpen(ws);
  router.handleMessage(
    ws,
    JSON.stringify({
      type: "register",
      sidecarId,
      token: "tok",
      agentAddresses: [],
    }),
  );
  await tick();
  return ws;
}

// Pull the correlation id off the outbound `workflow.probe.request` the router
// sent to the fake sidecar, so a reply can be minted against it.
function probeRequestId(ws: ReturnType<typeof createMockWs>): string {
  for (const raw of ws.sent) {
    const frame: unknown = JSON.parse(raw);
    if (
      typeof frame === "object" &&
      frame !== null &&
      "type" in frame &&
      frame.type === "workflow.probe.request" &&
      "requestId" in frame &&
      typeof frame.requestId === "string"
    ) {
      return frame.requestId;
    }
  }
  throw new Error("router sent no workflow.probe.request frame");
}

const source: WorkflowDefinitionSource = {
  kind: "registry",
  registry: "npmjs",
};

const closure: ToolPackageManifest = {
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

const probeArgs: SendProbeArgs = {
  source,
  closure,
  entry: "./workflow.js",
};

// A minimal projection that satisfies the closed `WorkflowProjectionDefinition`
// wire schema: a single `step`-kind step covered by stepOrder.
const projection = {
  id: "wf-probe",
  triggers: [],
  stepOrder: ["s1"],
  steps: { s1: { kind: "step", id: "s1" } },
};

describe("SidecarRouter workflow probe", () => {
  test("happy path resolves with the sidecar's inert probe result", async () => {
    const router = createSidecarRouter({
      authenticateSidecar: acceptAnySidecar,
    });
    const ws = await registerBareSidecar(router, "sc-1");

    const promise = sendProbe(router, probeArgs);
    const requestId = probeRequestId(ws);

    const grants = ["tool:send_mail", "mail.address:wf@local"];
    const wireHash = "abc123";
    // The un-flattened walk snapshot rides the frame alongside the flattened
    // `grants`, carrying the per-step `grantEffects` map (a `tool:` grant gated
    // behind approval) and the definition's `grantRequirements` that the
    // flattened union discards.
    const grantWalkSnapshot = {
      perStep: [
        {
          stepId: "s1",
          grants: ["tool:send_mail", "mail.address:wf@local"],
          grantEffects: { "tool:send_mail": "ask" as const },
        },
      ],
      grantRequirements: [
        { resource: "mailbox", action: "send", source: "creator" as const },
      ],
    };
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "workflow.probe.result",
        requestId,
        projection,
        grants,
        grantWalkSnapshot,
        wireHash,
      }),
    );

    await expect(promise).resolves.toEqual({
      projection,
      grants,
      grantWalkSnapshot,
      wireHash,
    });
  });

  test("a workflow.probe.error reply rejects the probe with its error", async () => {
    const router = createSidecarRouter({
      authenticateSidecar: acceptAnySidecar,
    });
    const ws = await registerBareSidecar(router, "sc-1");

    const promise = sendProbe(router, probeArgs);
    const requestId = probeRequestId(ws);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "workflow.probe.error",
        requestId,
        error: "evaluation threw",
      }),
    );

    await expect(promise).rejects.toThrow("evaluation threw");
  });

  test("no reply rejects the probe after probeTimeoutMs", async () => {
    const router = createSidecarRouter({
      authenticateSidecar: acceptAnySidecar,
      probeTimeoutMs: 20,
    });
    await registerBareSidecar(router, "sc-1");

    const promise = sendProbe(router, probeArgs);

    await expect(promise).rejects.toThrow(/timed out after 20ms/);
  });

  test("disconnect sweeps the in-flight probe and rejects it", async () => {
    const router = createSidecarRouter({
      authenticateSidecar: acceptAnySidecar,
    });
    const ws = await registerBareSidecar(router, "sc-1");

    const promise = sendProbe(router, probeArgs);
    // The probe never enters the address maps, so handleClose's ws-keyed sweep
    // is its only disconnect cleanup.
    router.handleClose(ws);

    await expect(promise).rejects.toThrow(/sc-1 disconnected/);
  });

  test("an empty connection registry throws immediately", async () => {
    const router = createSidecarRouter({
      authenticateSidecar: acceptAnySidecar,
    });

    // Never registers a probe that could only time out: the throw is
    // synchronous-immediate (a rejected promise), not deferred to a timeout.
    await expect(sendProbe(router, probeArgs)).rejects.toThrow(
      "No sidecar available to probe workflow",
    );
  });
});
