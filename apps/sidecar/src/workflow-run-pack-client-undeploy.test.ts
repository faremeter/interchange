// Pins the symmetric unregister contract on the multi-step routers and
// the `DeploymentAddressRegistry`. After a deployment is torn down, the
// boot-edge undeploy path must invoke `unregister` on every router and
// the registry so subsequent `signal.deliver` / `drain.deliver` /
// `mail.inbound` frames aimed at the dead deployment address are
// rejected by the router (`tryRoute` returns false) rather than
// dispatched into the orphaned supervisor handler.

import { describe, test, expect } from "bun:test";

import {
  createDeploymentAddressRegistry,
  createMultistepDrainRouter,
  createMultistepMailRouter,
  createMultistepSignalRouter,
} from "./workflow-run-pack-client";

describe("multistep router lifecycle: unregister", () => {
  test("MailRouter.unregister drops the handler so stale frames are not claimed", () => {
    const router = createMultistepMailRouter();
    const delivered: Uint8Array[] = [];
    router.register("dep-A@x.example", (msg) => {
      delivered.push(msg);
    });

    router.unregister("dep-A@x.example");

    const claimed = router.tryRoute("dep-A@x.example", new Uint8Array([1]));
    expect(claimed).toBe(false);
    expect(delivered).toHaveLength(0);
  });

  test("MailRouter.unregister of an unknown address is a no-op", () => {
    const router = createMultistepMailRouter();
    expect(() => {
      router.unregister("never-registered@x.example");
    }).not.toThrow();
  });

  test("SignalRouter.unregister drops the handler so stale frames are not claimed", async () => {
    const router = createMultistepSignalRouter();
    const delivered: string[] = [];
    router.register("dep-A@x.example", async (args) => {
      delivered.push(args.signalId);
    });

    router.unregister("dep-A@x.example");

    const claimed = await router.tryRoute({
      type: "signal.deliver",
      agentAddress: "dep-A@x.example",
      runId: "run-stale",
      signalName: "approve",
      signalId: "sig-stale",
      payload: null,
    });
    expect(claimed).toBe(false);
    expect(delivered).toEqual([]);
  });

  test("DrainRouter.unregister drops the handler so stale frames are not claimed", async () => {
    const router = createMultistepDrainRouter();
    const delivered: number[] = [];
    router.register("dep-A@x.example", async (args) => {
      delivered.push(args.deadlineMs);
    });

    router.unregister("dep-A@x.example");

    const claimed = await router.tryRoute({
      type: "drain.deliver",
      agentAddress: "dep-A@x.example",
      deadlineMs: 999,
    });
    expect(claimed).toBe(false);
    expect(delivered).toEqual([]);
  });

  test("DeploymentAddressRegistry exposes a removal API that breaks the deploymentId mapping", () => {
    const registry = createDeploymentAddressRegistry();
    registry.record("dep-A", "agent-a@x.example");

    expect(registry.resolve("dep-A")).toBe("agent-a@x.example");
    registry.unregister("dep-A");
    expect(registry.resolve("dep-A")).toBeNull();
  });

  test("DeploymentAddressRegistry.unregister of an unknown deploymentId is a no-op", () => {
    const registry = createDeploymentAddressRegistry();
    expect(() => {
      registry.unregister("never-recorded");
    }).not.toThrow();
  });

  test("DeploymentAddressRegistry's public surface includes the removal API", () => {
    const registry = createDeploymentAddressRegistry();
    const keys = Object.keys(registry).sort();
    expect(keys).toEqual(["record", "resolve", "unregister"]);
  });
});
