import { describe, test, expect } from "bun:test";
import { createSidecarEmitter } from "./sidecar-events";

describe("createSidecarEmitter", () => {
  test("delivers events to subscribed listeners", () => {
    const emitter = createSidecarEmitter();
    const seen: { addr: string; sid: string }[] = [];
    emitter.on("agent.event", ({ agentAddress, sessionId }) => {
      seen.push({ addr: agentAddress, sid: sessionId });
    });

    emitter.emit("agent.event", {
      agentAddress: "a@x",
      sessionId: "s-1",
      event: { type: "x" },
    });

    expect(seen).toEqual([{ addr: "a@x", sid: "s-1" }]);
  });

  test("supports multiple listeners on the same event", () => {
    const emitter = createSidecarEmitter();
    const order: string[] = [];
    emitter.on("sidecar.disconnect", () => {
      order.push("a");
    });
    emitter.on("sidecar.disconnect", () => {
      order.push("b");
    });

    emitter.emit("sidecar.disconnect", { ownedAddresses: [], allocated: [] });

    expect(order).toEqual(["a", "b"]);
  });

  test("unsubscribe removes the listener", () => {
    const emitter = createSidecarEmitter();
    let count = 0;
    const unsubscribe = emitter.on("sidecar.disconnect", () => {
      count++;
    });

    emitter.emit("sidecar.disconnect", { ownedAddresses: [], allocated: [] });
    unsubscribe();
    emitter.emit("sidecar.disconnect", { ownedAddresses: [], allocated: [] });

    expect(count).toBe(1);
  });

  test("emit swallows listener errors and continues", () => {
    const emitter = createSidecarEmitter();
    const seen: string[] = [];
    emitter.on("sidecar.disconnect", () => {
      throw new Error("boom");
    });
    emitter.on("sidecar.disconnect", () => {
      seen.push("ran");
    });

    emitter.emit("sidecar.disconnect", { ownedAddresses: [], allocated: [] });

    expect(seen).toEqual(["ran"]);
  });
});
