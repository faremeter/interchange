import { describe, expect, test } from "bun:test";

import { createTerminalChildHandle } from "./suspendable-child-handle";

describe("createTerminalChildHandle", () => {
  test("next returns the same terminal on every call", async () => {
    const handle = createTerminalChildHandle("failed");
    const first = await handle.next();
    const second = await handle.next();
    expect(first).toEqual({ kind: "terminal", terminalStatus: "failed" });
    expect(second).toBe(first);
  });

  test("resume and deliverSignal reject because the child has settled", async () => {
    const handle = createTerminalChildHandle("completed");
    await expect(
      handle.resume("corr", { outcome: "approved" }),
    ).rejects.toThrow(/already settled completed/);
    await expect(handle.deliverSignal("go", null, "sig-1")).rejects.toThrow(
      /already settled completed/,
    );
  });
});
