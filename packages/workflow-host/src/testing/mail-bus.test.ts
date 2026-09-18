import { describe, test, expect } from "bun:test";

import { createMockMailBus } from "./mail-bus";

describe("createMockMailBus", () => {
  test("awaitRegistered is satisfied by an address already registered", async () => {
    const bus = createMockMailBus();
    bus.registerAddress("run_x@example.com");
    // Registration happened before the wait. An edge wait would hang for a
    // re-registration that never comes.
    await bus.awaitRegistered("run_x@example.com");
  });

  test("awaitRegistered resolves on a later registration", async () => {
    const bus = createMockMailBus();
    const waited = bus.awaitRegistered("run_y@example.com");
    bus.registerAddress("run_y@example.com");
    await waited;
  });

  test("history distinguishes never-registered from registered-then-released", () => {
    const bus = createMockMailBus();
    bus.registerAddress("run_z@example.com");
    bus.unregisterAddress("run_z@example.com");
    // `registered()` cannot answer this: both cases read as absent. The two
    // copies of this double disagreed on which observable to keep, so the
    // shared one carries both.
    expect(bus.registered()).toEqual([]);
    expect(bus.registrationHistory()).toEqual([
      "register:run_z@example.com",
      "unregister:run_z@example.com",
    ]);
  });

  test("deliver reaches the handler subscribed for the address", async () => {
    const bus = createMockMailBus();
    const seen: string[] = [];
    bus.subscribeMailForAddress("run_w@example.com", async (raw) => {
      seen.push(new TextDecoder().decode(raw));
    });
    bus.deliver("run_w@example.com", new TextEncoder().encode("hello"));
    await waitForLength(seen, 1);
    expect(seen).toEqual(["hello"]);
  });
});

async function waitForLength(items: readonly unknown[], n: number) {
  while (items.length < n) {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
  }
}
