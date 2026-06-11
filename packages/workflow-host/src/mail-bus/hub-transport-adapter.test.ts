import { describe, test, expect } from "bun:test";

import { createInMemoryTransport } from "@intx/mail-memory";

import { wrapHubTransportAsMailBus } from "./hub-transport-adapter";

describe("wrapHubTransportAsMailBus", () => {
  test("routeInbound fans messages out to every subscribed handler", () => {
    const transport = createInMemoryTransport();
    const adapter = wrapHubTransportAsMailBus(transport);
    const observedA: string[] = [];
    const observedB: string[] = [];
    const decoder = new TextDecoder();
    adapter.subscribeMailForAddress("a@example.com", (bytes) => {
      observedA.push(decoder.decode(bytes));
    });
    adapter.subscribeMailForAddress("a@example.com", (bytes) => {
      observedB.push(decoder.decode(bytes));
    });
    adapter.routeInbound("a@example.com", new TextEncoder().encode("hello"));
    expect(observedA).toEqual(["hello"]);
    expect(observedB).toEqual(["hello"]);
  });

  test("subscribe disposer removes the handler from the per-address set", () => {
    const transport = createInMemoryTransport();
    const adapter = wrapHubTransportAsMailBus(transport);
    const observed: string[] = [];
    const dispose = adapter.subscribeMailForAddress(
      "a@example.com",
      (bytes) => {
        observed.push(new TextDecoder().decode(bytes));
      },
    );
    adapter.routeInbound("a@example.com", new TextEncoder().encode("first"));
    dispose();
    adapter.routeInbound("a@example.com", new TextEncoder().encode("second"));
    expect(observed).toEqual(["first"]);
  });

  test("routeInbound is a no-op when no handler is registered", () => {
    const transport = createInMemoryTransport();
    const adapter = wrapHubTransportAsMailBus(transport);
    expect(() =>
      adapter.routeInbound(
        "nobody@example.com",
        new TextEncoder().encode("ignored"),
      ),
    ).not.toThrow();
  });

  test("unregisterAddress drops the per-address subscriber set", () => {
    const transport = createInMemoryTransport();
    const adapter = wrapHubTransportAsMailBus(transport);
    const observed: string[] = [];
    adapter.subscribeMailForAddress("a@example.com", (bytes) => {
      observed.push(new TextDecoder().decode(bytes));
    });
    adapter.unregisterAddress("a@example.com");
    adapter.routeInbound("a@example.com", new TextEncoder().encode("ignored"));
    expect(observed).toEqual([]);
  });
});
