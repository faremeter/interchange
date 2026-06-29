import { describe, test, expect } from "bun:test";

import { createInMemoryTransport } from "@intx/mail-memory";
import { createNodeCrypto, generateKeyPair } from "@intx/crypto";

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

  test("sendOutbound signs as the sender and delivers through the host transport", async () => {
    const transport = createInMemoryTransport();
    const senderKeyPair = await generateKeyPair();
    const recipientKeyPair = await generateKeyPair();
    transport.register("sender@example.com", createNodeCrypto(senderKeyPair));
    transport.register(
      "recipient@example.com",
      createNodeCrypto(recipientKeyPair),
    );

    const adapter = wrapHubTransportAsMailBus(transport);
    const receipt = await adapter.sendOutbound("sender@example.com", {
      to: "recipient@example.com",
      type: "conversation.message",
      content: "outbound body",
    });
    expect(receipt.status).toBe("delivered");
    expect(receipt.messageId.length).toBeGreaterThan(0);

    // The send routed through the host transport's signed-send path: the
    // recipient's INBOX holds the message, signed by the sender's
    // CryptoProvider (fetchFull verifies the signature).
    const recipientView = transport.getTransportFor("recipient@example.com");
    const refs = await recipientView.search("INBOX", {});
    expect(refs).toHaveLength(1);
    const ref = refs[0];
    if (ref === undefined) throw new Error("missing inbox ref");
    const full = await recipientView.fetchFull(ref);
    expect(full.signatureStatus).toBe("valid");
    expect(full.headers.from).toBe("sender@example.com");
  });

  test("sendOutbound throws for an unregistered sender rather than emitting unsigned mail", async () => {
    const transport = createInMemoryTransport();
    const adapter = wrapHubTransportAsMailBus(transport);
    await expect(
      adapter.sendOutbound("nobody@example.com", {
        to: "recipient@example.com",
        type: "conversation.message",
        content: "should not send",
      }),
    ).rejects.toThrow(/not registered/);
  });
});
