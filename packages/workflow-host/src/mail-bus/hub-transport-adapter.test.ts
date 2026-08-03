import { describe, test, expect } from "bun:test";

import { createInMemoryTransport } from "@intx/mail-memory";
import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";

import { wrapHubTransportAsMailBus } from "./hub-transport-adapter";

describe("wrapHubTransportAsMailBus", () => {
  test("routeInbound fans messages out to every subscribed handler and resolves", async () => {
    const transport = createInMemoryTransport();
    const adapter = wrapHubTransportAsMailBus(transport);
    const observedA: string[] = [];
    const observedB: string[] = [];
    const decoder = new TextDecoder();
    adapter.subscribeMailForAddress("a@example.com", async (bytes) => {
      observedA.push(decoder.decode(bytes));
    });
    adapter.subscribeMailForAddress("a@example.com", async (bytes) => {
      observedB.push(decoder.decode(bytes));
    });
    await adapter.routeInbound(
      "a@example.com",
      new TextEncoder().encode("hello"),
    );
    expect(observedA).toEqual(["hello"]);
    expect(observedB).toEqual(["hello"]);
  });

  test("routeInbound rejects when a handler rejects", async () => {
    const transport = createInMemoryTransport();
    const adapter = wrapHubTransportAsMailBus(transport);
    adapter.subscribeMailForAddress("a@example.com", async () => {
      throw new Error("durable write failed");
    });
    await expect(
      adapter.routeInbound("a@example.com", new TextEncoder().encode("hi")),
    ).rejects.toThrow(/durable write failed/);
  });

  test("subscribe disposer removes the handler from the per-address set", async () => {
    const transport = createInMemoryTransport();
    const adapter = wrapHubTransportAsMailBus(transport);
    const observed: string[] = [];
    const dispose = adapter.subscribeMailForAddress(
      "a@example.com",
      async (bytes) => {
        observed.push(new TextDecoder().decode(bytes));
      },
    );
    await adapter.routeInbound(
      "a@example.com",
      new TextEncoder().encode("first"),
    );
    dispose();
    // No subscriber remains, so the delivery is not durably accepted:
    // routeInbound rejects and nothing new is observed.
    await expect(
      adapter.routeInbound("a@example.com", new TextEncoder().encode("second")),
    ).rejects.toThrow(/no active mail subscriber/);
    expect(observed).toEqual(["first"]);
  });

  test("routeInbound rejects when no handler is registered", async () => {
    const transport = createInMemoryTransport();
    const adapter = wrapHubTransportAsMailBus(transport);
    await expect(
      adapter.routeInbound(
        "nobody@example.com",
        new TextEncoder().encode("ignored"),
      ),
    ).rejects.toThrow(/no active mail subscriber/);
  });

  test("unregisterAddress drops the per-address subscriber set", async () => {
    const transport = createInMemoryTransport();
    const adapter = wrapHubTransportAsMailBus(transport);
    const observed: string[] = [];
    adapter.subscribeMailForAddress("a@example.com", async (bytes) => {
      observed.push(new TextDecoder().decode(bytes));
    });
    adapter.unregisterAddress("a@example.com");
    await expect(
      adapter.routeInbound(
        "a@example.com",
        new TextEncoder().encode("ignored"),
      ),
    ).rejects.toThrow(/no active mail subscriber/);
    expect(observed).toEqual([]);
  });

  test("sendOutbound signs as the sender and delivers through the host transport", async () => {
    const transport = createInMemoryTransport();
    const senderKeyPair = await generateKeyPair();
    const recipientKeyPair = await generateKeyPair();
    transport.register(
      "sender@example.com",
      createEd25519Crypto(senderKeyPair),
    );
    transport.register(
      "recipient@example.com",
      createEd25519Crypto(recipientKeyPair),
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
