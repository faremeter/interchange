import { sign as nodeSign } from "node:crypto";
import { describe, test, expect, beforeEach } from "bun:test";
import {
  generateKeyPair,
  importPrivateKeyBytes,
} from "@interchange/crypto-node";
import { createSidecarRouter, type WsHandle } from "./sidecar-handler";

function hexEncode(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function hexDecode(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.substring(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

function signChallenge(
  nonce: string,
  address: string,
  privateKeyBytes: Uint8Array,
): string {
  const nonceBytes = hexDecode(nonce);
  const addressBytes = new TextEncoder().encode(address);
  const payload = new Uint8Array(nonceBytes.length + addressBytes.length);
  payload.set(nonceBytes);
  payload.set(addressBytes, nonceBytes.length);
  const privateKey = importPrivateKeyBytes(privateKeyBytes);
  const sig = nodeSign(null, payload, privateKey);
  return hexEncode(new Uint8Array(sig));
}

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

function lastSent(ws: ReturnType<typeof createMockWs>) {
  const last = ws.sent[ws.sent.length - 1];
  if (last === undefined) throw new Error("No messages sent");
  return JSON.parse(last);
}

describe("SidecarRouter", () => {
  let router: ReturnType<typeof createSidecarRouter>;

  beforeEach(() => {
    router = createSidecarRouter({ requestTimeoutMs: 500 });
  });

  describe("registration", () => {
    test("register frame populates routing table", () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent-a@local", "agent-b@local"],
        }),
      );

      expect(router.getConnectedSidecars()).toEqual(["sc-1"]);
      expect(router.getRoutableAddresses().sort()).toEqual([
        "agent-a@local",
        "agent-b@local",
      ]);
    });

    test("re-registration updates addresses", () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent-a@local"],
        }),
      );

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent-c@local"],
        }),
      );

      expect(router.getRoutableAddresses()).toEqual(["agent-c@local"]);
    });

    test("disconnect cleans up routing table", () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent-a@local"],
        }),
      );

      router.handleClose(ws);
      expect(router.getConnectedSidecars()).toEqual([]);
      expect(router.getRoutableAddresses()).toEqual([]);
    });

    test("invalid token closes connection", () => {
      const router = createSidecarRouter({
        validateToken: () => false,
      });
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "bad",
          agentAddresses: [],
        }),
      );

      expect(ws.closed).toBe(true);
      expect(router.getConnectedSidecars()).toEqual([]);
    });

    test("re-registration by another sidecar cleans ghost from old connection", () => {
      const ws1 = createMockWs();
      router.handleOpen(ws1);
      router.handleMessage(
        ws1,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local", "other@local"],
        }),
      );

      const ws2 = createMockWs();
      router.handleOpen(ws2);
      router.handleMessage(
        ws2,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-2",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      // ws2 now owns agent@local. Closing ws1 should only remove
      // other@local (which ws1 still owns), not agent@local.
      router.handleClose(ws1);

      expect(router.getRoutableAddresses()).toContain("agent@local");
      expect(router.getRoutableAddresses()).not.toContain("other@local");
      expect(router.routeMail("agent@local", "hello")).toBe(true);
      expect(ws2.sent).toHaveLength(1);
    });
  });

  describe("mail routing", () => {
    test("routes mail between two sidecars", () => {
      const ws1 = createMockWs();
      const ws2 = createMockWs();
      router.handleOpen(ws1);
      router.handleOpen(ws2);

      router.handleMessage(
        ws1,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["sender@local"],
        }),
      );
      router.handleMessage(
        ws2,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-2",
          token: "tok",
          agentAddresses: ["receiver@local"],
        }),
      );

      router.handleMessage(
        ws1,
        JSON.stringify({
          type: "mail.outbound",
          rawMessage: "dGVzdA==",
          recipients: ["receiver@local"],
        }),
      );

      const delivered = lastSent(ws2);
      expect(delivered.type).toBe("mail.inbound");
      expect(delivered.agentAddress).toBe("receiver@local");
      expect(delivered.rawMessage).toBe("dGVzdA==");
    });

    test("routeMail returns false for unknown address", () => {
      expect(router.routeMail("nobody@local", "dGVzdA==")).toBe(false);
    });

    test("routeMail returns true for routable address", () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      expect(router.routeMail("agent@local", "dGVzdA==")).toBe(true);
      const delivered = lastSent(ws);
      expect(delivered.type).toBe("mail.inbound");
    });

    test("unroutable mail goes to onMailOutbound callback", () => {
      const outbound: { rawMessage: string; recipients: string[] }[] = [];
      const router = createSidecarRouter({
        onMailOutbound(rawMessage, recipients) {
          outbound.push({ rawMessage, recipients });
        },
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["sender@local"],
        }),
      );

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "mail.outbound",
          rawMessage: "dGVzdA==",
          recipients: ["external@remote"],
        }),
      );

      expect(outbound).toHaveLength(1);
      expect(outbound[0]?.recipients).toEqual(["external@remote"]);
    });
  });

  describe("agent lifecycle", () => {
    test("agent.deploy sends frame and resolves on ack", async () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      const config = {
        sessionId: "ses_test",
        agentId: "a1",
        tenantId: "t1",
        principalId: "prin_test",
        agentAddress: "new-agent@local",
        systemPrompt: "test",
        tools: [],
        grants: [],
        providers: [
          {
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
          },
        ],
        defaultModel: "claude-sonnet-4-20250514",
      };

      const promise = router.sendAgentDeploy("new-agent@local", config);

      const frame = lastSent(ws);
      expect(frame.type).toBe("agent.deploy");
      expect(frame.config.agentAddress).toBe("new-agent@local");

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.deploy.ack",
          agentAddress: "new-agent@local",
          publicKey: "deadbeef",
        }),
      );

      await promise;
      expect(router.getRoutableAddresses()).toContain("new-agent@local");
    });

    test("agent.deploy.ack calls onAgentDeployAck before resolving", async () => {
      const ackCalls: { address: string; publicKey: string }[] = [];
      const router = createSidecarRouter({
        requestTimeoutMs: 500,
        async onAgentDeployAck(address, publicKey) {
          ackCalls.push({ address, publicKey });
        },
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      const config = {
        sessionId: "ses_test",
        agentId: "a1",
        tenantId: "t1",
        principalId: "prin_test",
        agentAddress: "ack-agent@local",
        systemPrompt: "test",
        tools: [],
        grants: [],
        providers: [
          {
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
          },
        ],
        defaultModel: "claude-sonnet-4-20250514",
      };

      const promise = router.sendAgentDeploy("ack-agent@local", config);

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.deploy.ack",
          agentAddress: "ack-agent@local",
          publicKey: "aabbccdd",
        }),
      );

      await promise;
      expect(ackCalls).toEqual([
        { address: "ack-agent@local", publicKey: "aabbccdd" },
      ]);
    });

    test("agent.deploy rejects when onAgentDeployAck fails", async () => {
      const router = createSidecarRouter({
        requestTimeoutMs: 500,
        async onAgentDeployAck() {
          throw new Error("DB write failed");
        },
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      const config = {
        sessionId: "ses_test",
        agentId: "a1",
        tenantId: "t1",
        principalId: "prin_test",
        agentAddress: "fail-ack@local",
        systemPrompt: "test",
        tools: [],
        grants: [],
        providers: [
          {
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
          },
        ],
        defaultModel: "claude-sonnet-4-20250514",
      };

      const promise = router.sendAgentDeploy("fail-ack@local", config);

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.deploy.ack",
          agentAddress: "fail-ack@local",
          publicKey: "aabbccdd",
        }),
      );

      await expect(promise).rejects.toThrow("Failed to store public key");
      expect(router.getRoutableAddresses()).not.toContain("fail-ack@local");
    });

    test("agent.deploy rolls back routing on error", async () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      const config = {
        sessionId: "ses_test",
        agentId: "a1",
        tenantId: "t1",
        principalId: "prin_test",
        agentAddress: "fail-agent@local",
        systemPrompt: "test",
        tools: [],
        grants: [],
        providers: [
          {
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
          },
        ],
        defaultModel: "claude-sonnet-4-20250514",
      };

      const promise = router.sendAgentDeploy("fail-agent@local", config);

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.error",
          agentAddress: "fail-agent@local",
          error: "provider failed",
        }),
      );

      await expect(promise).rejects.toThrow("provider failed");
      expect(router.getRoutableAddresses()).not.toContain("fail-agent@local");
    });

    test("agent.undeploy sends frame and removes routing after ack", async () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      const promise = router.sendAgentUndeploy("agent@local", "session_ended");
      const frame = lastSent(ws);
      expect(frame.type).toBe("agent.undeploy");
      expect(frame.agentAddress).toBe("agent@local");
      expect(frame.reason).toBe("session_ended");

      // Routing persists until the ack arrives.
      expect(router.getRoutableAddresses()).toContain("agent@local");

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.undeploy.ack",
          agentAddress: "agent@local",
          statePushed: true,
        }),
      );

      await promise;
      expect(router.getRoutableAddresses()).not.toContain("agent@local");
    });

    test("deploy request times out", async () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      const config = {
        sessionId: "ses_test",
        agentId: "a1",
        tenantId: "t1",
        principalId: "prin_test",
        agentAddress: "timeout@local",
        systemPrompt: "test",
        tools: [],
        grants: [],
        providers: [
          {
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
          },
        ],
        defaultModel: "claude-sonnet-4-20250514",
      };

      await expect(
        router.sendAgentDeploy("timeout@local", config),
      ).rejects.toThrow(/timed out/);
    });

    test("undeploy to unknown agent rejects immediately", async () => {
      await expect(
        router.sendAgentUndeploy("unknown@local", "gone"),
      ).rejects.toThrow(/No sidecar connected/);
    });

    test("disconnect during undeploy does not queue messages", async () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["undeploy-dc@local"],
        }),
      );

      // Start an undeploy but disconnect before the ack arrives.
      const promise = router.sendAgentUndeploy("undeploy-dc@local", "teardown");
      router.handleClose(ws);
      await expect(promise).rejects.toThrow(/disconnected/);

      // The address should not be routable after undeploy + disconnect.
      expect(router.getRoutableAddresses()).not.toContain("undeploy-dc@local");
    });

    test("disconnect rejects pending deploy", async () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      const config = {
        sessionId: "ses_test",
        agentId: "a1",
        tenantId: "t1",
        principalId: "prin_test",
        agentAddress: "dc-agent@local",
        systemPrompt: "test",
        tools: [],
        grants: [],
        providers: [
          {
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
          },
        ],
        defaultModel: "claude-sonnet-4-20250514",
      };

      const promise = router.sendAgentDeploy("dc-agent@local", config);
      router.handleClose(ws);

      await expect(promise).rejects.toThrow(/disconnected/);
    });

    test("abort preserves routing when address re-registered during await", async () => {
      const ws1 = createMockWs();
      router.handleOpen(ws1);
      router.handleMessage(
        ws1,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      const promise = router.sendSessionAbort("agent@local", "user_disconnect");
      const frame = lastSent(ws1);

      const ws2 = createMockWs();
      router.handleOpen(ws2);
      router.handleMessage(
        ws2,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-2",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      router.handleMessage(
        ws1,
        JSON.stringify({ type: "session.ack", requestId: frame.requestId }),
      );
      await promise;

      expect(router.getRoutableAddresses()).toContain("agent@local");
    });

    test("closing stale sidecar after reconnect-during-abort does not evict address", async () => {
      const ws1 = createMockWs();
      router.handleOpen(ws1);
      router.handleMessage(
        ws1,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      const promise = router.sendSessionAbort("agent@local", "user_disconnect");
      const frame = lastSent(ws1);

      const ws2 = createMockWs();
      router.handleOpen(ws2);
      router.handleMessage(
        ws2,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-2",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      router.handleMessage(
        ws1,
        JSON.stringify({ type: "session.ack", requestId: frame.requestId }),
      );
      await promise;

      router.handleClose(ws1);

      expect(router.getRoutableAddresses()).toContain("agent@local");
    });
  });

  describe("agent events", () => {
    test("agent.event frames are forwarded to callback", () => {
      const events: { addr: string; sid: string; event: unknown }[] = [];
      const router = createSidecarRouter({
        onAgentEvent(addr, sid, event) {
          events.push({ addr, sid, event });
        },
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "turn.start" },
        }),
      );

      expect(events).toHaveLength(1);
      expect(events[0]?.addr).toBe("agent@local");
      expect(events[0]?.event).toEqual({ type: "turn.start" });
    });
  });

  describe("session subscriptions", () => {
    test("subscriber receives events for its session", () => {
      const received: unknown[] = [];
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      router.subscribeSession("sess-1", (event) => received.push(event));

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "turn.start" },
        }),
      );

      expect(received).toHaveLength(1);
      expect(received[0]).toEqual({ type: "turn.start" });
    });

    test("subscriber does not receive events for other sessions", () => {
      const received: unknown[] = [];
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      router.subscribeSession("sess-1", (event) => received.push(event));

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-2",
          event: { type: "turn.start" },
        }),
      );

      expect(received).toHaveLength(0);
    });

    test("unsubscribe stops delivery", () => {
      const received: unknown[] = [];
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      const unsub = router.subscribeSession("sess-1", (event) =>
        received.push(event),
      );

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "turn.start" },
        }),
      );

      unsub();

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "turn.end" },
        }),
      );

      expect(received).toHaveLength(1);
      expect(received[0]).toEqual({ type: "turn.start" });
    });

    test("multiple subscribers receive the same event", () => {
      const received1: unknown[] = [];
      const received2: unknown[] = [];
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      router.subscribeSession("sess-1", (event) => received1.push(event));
      router.subscribeSession("sess-1", (event) => received2.push(event));

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "turn.start" },
        }),
      );

      expect(received1).toHaveLength(1);
      expect(received2).toHaveLength(1);
    });

    test("stale unsubscribe does not evict a later subscriber", () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      // eslint-disable-next-line @typescript-eslint/no-empty-function
      const unsub = router.subscribeSession("sess-1", () => {});
      unsub();

      const received: unknown[] = [];
      router.subscribeSession("sess-1", (event) => received.push(event));

      // Double-unsubscribe with the stale closure
      unsub();

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "turn.start" },
        }),
      );

      expect(received).toHaveLength(1);
    });

    test("subscriber that unsubscribes mid-dispatch does not drop later subscribers", () => {
      const received: unknown[] = [];
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      const unsub1Ref: { current: (() => void) | null } = { current: null };
      unsub1Ref.current = router.subscribeSession("sess-1", () => {
        unsub1Ref.current?.();
      });
      router.subscribeSession("sess-1", (event) => received.push(event));

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "turn.start" },
        }),
      );

      expect(received).toHaveLength(1);
      expect(received[0]).toEqual({ type: "turn.start" });
    });
  });

  describe("challenge/response reconnect", () => {
    test("reconnect issues challenge and verifies signature", async () => {
      const kp = await generateKeyPair();
      const publicKeyHex = hexEncode(kp.publicKey);

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookupPublicKey: async (addr) =>
          addr === "agent@local" ? publicKeyHex : null,
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      // Wait for async handleReconnect to complete.
      await new Promise((r) => setTimeout(r, 50));

      // Should have received a challenge frame.
      const challengeFrame = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge");
      expect(challengeFrame).toBeDefined();
      expect(challengeFrame.challenges).toHaveLength(1);

      const { address, nonce } = challengeFrame.challenges[0];
      expect(address).toBe("agent@local");

      // Sign and respond.
      const signature = signChallenge(nonce, address, kp.privateKey);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "challenge.response",
          responses: [{ address, signature }],
        }),
      );

      expect(router.getRoutableAddresses()).toContain("agent@local");
    });

    test("reconnect rejects invalid signature", async () => {
      const kp = await generateKeyPair();
      const wrongKp = await generateKeyPair();
      const publicKeyHex = hexEncode(kp.publicKey);

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookupPublicKey: async (addr) =>
          addr === "agent@local" ? publicKeyHex : null,
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      const challengeFrame = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge");

      const { address, nonce } = challengeFrame.challenges[0];

      // Sign with wrong key.
      const badSig = signChallenge(nonce, address, wrongKp.privateKey);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "challenge.response",
          responses: [{ address, signature: badSig }],
        }),
      );

      expect(router.getRoutableAddresses()).not.toContain("agent@local");

      const failedFrame = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge.failed");
      expect(failedFrame).toBeDefined();
      expect(failedFrame.address).toBe("agent@local");
    });

    test("reconnect sends challenge.failed for unknown address", async () => {
      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookupPublicKey: async () => null,
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["unknown@local"],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      const failedFrame = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge.failed");
      expect(failedFrame).toBeDefined();
      expect(failedFrame.address).toBe("unknown@local");
      expect(failedFrame.reason).toBe("Unknown agent address");
    });

    test("partial success routes verified addresses only", async () => {
      const kp1 = await generateKeyPair();
      const kp2 = await generateKeyPair();
      const wrongKp = await generateKeyPair();

      const keys = new Map([
        ["agent-a@local", hexEncode(kp1.publicKey)],
        ["agent-b@local", hexEncode(kp2.publicKey)],
      ]);

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookupPublicKey: async (addr) => keys.get(addr) ?? null,
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent-a@local", "agent-b@local"],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      const challengeFrame = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge");
      expect(challengeFrame.challenges).toHaveLength(2);

      const responses = challengeFrame.challenges.map(
        (c: { address: string; nonce: string }) => {
          const key =
            c.address === "agent-a@local" ? kp1.privateKey : wrongKp.privateKey;
          return {
            address: c.address,
            signature: signChallenge(c.nonce, c.address, key),
          };
        },
      );

      router.handleMessage(
        ws,
        JSON.stringify({ type: "challenge.response", responses }),
      );

      expect(router.getRoutableAddresses()).toContain("agent-a@local");
      expect(router.getRoutableAddresses()).not.toContain("agent-b@local");
    });

    test("disconnect cleans up pending challenge", async () => {
      const kp = await generateKeyPair();

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookupPublicKey: async (addr) =>
          addr === "agent@local" ? hexEncode(kp.publicKey) : null,
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      // Disconnect before responding.
      router.handleClose(ws);

      expect(router.getConnectedSidecars()).toEqual([]);
      expect(router.getRoutableAddresses()).toEqual([]);
    });
  });

  describe("disconnect message queuing", () => {
    test("mail queued during disconnect is flushed on reconnect", async () => {
      const kp = await generateKeyPair();
      const router = createSidecarRouter({
        requestTimeoutMs: 500,
        async lookupPublicKey() {
          return hexEncode(kp.publicKey);
        },
      });

      // Initial connection with one agent.
      const ws1 = createMockWs();
      router.handleOpen(ws1);
      router.handleMessage(
        ws1,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      // Disconnect — creates a queue entry.
      router.handleClose(ws1);

      // Send mail while disconnected.
      const queued = router.routeMail("agent@local", "queued-message");
      expect(queued).toBe(true);

      // Reconnect with challenge/response.
      const ws2 = createMockWs();
      router.handleOpen(ws2);
      router.handleMessage(
        ws2,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      const challengeFrame = ws2.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge");

      const responses = challengeFrame.challenges.map(
        (c: { address: string; nonce: string }) => ({
          address: c.address,
          signature: signChallenge(c.nonce, c.address, kp.privateKey),
        }),
      );

      router.handleMessage(
        ws2,
        JSON.stringify({ type: "challenge.response", responses }),
      );

      // The queued mail should have been flushed to the new connection.
      const flushed = ws2.sent
        .map((s) => JSON.parse(s))
        .filter((f: { type: string }) => f.type === "mail.inbound");
      expect(flushed).toHaveLength(1);
      expect(flushed[0].rawMessage).toBe("queued-message");
    });

    test("mail to unknown address returns false", () => {
      expect(router.routeMail("unknown@local", "msg")).toBe(false);
    });

    test("queue evicts oldest when full", async () => {
      const kp = await generateKeyPair();
      const router = createSidecarRouter({
        requestTimeoutMs: 500,
        disconnectQueueMaxSize: 2,
        async lookupPublicKey() {
          return hexEncode(kp.publicKey);
        },
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );
      router.handleClose(ws);

      // Queue 3 messages with max size 2 — oldest should be evicted.
      router.routeMail("agent@local", "msg-0");
      router.routeMail("agent@local", "msg-1");
      router.routeMail("agent@local", "msg-2");

      // Reconnect with challenge/response to flush.
      const ws2 = createMockWs();
      router.handleOpen(ws2);
      router.handleMessage(
        ws2,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      const challengeFrame = ws2.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge");

      const responses = challengeFrame.challenges.map(
        (c: { address: string; nonce: string }) => ({
          address: c.address,
          signature: signChallenge(c.nonce, c.address, kp.privateKey),
        }),
      );

      router.handleMessage(
        ws2,
        JSON.stringify({ type: "challenge.response", responses }),
      );

      const flushed = ws2.sent
        .map((s) => JSON.parse(s))
        .filter((f: { type: string }) => f.type === "mail.inbound");

      // Only the 2 newest messages should have been flushed.
      expect(flushed).toHaveLength(2);
      expect(flushed[0].rawMessage).toBe("msg-1");
      expect(flushed[1].rawMessage).toBe("msg-2");
    });

    test("sendSessionAbort rejects when agent is disconnected", async () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );
      router.handleClose(ws);

      await expect(
        router.sendSessionAbort("agent@local", "user_disconnect"),
      ).rejects.toThrow("No sidecar connected");
    });
  });

  describe("ping/pong keepalive", () => {
    test("hub responds to ping with pong", () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      router.handleMessage(ws, JSON.stringify({ type: "ping" }));

      const pong = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "pong");
      expect(pong).toEqual({ type: "pong" });
    });

    test("connection closed after ping timeout", async () => {
      const router = createSidecarRouter({
        requestTimeoutMs: 500,
        pingTimeoutMs: 100,
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      // Wait for the ping timeout to fire.
      await new Promise((r) => setTimeout(r, 150));

      expect(ws.closed).toBe(true);
    });

    test("ping resets the liveness timer", async () => {
      const router = createSidecarRouter({
        requestTimeoutMs: 500,
        pingTimeoutMs: 100,
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      // Send pings to keep the connection alive past the timeout.
      await new Promise((r) => setTimeout(r, 60));
      router.handleMessage(ws, JSON.stringify({ type: "ping" }));
      await new Promise((r) => setTimeout(r, 60));
      router.handleMessage(ws, JSON.stringify({ type: "ping" }));
      await new Promise((r) => setTimeout(r, 60));

      expect(ws.closed).toBe(false);
    });
  });

  describe("onAgentReconnected callback", () => {
    test("fires for each verified address on reconnect", async () => {
      const kp = await generateKeyPair();
      const publicKeyHex = hexEncode(kp.publicKey);
      const reconnected: string[] = [];

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookupPublicKey: async (addr) =>
          addr === "agent@local" ? publicKeyHex : null,
        onAgentReconnected: async (addr) => {
          reconnected.push(addr);
        },
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      const challengeFrame = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge");
      const { address, nonce } = challengeFrame.challenges[0];
      const signature = signChallenge(nonce, address, kp.privateKey);

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "challenge.response",
          responses: [{ address, signature }],
        }),
      );

      // Wait for async handleChallengeResponse to complete.
      await new Promise((r) => setTimeout(r, 50));

      expect(reconnected).toEqual(["agent@local"]);
    });

    test("does not fire for unverified addresses", async () => {
      const kp = await generateKeyPair();
      const wrongKp = await generateKeyPair();
      const publicKeyHex = hexEncode(kp.publicKey);
      const reconnected: string[] = [];

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookupPublicKey: async (addr) =>
          addr === "agent@local" ? publicKeyHex : null,
        onAgentReconnected: async (addr) => {
          reconnected.push(addr);
        },
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent@local"],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      const challengeFrame = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge");
      const { address, nonce } = challengeFrame.challenges[0];
      const signature = signChallenge(nonce, address, wrongKp.privateKey);

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "challenge.response",
          responses: [{ address, signature }],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      expect(reconnected).toEqual([]);
    });

    test("callback error does not prevent other addresses from reconnecting", async () => {
      const kp1 = await generateKeyPair();
      const kp2 = await generateKeyPair();
      const reconnected: string[] = [];

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookupPublicKey: async (addr) => {
          if (addr === "agent1@local") return hexEncode(kp1.publicKey);
          if (addr === "agent2@local") return hexEncode(kp2.publicKey);
          return null;
        },
        onAgentReconnected: async (addr) => {
          if (addr === "agent1@local") throw new Error("DB failure");
          reconnected.push(addr);
        },
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["agent1@local", "agent2@local"],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      const challengeFrame = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge");

      const responses = challengeFrame.challenges.map(
        (c: { address: string; nonce: string }) => ({
          address: c.address,
          signature: signChallenge(
            c.nonce,
            c.address,
            c.address === "agent1@local" ? kp1.privateKey : kp2.privateKey,
          ),
        }),
      );

      router.handleMessage(
        ws,
        JSON.stringify({ type: "challenge.response", responses }),
      );

      await new Promise((r) => setTimeout(r, 50));

      // agent2 should still be reconnected despite agent1's callback failure.
      expect(reconnected).toEqual(["agent2@local"]);
      expect(router.getRoutableAddresses()).not.toContain("agent1@local");
      expect(router.getRoutableAddresses()).toContain("agent2@local");

      // agent1 should receive a challenge.failed frame indicating governance rejection.
      const failedFrames = ws.sent
        .map((s) => JSON.parse(s))
        .filter(
          (f: { type: string; address?: string }) =>
            f.type === "challenge.failed" && f.address === "agent1@local",
        );
      expect(failedFrames).toHaveLength(1);
      expect(failedFrames[0].reason).toContain("governance");
    });
  });
});
