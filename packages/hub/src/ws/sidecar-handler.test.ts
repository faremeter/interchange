import { describe, test, expect, beforeEach } from "bun:test";
import { createSidecarRouter, type WsHandle } from "./sidecar-handler";

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

  describe("session lifecycle", () => {
    test("session.create sends frame and resolves on ack", async () => {
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
        agentAddress: "new-agent@local",
        systemPrompt: "test",
        tools: [],
        toolPolicy: [],
        providers: [
          {
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
          },
        ],
        defaultModel: "claude-sonnet-4-20250514",
      };

      const promise = router.sendSessionCreate("new-agent@local", config);

      const frame = lastSent(ws);
      expect(frame.type).toBe("session.create");
      expect(frame.config.agentAddress).toBe("new-agent@local");

      router.handleMessage(
        ws,
        JSON.stringify({ type: "session.ack", requestId: frame.requestId }),
      );

      await promise;
      expect(router.getRoutableAddresses()).toContain("new-agent@local");
    });

    test("session.create rolls back routing on error", async () => {
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
        agentAddress: "fail-agent@local",
        systemPrompt: "test",
        tools: [],
        toolPolicy: [],
        providers: [
          {
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
          },
        ],
        defaultModel: "claude-sonnet-4-20250514",
      };

      const promise = router.sendSessionCreate("fail-agent@local", config);

      const frame = lastSent(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "session.error",
          requestId: frame.requestId,
          error: "provider failed",
        }),
      );

      await expect(promise).rejects.toThrow("provider failed");
      expect(router.getRoutableAddresses()).not.toContain("fail-agent@local");
    });

    test("session.destroy sends frame and resolves on ack", async () => {
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

      const promise = router.sendSessionDestroy("agent@local");
      const frame = lastSent(ws);
      expect(frame.type).toBe("session.destroy");

      router.handleMessage(
        ws,
        JSON.stringify({ type: "session.ack", requestId: frame.requestId }),
      );

      await promise;
    });

    test("session request times out", async () => {
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

      await expect(router.sendSessionDestroy("agent@local")).rejects.toThrow(
        /timed out/,
      );
    });

    test("session request to unknown agent rejects immediately", async () => {
      await expect(router.sendSessionDestroy("unknown@local")).rejects.toThrow(
        /No sidecar connected/,
      );
    });

    test("disconnect rejects pending requests immediately", async () => {
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

      const promise = router.sendSessionDestroy("agent@local");
      router.handleClose(ws);

      await expect(promise).rejects.toThrow(/disconnected/);
    });

    test("concurrent create nack does not remove address with pending ack", async () => {
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
        sessionId: "ses_concurrent",
        agentId: "a1",
        tenantId: "t1",
        agentAddress: "race@local",
        systemPrompt: "test",
        tools: [],
        toolPolicy: [],
        providers: [
          {
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
          },
        ],
        defaultModel: "claude-sonnet-4-20250514",
      };

      // Launch two concurrent creates for the same address.
      const sentBefore = ws.sent.length;
      const p1 = router.sendSessionCreate("race@local", {
        ...config,
        sessionId: "ses_1",
      });
      const p2 = router.sendSessionCreate("race@local", {
        ...config,
        sessionId: "ses_2",
      });
      expect(ws.sent.length).toBe(sentBefore + 2);
      const parsed1 = JSON.parse(ws.sent[sentBefore] as string);
      const parsed2 = JSON.parse(ws.sent[sentBefore + 1] as string);

      // Nack the first request.
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "session.error",
          requestId: parsed1.requestId,
          error: "first failed",
        }),
      );

      await expect(p1).rejects.toThrow("first failed");

      // The address must still be routable because p2 is still pending.
      expect(router.getRoutableAddresses()).toContain("race@local");

      // Ack the second request.
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "session.ack",
          requestId: parsed2.requestId,
        }),
      );

      await p2;
      expect(router.getRoutableAddresses()).toContain("race@local");
    });

    test("destroy removes agent from routing table", async () => {
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

      const promise = router.sendSessionDestroy("agent@local");
      const frame = lastSent(ws);
      router.handleMessage(
        ws,
        JSON.stringify({ type: "session.ack", requestId: frame.requestId }),
      );
      await promise;

      expect(router.getRoutableAddresses()).not.toContain("agent@local");
    });

    test("destroy preserves routing when address re-registered during await", async () => {
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

      // Start destroy — request is in flight
      const promise = router.sendSessionDestroy("agent@local");
      const frame = lastSent(ws1);

      // While destroy is in flight, a new sidecar registers the same address
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

      // Ack the original destroy
      router.handleMessage(
        ws1,
        JSON.stringify({ type: "session.ack", requestId: frame.requestId }),
      );
      await promise;

      // The address must still be routable via the new sidecar
      expect(router.getRoutableAddresses()).toContain("agent@local");
      expect(router.routeMail("agent@local", "hello")).toBe(true);
      expect(ws2.sent).toHaveLength(1);
    });

    test("failed destroy leaves address routable", async () => {
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

      const promise = router.sendSessionDestroy("agent@local");
      const frame = lastSent(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "session.error",
          requestId: frame.requestId,
          error: "destroy failed",
        }),
      );

      await expect(promise).rejects.toThrow("destroy failed");
      expect(router.getRoutableAddresses()).toContain("agent@local");
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

    test("closing stale sidecar after reconnect-during-destroy does not evict address", async () => {
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

      const promise = router.sendSessionDestroy("agent@local");
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

      // Old sidecar disconnects — must not evict the new sidecar's address
      router.handleClose(ws1);

      expect(router.getRoutableAddresses()).toContain("agent@local");
      expect(router.routeMail("agent@local", "hello")).toBe(true);
      expect(ws2.sent).toHaveLength(1);
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
});
