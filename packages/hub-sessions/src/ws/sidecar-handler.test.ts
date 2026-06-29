import { describe, test, expect, beforeEach } from "bun:test";
import { generateKeyPair, signEd25519 } from "@intx/crypto-node";
import { hexDecode, hexEncode, parseAgentAddress } from "@intx/types";
import { chunkPack } from "@intx/pack-transport";
import type { PackRejectReason, RepoId } from "@intx/types/sidecar";
import { createSidecarRouter, type WsHandle } from "./sidecar-handler";

async function signChallenge(
  nonce: string,
  address: string,
  privateKeyBytes: Uint8Array,
): Promise<string> {
  const nonceBytes = hexDecode(nonce);
  const addressBytes = new TextEncoder().encode(address);
  const payload = new Uint8Array(nonceBytes.length + addressBytes.length);
  payload.set(nonceBytes);
  payload.set(addressBytes, nonceBytes.length);
  const sig = await signEd25519(privateKeyBytes, payload);
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

  const TEST_HUB_KEY = "a".repeat(64);

  beforeEach(() => {
    router = createSidecarRouter({
      requestTimeoutMs: 500,
      hubPublicKey: TEST_HUB_KEY,
    });
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

    test("unroutable mail emits mail.outbound.undelivered", () => {
      const outbound: { rawMessage: string; recipients: string[] }[] = [];
      const router = createSidecarRouter({});
      router.events.on("mail.outbound.undelivered", (event) => {
        outbound.push({
          rawMessage: event.rawMessage,
          recipients: event.recipients,
        });
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

    test("agent reply to non-agent address still persists outbound record", async () => {
      // When an agent replies to a human user (usr_ address), the
      // persistMail lookup must still persist at least the outbound
      // record on the sender's session and emit mail.persisted so a
      // mail.delivered SSE event is dispatched.
      const persisted: { id: string; address: string }[] = [];
      const router = createSidecarRouter({
        lookups: {
          persistMail: async ({ senderAddress, recipients }) => {
            // Mirrors the fixed persistMail: always create the
            // outbound record for the sender, and only create
            // inbound records for recipients that are agent
            // instances (ins_ prefix).
            const results: {
              id: string;
              direction: "inbound" | "outbound";
              instanceId: string | null;
              address: string;
              createdAt: Date;
            }[] = [
              {
                id: "mail_outbound",
                direction: "outbound",
                instanceId:
                  parseAgentAddress(senderAddress)?.instanceId ?? senderAddress,
                address: senderAddress,
                createdAt: new Date(),
              },
            ];
            for (const addr of recipients) {
              if (addr.startsWith("ins_")) {
                results.push({
                  id: `mail_in_${addr}`,
                  direction: "inbound",
                  instanceId: parseAgentAddress(addr)?.instanceId ?? addr,
                  address: addr,
                  createdAt: new Date(),
                });
              }
            }
            return results;
          },
        },
      });
      router.events.on("mail.persisted", (row) => {
        persisted.push({ id: row.id, address: row.address });
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["ins_sender@tenant.example"],
        }),
      );

      // Agent sends a reply to a human user address.
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "mail.outbound",
          delivered: true,
          senderAddress: "ins_sender@tenant.example",
          rawMessage: btoa("test message"),
          recipients: ["usr_human@tenant.example"],
        }),
      );

      // Allow the async handleMailPersist to settle.
      await new Promise((resolve) => setTimeout(resolve, 50));

      // The outbound record for the sender must always be persisted,
      // regardless of whether recipients are agent instances. No inbound
      // record should be created for the non-agent recipient.
      expect(persisted).toHaveLength(1);
      expect(persisted[0]?.address).toBe("ins_sender@tenant.example");
    });

    test("persistMail lookup result fans out as mail.persisted events", async () => {
      const persisted: { id: string; address: string }[] = [];
      const router = createSidecarRouter({
        lookups: {
          persistMail: async ({ senderAddress, recipients }) => {
            return [
              {
                id: "mail_out",
                direction: "outbound" as const,
                instanceId:
                  parseAgentAddress(senderAddress)?.instanceId ?? senderAddress,
                address: senderAddress,
                createdAt: new Date(),
              },
              ...recipients.map((addr) => ({
                id: `mail_in_${addr}`,
                direction: "inbound" as const,
                instanceId: parseAgentAddress(addr)?.instanceId ?? addr,
                address: addr,
                createdAt: new Date(),
              })),
            ];
          },
        },
      });
      router.events.on("mail.persisted", (row) => {
        persisted.push({ id: row.id, address: row.address });
      });

      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["ins_sender@tenant.example"],
        }),
      );

      // Agent sends mail to another agent (both are instances).
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "mail.outbound",
          delivered: true,
          senderAddress: "ins_sender@tenant.example",
          rawMessage: btoa("test message"),
          recipients: ["ins_receiver@tenant.example"],
        }),
      );

      await new Promise((resolve) => setTimeout(resolve, 50));

      // Both outbound (sender) and inbound (receiver) records persisted.
      expect(persisted).toHaveLength(2);
      expect(persisted[0]?.address).toBe("ins_sender@tenant.example");
      expect(persisted[1]?.address).toBe("ins_receiver@tenant.example");
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
        sources: [
          {
            id: "anthropic:claude-sonnet-4-20250514",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
            model: "claude-sonnet-4-20250514",
          },
        ],
        defaultSource: "anthropic:claude-sonnet-4-20250514",
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

    test("agent.deploy.ack invokes subscribers before resolving", async () => {
      const ackCalls: { address: string; publicKey: string }[] = [];
      const router = createSidecarRouter({
        requestTimeoutMs: 500,
        hubPublicKey: TEST_HUB_KEY,
      });
      router.events.on("agent.deploy.ack", ({ agentAddress, publicKey }) => {
        ackCalls.push({ address: agentAddress, publicKey });
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
        sources: [
          {
            id: "anthropic:claude-sonnet-4-20250514",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
            model: "claude-sonnet-4-20250514",
          },
        ],
        defaultSource: "anthropic:claude-sonnet-4-20250514",
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

    test("agent.deploy rejects when agent.deploy.ack subscriber throws", async () => {
      const router = createSidecarRouter({
        requestTimeoutMs: 500,
        hubPublicKey: TEST_HUB_KEY,
      });
      router.events.on("agent.deploy.ack", () => {
        throw new Error("DB write failed");
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
        sources: [
          {
            id: "anthropic:claude-sonnet-4-20250514",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
            model: "claude-sonnet-4-20250514",
          },
        ],
        defaultSource: "anthropic:claude-sonnet-4-20250514",
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
        sources: [
          {
            id: "anthropic:claude-sonnet-4-20250514",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
            model: "claude-sonnet-4-20250514",
          },
        ],
        defaultSource: "anthropic:claude-sonnet-4-20250514",
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
        sources: [
          {
            id: "anthropic:claude-sonnet-4-20250514",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
            model: "claude-sonnet-4-20250514",
          },
        ],
        defaultSource: "anthropic:claude-sonnet-4-20250514",
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
        sources: [
          {
            id: "anthropic:claude-sonnet-4-20250514",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
            model: "claude-sonnet-4-20250514",
          },
        ],
        defaultSource: "anthropic:claude-sonnet-4-20250514",
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
    test("agent.event frames are forwarded to subscribers", () => {
      const events: { addr: string; sid: string; event: unknown }[] = [];
      const router = createSidecarRouter({});
      router.events.on("agent.event", ({ agentAddress, sessionId, event }) => {
        events.push({ addr: agentAddress, sid: sessionId, event });
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
          event: { type: "reactor.start", seq: 0, data: {} },
        }),
      );

      expect(events).toHaveLength(1);
      expect(events[0]?.addr).toBe("agent@local");
      expect(events[0]?.event).toEqual({
        type: "reactor.start",
        seq: 0,
        data: {},
      });
    });

    test("agent.event frames are emitted on router.events", () => {
      const seen: { addr: string; sid: string }[] = [];
      const router = createSidecarRouter({});
      router.events.on("agent.event", ({ agentAddress, sessionId }) => {
        seen.push({ addr: agentAddress, sid: sessionId });
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
          event: { type: "reactor.start", seq: 0, data: {} },
        }),
      );

      expect(seen).toEqual([{ addr: "agent@local", sid: "sess-1" }]);
    });

    test("sidecar.disconnect is emitted on router.events", () => {
      const router = createSidecarRouter({});
      const seen: string[][] = [];
      router.events.on("sidecar.disconnect", ({ agentAddresses }) => {
        seen.push(agentAddresses);
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

      expect(seen).toEqual([["agent@local"]]);
    });

    test("connector.state.changed populates the cache and is readable via getConnectorState", () => {
      const router = createSidecarRouter({});
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

      // Before any state frame, the cache is absent → null.
      expect(router.getConnectorState("agent@local")).toBeNull();

      const state = {
        threadRoot: "<root@example.com>",
        lastMessageId: "<last@example.com>",
        replyTo: "user@example.com",
        cc: [],
      };
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "connector.state.changed",
          agentAddress: "agent@local",
          connectorState: state,
        }),
      );

      expect(router.getConnectorState("agent@local")).toEqual(state);

      // An explicit-null frame clears the cached state.
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "connector.state.changed",
          agentAddress: "agent@local",
          connectorState: null,
        }),
      );

      expect(router.getConnectorState("agent@local")).toBeNull();
    });

    test("connector.state.changed is emitted on router.events", () => {
      const router = createSidecarRouter({});
      const seen: { addr: string; state: unknown }[] = [];
      router.events.on(
        "connector.state.changed",
        ({ agentAddress, connectorState }) => {
          seen.push({ addr: agentAddress, state: connectorState });
        },
      );

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

      const state = {
        threadRoot: "<root@example.com>",
        lastMessageId: "<last@example.com>",
        replyTo: "user@example.com",
        cc: [],
        subject: "Hi",
      };
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "connector.state.changed",
          agentAddress: "agent@local",
          connectorState: state,
        }),
      );

      expect(seen).toEqual([{ addr: "agent@local", state }]);
    });

    test("live takeover via register evicts the prior owner's cached connector state", () => {
      const router = createSidecarRouter({});

      // First sidecar registers and reports connector state.
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
      router.handleMessage(
        ws1,
        JSON.stringify({
          type: "connector.state.changed",
          agentAddress: "agent@local",
          connectorState: {
            threadRoot: "<root@example.com>",
            lastMessageId: "<last@example.com>",
            replyTo: "user@example.com",
            cc: [],
          },
        }),
      );
      expect(router.getConnectorState("agent@local")).not.toBeNull();

      // A second sidecar registers claiming the same address without
      // the first having disconnected. The prior cache must be evicted
      // so it cannot mis-thread mail in the window before the new
      // owner bootstraps.
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

      expect(router.getConnectorState("agent@local")).toBeNull();
    });

    test("disconnect clears cached connector state for affected agents", () => {
      const router = createSidecarRouter({});
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

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "connector.state.changed",
          agentAddress: "agent@local",
          connectorState: {
            threadRoot: "<root@example.com>",
            lastMessageId: "<last@example.com>",
            replyTo: "user@example.com",
            cc: [],
          },
        }),
      );

      expect(router.getConnectorState("agent@local")).not.toBeNull();

      router.handleClose(ws);

      expect(router.getConnectorState("agent@local")).toBeNull();
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

      router.subscribeAgent("agent@local", (event) => received.push(event));

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "reactor.start", seq: 0, data: {} },
        }),
      );

      expect(received).toHaveLength(1);
      expect(received[0]).toEqual({ type: "reactor.start", seq: 0, data: {} });
    });

    test("subscriber does not receive events for other agents", () => {
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

      router.subscribeAgent("agent@local", (event) => received.push(event));

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "other-agent@local",
          sessionId: "sess-2",
          event: { type: "reactor.start", seq: 0, data: {} },
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

      const unsub = router.subscribeAgent("agent@local", (event) =>
        received.push(event),
      );

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "reactor.start", seq: 0, data: {} },
        }),
      );

      unsub();

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "reactor.end", seq: 1, data: {} },
        }),
      );

      expect(received).toHaveLength(1);
      expect(received[0]).toEqual({ type: "reactor.start", seq: 0, data: {} });
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

      router.subscribeAgent("agent@local", (event) => received1.push(event));
      router.subscribeAgent("agent@local", (event) => received2.push(event));

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "reactor.start", seq: 0, data: {} },
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
      const unsub = router.subscribeAgent("agent@local", () => {});
      unsub();

      const received: unknown[] = [];
      router.subscribeAgent("agent@local", (event) => received.push(event));

      // Double-unsubscribe with the stale closure
      unsub();

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "reactor.start", seq: 0, data: {} },
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
      unsub1Ref.current = router.subscribeAgent("agent@local", () => {
        unsub1Ref.current?.();
      });
      router.subscribeAgent("agent@local", (event) => received.push(event));

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.event",
          agentAddress: "agent@local",
          sessionId: "sess-1",
          event: { type: "reactor.start", seq: 0, data: {} },
        }),
      );

      expect(received).toHaveLength(1);
      expect(received[0]).toEqual({ type: "reactor.start", seq: 0, data: {} });
    });
  });

  describe("challenge/response reconnect", () => {
    test("a provision routed during the restore window survives a reconnect", async () => {
      // An agent provisioned while the sidecar is restoring must stay
      // routable after the reconnect frame for the disk-restored agents
      // lands. lookupPublicKey returns null so the challenge
      // short-circuits; the eviction under test happens in
      // handleReconnect's internal register before any challenge work.
      const router = createSidecarRouter({
        requestTimeoutMs: 500,
        hubPublicKey: TEST_HUB_KEY,
        lookups: {
          lookupPublicKey: async () => null,
        },
      });

      const config = {
        sessionId: "ses_test",
        agentId: "a1",
        tenantId: "t1",
        principalId: "prin_test",
        agentAddress: "window-agent@local",
        systemPrompt: "test",
        tools: [],
        grants: [],
        sources: [
          {
            id: "anthropic:claude-sonnet-4-20250514",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
            model: "claude-sonnet-4-20250514",
          },
        ],
        defaultSource: "anthropic:claude-sonnet-4-20250514",
      };

      const ws = createMockWs();
      router.handleOpen(ws);

      // Empty register on socket open establishes routability before restore.
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [],
        }),
      );

      // A fresh provision routes to this sidecar during the restore window.
      const deployPromise = router.sendAgentDeploy(
        "window-agent@local",
        config,
      );
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.deploy.ack",
          agentAddress: "window-agent@local",
          publicKey: "deadbeef",
        }),
      );
      await deployPromise;
      expect(router.getRoutableAddresses()).toContain("window-agent@local");

      // Restore finishes; the sidecar reconnects with only its
      // disk-restored addresses.
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["restored@local"],
          deployRefs: {},
        }),
      );
      await new Promise((r) => setTimeout(r, 50));

      // The window-provisioned agent stays routable.
      expect(router.getRoutableAddresses()).toContain("window-agent@local");
    });

    test("a window-provisioned agent's connector state survives a reconnect", async () => {
      // The same eviction that drops routing also drops the cached
      // connector thread state. A window agent that established a thread
      // during the restore window must keep it across the reconnect, or a
      // following no-history user message forks a new thread instead of
      // continuing the agent's active one.
      const router = createSidecarRouter({
        requestTimeoutMs: 500,
        hubPublicKey: TEST_HUB_KEY,
        lookups: {
          lookupPublicKey: async () => null,
        },
      });

      const config = {
        sessionId: "ses_test",
        agentId: "a1",
        tenantId: "t1",
        principalId: "prin_test",
        agentAddress: "window-agent@local",
        systemPrompt: "test",
        tools: [],
        grants: [],
        sources: [
          {
            id: "anthropic:claude-sonnet-4-20250514",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-test",
            model: "claude-sonnet-4-20250514",
          },
        ],
        defaultSource: "anthropic:claude-sonnet-4-20250514",
      };

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

      const deployPromise = router.sendAgentDeploy(
        "window-agent@local",
        config,
      );
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.deploy.ack",
          agentAddress: "window-agent@local",
          publicKey: "deadbeef",
        }),
      );
      await deployPromise;

      // The window agent establishes a connector thread during the window.
      const connectorState = {
        threadRoot: "<root@example.com>",
        lastMessageId: "<last@example.com>",
        replyTo: "user@example.com",
        cc: [],
      };
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "connector.state.changed",
          agentAddress: "window-agent@local",
          connectorState,
        }),
      );
      expect(router.getConnectorState("window-agent@local")).toEqual(
        connectorState,
      );

      // Restore finishes; the sidecar reconnects with only its
      // disk-restored addresses.
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: ["restored@local"],
          deployRefs: {},
        }),
      );
      await new Promise((r) => setTimeout(r, 50));

      // The window agent's connector thread state survives.
      expect(router.getConnectorState("window-agent@local")).toEqual(
        connectorState,
      );
    });

    test("reconnect issues challenge and verifies signature", async () => {
      const kp = await generateKeyPair();
      const publicKeyHex = hexEncode(kp.publicKey);

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookups: {
          lookupPublicKey: async (addr) =>
            addr === "agent@local" ? publicKeyHex : null,
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
      const signature = await signChallenge(nonce, address, kp.privateKey);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "challenge.response",
          responses: [{ address, signature }],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      expect(router.getRoutableAddresses()).toContain("agent@local");
    });

    test("reconnect rejects invalid signature", async () => {
      const kp = await generateKeyPair();
      const wrongKp = await generateKeyPair();
      const publicKeyHex = hexEncode(kp.publicKey);

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookups: {
          lookupPublicKey: async (addr) =>
            addr === "agent@local" ? publicKeyHex : null,
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

      // Sign with wrong key.
      const badSig = await signChallenge(nonce, address, wrongKp.privateKey);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "challenge.response",
          responses: [{ address, signature: badSig }],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

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
        lookups: {
          lookupPublicKey: async () => null,
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
        lookups: {
          lookupPublicKey: async (addr) => keys.get(addr) ?? null,
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
          agentAddresses: ["agent-a@local", "agent-b@local"],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      const challengeFrame = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge");
      expect(challengeFrame.challenges).toHaveLength(2);

      const responses = await Promise.all(
        challengeFrame.challenges.map(
          async (c: { address: string; nonce: string }) => {
            const key =
              c.address === "agent-a@local"
                ? kp1.privateKey
                : wrongKp.privateKey;
            return {
              address: c.address,
              signature: await signChallenge(c.nonce, c.address, key),
            };
          },
        ),
      );

      router.handleMessage(
        ws,
        JSON.stringify({ type: "challenge.response", responses }),
      );

      await new Promise((r) => setTimeout(r, 50));

      expect(router.getRoutableAddresses()).toContain("agent-a@local");
      expect(router.getRoutableAddresses()).not.toContain("agent-b@local");
    });

    test("reconnect with stale deployRef emits deploy.ref.stale", async () => {
      const kp = await generateKeyPair();
      const publicKeyHex = hexEncode(kp.publicKey);
      const staleAddresses: string[] = [];

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookups: {
          lookupPublicKey: async (addr) =>
            addr === "agent@local" ? publicKeyHex : null,
          lookupDeployRef: async () => "aaaa",
        },
      });
      router.events.on("deploy.ref.stale", ({ agentAddress }) => {
        staleAddresses.push(agentAddress);
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
          deployRefs: { "agent@local": "bbbb" },
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      const challengeFrame = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge");
      const { address, nonce } = challengeFrame.challenges[0];
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "challenge.response",
          responses: [
            {
              address,
              signature: await signChallenge(nonce, address, kp.privateKey),
            },
          ],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));
      expect(staleAddresses).toContain("agent@local");
    });

    test("reconnect with matching deployRef skips deploy.ref.stale", async () => {
      const kp = await generateKeyPair();
      const publicKeyHex = hexEncode(kp.publicKey);
      const staleAddresses: string[] = [];

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookups: {
          lookupPublicKey: async (addr) =>
            addr === "agent@local" ? publicKeyHex : null,
          lookupDeployRef: async () => "aaaa",
        },
      });
      router.events.on("deploy.ref.stale", ({ agentAddress }) => {
        staleAddresses.push(agentAddress);
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
          deployRefs: { "agent@local": "aaaa" },
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      const challengeFrame = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge");
      const { address, nonce } = challengeFrame.challenges[0];
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "challenge.response",
          responses: [
            {
              address,
              signature: await signChallenge(nonce, address, kp.privateKey),
            },
          ],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));
      expect(staleAddresses).toEqual([]);
    });

    test("reconnect with absent deployRef emits deploy.ref.stale", async () => {
      const kp = await generateKeyPair();
      const publicKeyHex = hexEncode(kp.publicKey);
      const staleAddresses: string[] = [];

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookups: {
          lookupPublicKey: async (addr) =>
            addr === "agent@local" ? publicKeyHex : null,
          lookupDeployRef: async () => "aaaa",
        },
      });
      router.events.on("deploy.ref.stale", ({ agentAddress }) => {
        staleAddresses.push(agentAddress);
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
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "challenge.response",
          responses: [
            {
              address,
              signature: await signChallenge(nonce, address, kp.privateKey),
            },
          ],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));
      expect(staleAddresses).toContain("agent@local");
    });

    test("reconnect skips re-deploy when hub has no deploy ref", async () => {
      const kp = await generateKeyPair();
      const publicKeyHex = hexEncode(kp.publicKey);
      const staleAddresses: string[] = [];

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookups: {
          lookupPublicKey: async (addr) =>
            addr === "agent@local" ? publicKeyHex : null,
          lookupDeployRef: async () => null,
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
          deployRefs: { "agent@local": "bbbb" },
        }),
      );

      await new Promise((r) => setTimeout(r, 50));

      const challengeFrame = ws.sent
        .map((s) => JSON.parse(s))
        .find((f: { type: string }) => f.type === "challenge");
      const { address, nonce } = challengeFrame.challenges[0];
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "challenge.response",
          responses: [
            {
              address,
              signature: await signChallenge(nonce, address, kp.privateKey),
            },
          ],
        }),
      );

      await new Promise((r) => setTimeout(r, 50));
      expect(staleAddresses).toEqual([]);
    });

    test("disconnect cleans up pending challenge", async () => {
      const kp = await generateKeyPair();

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookups: {
          lookupPublicKey: async (addr) =>
            addr === "agent@local" ? hexEncode(kp.publicKey) : null,
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
        lookups: {
          async lookupPublicKey() {
            return hexEncode(kp.publicKey);
          },
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

      const responses = await Promise.all(
        challengeFrame.challenges.map(
          async (c: { address: string; nonce: string }) => ({
            address: c.address,
            signature: await signChallenge(c.nonce, c.address, kp.privateKey),
          }),
        ),
      );

      router.handleMessage(
        ws2,
        JSON.stringify({ type: "challenge.response", responses }),
      );

      await new Promise((r) => setTimeout(r, 50));

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
        lookups: {
          async lookupPublicKey() {
            return hexEncode(kp.publicKey);
          },
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

      const responses = await Promise.all(
        challengeFrame.challenges.map(
          async (c: { address: string; nonce: string }) => ({
            address: c.address,
            signature: await signChallenge(c.nonce, c.address, kp.privateKey),
          }),
        ),
      );

      router.handleMessage(
        ws2,
        JSON.stringify({ type: "challenge.response", responses }),
      );

      await new Promise((r) => setTimeout(r, 50));

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

  describe("agent.reconnected event", () => {
    test("fires for each verified address on reconnect", async () => {
      const kp = await generateKeyPair();
      const publicKeyHex = hexEncode(kp.publicKey);
      const reconnected: string[] = [];

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookups: {
          lookupPublicKey: async (addr) =>
            addr === "agent@local" ? publicKeyHex : null,
        },
      });
      router.events.on("agent.reconnected", ({ agentAddress }) => {
        reconnected.push(agentAddress);
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
      const signature = await signChallenge(nonce, address, kp.privateKey);

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
        lookups: {
          lookupPublicKey: async (addr) =>
            addr === "agent@local" ? publicKeyHex : null,
        },
      });
      router.events.on("agent.reconnected", ({ agentAddress }) => {
        reconnected.push(agentAddress);
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
      const signature = await signChallenge(nonce, address, wrongKp.privateKey);

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

    test("listener error does not prevent other addresses from reconnecting", async () => {
      const kp1 = await generateKeyPair();
      const kp2 = await generateKeyPair();
      const reconnected: string[] = [];

      const router = createSidecarRouter({
        requestTimeoutMs: 5000,
        lookups: {
          lookupPublicKey: async (addr) => {
            if (addr === "agent1@local") return hexEncode(kp1.publicKey);
            if (addr === "agent2@local") return hexEncode(kp2.publicKey);
            return null;
          },
        },
      });
      router.events.on("agent.reconnected", ({ agentAddress }) => {
        if (agentAddress === "agent1@local") throw new Error("DB failure");
        reconnected.push(agentAddress);
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

      const responses = await Promise.all(
        challengeFrame.challenges.map(
          async (c: { address: string; nonce: string }) => ({
            address: c.address,
            signature: await signChallenge(
              c.nonce,
              c.address,
              c.address === "agent1@local" ? kp1.privateKey : kp2.privateKey,
            ),
          }),
        ),
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

  describe("configuration guards", () => {
    test("sendAgentDeploy without hub key throws without mutating routing table", async () => {
      const router = createSidecarRouter({
        requestTimeoutMs: 500,
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

      await expect(
        router.sendAgentDeploy("new-agent@local", {
          agentId: "a1",
          agentAddress: "new-agent@local",
          sessionId: "s1",
          principalId: "p1",
          tenantId: "t1",
          systemPrompt: "test",
          tools: [],
          grants: [],
          sources: [
            {
              id: "test:m",
              provider: "test",
              apiKey: "k",
              baseURL: "http://localhost",
              model: "m",
            },
          ],
          defaultSource: "test:m",
        }),
      ).rejects.toThrow("Hub signing key is required");

      expect(router.getRoutableAddresses()).not.toContain("new-agent@local");
    });
  });

  describe("pack receive dispatch", () => {
    type RecordedReceive = {
      method: "receiveAgentStatePack" | "receiveWorkflowRunPack";
      repoId: RepoId;
      pack: Uint8Array;
      ref: string;
      commitSha: string;
    };

    function buildPackRouter(
      verdicts: {
        agentState?:
          | { accepted: true }
          | { accepted: false; reason: PackRejectReason };
        workflowRun?:
          | { accepted: true }
          | { accepted: false; reason: PackRejectReason };
      } = {},
    ) {
      const calls: RecordedReceive[] = [];
      const stateVerdict = verdicts.agentState ?? ({ accepted: true } as const);
      const wfrVerdict = verdicts.workflowRun ?? ({ accepted: true } as const);
      const packRouter = createSidecarRouter({
        requestTimeoutMs: 500,
        hubPublicKey: TEST_HUB_KEY,
        lookups: {
          async receiveAgentStatePack(repoId, pack, ref, commitSha) {
            calls.push({
              method: "receiveAgentStatePack",
              repoId,
              pack,
              ref,
              commitSha,
            });
            return stateVerdict;
          },
          async receiveWorkflowRunPack(repoId, pack, ref, commitSha) {
            calls.push({
              method: "receiveWorkflowRunPack",
              repoId,
              pack,
              ref,
              commitSha,
            });
            return wfrVerdict;
          },
        },
      });
      return { router: packRouter, calls };
    }

    function registerAddr(
      r: ReturnType<typeof createSidecarRouter>,
      ws: ReturnType<typeof createMockWs>,
      sidecarId: string,
      addr: string,
    ) {
      r.handleOpen(ws);
      r.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId,
          token: "tok",
          agentAddresses: [addr],
        }),
      );
    }

    function pushPack(
      r: ReturnType<typeof createSidecarRouter>,
      ws: ReturnType<typeof createMockWs>,
      args: {
        agentAddress: string;
        repoId: RepoId;
        transferId: string;
        pack: Uint8Array;
        ref: string;
        commitSha: string;
      },
    ) {
      for (const chunk of chunkPack(args.pack)) {
        r.handleMessage(
          ws,
          JSON.stringify({
            type: "repo.pack.push",
            agentAddress: args.agentAddress,
            repoId: args.repoId,
            transferId: args.transferId,
            seq: chunk.seq,
            data: chunk.data,
          }),
        );
      }
      r.handleMessage(
        ws,
        JSON.stringify({
          type: "repo.pack.done",
          agentAddress: args.agentAddress,
          repoId: args.repoId,
          transferId: args.transferId,
          ref: args.ref,
          commitSha: args.commitSha,
        }),
      );
    }

    test("workflow-run pack frames invoke receiveWorkflowRunPack and ack the sidecar", async () => {
      const { router: r, calls } = buildPackRouter();
      const ws = createMockWs();
      const addr = "agent-wfr@local";
      registerAddr(r, ws, "sc-wfr", addr);

      const transferId = "t-wfr-1";
      const repoId: RepoId = { kind: "workflow-run", id: "dep-wfr-1" };
      const ref = "refs/heads/events";
      const commitSha = "f".repeat(40);
      const pack = new Uint8Array([1, 2, 3, 4, 5]);

      pushPack(r, ws, {
        agentAddress: addr,
        repoId,
        transferId,
        pack,
        ref,
        commitSha,
      });

      // The receiveWorkflowRunPack lookup is async; wait a tick.
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(calls.length).toBe(1);
      const [call] = calls;
      if (call === undefined) throw new Error("expected one recorded call");
      expect(call.method).toBe("receiveWorkflowRunPack");
      expect(call.repoId).toEqual(repoId);
      expect(call.ref).toBe(ref);
      expect(call.commitSha).toBe(commitSha);
      expect(Array.from(call.pack)).toEqual(Array.from(pack));

      const ack = lastSent(ws);
      expect(ack.type).toBe("repo.pack.ack");
      expect(ack.transferId).toBe(transferId);
      expect(ack.repoId).toEqual(repoId);
    });

    test("agent-state and workflow-run packs use independent receivers (concurrent transferIds)", async () => {
      const { router: r, calls } = buildPackRouter();
      const ws = createMockWs();
      const addr = "agent-mix@local";
      registerAddr(r, ws, "sc-mix", addr);

      // Reuse the same transferId across kinds. The two receivers'
      // in-flight state must be independent, so this must NOT collide.
      const transferId = "shared-transfer";

      const statePack = new Uint8Array([10, 11, 12]);
      const stateRepoId: RepoId = { kind: "agent-state", id: addr };

      const wfrPack = new Uint8Array([20, 21, 22]);
      const wfrRepoId: RepoId = { kind: "workflow-run", id: "dep-mix-1" };

      // Push the agent-state chunk first, then a workflow-run chunk
      // sharing the same transferId. If state were shared, the
      // workflow-run push would either evict the state transfer or get
      // rejected as a duplicate.
      r.handleMessage(
        ws,
        JSON.stringify({
          type: "repo.pack.push",
          agentAddress: addr,
          repoId: stateRepoId,
          transferId,
          seq: 0,
          data: btoa(String.fromCharCode(...statePack)),
        }),
      );
      r.handleMessage(
        ws,
        JSON.stringify({
          type: "repo.pack.push",
          agentAddress: addr,
          repoId: wfrRepoId,
          transferId,
          seq: 0,
          data: btoa(String.fromCharCode(...wfrPack)),
        }),
      );

      // Verify no rejection was sent before the done frames arrive.
      for (const sent of ws.sent) {
        const parsed: { type: string } = JSON.parse(sent);
        expect(parsed.type).not.toBe("repo.pack.reject");
      }

      // Complete both transfers.
      r.handleMessage(
        ws,
        JSON.stringify({
          type: "repo.pack.done",
          agentAddress: addr,
          repoId: stateRepoId,
          transferId,
          ref: "refs/instances/test",
          commitSha: "a".repeat(40),
        }),
      );
      r.handleMessage(
        ws,
        JSON.stringify({
          type: "repo.pack.done",
          agentAddress: addr,
          repoId: wfrRepoId,
          transferId,
          ref: "refs/heads/events",
          commitSha: "b".repeat(40),
        }),
      );

      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(calls.map((c) => c.method).sort()).toEqual([
        "receiveAgentStatePack",
        "receiveWorkflowRunPack",
      ]);

      const stateCall = calls.find((c) => c.method === "receiveAgentStatePack");
      const wfrCall = calls.find((c) => c.method === "receiveWorkflowRunPack");
      if (stateCall === undefined) {
        throw new Error("expected an agent-state receive call");
      }
      if (wfrCall === undefined) {
        throw new Error("expected a workflow-run receive call");
      }
      expect(Array.from(stateCall.pack)).toEqual(Array.from(statePack));
      expect(Array.from(wfrCall.pack)).toEqual(Array.from(wfrPack));
    });

    test("workflow-run pack receive rejection is forwarded to the sidecar", async () => {
      const { router: r } = buildPackRouter({
        workflowRun: { accepted: false, reason: "path_violation" },
      });
      const ws = createMockWs();
      const addr = "agent-wfr-rej@local";
      registerAddr(r, ws, "sc-wfr-rej", addr);

      const transferId = "t-wfr-rej";
      const repoId: RepoId = { kind: "workflow-run", id: "dep-wfr-rej" };
      pushPack(r, ws, {
        agentAddress: addr,
        repoId,
        transferId,
        pack: new Uint8Array([9, 9, 9]),
        ref: "refs/heads/events",
        commitSha: "c".repeat(40),
      });

      await new Promise((resolve) => setTimeout(resolve, 0));

      const last = lastSent(ws);
      expect(last.type).toBe("repo.pack.reject");
      expect(last.reason).toBe("path_violation");
      expect(last.transferId).toBe(transferId);
      expect(last.repoId).toEqual(repoId);
    });
  });

  describe("sendDrain", () => {
    test("ships a drain.deliver frame to the sidecar holding the deployment", () => {
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: "sc-drain-1",
          token: "tok",
          agentAddresses: ["dep@integration.interchange"],
        }),
      );
      ws.sent.length = 0;

      router.sendDrain({
        agentAddress: "dep@integration.interchange",
        deadlineMs: 7_500,
      });

      const last = lastSent(ws);
      expect(last.type).toBe("drain.deliver");
      expect(last.agentAddress).toBe("dep@integration.interchange");
      expect(last.deadlineMs).toBe(7_500);
    });

    test("throws when no sidecar is registered for the address", () => {
      expect(() =>
        router.sendDrain({
          agentAddress: "absent@integration.interchange",
          deadlineMs: 1_000,
        }),
      ).toThrow(/No sidecar connected/);
    });
  });
});
