import { describe, test, expect } from "bun:test";

import { createAuditCollector } from "./audit-collector";
import type { AuthzDecision } from "./authz-extension";
import type { InferenceEvent } from "@interchange/types/runtime";

function makeDecision(overrides: Partial<AuthzDecision> = {}): AuthzDecision {
  return {
    callId: "call-1",
    tool: "bash",
    resource: "tool:bash",
    action: "invoke",
    effect: "allow",
    resolvedBy: null,
    matchingGrants: [],
    blocked: false,
    blockReason: undefined,
    error: undefined,
    ...overrides,
  };
}

function toolStart(
  callId: string,
  name: string,
  args: Record<string, unknown> = {},
  seq = 1,
): InferenceEvent {
  return {
    type: "tool.start",
    seq,
    data: { call: { id: callId, name, arguments: args } },
  };
}

function toolDone(
  callId: string,
  content: string | Record<string, unknown>,
  seq = 2,
  isError = false,
): InferenceEvent {
  return {
    type: "tool.done",
    seq,
    data: { result: { callId, content, isError } },
  };
}

describe("createAuditCollector", () => {
  test("collects a complete allowed tool invocation", () => {
    const collector = createAuditCollector("session-1");

    collector.onDecision(makeDecision({ callId: "c1" }));
    collector.onEvent(toolStart("c1", "bash", { cmd: "ls" }));
    collector.onEvent(toolDone("c1", "file.txt", 5));

    const records = collector.flush();
    expect(records.length).toBe(1);
    const r = records[0];
    if (r === undefined) throw new Error("expected record");

    expect(r.callId).toBe("c1");
    expect(r.tool).toBe("bash");
    expect(r.arguments).toEqual({ cmd: "ls" });
    expect(r.result.content).toBe("file.txt");
    expect(r.result.isError).toBe(false);
    expect(r.sessionId).toBe("session-1");
    expect(r.seq).toBe(5);
    expect(r.authz).not.toBeNull();
    if (r.authz === null) throw new Error("expected authz");
    expect(r.authz.effect).toBe("allow");
    expect(r.authz.blocked).toBe(false);
  });

  test("collects a blocked tool invocation (no tool.start)", () => {
    const collector = createAuditCollector("session-1");

    collector.onDecision(
      makeDecision({
        callId: "c1",
        effect: "deny",
        blocked: true,
        blockReason: "Denied by policy: tool:bash/invoke",
      }),
    );
    collector.onEvent(
      toolDone("c1", "Denied by policy: tool:bash/invoke", 3, true),
    );

    const records = collector.flush();
    expect(records.length).toBe(1);
    const r = records[0];
    if (r === undefined) throw new Error("expected record");

    expect(r.callId).toBe("c1");
    expect(r.tool).toBe("bash");
    expect(r.result.isError).toBe(true);
    expect(r.result.content).toBe("Denied by policy: tool:bash/invoke");
    expect(r.authz).not.toBeNull();
    if (r.authz === null) throw new Error("expected authz");
    expect(r.authz.effect).toBe("deny");
    expect(r.authz.blocked).toBe(true);
  });

  test("collects tool invocation without authz extension", () => {
    const collector = createAuditCollector("session-1");

    collector.onEvent(toolStart("c1", "curl", { url: "http://x" }));
    collector.onEvent(toolDone("c1", { status: 200 }, 4));

    const records = collector.flush();
    expect(records.length).toBe(1);
    const r = records[0];
    if (r === undefined) throw new Error("expected record");

    expect(r.authz).toBeNull();
    expect(r.result.content).toEqual({ status: 200 });
  });

  test("handles parallel tool calls with same name", () => {
    const collector = createAuditCollector("session-1");

    collector.onDecision(makeDecision({ callId: "c1", tool: "bash" }));
    collector.onDecision(makeDecision({ callId: "c2", tool: "bash" }));
    collector.onEvent(toolStart("c1", "bash", { cmd: "ls" }, 1));
    collector.onEvent(toolStart("c2", "bash", { cmd: "pwd" }, 2));
    collector.onEvent(toolDone("c1", "/home", 3));
    collector.onEvent(toolDone("c2", "/tmp", 4));

    const records = collector.flush();
    expect(records.length).toBe(2);

    const r1 = records.find((r) => r.callId === "c1");
    const r2 = records.find((r) => r.callId === "c2");
    if (r1 === undefined || r2 === undefined)
      throw new Error("expected both records");

    expect(r1.arguments).toEqual({ cmd: "ls" });
    expect(r1.result.content).toBe("/home");
    expect(r2.arguments).toEqual({ cmd: "pwd" });
    expect(r2.result.content).toBe("/tmp");
  });

  test("parallel mixed batch: one allowed, one blocked", () => {
    const collector = createAuditCollector("session-1");

    collector.onDecision(
      makeDecision({ callId: "c1", effect: "allow", blocked: false }),
    );
    collector.onDecision(
      makeDecision({
        callId: "c2",
        effect: "deny",
        blocked: true,
        blockReason: "denied",
      }),
    );

    collector.onEvent(toolStart("c1", "bash", {}, 1));
    collector.onEvent(toolDone("c2", "denied", 2, true));
    collector.onEvent(toolDone("c1", "ok", 3));

    const records = collector.flush();
    expect(records.length).toBe(2);

    const allowed = records.find((r) => r.callId === "c1");
    const blocked = records.find((r) => r.callId === "c2");
    if (allowed === undefined || blocked === undefined)
      throw new Error("expected both");

    expect(allowed.result.isError).toBe(false);
    expect(blocked.result.isError).toBe(true);
    if (blocked.authz === null) throw new Error("expected authz");
    expect(blocked.authz.blocked).toBe(true);
  });

  test("flush clears completed records", () => {
    const collector = createAuditCollector("session-1");

    collector.onEvent(toolStart("c1", "bash", {}, 1));
    collector.onEvent(toolDone("c1", "ok", 2));

    const first = collector.flush();
    expect(first.length).toBe(1);

    const second = collector.flush();
    expect(second.length).toBe(0);
  });

  test("flush does not return in-flight records", () => {
    const collector = createAuditCollector("session-1");

    collector.onDecision(makeDecision({ callId: "c1" }));
    collector.onEvent(toolStart("c1", "bash", {}, 1));
    // No tool.done yet

    const records = collector.flush();
    expect(records.length).toBe(0);
    expect(collector.pending()).toBe(1);
  });

  test("pending counts buffered decisions and in-flight records", () => {
    const collector = createAuditCollector("session-1");
    expect(collector.pending()).toBe(0);

    collector.onDecision(makeDecision({ callId: "c1" }));
    expect(collector.pending()).toBe(1);

    collector.onEvent(toolStart("c1", "bash", {}, 1));
    expect(collector.pending()).toBe(1);

    collector.onEvent(toolDone("c1", "ok", 2));
    expect(collector.pending()).toBe(0);
  });

  test("records across multiple flush cycles accumulate correctly", () => {
    const collector = createAuditCollector("session-1");

    collector.onEvent(toolStart("c1", "bash", {}, 1));
    collector.onEvent(toolDone("c1", "a", 2));
    expect(collector.flush().length).toBe(1);

    collector.onEvent(toolStart("c2", "curl", {}, 3));
    collector.onEvent(toolDone("c2", "b", 4));
    collector.onEvent(toolStart("c3", "node", {}, 5));
    collector.onEvent(toolDone("c3", "c", 6));
    expect(collector.flush().length).toBe(2);
  });

  test("authz decision with resolvedBy and matchingGrants", () => {
    const collector = createAuditCollector("session-1");

    collector.onDecision(
      makeDecision({
        callId: "c1",
        effect: "allow",
        resolvedBy: {
          id: "grant-1",
          resource: "tool:bash",
          action: "invoke",
          effect: "allow",
          source: "creator",
          specificity: 1009,
        },
        matchingGrants: [
          {
            id: "grant-1",
            resource: "tool:bash",
            action: "invoke",
            effect: "allow",
            source: "creator",
            specificity: 1009,
          },
        ],
      }),
    );
    collector.onEvent(toolStart("c1", "bash", {}, 1));
    collector.onEvent(toolDone("c1", "ok", 2));

    const records = collector.flush();
    const r = records[0];
    if (r === undefined) throw new Error("expected record");
    if (r.authz === null) throw new Error("expected authz");
    if (r.authz.resolvedBy === undefined || r.authz.resolvedBy === null)
      throw new Error("expected resolvedBy");

    expect(r.authz.resolvedBy.id).toBe("grant-1");
    expect(r.authz.resolvedBy.specificity).toBe(1009);
    expect(r.authz.matchingGrants.length).toBe(1);
  });

  test("ignores non-tool events", () => {
    const collector = createAuditCollector("session-1");

    collector.onEvent({
      type: "inference.start",
      seq: 1,
      data: { model: "test" },
    });
    collector.onEvent({
      type: "reactor.start",
      seq: 2,
      data: {},
    });

    expect(collector.flush().length).toBe(0);
    expect(collector.pending()).toBe(0);
  });

  test("emits degraded record on orphaned tool.done with no prior context", () => {
    const collector = createAuditCollector("session-1");

    collector.onEvent(toolDone("unknown-call", "result", 1));

    const records = collector.flush();
    expect(records.length).toBe(1);
    const r = records[0];
    if (r === undefined) throw new Error("expected record");

    expect(r.callId).toBe("unknown-call");
    expect(r.tool).toBe("$orphaned");
    expect(r.arguments).toEqual({});
    expect(r.authz).toBeNull();
    expect(r.result.content).toBe("result");
    expect(r.sessionId).toBe("session-1");
    expect(r.seq).toBe(1);
  });

  test("orphaned tool.done propagates isError into degraded record", () => {
    const collector = createAuditCollector("session-1");

    collector.onEvent(toolDone("orphan-err", "denied", 1, true));

    const records = collector.flush();
    expect(records.length).toBe(1);
    const r = records[0];
    if (r === undefined) throw new Error("expected record");

    expect(r.tool).toBe("$orphaned");
    expect(r.result.isError).toBe(true);
    expect(r.result.content).toBe("denied");
  });

  test("orphaned tool.done does not leak into pending count", () => {
    const collector = createAuditCollector("session-1");

    collector.onEvent(toolDone("orphan-1", "result", 1));

    expect(collector.pending()).toBe(0);
    expect(collector.flush().length).toBe(1);
  });
});
