import { describe, test, expect } from "bun:test";

import {
  createAuthzExtension,
  type AuthzCallResult,
  type AuthzDecision,
} from "./authz-extension";

import type {
  BeforeToolContext,
  ToolCall,
  ToolDefinition,
  ReactorState,
  TokenUsage,
} from "@intx/types/runtime";

// The before-tool context of a call no approver approved, and of a call
// whose suspension an approver approved.
const unapproved: BeforeToolContext = { approved: false };
const approved: BeforeToolContext = { approved: true };

function emptyUsage(): TokenUsage {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
}

function makeState(): ReactorState {
  return {
    sessionId: "test",
    turns: [],
    activeForks: [],
    pendingOperations: [],
    activeGates: [],
    tokenUsage: emptyUsage(),
    lastCycleUsage: null,
    lastCycleSource: null,
  };
}

function makeCall(name = "bash"): ToolCall {
  return { id: "call-1", name, arguments: {} };
}

function getDecision(decisions: AuthzDecision[]): AuthzDecision {
  const d = decisions[0];
  if (d === undefined) throw new Error("no decisions recorded");
  return d;
}

function allowResult(): AuthzCallResult {
  return {
    effect: "allow",
    matchingGrants: [
      {
        id: "grant-1",
        resource: "tool:bash",
        action: "invoke",
        effect: "allow",
        origin: "creator",
        specificity: 1009,
      },
    ],
    resolvedBy: {
      id: "grant-1",
      resource: "tool:bash",
      action: "invoke",
      effect: "allow",
      origin: "creator",
      specificity: 1009,
    },
  };
}

function denyResult(): AuthzCallResult {
  return {
    effect: "deny",
    matchingGrants: [
      {
        id: "grant-2",
        resource: "tool:bash",
        action: "invoke",
        effect: "deny",
        origin: "system",
        specificity: 1009,
      },
    ],
    resolvedBy: {
      id: "grant-2",
      resource: "tool:bash",
      action: "invoke",
      effect: "deny",
      origin: "system",
      specificity: 1009,
    },
  };
}

describe("createAuthzExtension", () => {
  const signal = new AbortController().signal;

  test("allow effect returns undefined and calls onDecision", async () => {
    const decisions: AuthzDecision[] = [];

    const ext = createAuthzExtension({
      authorize: async () => allowResult(),
      onDecision: (d) => decisions.push(d),
    });

    const result = await ext.beforeTool(
      makeCall(),
      makeState(),
      signal,
      unapproved,
    );

    expect(result.type).toBe("allow");
    const d = getDecision(decisions);
    expect(d.callId).toBe("call-1");
    expect(d.effect).toBe("allow");
    expect(d.blocked).toBe(false);
    expect(d.blockReason).toBeUndefined();
    expect(d.tool).toBe("bash");
    expect(d.resource).toBe("tool:bash");
    expect(d.action).toBe("invoke");
  });

  test("deny effect returns block reason and calls onDecision", async () => {
    const decisions: AuthzDecision[] = [];

    const ext = createAuthzExtension({
      authorize: async () => denyResult(),
      onDecision: (d) => decisions.push(d),
    });

    const result = await ext.beforeTool(
      makeCall(),
      makeState(),
      signal,
      unapproved,
    );

    expect(result.type).toBe("block");
    if (result.type !== "block") throw new Error("expected block");
    expect(result.reason).toBe("Denied by policy: tool:bash/invoke");
    const d = getDecision(decisions);
    expect(d.effect).toBe("deny");
    expect(d.blocked).toBe(true);
    expect(d.blockReason).toBe("Denied by policy: tool:bash/invoke");
    expect(d.resolvedBy).toBeDefined();
    if (d.resolvedBy === null) throw new Error("expected resolvedBy");
    expect(d.resolvedBy.id).toBe("grant-2");
  });

  test("ask effect suspends with a minted correlation and pending operation", async () => {
    const decisions: AuthzDecision[] = [];

    const ext = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      onDecision: (d) => decisions.push(d),
      approvalTimeoutMs: 60_000,
    });

    const before = Date.now();
    const result = await ext.beforeTool(
      makeCall(),
      makeState(),
      signal,
      unapproved,
    );
    const after = Date.now();

    expect(result.type).toBe("suspend");
    if (result.type !== "suspend") throw new Error("expected suspend");
    expect(result.gate.type).toBe("approval");
    expect(result.gate.correlationId).toBe(result.pendingOp.correlationId);
    expect(result.gate.gateId).toBe(`pending-${result.gate.correlationId}`);
    expect(result.pendingOp.gateId).toBe(result.gate.gateId);
    expect(result.pendingOp.kind).toBe("approval");
    expect(result.pendingOp.timeoutAt).toBe(result.gate.timeoutAt);
    // The deadline is the mint time plus the configured approval timeout.
    expect(result.gate.timeoutAt).toBeGreaterThanOrEqual(before + 60_000);
    expect(result.gate.timeoutAt).toBeLessThanOrEqual(after + 60_000);

    // A suspended call is neither cleanly blocked nor allowed: the recorded
    // decision keeps effect "ask" but is not marked blocked.
    const d = getDecision(decisions);
    expect(d.effect).toBe("ask");
    expect(d.blocked).toBe(false);
    expect(d.blockReason).toBeUndefined();
  });

  const bashToolDef: ToolDefinition = {
    name: "bash",
    description: "Run a shell command",
    inputSchema: { type: "object", properties: { cmd: { type: "string" } } },
  };

  test("ask builds an approval snapshot from the wired tool definition", async () => {
    const ext = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      toolDefinitions: [bashToolDef],
    });

    const call: ToolCall = {
      id: "call-1",
      name: "bash",
      arguments: { cmd: "ls" },
    };
    const result = await ext.beforeTool(call, makeState(), signal, unapproved);

    expect(result.type).toBe("suspend");
    if (result.type !== "suspend") throw new Error("expected suspend");
    expect(result.pendingOp.approvalSnapshot).toEqual({
      name: "bash",
      description: "Run a shell command",
      inputSchema: { type: "object", properties: { cmd: { type: "string" } } },
      arguments: { cmd: "ls" },
    });
  });

  test("ask throws when a wired extension lacks the tool's definition", async () => {
    const ext = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      toolDefinitions: [bashToolDef],
    });

    await expect(
      ext.beforeTool(
        makeCall("stripe_charge"),
        makeState(),
        signal,
        unapproved,
      ),
    ).rejects.toThrow(/stripe_charge.*wiring defect/s);
  });

  test("ask omits the snapshot when no tool definitions are wired", async () => {
    const ext = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
    });

    const result = await ext.beforeTool(
      makeCall(),
      makeState(),
      signal,
      unapproved,
    );
    expect(result.type).toBe("suspend");
    if (result.type !== "suspend") throw new Error("expected suspend");
    expect(result.pendingOp.approvalSnapshot).toBeUndefined();
  });

  test("ask throws for every tool when wired with an empty definition set", async () => {
    // An empty (but present) toolDefinitions array is the "wired" regime, not
    // the "unwired" one: the extension promised a definition for every tool it
    // authorizes and has none, so any ask is a wiring defect. This is the
    // sentinel distinction between a defined-but-empty map and no map at all.
    const ext = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      toolDefinitions: [],
    });

    await expect(
      ext.beforeTool(makeCall(), makeState(), signal, unapproved),
    ).rejects.toThrow(/bash.*wiring defect/s);
  });

  test("an approved call with a missing definition is allowed without throwing", async () => {
    const ext = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      toolDefinitions: [bashToolDef],
    });

    // A tool absent from the wired set throws at the ask branch, but an
    // approved call returns allow before the snapshot is built, so a
    // re-dispatched approved call is never blocked by the snapshot contract.
    const call: ToolCall = {
      id: "call-9",
      name: "stripe_charge",
      arguments: {},
    };
    const result = await ext.beforeTool(call, makeState(), signal, approved);
    expect(result.type).toBe("allow");
  });

  test("null effect (no matching grants) blocks fail-closed", async () => {
    const decisions: AuthzDecision[] = [];

    const ext = createAuthzExtension({
      authorize: async () => ({
        effect: null,
        matchingGrants: [],
        resolvedBy: null,
      }),
      onDecision: (d) => decisions.push(d),
    });

    const result = await ext.beforeTool(
      makeCall(),
      makeState(),
      signal,
      unapproved,
    );

    expect(result.type).toBe("block");
    if (result.type !== "block") throw new Error("expected block");
    expect(result.reason).toBe("No matching grants for tool:bash/invoke");
    const d = getDecision(decisions);
    expect(d.effect).toBeNull();
    expect(d.blocked).toBe(true);
  });

  test("authorize throwing calls onDecision with error and rethrows", async () => {
    const decisions: AuthzDecision[] = [];

    const ext = createAuthzExtension({
      authorize: async () => {
        throw new Error("DB connection failed");
      },
      onDecision: (d) => decisions.push(d),
    });

    let thrown: Error | undefined;
    try {
      await ext.beforeTool(makeCall(), makeState(), signal, unapproved);
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toBe("DB connection failed");

    const d = getDecision(decisions);
    expect(d.callId).toBe("call-1");
    expect(d.blocked).toBe(true);
    expect(d.error).toBe("DB connection failed");
    expect(d.blockReason).toBe("Authorization failed: DB connection failed");
    expect(d.effect).toBeNull();
  });

  test("onDecision is optional", async () => {
    const ext = createAuthzExtension({
      authorize: async () => allowResult(),
    });

    const result = await ext.beforeTool(
      makeCall(),
      makeState(),
      signal,
      unapproved,
    );
    expect(result.type).toBe("allow");
  });

  test("resource format uses tool:{name}", async () => {
    let capturedResource = "";

    const ext = createAuthzExtension({
      authorize: async (resource) => {
        capturedResource = resource;
        return allowResult();
      },
    });

    await ext.beforeTool(
      makeCall("stripe_charge"),
      makeState(),
      signal,
      unapproved,
    );
    expect(capturedResource).toBe("tool:stripe_charge");
  });

  test("matchingGrants are passed through to decision", async () => {
    const decisions: AuthzDecision[] = [];
    const expectedGrants = denyResult().matchingGrants;

    const ext = createAuthzExtension({
      authorize: async () => denyResult(),
      onDecision: (d) => decisions.push(d),
    });

    await ext.beforeTool(makeCall(), makeState(), signal, unapproved);

    const d = getDecision(decisions);
    expect(d.matchingGrants.length).toBe(expectedGrants.length);
    const firstGrant = d.matchingGrants[0];
    const expectedFirst = expectedGrants[0];
    if (firstGrant === undefined || expectedFirst === undefined)
      throw new Error("expected at least one grant");
    expect(firstGrant.id).toBe(expectedFirst.id);
  });

  test("onDecision throwing does not affect allow decision", async () => {
    const ext = createAuthzExtension({
      authorize: async () => allowResult(),
      onDecision: () => {
        throw new Error("audit log failed");
      },
    });

    const result = await ext.beforeTool(
      makeCall(),
      makeState(),
      signal,
      unapproved,
    );
    expect(result.type).toBe("allow");
  });

  test("onDecision throwing does not mask authorize error", async () => {
    const ext = createAuthzExtension({
      authorize: async () => {
        throw new Error("DB connection failed");
      },
      onDecision: () => {
        throw new Error("audit log failed");
      },
    });

    let thrown: Error | undefined;
    try {
      await ext.beforeTool(makeCall(), makeState(), signal, unapproved);
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toBe("DB connection failed");
  });

  function askResult(): AuthzCallResult {
    return { effect: "ask", matchingGrants: [], resolvedBy: null };
  }

  test("an approved call passes its ask gate", async () => {
    const ext = createAuthzExtension({ authorize: async () => askResult() });
    const result = await ext.beforeTool(
      makeCall(),
      makeState(),
      signal,
      approved,
    );
    expect(result.type).toBe("allow");
  });

  test("an unapproved call hits its ask gate", async () => {
    const ext = createAuthzExtension({ authorize: async () => askResult() });
    const result = await ext.beforeTool(
      makeCall(),
      makeState(),
      signal,
      unapproved,
    );
    expect(result.type).toBe("suspend");
  });

  test("an approved call is still blocked by a deny", async () => {
    const ext = createAuthzExtension({ authorize: async () => denyResult() });
    const result = await ext.beforeTool(
      makeCall(),
      makeState(),
      signal,
      approved,
    );
    expect(result.type).toBe("block");
  });

  test("an approval is not stored: a later unapproved run of the same id hits the gate", async () => {
    const ext = createAuthzExtension({ authorize: async () => askResult() });
    const first = await ext.beforeTool(
      makeCall(),
      makeState(),
      signal,
      approved,
    );
    expect(first.type).toBe("allow");
    const second = await ext.beforeTool(
      makeCall(),
      makeState(),
      signal,
      unapproved,
    );
    expect(second.type).toBe("suspend");
  });
});
