import { describe, test, expect } from "bun:test";

import {
  createAuthzExtension,
  type AuthzCallResult,
  type AuthzDecision,
} from "./authz-extension";

import type { ToolCall, ReactorState, TokenUsage } from "@intx/types/runtime";

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

    const result = await ext.beforeTool(makeCall(), makeState(), signal);

    expect(result).toBeUndefined();
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

    const result = await ext.beforeTool(makeCall(), makeState(), signal);

    expect(result).toBe("Denied by policy: tool:bash/invoke");
    const d = getDecision(decisions);
    expect(d.effect).toBe("deny");
    expect(d.blocked).toBe(true);
    expect(d.blockReason).toBe("Denied by policy: tool:bash/invoke");
    expect(d.resolvedBy).toBeDefined();
    if (d.resolvedBy === null) throw new Error("expected resolvedBy");
    expect(d.resolvedBy.id).toBe("grant-2");
  });

  test("ask effect blocks with approval message", async () => {
    const decisions: AuthzDecision[] = [];

    const ext = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      onDecision: (d) => decisions.push(d),
    });

    const result = await ext.beforeTool(makeCall(), makeState(), signal);

    expect(result).toBe("Requires approval: tool:bash/invoke");
    const d = getDecision(decisions);
    expect(d.effect).toBe("ask");
    expect(d.blocked).toBe(true);
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

    const result = await ext.beforeTool(makeCall(), makeState(), signal);

    expect(result).toBe("No matching grants for tool:bash/invoke");
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
      await ext.beforeTool(makeCall(), makeState(), signal);
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

    const result = await ext.beforeTool(makeCall(), makeState(), signal);
    expect(result).toBeUndefined();
  });

  test("resource format uses tool:{name}", async () => {
    let capturedResource = "";

    const ext = createAuthzExtension({
      authorize: async (resource) => {
        capturedResource = resource;
        return allowResult();
      },
    });

    await ext.beforeTool(makeCall("stripe_charge"), makeState(), signal);
    expect(capturedResource).toBe("tool:stripe_charge");
  });

  test("matchingGrants are passed through to decision", async () => {
    const decisions: AuthzDecision[] = [];
    const expectedGrants = denyResult().matchingGrants;

    const ext = createAuthzExtension({
      authorize: async () => denyResult(),
      onDecision: (d) => decisions.push(d),
    });

    await ext.beforeTool(makeCall(), makeState(), signal);

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

    const result = await ext.beforeTool(makeCall(), makeState(), signal);
    expect(result).toBeUndefined();
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
      await ext.beforeTool(makeCall(), makeState(), signal);
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toBe("DB connection failed");
  });
});
