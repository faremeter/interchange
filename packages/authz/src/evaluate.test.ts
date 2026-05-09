import { describe, test, expect } from "bun:test";

import { authorize, evaluateGrants } from "./evaluate";
import type { ConditionRegistry, GrantRule, GrantStore } from "./types";

function grant(
  overrides: Partial<GrantRule> &
    Pick<GrantRule, "resource" | "action" | "effect">,
): GrantRule {
  return {
    id: `grt_${Math.random().toString(36).slice(2, 10)}`,
    origin: "role",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
    ...overrides,
  };
}

describe("evaluateGrants", () => {
  test("returns null effect when no grants match", async () => {
    const result = await evaluateGrants([], "agent:*", "read");

    expect(result.effect).toBeNull();
    expect(result.matchingGrants).toHaveLength(0);
    expect(result.resolvedBy).toBeNull();
  });

  test("returns null when grants exist but none match", async () => {
    const grants = [
      grant({ resource: "wallet:*", action: "read", effect: "allow" }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "create");

    expect(result.effect).toBeNull();
    expect(result.matchingGrants).toHaveLength(0);
  });

  test("single matching grant determines effect", async () => {
    const grants = [grant({ resource: "*", action: "read", effect: "allow" })];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read");

    expect(result.effect).toBe("allow");
    expect(result.matchingGrants).toHaveLength(1);
    expect(result.resolvedBy).not.toBeNull();
  });

  test("more specific grant beats less specific", async () => {
    const grants = [
      grant({ resource: "*", action: "*", effect: "allow" }),
      grant({ resource: "agent:*", action: "read", effect: "deny" }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read");

    expect(result.effect).toBe("deny");
    expect(result.matchingGrants).toHaveLength(2);
    expect(result.resolvedBy?.resource).toBe("agent:*");
  });

  test("at equal specificity deny beats allow", async () => {
    const grants = [
      grant({ resource: "agent:*", action: "read", effect: "allow" }),
      grant({ resource: "agent:*", action: "read", effect: "deny" }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read");

    expect(result.effect).toBe("deny");
  });

  test("at equal specificity ask beats allow", async () => {
    const grants = [
      grant({ resource: "agent:*", action: "read", effect: "allow" }),
      grant({ resource: "agent:*", action: "read", effect: "ask" }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read");

    expect(result.effect).toBe("ask");
  });

  test("at equal specificity deny beats ask", async () => {
    const grants = [
      grant({ resource: "agent:*", action: "read", effect: "ask" }),
      grant({ resource: "agent:*", action: "read", effect: "deny" }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read");

    expect(result.effect).toBe("deny");
  });

  test("exact resource match beats wildcard even with weaker effect", async () => {
    const grants = [
      grant({ resource: "agent:*", action: "read", effect: "deny" }),
      grant({ resource: "agent:agt_abc", action: "read", effect: "allow" }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read");

    expect(result.effect).toBe("allow");
    expect(result.resolvedBy?.resource).toBe("agent:agt_abc");
  });

  test("wildcard action matches specific action", async () => {
    const grants = [grant({ resource: "*", action: "*", effect: "allow" })];

    const result = await evaluateGrants(grants, "credential:crd_123", "manage");

    expect(result.effect).toBe("allow");
  });

  test("owner wildcard grant allows everything", async () => {
    const grants = [
      grant({
        resource: "*",
        action: "*",
        effect: "allow",
        origin: "system",
      }),
    ];

    expect((await evaluateGrants(grants, "agent:*", "read")).effect).toBe(
      "allow",
    );
    expect(
      (await evaluateGrants(grants, "wallet:wal_123", "manage")).effect,
    ).toBe("allow");
    expect((await evaluateGrants(grants, "grant:*", "create")).effect).toBe(
      "allow",
    );
  });

  test("member read-only grant allows read but not create", async () => {
    const grants = [grant({ resource: "*", action: "read", effect: "allow" })];

    expect((await evaluateGrants(grants, "agent:*", "read")).effect).toBe(
      "allow",
    );
    expect(
      (await evaluateGrants(grants, "agent:*", "create")).effect,
    ).toBeNull();
    expect(
      (await evaluateGrants(grants, "agent:*", "manage")).effect,
    ).toBeNull();
  });

  test("admin grants allow read, create, and manage", async () => {
    const grants = [
      grant({ resource: "*", action: "read", effect: "allow" }),
      grant({ resource: "*", action: "create", effect: "allow" }),
      grant({ resource: "*", action: "manage", effect: "allow" }),
    ];

    expect((await evaluateGrants(grants, "agent:*", "read")).effect).toBe(
      "allow",
    );
    expect((await evaluateGrants(grants, "agent:*", "create")).effect).toBe(
      "allow",
    );
    expect(
      (await evaluateGrants(grants, "wallet:wal_123", "manage")).effect,
    ).toBe("allow");
  });

  test("agent-specific grants from seed data", async () => {
    const grants = [
      grant({
        resource: "documents:*",
        action: "read",
        effect: "allow",
        origin: "creator",
      }),
      grant({
        resource: "documents:*",
        action: "write",
        effect: "ask",
        origin: "creator",
      }),
    ];

    expect(
      (await evaluateGrants(grants, "documents:doc_1", "read")).effect,
    ).toBe("allow");
    expect(
      (await evaluateGrants(grants, "documents:doc_1", "write")).effect,
    ).toBe("ask");
    expect(
      (await evaluateGrants(grants, "repos:repo_1", "read")).effect,
    ).toBeNull();
  });

  test("reports all matching grants in result", async () => {
    const grants = [
      grant({ id: "g1", resource: "*", action: "*", effect: "allow" }),
      grant({ id: "g2", resource: "agent:*", action: "*", effect: "allow" }),
      grant({ id: "g3", resource: "agent:*", action: "read", effect: "allow" }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read");

    expect(result.matchingGrants).toHaveLength(3);
    expect(result.resolvedBy?.id).toBe("g3");
  });

  test("grant with null expiresAt is always evaluated", async () => {
    const grants = [grant({ resource: "*", action: "read", effect: "allow" })];

    const result = await evaluateGrants(grants, "agent:*", "read");
    expect(result.effect).toBe("allow");
  });
});

describe("evaluateGrants determinism", () => {
  test("duplicate grants with identical specificity and effect resolve consistently", async () => {
    const grants = [
      grant({ id: "g1", resource: "agent:*", action: "read", effect: "allow" }),
      grant({ id: "g2", resource: "agent:*", action: "read", effect: "allow" }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read");

    expect(result.effect).toBe("allow");
    expect(result.matchingGrants).toHaveLength(2);
    expect(result.resolvedBy).not.toBeNull();
  });

  test("three-way effect tiebreaker at equal specificity: deny wins", async () => {
    const grants = [
      grant({ resource: "agent:*", action: "read", effect: "allow" }),
      grant({ resource: "agent:*", action: "read", effect: "ask" }),
      grant({ resource: "agent:*", action: "read", effect: "deny" }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read");

    expect(result.effect).toBe("deny");
    expect(result.matchingGrants).toHaveLength(3);
  });

  test("effect tiebreaker is independent of grant input order", async () => {
    const perms: ("allow" | "ask" | "deny")[] = ["deny", "allow", "ask"];

    for (const effect1 of perms) {
      const grants = [
        grant({ resource: "agent:*", action: "read", effect: effect1 }),
        grant({ resource: "agent:*", action: "read", effect: "deny" }),
      ];

      const result = await evaluateGrants(grants, "agent:agt_abc", "read");
      expect(result.effect).toBe("deny");
    }
  });
});

describe("evaluateGrants origin neutrality", () => {
  test("origin does not affect evaluation precedence", async () => {
    const origins = ["system", "role", "creator", "invoker"] as const;

    for (const origin of origins) {
      const grants = [
        grant({ resource: "agent:*", action: "read", effect: "allow", origin }),
      ];

      const result = await evaluateGrants(grants, "agent:agt_abc", "read");
      expect(result.effect).toBe("allow");
    }
  });

  test("grants with different sources but same pattern resolve by specificity", async () => {
    const grants = [
      grant({
        resource: "*",
        action: "*",
        effect: "allow",
        origin: "system",
      }),
      grant({
        resource: "agent:*",
        action: "read",
        effect: "deny",
        origin: "creator",
      }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read");

    expect(result.effect).toBe("deny");
  });
});

describe("evaluateGrants conditions", () => {
  test("grants with null conditions are unaffected by missing registry", async () => {
    const grants = [grant({ resource: "*", action: "read", effect: "allow" })];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read");
    expect(result.effect).toBe("allow");
  });

  test("grants with non-null conditions are skipped when no registry provided (fail-closed)", async () => {
    const grants = [
      grant({
        resource: "wallet:*",
        action: "spend",
        effect: "allow",
        conditions: { max_spend_per_day: 100 },
      }),
    ];

    const result = await evaluateGrants(grants, "wallet:wal_abc", "spend");
    expect(result.effect).toBeNull();
    expect(result.matchingGrants).toHaveLength(0);
  });

  test("grant with conditions evaluated to true is included", async () => {
    const registry: ConditionRegistry = {
      max_spend_per_day: () => true,
    };
    const grants = [
      grant({
        resource: "wallet:*",
        action: "spend",
        effect: "allow",
        conditions: { max_spend_per_day: 100 },
      }),
    ];

    const result = await evaluateGrants(grants, "wallet:wal_abc", "spend", {
      registry,
    });
    expect(result.effect).toBe("allow");
    expect(result.matchingGrants).toHaveLength(1);
  });

  test("grant with conditions evaluated to false is excluded", async () => {
    const registry: ConditionRegistry = {
      max_spend_per_day: () => false,
    };
    const grants = [
      grant({
        resource: "wallet:*",
        action: "spend",
        effect: "allow",
        conditions: { max_spend_per_day: 100 },
      }),
    ];

    const result = await evaluateGrants(grants, "wallet:wal_abc", "spend", {
      registry,
    });
    expect(result.effect).toBeNull();
  });

  test("unknown condition key in registry throws during evaluation", async () => {
    const grants = [
      grant({
        resource: "wallet:*",
        action: "spend",
        effect: "allow",
        conditions: { unknown_cond: true },
      }),
    ];

    expect(
      evaluateGrants(grants, "wallet:wal_abc", "spend", { registry: {} }),
    ).rejects.toThrow('Unknown condition: "unknown_cond"');
  });

  test("conditional and unconditional grants coexist", async () => {
    const registry: ConditionRegistry = {
      time_window: () => false,
    };
    const grants = [
      grant({
        id: "g1",
        resource: "agent:*",
        action: "read",
        effect: "allow",
        conditions: { time_window: { start: "09:00", end: "17:00" } },
      }),
      grant({
        id: "g2",
        resource: "*",
        action: "*",
        effect: "allow",
      }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read", {
      registry,
    });

    // g1 is skipped (condition failed), g2 matches
    expect(result.effect).toBe("allow");
    expect(result.matchingGrants).toHaveLength(1);
    expect(result.resolvedBy?.id).toBe("g2");
  });

  test("more specific conditional grant beats less specific unconditional", async () => {
    const registry: ConditionRegistry = {
      time_window: () => true,
    };
    const grants = [
      grant({
        id: "g1",
        resource: "*",
        action: "*",
        effect: "allow",
      }),
      grant({
        id: "g2",
        resource: "agent:*",
        action: "read",
        effect: "deny",
        conditions: { time_window: { start: "09:00", end: "17:00" } },
      }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read", {
      registry,
    });

    expect(result.effect).toBe("deny");
    expect(result.resolvedBy?.id).toBe("g2");
  });

  test("context fields are passed through to condition evaluators", async () => {
    let capturedPrincipal = "";
    let capturedTenant = "";
    const registry: ConditionRegistry = {
      spy: (_value, ctx) => {
        capturedPrincipal = ctx.principalId;
        capturedTenant = ctx.tenantId;
        return true;
      },
    };
    const grants = [
      grant({
        resource: "*",
        action: "*",
        effect: "allow",
        conditions: { spy: null },
      }),
    ];

    await evaluateGrants(grants, "agent:*", "read", {
      registry,
      principalId: "prn_ctx",
      tenantId: "tnt_ctx",
    });

    expect(capturedPrincipal).toBe("prn_ctx");
    expect(capturedTenant).toBe("tnt_ctx");
  });
});

describe("evaluateGrants nested resource patterns", () => {
  test("api:stripe:* matches api:stripe:charges", async () => {
    const grants = [
      grant({ resource: "api:stripe:*", action: "invoke", effect: "allow" }),
    ];

    const result = await evaluateGrants(grants, "api:stripe:charges", "invoke");

    expect(result.effect).toBe("allow");
  });

  test("api:* matches api:stripe:charges (broader wildcard)", async () => {
    const grants = [
      grant({ resource: "api:*", action: "invoke", effect: "allow" }),
    ];

    const result = await evaluateGrants(grants, "api:stripe:charges", "invoke");

    expect(result.effect).toBe("allow");
  });

  test("more specific nested pattern beats broader wildcard", async () => {
    const grants = [
      grant({ resource: "api:*", action: "invoke", effect: "allow" }),
      grant({ resource: "api:stripe:*", action: "invoke", effect: "deny" }),
    ];

    const result = await evaluateGrants(grants, "api:stripe:charges", "invoke");

    expect(result.effect).toBe("deny");
    expect(result.resolvedBy?.resource).toBe("api:stripe:*");
  });
});

describe("evaluateGrants no-match contract", () => {
  test("no-match returns null effect, not ask", async () => {
    const grants = [
      grant({ resource: "wallet:*", action: "read", effect: "allow" }),
    ];

    const result = await evaluateGrants(grants, "agent:agt_abc", "read");

    expect(result.effect).toBeNull();
    expect(result.effect).not.toBe("ask");
  });

  test("empty grants with empty resource and action returns null", async () => {
    const result = await evaluateGrants([], "", "");

    expect(result.effect).toBeNull();
    expect(result.matchingGrants).toHaveLength(0);
    expect(result.resolvedBy).toBeNull();
  });
});

describe("evaluateGrants action verbs from spec", () => {
  test("invoke action is matched correctly", async () => {
    const grants = [
      grant({ resource: "tool:bash", action: "invoke", effect: "allow" }),
    ];

    expect((await evaluateGrants(grants, "tool:bash", "invoke")).effect).toBe(
      "allow",
    );
    expect(
      (await evaluateGrants(grants, "tool:bash", "read")).effect,
    ).toBeNull();
  });

  test("spend action with conditions via registry", async () => {
    const registry: ConditionRegistry = {
      max_spend_per_day: (value) => typeof value === "number" && value > 50,
    };
    const grants = [
      grant({
        resource: "wallet:*",
        action: "spend",
        effect: "allow",
        conditions: { max_spend_per_day: 100 },
      }),
    ];

    expect(
      (await evaluateGrants(grants, "wallet:wal_123", "spend", { registry }))
        .effect,
    ).toBe("allow");
    expect(
      (await evaluateGrants(grants, "wallet:wal_123", "manage", { registry }))
        .effect,
    ).toBeNull();
  });

  test("wildcard action at lower specificity loses to exact action", async () => {
    const grants = [
      grant({ resource: "tool:*", action: "*", effect: "allow" }),
      grant({ resource: "tool:*", action: "invoke", effect: "deny" }),
    ];

    const result = await evaluateGrants(grants, "tool:bash", "invoke");

    expect(result.effect).toBe("deny");
  });
});

describe("evaluateGrants large grant set", () => {
  test("correct resolution across many grants of varying specificity", async () => {
    const grants = [
      grant({ id: "g01", resource: "*", action: "*", effect: "allow" }),
      grant({ id: "g02", resource: "agent:*", action: "*", effect: "allow" }),
      grant({
        id: "g03",
        resource: "agent:*",
        action: "read",
        effect: "allow",
      }),
      grant({
        id: "g04",
        resource: "wallet:*",
        action: "read",
        effect: "allow",
      }),
      grant({
        id: "g05",
        resource: "wallet:*",
        action: "spend",
        effect: "ask",
      }),
      grant({
        id: "g06",
        resource: "tool:*",
        action: "invoke",
        effect: "allow",
      }),
      grant({
        id: "g07",
        resource: "tool:bash",
        action: "invoke",
        effect: "deny",
      }),
      grant({
        id: "g08",
        resource: "credential:*",
        action: "use",
        effect: "ask",
      }),
      grant({
        id: "g09",
        resource: "api:stripe:*",
        action: "invoke",
        effect: "allow",
      }),
      grant({
        id: "g10",
        resource: "api:stripe:charges",
        action: "invoke",
        effect: "deny",
      }),
      grant({
        id: "g11",
        resource: "documents:*",
        action: "read",
        effect: "allow",
      }),
      grant({
        id: "g12",
        resource: "documents:*",
        action: "write",
        effect: "ask",
      }),
    ];

    const bash = await evaluateGrants(grants, "tool:bash", "invoke");
    expect(bash.effect).toBe("deny");
    expect(bash.resolvedBy?.id).toBe("g07");

    const charges = await evaluateGrants(
      grants,
      "api:stripe:charges",
      "invoke",
    );
    expect(charges.effect).toBe("deny");
    expect(charges.resolvedBy?.id).toBe("g10");

    const agentRead = await evaluateGrants(grants, "agent:agt_abc", "read");
    expect(agentRead.effect).toBe("allow");
    expect(agentRead.resolvedBy?.id).toBe("g03");

    const walletSpend = await evaluateGrants(grants, "wallet:wal_123", "spend");
    expect(walletSpend.effect).toBe("ask");
    expect(walletSpend.resolvedBy?.id).toBe("g05");

    const agentCreate = await evaluateGrants(grants, "agent:agt_abc", "create");
    expect(agentCreate.effect).toBe("allow");
    expect(agentCreate.resolvedBy?.id).toBe("g02");

    const docWrite = await evaluateGrants(grants, "documents:doc_1", "write");
    expect(docWrite.effect).toBe("ask");
    expect(docWrite.resolvedBy?.id).toBe("g12");
  });
});

function memoryStore(grants: GrantRule[]): GrantStore {
  return {
    async collectGrants() {
      return grants;
    },
  };
}

describe("authorize with in-memory store", () => {
  test("delegates to evaluateGrants with collected grants", async () => {
    const store = memoryStore([
      grant({ resource: "*", action: "*", effect: "allow" }),
    ]);

    const result = await authorize(
      store,
      "prn_1",
      "tnt_1",
      "agent:agt_abc",
      "read",
    );

    expect(result.effect).toBe("allow");
    expect(result.matchingGrants).toHaveLength(1);
  });

  test("returns null when store returns no grants", async () => {
    const store = memoryStore([]);

    const result = await authorize(
      store,
      "prn_1",
      "tnt_1",
      "agent:agt_abc",
      "read",
    );

    expect(result.effect).toBeNull();
  });

  test("specificity resolution works through authorize", async () => {
    const store = memoryStore([
      grant({ resource: "*", action: "*", effect: "allow" }),
      grant({ resource: "agent:*", action: "read", effect: "deny" }),
    ]);

    const result = await authorize(
      store,
      "prn_1",
      "tnt_1",
      "agent:agt_abc",
      "read",
    );

    expect(result.effect).toBe("deny");
    expect(result.resolvedBy?.resource).toBe("agent:*");
  });

  test("store receives principalId and tenantId", async () => {
    let receivedPrincipalId = "";
    let receivedTenantId = "";

    const store: GrantStore = {
      async collectGrants(principalId, tenantId) {
        receivedPrincipalId = principalId;
        receivedTenantId = tenantId;
        return [grant({ resource: "*", action: "*", effect: "allow" })];
      },
    };

    await authorize(store, "prn_abc", "tnt_xyz", "agent:*", "read");

    expect(receivedPrincipalId).toBe("prn_abc");
    expect(receivedTenantId).toBe("tnt_xyz");
  });

  test("full scenario: role-based grants via store", async () => {
    const store = memoryStore([
      grant({ resource: "*", action: "read", effect: "allow", origin: "role" }),
      grant({
        resource: "wallet:*",
        action: "spend",
        effect: "ask",
        origin: "role",
      }),
      grant({
        resource: "tool:bash",
        action: "invoke",
        effect: "deny",
        origin: "creator",
      }),
    ]);

    const read = await authorize(
      store,
      "prn_1",
      "tnt_1",
      "agent:agt_1",
      "read",
    );
    expect(read.effect).toBe("allow");

    const spend = await authorize(
      store,
      "prn_1",
      "tnt_1",
      "wallet:wal_1",
      "spend",
    );
    expect(spend.effect).toBe("ask");

    const bash = await authorize(
      store,
      "prn_1",
      "tnt_1",
      "tool:bash",
      "invoke",
    );
    expect(bash.effect).toBe("deny");

    const noMatch = await authorize(
      store,
      "prn_1",
      "tnt_1",
      "agent:agt_1",
      "manage",
    );
    expect(noMatch.effect).toBeNull();
  });

  test("passes registry through to condition evaluation", async () => {
    const registry: ConditionRegistry = {
      time_window: () => true,
    };
    const store = memoryStore([
      grant({
        resource: "agent:*",
        action: "read",
        effect: "allow",
        conditions: { time_window: { start: "09:00", end: "17:00" } },
      }),
    ]);

    const result = await authorize(
      store,
      "prn_1",
      "tnt_1",
      "agent:agt_abc",
      "read",
      registry,
    );

    expect(result.effect).toBe("allow");
  });

  test("conditional grant skipped without registry in authorize", async () => {
    const store = memoryStore([
      grant({
        resource: "agent:*",
        action: "read",
        effect: "allow",
        conditions: { time_window: { start: "09:00", end: "17:00" } },
      }),
    ]);

    const result = await authorize(
      store,
      "prn_1",
      "tnt_1",
      "agent:agt_abc",
      "read",
    );

    expect(result.effect).toBeNull();
  });

  test("authorize passes principalId and tenantId to condition context", async () => {
    let capturedPrincipal = "";
    let capturedTenant = "";
    const registry: ConditionRegistry = {
      spy: (_v, ctx) => {
        capturedPrincipal = ctx.principalId;
        capturedTenant = ctx.tenantId;
        return true;
      },
    };
    const store = memoryStore([
      grant({
        resource: "*",
        action: "*",
        effect: "allow",
        conditions: { spy: null },
      }),
    ]);

    await authorize(
      store,
      "prn_check",
      "tnt_check",
      "agent:*",
      "read",
      registry,
    );

    expect(capturedPrincipal).toBe("prn_check");
    expect(capturedTenant).toBe("tnt_check");
  });
});

describe("evaluateGrants — expiry", () => {
  test("expired grant is skipped", async () => {
    const past = new Date(Date.now() - 60_000);
    const grants = [
      grant({
        resource: "tool:bash",
        action: "invoke",
        effect: "allow",
        expiresAt: past,
      }),
    ];

    const result = await evaluateGrants(grants, "tool:bash", "invoke");
    expect(result.effect).toBeNull();
  });

  test("non-expired grant is evaluated normally", async () => {
    const future = new Date(Date.now() + 60_000);
    const grants = [
      grant({
        resource: "tool:bash",
        action: "invoke",
        effect: "allow",
        expiresAt: future,
      }),
    ];

    const result = await evaluateGrants(grants, "tool:bash", "invoke");
    expect(result.effect).toBe("allow");
  });

  test("expired deny does not override a live allow", async () => {
    const past = new Date(Date.now() - 60_000);
    const grants = [
      grant({
        resource: "tool:bash",
        action: "invoke",
        effect: "allow",
        expiresAt: null,
      }),
      grant({
        resource: "tool:bash",
        action: "invoke",
        effect: "deny",
        expiresAt: past,
      }),
    ];

    const result = await evaluateGrants(grants, "tool:bash", "invoke");
    expect(result.effect).toBe("allow");
  });

  test("null expiresAt is treated as never-expiring", async () => {
    const grants = [
      grant({
        resource: "tool:bash",
        action: "invoke",
        effect: "allow",
        expiresAt: null,
      }),
    ];

    const result = await evaluateGrants(grants, "tool:bash", "invoke");
    expect(result.effect).toBe("allow");
  });
});
