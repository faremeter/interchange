import { describe, test, expect } from "bun:test";

import { createInMemoryGrantStore } from "./memory-store";
import type { GrantRule } from "./types";

function makeGrant(overrides: Partial<GrantRule> = {}): GrantRule {
  return {
    id: "grant-1",
    resource: "tool:bash",
    action: "invoke",
    effect: "allow",
    source: "creator",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: "p-1",
    ...overrides,
  };
}

describe("createInMemoryGrantStore", () => {
  test("returns grants matching the principalId", async () => {
    const store = createInMemoryGrantStore([
      makeGrant({ id: "g1", principalId: "p-1" }),
      makeGrant({ id: "g2", principalId: "p-2" }),
    ]);

    const results = await store.collectGrants("p-1", "t-1");
    expect(results.length).toBe(1);
    const first = results[0];
    if (first === undefined) throw new Error("expected a grant");
    expect(first.id).toBe("g1");
  });

  test("returns empty array when no grants match", async () => {
    const store = createInMemoryGrantStore([
      makeGrant({ principalId: "p-other" }),
    ]);

    const results = await store.collectGrants("p-1", "t-1");
    expect(results.length).toBe(0);
  });

  test("filters out expired grants", async () => {
    const past = new Date(Date.now() - 60_000);
    const store = createInMemoryGrantStore([
      makeGrant({ id: "expired", principalId: "p-1", expiresAt: past }),
      makeGrant({ id: "valid", principalId: "p-1" }),
    ]);

    const results = await store.collectGrants("p-1", "t-1");
    expect(results.length).toBe(1);
    const first = results[0];
    if (first === undefined) throw new Error("expected a grant");
    expect(first.id).toBe("valid");
  });

  test("includes grants with future expiry", async () => {
    const future = new Date(Date.now() + 60_000);
    const store = createInMemoryGrantStore([
      makeGrant({ principalId: "p-1", expiresAt: future }),
    ]);

    const results = await store.collectGrants("p-1", "t-1");
    expect(results.length).toBe(1);
  });

  test("tenantId does not affect filtering", async () => {
    const store = createInMemoryGrantStore([makeGrant({ principalId: "p-1" })]);

    const a = await store.collectGrants("p-1", "tenant-a");
    const b = await store.collectGrants("p-1", "tenant-b");
    expect(a.length).toBe(1);
    expect(b.length).toBe(1);
  });

  test("returns multiple matching grants", async () => {
    const store = createInMemoryGrantStore([
      makeGrant({ id: "g1", principalId: "p-1", resource: "tool:bash" }),
      makeGrant({ id: "g2", principalId: "p-1", resource: "tool:curl" }),
      makeGrant({ id: "g3", principalId: "p-1", resource: "tool:node" }),
    ]);

    const results = await store.collectGrants("p-1", "t-1");
    expect(results.length).toBe(3);
  });

  test("empty store returns empty array", async () => {
    const store = createInMemoryGrantStore([]);
    const results = await store.collectGrants("p-1", "t-1");
    expect(results.length).toBe(0);
  });
});
