import { describe, expect, test } from "bun:test";

import { RepoAction } from "@intx/types/sidecar";

import { GitTokenKindValidator, parseGitTokenRow } from "./parse-row";
import type { gitToken } from "./schema";

type GitTokenRow = typeof gitToken.$inferSelect;

function makeRow(overrides: Partial<GitTokenRow> = {}): GitTokenRow {
  const now = new Date();
  return {
    id: "gtk_0123456789abcdef0123456789abcdef",
    tenantId: null,
    userId: "user_alice",
    principalId: null,
    name: "laptop",
    kind: "pat",
    tokenHashSha256: new Uint8Array(32),
    resource: "agent-state:ins_test",
    refPattern: "refs/heads/*",
    actions: ["receivePack", "createPack", "resolveRef"],
    expiresAt: new Date("2027-01-01T00:00:00Z"),
    revokedAt: null,
    createdAt: now,
    ...overrides,
  };
}

describe("parseGitTokenRow", () => {
  test("round-trips a personal pat with concrete repo scope", () => {
    const row = makeRow();
    const parsed = parseGitTokenRow(row);

    expect(parsed.id).toBe(row.id);
    expect(parsed.tenantId).toBeNull();
    expect(parsed.userId).toBe(row.userId);
    expect(parsed.principalId).toBeNull();
    expect(parsed.name).toBe(row.name);
    expect(parsed.kind).toBe("pat");
    expect(parsed.tokenHashSha256).toBe(row.tokenHashSha256);
    expect(parsed.actions).toEqual(["receivePack", "createPack", "resolveRef"]);
    expect(parsed.resource).toBe("agent-state:ins_test");
    expect(parsed.refPattern).toBe("refs/heads/*");
    expect(parsed.expiresAt).toBe(row.expiresAt);
    expect(parsed.revokedAt).toBeNull();
    expect(parsed.createdAt).toBe(row.createdAt);
  });

  test("round-trips a tenant-restricted pat", () => {
    const row = makeRow({
      tenantId: "tnt_acme",
      name: "acme-only",
    });
    const parsed = parseGitTokenRow(row);

    expect(parsed.kind).toBe("pat");
    expect(parsed.tenantId).toBe("tnt_acme");
    expect(parsed.principalId).toBeNull();
  });

  test("round-trips a tenant-bound svc token", () => {
    const row = makeRow({
      kind: "svc",
      tenantId: "tnt_acme",
      principalId: "prn_tenant_user",
      name: "ci-runner",
      actions: ["createPack", "resolveRef"],
      resource: "asset:def_skill_xyz",
      refPattern: "refs/tags/v*",
    });
    const parsed = parseGitTokenRow(row);

    expect(parsed.kind).toBe("svc");
    expect(parsed.tenantId).toBe("tnt_acme");
    expect(parsed.principalId).toBe("prn_tenant_user");
    expect(parsed.actions).toEqual(["createPack", "resolveRef"]);
    expect(parsed.resource).toBe("asset:def_skill_xyz");
    expect(parsed.refPattern).toBe("refs/tags/v*");
  });

  test("preserves revokedAt for soft-revoked rows", () => {
    const revoked = new Date("2026-01-15T00:00:00Z");
    const row = makeRow({ revokedAt: revoked });
    const parsed = parseGitTokenRow(row);

    expect(parsed.revokedAt).toBe(revoked);
  });

  test("preserves expiresAt", () => {
    const expires = new Date("2027-01-01T00:00:00Z");
    const row = makeRow({ expiresAt: expires });
    const parsed = parseGitTokenRow(row);

    expect(parsed.expiresAt).toBe(expires);
  });

  test("rejects an unknown kind", () => {
    expect(() => GitTokenKindValidator.assert("rogue")).toThrow();
  });

  test("rejects an unknown action in the actions array", () => {
    expect(() =>
      RepoAction.array().assert(["receivePack", "fly-the-helicopter"]),
    ).toThrow();
  });

  test("accepts an empty actions array", () => {
    const row = makeRow({ actions: [] });
    const parsed = parseGitTokenRow(row);
    expect(parsed.actions).toEqual([]);
  });
});
