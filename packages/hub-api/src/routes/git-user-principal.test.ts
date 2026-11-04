import { describe, expect, test } from "bun:test";

import { createInMemoryGrantStore, HUB_CONDITION_REGISTRY } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";

import { resolveAuthzVerdict } from "./git-user-principal";

const PRINCIPAL_ID = "prn_caller";
const TENANT_ID = "tnt_a";
const RESOURCE = "asset:a1";

function readGrant(
  effect: GrantRule["effect"],
  tool: string | null,
): GrantRule {
  return {
    id: `grant-${effect}-${tool ?? "any"}`,
    resource: RESOURCE,
    action: "read",
    effect,
    origin: "system",
    conditions: tool === null ? null : { tool },
    expiresAt: null,
    roleId: null,
    principalId: PRINCIPAL_ID,
  };
}

async function verdictFor(grants: GrantRule[], consumer: string | undefined) {
  const authz = await resolveAuthzVerdict({
    grantStore: createInMemoryGrantStore(grants),
    conditionRegistry: HUB_CONDITION_REGISTRY,
    principalId: PRINCIPAL_ID,
    tenantId: TENANT_ID,
    resource: RESOURCE,
    action: "createPack",
    consumer,
  });
  return authz.effect;
}

describe("resolveAuthzVerdict with a consumer", () => {
  const scopedDeny = [readGrant("allow", null), readGrant("deny", "action:a")];

  test("a deny scoped to the consumer overrides an unscoped allow", async () => {
    expect(await verdictFor(scopedDeny, "action:a")).toBe("deny");
  });

  test("a deny scoped to another consumer does not apply", async () => {
    expect(await verdictFor(scopedDeny, "action:b")).toBe("allow");
  });

  test("a deny scoped to a consumer does not apply without one", async () => {
    expect(await verdictFor(scopedDeny, undefined)).toBe("allow");
  });

  test("an unscoped deny applies to every consumer", async () => {
    const grants = [readGrant("allow", null), readGrant("deny", null)];
    expect(await verdictFor(grants, "action:a")).toBe("deny");
    expect(await verdictFor(grants, undefined)).toBe("deny");
  });
});
