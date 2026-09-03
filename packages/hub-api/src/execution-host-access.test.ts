import { describe, expect, test } from "bun:test";

import { createInMemoryGrantStore } from "@intx/authz";
import type { ConditionRegistry, Effect, GrantRule } from "@intx/types/authz";

import { canUseExecutionHost } from "./execution-host-access";

const TENANT_ID = "tnt_test";
const PRINCIPAL_ID = "prn_test";
const HOST_ID = "hst_sensitive";

function grant(
  id: string,
  resource: string,
  effect: Effect,
  conditions: Record<string, unknown> | null = null,
): GrantRule {
  return {
    id,
    resource,
    action: "use",
    effect,
    origin: "system",
    conditions,
    expiresAt: null,
    roleId: null,
    principalId: PRINCIPAL_ID,
  };
}

describe("canUseExecutionHost", () => {
  test.each(["deny", "ask"] as const)(
    "applies a conditioned %s before a broader allow",
    async (effect) => {
      const conditionRegistry: ConditionRegistry = {
        time_window: (_value, context) =>
          context.principalId === PRINCIPAL_ID &&
          context.tenantId === TENANT_ID,
      };
      const grantStore = createInMemoryGrantStore([
        grant("broad-allow", "host:*", "allow"),
        grant("exact-restriction", `host:${HOST_ID}`, effect, {
          time_window: {},
        }),
      ]);

      expect(
        await canUseExecutionHost({
          grantStore,
          conditionRegistry,
          tenantId: TENANT_ID,
          placementPrincipalId: PRINCIPAL_ID,
          hostId: HOST_ID,
          ownerPrincipalId: "prn_owner",
        }),
      ).toBe(false);
    },
  );
});
