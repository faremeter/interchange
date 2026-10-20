import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { type } from "arktype";

import { createInMemoryGrantStore } from "@intx/authz";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createEventCollectorRegistry,
  createSidecarRouter,
  type SessionService,
} from "@intx/hub-sessions";
import type { GrantRule } from "@intx/types/authz";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// The evaluate endpoint explains an authorization decision. A run token's
// requests carry a workflow consumer, so an operator evaluating a run
// principal must be able to name that consumer to see the decision the token
// gets.

const TENANT_ID = "tnt_evaluate";
const OPERATOR_PRINCIPAL_ID = "prn_operator";
const OPERATOR_USER_ID = "usr_operator";
const RUN_PRINCIPAL_ID = "prn_run";

function grantRule(
  id: string,
  principalId: string,
  resource: string,
  action: string,
  conditions: Record<string, unknown> | null,
): GrantRule {
  return {
    id,
    resource,
    action,
    effect: "allow",
    origin: "system",
    conditions,
    expiresAt: null,
    roleId: null,
    principalId,
  };
}

const GRANTS: GrantRule[] = [
  grantRule("grt_operator", OPERATOR_PRINCIPAL_ID, "*", "*", null),
  grantRule("grt_run", RUN_PRINCIPAL_ID, "widget:*", "read", {
    tool: "action:a",
  }),
  grantRule("grt_run_gadget", RUN_PRINCIPAL_ID, "gadget:*", "read", null),
  {
    ...grantRule("grt_run_gadget_deny", RUN_PRINCIPAL_ID, "gadget:*", "read", {
      tool: "action:a",
    }),
    effect: "deny",
  },
];

function mockGetSession(userId: string): GetSession {
  const now = new Date("2026-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "operator@example.com",
      emailVerified: true,
      name: "Operator",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_test",
      userId,
      token: "tok_test",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function mockSessionService(): SessionService {
  const notImpl = (name: string) => (): never => {
    throw new Error(`mock: sessionService.${name} not implemented`);
  };
  return {
    stageWorkflowStep: notImpl("stageWorkflowStep"),
    endSession: notImpl("endSession"),
  };
}

const EvaluateBody = type({ effect: "string" });

describe.skipIf(!harnessDbEnvAvailable())(
  "POST /api/tenants/:tenantId/principals/:principalId/evaluate with a consumer",
  () => {
    let h: TestDb;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedPrincipal(h.db, {
        id: OPERATOR_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        refId: OPERATOR_USER_ID,
      });
      await seedPrincipal(h.db, {
        id: RUN_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        refId: "run_evaluate",
      });
    });

    async function evaluateResponse(
      body: Record<string, unknown>,
    ): Promise<Response> {
      const app = createApp({
        getSession: mockGetSession(OPERATOR_USER_ID),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore: createInMemoryGrantStore(GRANTS),
        sidecarRouter: createSidecarRouter({
          withExecutableWorkflowRun: async (_target, send) => send(),
          authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
          resolveSidecarBindings: async () => [],
          validateSidecarIdentity: async () => true,
        }),
        sessionService: mockSessionService(),
        eventCollectors: createEventCollectorRegistry({ db: h.db }),
        assetService: null,
        repoStore: null,
        maxTarballBytes: 10_000_000,
      });
      return await app.request(
        `/api/tenants/${TENANT_ID}/principals/${RUN_PRINCIPAL_ID}/evaluate`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        },
      );
    }

    async function evaluate(body: Record<string, unknown>): Promise<string> {
      const res = await evaluateResponse(body);
      expect(res.status).toBe(200);
      return EvaluateBody.assert(await res.json()).effect;
    }

    test("allows the consumer a scoped grant names", async () => {
      expect(
        await evaluate({
          resource: "widget:w1",
          action: "read",
          consumer: "action:a",
        }),
      ).toBe("allow");
    });

    test("denies another consumer", async () => {
      expect(
        await evaluate({
          resource: "widget:w1",
          action: "read",
          consumer: "action:b",
        }),
      ).toBe("deny");
    });

    test("denies when no consumer is named", async () => {
      expect(await evaluate({ resource: "widget:w1", action: "read" })).toBe(
        "deny",
      );
    });

    test("applies a deny scoped to the named consumer", async () => {
      expect(
        await evaluate({
          resource: "gadget:g1",
          action: "read",
          consumer: "action:a",
        }),
      ).toBe("deny");
      expect(
        await evaluate({
          resource: "gadget:g1",
          action: "read",
          consumer: "action:b",
        }),
      ).toBe("allow");
    });

    test("rejects an empty consumer", async () => {
      const res = await evaluateResponse({
        resource: "widget:w1",
        action: "read",
        consumer: "",
      });
      expect(res.status).toBe(400);
    });
  },
);
