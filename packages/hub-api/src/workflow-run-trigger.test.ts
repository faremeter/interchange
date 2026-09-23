import { describe, test, expect } from "bun:test";

import { MAX_MAIL_OUTBOUND_BODY_BYTES } from "@intx/types/sidecar";

import {
  createWorkflowRunTrigger,
  MAX_MAIL_BODY_BYTES,
  type TriggerWorkflowRunDeps,
} from "./workflow-run-trigger";
import type { PrincipalRow, TenantRow } from "./context";

// The signing guard runs before any dependency is touched, so each dep is a
// proxy that throws if execution ever reaches it. The deps object itself is a
// real literal -- the factory destructures it, which reads the proxy VALUES
// without accessing anything ON them; a trap only fires when the trigger later
// uses a store or the db, which only a principal that PASSES the guard does.
function unusedDep<T extends object>(label: string): T {
  const trap = new Proxy(
    {},
    {
      get() {
        throw new Error(
          `triggerWorkflowRun guard test: ${label} must not be used`,
        );
      },
    },
  );
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the guard throws before any dep is accessed; a structural mock of these rich store interfaces would be dead weight the test never exercises
  return trap as T;
}

function makeTenant(): TenantRow {
  const now = new Date("2025-01-01");
  return {
    id: "tnt_guard",
    name: "guard",
    slug: "guard",
    domain: "guard.interchange",
    parentId: null,
    config: null,
    createdAt: now,
    updatedAt: now,
  };
}

function makePrincipal(kind: PrincipalRow["kind"]): PrincipalRow {
  const now = new Date("2025-01-01");
  return {
    id: `prn_${kind}`,
    tenantId: "tnt_guard",
    kind,
    refId: `ref_${kind}`,
    status: "active",
    createdAt: now,
    updatedAt: now,
  };
}

function makeTrigger() {
  return createWorkflowRunTrigger({
    db: unusedDep("db"),
    principalKeyStore: unusedDep("principalKeyStore"),
    grantStore: unusedDep("grantStore"),
    sidecarRouter: unusedDep("sidecarRouter"),
    repoStore: unusedDep("repoStore"),
  });
}

describe("triggerWorkflowRun signing-principal guard", () => {
  test("rejects a workflow-kind principal before any signing", async () => {
    const trigger = makeTrigger();
    await expect(
      trigger({
        tenant: makeTenant(),
        principal: makePrincipal("workflow"),
        userName: null,
        anchorRunId: "run_guard",
        message: { content: "hi" },
      }),
    ).rejects.toThrow(/only a user principal may originate hub-signed mail/);
  });

  test("rejects an agent-kind principal before any signing", async () => {
    const trigger = makeTrigger();
    await expect(
      trigger({
        tenant: makeTenant(),
        principal: makePrincipal("agent"),
        userName: null,
        anchorRunId: "run_guard",
        message: { content: "hi" },
      }),
    ).rejects.toThrow(/only a user principal may originate hub-signed mail/);
  });

  test("lets a user-kind principal past the guard into dependency use", async () => {
    const trigger = makeTrigger();
    // A user principal clears the guard and proceeds until it touches a dep;
    // the first touch is the db read, which the proxy rejects. Asserting on the
    // dep-use error (not the guard error) proves the guard gates ONLY non-user
    // principals rather than rejecting every caller.
    await expect(
      trigger({
        tenant: makeTenant(),
        principal: makePrincipal("user"),
        userName: null,
        anchorRunId: "run_guard",
        message: { content: "hi" },
      }),
    ).rejects.toThrow(/db must not be used/);
  });
});

describe("triggerWorkflowRun definition asset lookup", () => {
  test("names a missing package-registry asset as the definition asset", async () => {
    const definitionAssetId = "ast_package_registry";
    const rowsBySelect = [
      [
        {
          definitionId: "wfd_package_registry",
          definitionAssetId,
          allocationId: null,
          allocationStatus: null,
          anchorStatus: "deployed",
          runStatus: null,
        },
      ],
      [],
      [{ grantSnapshot: { perStep: [], grantRequirements: [] } }],
    ];
    let selectIndex = 0;
    const dbQuery = {
      from() {
        return dbQuery;
      },
      innerJoin() {
        return dbQuery;
      },
      leftJoin() {
        return dbQuery;
      },
      where() {
        return dbQuery;
      },
      orderBy() {
        return dbQuery;
      },
      limit() {
        return Promise.resolve(rowsBySelect[selectIndex - 1]);
      },
    };
    const db = {
      select() {
        selectIndex += 1;
        return dbQuery;
      },
      query: {
        asset: {
          findFirst: async () => undefined,
        },
      },
    };
    const repoStore = {
      openCommittedReads: async () => null,
    };
    const trigger = createWorkflowRunTrigger({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- this branch-specific mock implements only the query chains reached before the missing-asset response
      db: db as unknown as TriggerWorkflowRunDeps["db"],
      principalKeyStore: unusedDep("principalKeyStore"),
      grantStore: unusedDep("grantStore"),
      sidecarRouter: unusedDep("sidecarRouter"),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- this branch only opens committed reads and treats a null result as an absent lifecycle
      repoStore: repoStore as unknown as TriggerWorkflowRunDeps["repoStore"],
    });

    const result = await trigger({
      tenant: makeTenant(),
      principal: makePrincipal("user"),
      userName: null,
      anchorRunId: "run_package_registry",
      message: { content: "hi" },
    });

    expect(result).toEqual({
      ok: false,
      status: 409,
      body: {
        error: {
          code: "invalid_workflow",
          message: `Definition asset ${definitionAssetId} not found`,
        },
      },
    });
  });
});

describe("mail body cap shared policy", () => {
  test("the HTTP body cap and the frame body cap hold the same ceiling", () => {
    // The inbound HTTP mail route caps its whole request body at
    // MAX_MAIL_BODY_BYTES; the mail.outbound frame caps its rawMessage at
    // @intx/types' MAX_MAIL_OUTBOUND_BODY_BYTES. They measure different
    // quantities but must admit one mail of the same size, so they carry the
    // same number. This is the one place both are visible (@intx/types cannot
    // import @intx/hub-api), so it guards the shared policy against drift.
    expect(MAX_MAIL_OUTBOUND_BODY_BYTES).toBe(MAX_MAIL_BODY_BYTES);
  });
});
