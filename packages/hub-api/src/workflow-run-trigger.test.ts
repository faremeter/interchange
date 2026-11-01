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
    runTokenStore: unusedDep("runTokenStore"),
    grantStoreFor: unusedDep("grantStoreFor"),
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
      [{ snapshot: { perStep: [], grantRequirements: [] } }],
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
      runTokenStore: unusedDep("runTokenStore"),
      grantStoreFor: unusedDep("grantStoreFor"),
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

describe("triggerWorkflowRun first-trigger staging", () => {
  const ASSET_ID = "ast_staging";
  const CREATOR_ID = "prn_creator";

  // A chain of query-builder calls that resolves each read to the next queued
  // row set: `limit()` (also awaited with `.for()` for a row lock) and
  // `orderBy()`.
  function queuedSelect(rowSets: unknown[][], guard?: () => void) {
    let index = 0;
    const next = () => rowSets[index++] ?? [];
    const chain = {
      from: () => chain,
      innerJoin: () => chain,
      leftJoin: () => chain,
      where: () => chain,
      orderBy: () => Promise.resolve(next()),
      limit: () => {
        const rows = next();
        return Object.assign(Promise.resolve(rows), {
          for: () => Promise.resolve(rows),
        });
      },
    };
    return () => {
      guard?.();
      return chain;
    };
  }

  // The outer handle serves the reads before the anchor lock and refuses
  // every read while the transaction is open, so a read that leaves the
  // transaction's connection fails the test.
  function makeTrigger(opts: {
    txRows: unknown[][];
    grantStoreFor: TriggerWorkflowRunDeps["grantStoreFor"];
    routeMail?: (...args: unknown[]) => Promise<boolean>;
  }) {
    let inTransaction = false;
    const refuseInTransaction = () => {
      if (inTransaction) {
        throw new Error("read on the outer handle inside the transaction");
      }
    };
    const asset = {
      id: ASSET_ID,
      tenantId: "tnt_guard",
      creatorPrincipalId: CREATOR_ID,
    };
    const tx = {
      select: queuedSelect(opts.txRows),
      query: { asset: { findFirst: async () => asset } },
    };
    const db = {
      select: queuedSelect(
        [
          [
            {
              definitionId: "wfd_staging",
              definitionAssetId: ASSET_ID,
              allocationId: null,
              allocationStatus: null,
              anchorStatus: "running",
              runStatus: null,
              credentialRefs: null,
              legacyMailAccept: null,
            },
          ],
          // No run grants committed yet.
          [],
          [
            {
              snapshot: {
                perStep: [],
                grantRequirements: [
                  {
                    resource: "secret:vault",
                    action: "use",
                    source: "creator",
                  },
                ],
              },
            },
          ],
        ],
        refuseInTransaction,
      ),
      query: {
        asset: {
          findFirst: async () => {
            refuseInTransaction();
            return asset;
          },
        },
      },
      transaction: async (fn: (handle: typeof tx) => Promise<unknown>) => {
        inTransaction = true;
        try {
          return await fn(tx);
        } finally {
          inTransaction = false;
        }
      },
    };
    const trigger = createWorkflowRunTrigger({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- this mock implements only the query chains the first trigger reaches
      db: db as unknown as TriggerWorkflowRunDeps["db"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- signing and the sender-key read are the only key-store uses
      principalKeyStore: {
        sign: async () => new Uint8Array(64),
      } as unknown as TriggerWorkflowRunDeps["principalKeyStore"],
      runTokenStore: unusedDep("runTokenStore"),
      grantStoreFor: opts.grantStoreFor,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the direct path only routes the trigger mail
      sidecarRouter: {
        routeMail: opts.routeMail ?? (async () => true),
      } as unknown as TriggerWorkflowRunDeps["sidecarRouter"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- this branch only opens committed reads and treats a null result as an absent lifecycle
      repoStore: {
        openCommittedReads: async () => null,
      } as unknown as TriggerWorkflowRunDeps["repoStore"],
    });
    return { trigger, tx };
  }

  const triggerArgs = {
    tenant: makeTenant(),
    principal: makePrincipal("user"),
    userName: null,
    anchorRunId: "run_staging",
    message: { content: "hi" },
  };

  test("stages under the anchor lock with reads on the transaction", async () => {
    const executors: unknown[] = [];
    const collected: string[] = [];
    const { trigger, tx } = makeTrigger({
      txRows: [
        // The anchor lock.
        [{ status: "running", expiresAt: null, cancellationRequestedAt: null }],
        // No run grants committed by a concurrent trigger.
        [],
      ],
      grantStoreFor: (executor) => {
        executors.push(executor);
        return {
          collectGrants: async (principalId) => {
            collected.push(principalId);
            return [];
          },
          collectGrantsInChain: async () => [],
        };
      },
    });

    // The creator holds nothing, so its requirement refuses.
    const result = await trigger(triggerArgs);

    expect(result).toMatchObject({
      ok: false,
      status: 403,
      body: { error: { code: "insufficient_grants" } },
    });
    expect(executors).toEqual([tx]);
    expect(collected).toEqual([CREATOR_ID]);
  });

  test("a trigger that lost the first-fire race sends the winner's grants without staging", async () => {
    const winnerGrant = {
      id: "grt_winner",
      resource: "tool:read_file",
      action: "invoke",
      effect: "allow",
      origin: "creator",
      conditions: null,
      expiresAt: null,
      roleId: null,
      principalId: "prn_run",
    };
    const routed: unknown[] = [];
    const { trigger } = makeTrigger({
      txRows: [
        // The anchor lock.
        [{ status: "running", expiresAt: null, cancellationRequestedAt: null }],
        // The winner's run principal and its committed grants.
        [{ id: "prn_run" }],
        [winnerGrant],
        // `commitRunGrants` finds the same committed run.
        [{ id: "prn_run" }],
        [winnerGrant],
      ],
      grantStoreFor: () => {
        throw new Error("a lost race must not stage");
      },
      routeMail: async (...args) => {
        routed.push(args[4]);
        return true;
      },
    });

    const result = await trigger(triggerArgs);

    expect(result.ok).toBe(true);
    expect(routed).toEqual([
      expect.objectContaining({ stepGrants: [winnerGrant] }),
    ]);
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
