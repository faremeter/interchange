import { describe, test, expect } from "bun:test";

import { createInMemoryGrantStore } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";
import type { GrantWalkSnapshot } from "@intx/types";
import {
  workflowDefinitionVersion as workflowDefinitionVersionTable,
  workflowRun as workflowRunTable,
} from "@intx/db/schema";

import { createMailTriggeredRunGrantsMaterializer } from "./run-grant-materialization";

const TENANT_ID = "tenant-1";
const ASSET_ID = "asset-wf";
const CREATOR_PRINCIPAL_ID = "prn_creator";
const WORKFLOW_ADDRESS = "run_wf1@tenant.example";

// The deploy-approved grant-walk snapshot for a one-step workflow: one `tool:`
// runtime grant plus a creator-sourced and an invoker-sourced requirement. The
// walk yields the `tool:read_file` grant; the creator requirement resolves
// against the creator's grants; the invoker requirement must be OMITTED on the
// mail path.
function snapshot(): GrantWalkSnapshot {
  return {
    perStep: [
      {
        stepId: "work",
        grants: ["tool:read_file"],
        grantEffects: { "tool:read_file": "allow" },
      },
    ],
    grantRequirements: [
      { resource: "secret:vault", action: "use", source: "creator" },
      { resource: "secret:other", action: "use", source: "invoker" },
    ],
  };
}

// A snapshot carrying ONLY an invoker-sourced requirement plus the tool grant.
function invokerOnlySnapshot(): GrantWalkSnapshot {
  return {
    perStep: [
      {
        stepId: "work",
        grants: ["tool:read_file"],
        grantEffects: { "tool:read_file": "allow" },
      },
    ],
    grantRequirements: [
      { resource: "secret:other", action: "use", source: "invoker" },
    ],
  };
}

// A DB stand-in for the deployment lookup, the frozen-snapshot read, and the
// first-run reservation. It reports no pre-existing run principal, so each test
// exercises the winning reservation path. The frozen snapshot is served from
// the `workflow_definition_version` row read; `snapshotReads` counts how many
// times that row is actually read, so a stable count across triggers proves the
// materializer caches the snapshot rather than re-reading it per run.
function mockDb(opts: {
  deploymentRow:
    | { id: string; tenantId: string; definitionAssetId: string }
    | undefined;
  assetRow: unknown;
  grantSnapshot: GrantWalkSnapshot | null;
  snapshotReads?: { count: number };
  topLevelRunStatus?: "running" | "completed" | "failed" | "cancelled" | null;
  lockedRunStatus?: "running" | "completed" | "failed" | "cancelled";
}) {
  function rows(table: unknown, joined: boolean): unknown[] {
    if (table === workflowRunTable && opts.deploymentRow) {
      return joined
        ? [
            {
              anchorRunId: opts.deploymentRow.id,
              tenantId: opts.deploymentRow.tenantId,
              definitionId: `wfd_${opts.deploymentRow.id}`,
              definitionAssetId: opts.deploymentRow.definitionAssetId,
              anchorStatus: "running",
              topLevelRunStatus: opts.topLevelRunStatus ?? null,
            },
          ]
        : [{ status: "running" }];
    }
    if (table === workflowDefinitionVersionTable) {
      if (opts.snapshotReads) opts.snapshotReads.count += 1;
      return [{ grantSnapshot: opts.grantSnapshot }];
    }
    return [];
  }
  const select = () => ({
    from: (table: unknown) => {
      let joined = false;
      const chain = {
        innerJoin: () => {
          joined = true;
          return chain;
        },
        leftJoin: () => {
          joined = true;
          return chain;
        },
        where: () => ({
          limit: () =>
            Object.assign(Promise.resolve(rows(table, joined)), {
              for: () =>
                Promise.resolve(
                  table === workflowRunTable && opts.deploymentRow
                    ? [{ status: opts.lockedRunStatus ?? "running" }]
                    : [],
                ),
            }),
        }),
      };
      return chain;
    },
  });
  const insert = (_table: unknown) => ({
    values: (values: unknown) =>
      Object.assign(Promise.resolve(), {
        onConflictDoNothing: () => ({
          returning: () => Promise.resolve([values]),
        }),
      }),
  });
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return {
    query: {
      asset: { findFirst: async () => opts.assetRow },
    },
    select,
    insert,
    transaction: async (
      fn: (tx: {
        select: typeof select;
        insert: typeof insert;
      }) => Promise<unknown> | unknown,
    ) => fn({ select, insert }),
  } as unknown as Parameters<
    typeof createMailTriggeredRunGrantsMaterializer
  >[0]["db"];
}

const RUN_ID = "<mail-run-frozen@tenant.example>";

const deploymentRow = {
  id: "dep-1",
  tenantId: TENANT_ID,
  definitionAssetId: ASSET_ID,
  address: WORKFLOW_ADDRESS,
  status: "deployed" as const,
};

const assetRow = {
  id: ASSET_ID,
  tenantId: TENANT_ID,
  kind: "workflow" as const,
  creatorPrincipalId: CREATOR_PRINCIPAL_ID,
};

function creatorGrant(): GrantRule {
  return {
    id: "grant-creator-vault",
    resource: "secret:vault",
    action: "use",
    effect: "allow",
    origin: "creator",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: CREATOR_PRINCIPAL_ID,
  };
}

describe("createMailTriggeredRunGrantsMaterializer staging", () => {
  test("skips when the address names no deployed deployment", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow: undefined,
        assetRow,
        grantSnapshot: snapshot(),
      }),
      grantStore: createInMemoryGrantStore([creatorGrant()]),
    });
    const result = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: WORKFLOW_ADDRESS,
    });
    expect(result.outcome).toBe("skip");
  });

  test("rejects mail for a terminal stable run before reading its snapshot", async () => {
    const reads = { count: 0 };
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: snapshot(),
        snapshotReads: reads,
        topLevelRunStatus: "completed",
      }),
      grantStore: createInMemoryGrantStore([creatorGrant()]),
    });

    await expect(
      materialize({
        agentAddress: WORKFLOW_ADDRESS,
        runId: WORKFLOW_ADDRESS,
      }),
    ).resolves.toMatchObject({
      outcome: "rejected",
      status: 409,
      code: "workflow_run_terminal",
    });
    // The terminal preflight rejects before the snapshot is ever read.
    expect(reads.count).toBe(0);
  });

  test("revalidates under lock before reserving a run that became terminal", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: snapshot(),
        topLevelRunStatus: null,
        lockedRunStatus: "failed",
      }),
      grantStore: createInMemoryGrantStore([creatorGrant()]),
    });

    await expect(
      materialize({
        agentAddress: WORKFLOW_ADDRESS,
        runId: WORKFLOW_ADDRESS,
      }),
    ).resolves.toMatchObject({
      outcome: "rejected",
      status: 409,
      code: "workflow_run_terminal",
    });
  });

  test("stages the tool grant and the creator requirement, omitting the invoker one", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({ deploymentRow, assetRow, grantSnapshot: snapshot() }),
      grantStore: createInMemoryGrantStore([creatorGrant()]),
    });

    const result = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: WORKFLOW_ADDRESS,
    });

    if (result.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${result.outcome}`);
    }
    const resources = result.stepGrants
      .map((g) => `${g.resource}/${g.action}`)
      .sort();
    // The snapshot's tool grant and the resolved creator requirement are
    // present.
    expect(resources).toContain("tool:read_file/invoke");
    expect(resources).toContain("secret:vault/use");
    // The invoker-sourced requirement is silently omitted (no invoker on the
    // wire), so it never materializes.
    expect(resources).not.toContain("secret:other/use");
    // Every reserved grant is principal-scoped on the run principal.
    for (const g of result.stepGrants) {
      expect(g.roleId).toBeNull();
      expect(g.principalId).not.toBeNull();
    }
  });

  test("still stages when the run launches with an omitted invoker grant", async () => {
    // No creator grant held: a creator requirement would fail closed. But the
    // invoker requirement is filtered out before staging, so a snapshot with
    // ONLY an invoker requirement still launches.
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: invokerOnlySnapshot(),
      }),
      grantStore: createInMemoryGrantStore([]),
    });
    const result = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: WORKFLOW_ADDRESS,
    });
    if (result.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${result.outcome}`);
    }
    const resources = result.stepGrants.map((g) => `${g.resource}/${g.action}`);
    // Only the snapshot's tool grant survives; the invoker requirement is
    // omitted and no creator requirement exists, so the run launches with the
    // tool.
    expect(resources).toEqual(["tool:read_file/invoke"]);
  });

  test("fails closed when the definition has no approved grant snapshot", async () => {
    // A null `grant_snapshot` column is the "not yet approved" state. The
    // materializer must raise rather than launch a run with an empty grant set.
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({ deploymentRow, assetRow, grantSnapshot: null }),
      grantStore: createInMemoryGrantStore([creatorGrant()]),
    });

    await expect(
      materialize({
        agentAddress: WORKFLOW_ADDRESS,
        runId: WORKFLOW_ADDRESS,
      }),
    ).rejects.toThrow(/no approved grant snapshot/);
  });
});

describe("createMailTriggeredRunGrantsMaterializer frozen basis", () => {
  test("reads the frozen snapshot once and caches it per definition", async () => {
    const reads = { count: 0 };
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: snapshot(),
        snapshotReads: reads,
      }),
      grantStore: createInMemoryGrantStore([creatorGrant()]),
    });

    const first = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: RUN_ID,
    });
    if (first.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${first.outcome}`);
    }
    // The first trigger reads the frozen snapshot exactly once.
    expect(reads.count).toBe(1);

    // A second trigger of the SAME deployment consumes the closure-cached
    // snapshot: the version row is never read again.
    const second = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: RUN_ID,
    });
    if (second.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${second.outcome}`);
    }
    expect(reads.count).toBe(1);
  });
});
