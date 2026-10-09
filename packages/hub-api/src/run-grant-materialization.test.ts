import { describe, test, expect } from "bun:test";

import { createInMemoryGrantStore, toolConsumer } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";
import type { GrantWalkSnapshot } from "@intx/types";
import {
  sidecarAllocation as sidecarAllocationTable,
  workflowDefinitionVersion as workflowDefinitionVersionTable,
  workflowRun as workflowRunTable,
} from "@intx/db/schema";
import type { PrincipalKeyStore } from "@intx/db";

import { makeGrantRow } from "./grant-materialization";
import {
  createMailTriggeredRunGrantsMaterializer,
  credentialResolutionUnrecorded,
  deriveCredentialUseGrantRows,
  grantRowsWithLockedCredentialResolution,
} from "./run-grant-materialization";

// This suite exercises grant materialization, not key minting. The key store is
// a no-op stub so the winning-reservation path does not try to insert into
// principal_key against the hand-rolled mock DB.
const stubPrincipalKeyStore: PrincipalKeyStore = {
  generate: async () => "pky_stub",
  sign: async () => new Uint8Array(64),
  getPublicKey: async () => "pky_stub_public",
};

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
type CredentialRefs = {
  credentialIds: string[];
  bindings: { handle: string; credentialId: string; consumer: string }[];
};

function mockDb(opts: {
  deploymentRow:
    | { id: string; tenantId: string; definitionAssetId: string }
    | undefined;
  assetRow: unknown;
  grantSnapshot: GrantWalkSnapshot | null;
  snapshotReads?: { count: number };
  topLevelRunStatus?: "running" | "completed" | "failed" | "cancelled" | null;
  lockedRunStatus?: "running" | "completed" | "failed" | "cancelled";
  anchorCancellationRequestedAt?: Date;
  lockedCancellationRequestedAt?: Date;
  credentialRefs?: CredentialRefs;
  /** Null models an anchor whose initialization has not published a key. */
  publicKey?: string | null;
  /**
   * Set while a deploy has cleared the key and left the previous refs.
   * Absent means the anchor has no allocation row.
   */
  initializationLeaseId?: string | null;
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
              anchorExpiresAt: null,
              anchorCancellationRequestedAt:
                opts.anchorCancellationRequestedAt ?? null,
              topLevelRunStatus: opts.topLevelRunStatus ?? null,
              ...(opts.credentialRefs !== undefined
                ? { credentialRefs: opts.credentialRefs }
                : {}),
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
          limit: () => {
            const selected =
              table === sidecarAllocationTable
                ? opts.initializationLeaseId === undefined
                  ? []
                  : [
                      {
                        initializationLeaseId: opts.initializationLeaseId,
                      },
                    ]
                : rows(table, joined);
            return Object.assign(Promise.resolve(selected), {
              for: () =>
                Promise.resolve(
                  table === workflowRunTable && opts.deploymentRow
                    ? [
                        {
                          status: opts.lockedRunStatus ?? "running",
                          expiresAt: null,
                          cancellationRequestedAt:
                            opts.lockedCancellationRequestedAt ?? null,
                          publicKey:
                            opts.publicKey !== undefined
                              ? opts.publicKey
                              : "recorded-key",
                          credentialRefs:
                            opts.credentialRefs !== undefined
                              ? opts.credentialRefs
                              : null,
                        },
                      ]
                    : table === sidecarAllocationTable
                      ? selected
                      : [],
                ),
            });
          },
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

describe("deriveCredentialUseGrantRows", () => {
  test("stamps one row per credential that shares a consumer", () => {
    const now = new Date("2026-01-01T00:00:00.000Z");
    const consumer = toolConsumer("bundle-id");
    const rows = deriveCredentialUseGrantRows({
      bindings: [
        { credentialId: "cred-a", consumer },
        { credentialId: "cred-b", consumer },
        { credentialId: "cred-a", consumer },
      ],
      tenantId: TENANT_ID,
      runPrincipalId: "prn_run",
      now,
    });
    expect(rows.map((row) => row.resource)).toEqual([
      "credential:cred-a",
      "credential:cred-b",
    ]);
    expect(rows[0]).toMatchObject({
      action: "use",
      effect: "allow",
      origin: "system",
      conditions: { tool: consumer },
      expiresAt: null,
    });
    expect(rows[1]).toMatchObject({
      action: "use",
      effect: "allow",
      origin: "system",
      conditions: { tool: consumer },
      expiresAt: null,
    });
  });
});

describe("grantRowsWithLockedCredentialResolution", () => {
  const now = new Date("2026-01-01T00:00:00.000Z");
  const args = {
    tenantId: TENANT_ID,
    runPrincipalId: "prn_run",
    now,
  };

  test("a recorded key with null refs stamps nothing and keeps a creator credential grant", () => {
    expect(
      credentialResolutionUnrecorded({
        credentialRefs: null,
        publicKey: "recorded-key",
        initializationLeaseId: null,
      }),
    ).toBe(false);
    const creator = makeGrantRow({
      tenantId: TENANT_ID,
      principalId: "prn_run",
      resource: "credential:creator-owned",
      action: "use",
      effect: "allow",
      conditions: null,
      origin: "creator",
      expiresAt: null,
      now,
    });
    const stale = makeGrantRow({
      tenantId: TENANT_ID,
      principalId: "prn_run",
      resource: "credential:stale",
      action: "use",
      effect: "allow",
      conditions: { tool: "tool:stale" },
      origin: "system",
      expiresAt: null,
      now,
    });
    const result = grantRowsWithLockedCredentialResolution(
      [creator, stale],
      {
        credentialRefs: null,
        publicKey: "recorded-key",
        initializationLeaseId: null,
      },
      args,
    );
    if (result === "not-ready") throw new Error("expected rows");
    expect(result.map((row) => row.resource)).toEqual([
      "credential:creator-owned",
    ]);
    expect(result[0]?.origin).toBe("creator");
  });

  test("a null key with recorded bindings replaces staged system rows", () => {
    const refs = {
      credentialIds: ["cred-1", "inference-only"],
      bindings: [
        { handle: "api", credentialId: "cred-1", consumer: "tool:pkg" },
        { handle: "other", credentialId: "cred-1", consumer: "tool:pkg" },
        { handle: "b", credentialId: "cred-2", consumer: "tool:other" },
      ],
    };
    expect(
      credentialResolutionUnrecorded({
        credentialRefs: refs,
        publicKey: null,
        initializationLeaseId: null,
      }),
    ).toBe(false);
    const result = grantRowsWithLockedCredentialResolution(
      [],
      {
        credentialRefs: refs,
        publicKey: null,
        initializationLeaseId: null,
      },
      args,
    );
    if (result === "not-ready") throw new Error("expected rows");
    expect(
      result.map((row) => ({
        resource: row.resource,
        origin: row.origin,
        conditions: row.conditions,
      })),
    ).toEqual([
      {
        resource: "credential:cred-1",
        origin: "system",
        conditions: { tool: "tool:pkg" },
      },
      {
        resource: "credential:cred-2",
        origin: "system",
        conditions: { tool: "tool:other" },
      },
    ]);
  });

  test("an initialization lease keeps leftover bindings from being stamped", () => {
    const refs = {
      credentialIds: ["cred-old"],
      bindings: [
        { handle: "api", credentialId: "cred-old", consumer: "tool:pkg" },
      ],
    };
    expect(
      credentialResolutionUnrecorded({
        credentialRefs: refs,
        publicKey: null,
        initializationLeaseId: "lease-1",
      }),
    ).toBe(true);
    expect(
      grantRowsWithLockedCredentialResolution(
        [],
        {
          credentialRefs: refs,
          publicKey: null,
          initializationLeaseId: "lease-1",
        },
        args,
      ),
    ).toBe("not-ready");
  });
});

describe("createMailTriggeredRunGrantsMaterializer staging", () => {
  test("skips when the address names no deployed deployment", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow: undefined,
        assetRow,
        grantSnapshot: snapshot(),
      }),
      principalKeyStore: stubPrincipalKeyStore,
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
      principalKeyStore: stubPrincipalKeyStore,
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
      principalKeyStore: stubPrincipalKeyStore,
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

  test("rejects mail for a stopping deployment before reading its snapshot", async () => {
    const reads = { count: 0 };
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: snapshot(),
        snapshotReads: reads,
        anchorCancellationRequestedAt: new Date(),
      }),
      principalKeyStore: stubPrincipalKeyStore,
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
      code: "workflow_run_stopping",
    });
    expect(reads.count).toBe(0);
  });

  test("revalidates under lock before reserving a run that started stopping", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: snapshot(),
        topLevelRunStatus: null,
        lockedCancellationRequestedAt: new Date(),
      }),
      principalKeyStore: stubPrincipalKeyStore,
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
      code: "workflow_run_stopping",
    });
  });

  test("stages the tool grant and the creator requirement, omitting the invoker one", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({ deploymentRow, assetRow, grantSnapshot: snapshot() }),
      principalKeyStore: stubPrincipalKeyStore,
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
      principalKeyStore: stubPrincipalKeyStore,
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

  test("stamps a system credential use grant from the anchor bindings", async () => {
    const consumer = toolConsumer("bundle-id");
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: invokerOnlySnapshot(),
        credentialRefs: {
          credentialIds: ["cred-1", "inference-only"],
          bindings: [
            { handle: "api", credentialId: "cred-1", consumer },
            { handle: "other", credentialId: "cred-1", consumer },
          ],
        },
      }),
      principalKeyStore: stubPrincipalKeyStore,
      grantStore: createInMemoryGrantStore([]),
    });
    const result = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: WORKFLOW_ADDRESS,
    });
    if (result.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${result.outcome}`);
    }
    const credentialGrants = result.stepGrants.filter((g) =>
      g.resource.startsWith("credential:"),
    );
    expect(credentialGrants).toEqual([
      expect.objectContaining({
        resource: "credential:cred-1",
        action: "use",
        effect: "allow",
        origin: "system",
        conditions: { tool: consumer },
        expiresAt: null,
      }),
    ]);
  });

  test("stamps one credential use grant per consumer", async () => {
    const first = toolConsumer("bundle-a");
    const second = toolConsumer("bundle-b");
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: invokerOnlySnapshot(),
        credentialRefs: {
          credentialIds: ["cred-1"],
          bindings: [
            { handle: "api", credentialId: "cred-1", consumer: first },
            { handle: "api", credentialId: "cred-1", consumer: second },
          ],
        },
      }),
      principalKeyStore: stubPrincipalKeyStore,
      grantStore: createInMemoryGrantStore([]),
    });
    const result = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: WORKFLOW_ADDRESS,
    });
    if (result.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${result.outcome}`);
    }
    const tools = result.stepGrants
      .filter((g) => g.resource.startsWith("credential:"))
      .map((g) =>
        g.conditions !== null &&
        typeof g.conditions === "object" &&
        "tool" in g.conditions
          ? g.conditions.tool
          : undefined,
      )
      .sort();
    expect(tools).toEqual([first, second].sort());
  });

  test("does not stamp an inference-only credential id", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: invokerOnlySnapshot(),
        credentialRefs: { credentialIds: ["inference-only"], bindings: [] },
      }),
      principalKeyStore: stubPrincipalKeyStore,
      grantStore: createInMemoryGrantStore([]),
    });
    const result = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: WORKFLOW_ADDRESS,
    });
    if (result.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${result.outcome}`);
    }
    expect(
      result.stepGrants.some((g) => g.resource.startsWith("credential:")),
    ).toBe(false);
  });

  test("does not commit when credential resolution is unrecorded", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: invokerOnlySnapshot(),
        publicKey: null,
      }),
      principalKeyStore: stubPrincipalKeyStore,
      grantStore: createInMemoryGrantStore([]),
    });
    const result = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: WORKFLOW_ADDRESS,
    });
    expect(result).toEqual({ outcome: "notReady" });
  });

  test("does not commit leftover bindings while an initialization lease is held", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: invokerOnlySnapshot(),
        publicKey: null,
        credentialRefs: {
          credentialIds: ["cred-old"],
          bindings: [
            {
              handle: "api",
              credentialId: "cred-old",
              consumer: "tool:pkg",
            },
          ],
        },
        initializationLeaseId: "lease-1",
      }),
      principalKeyStore: stubPrincipalKeyStore,
      grantStore: createInMemoryGrantStore([]),
    });
    const result = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: WORKFLOW_ADDRESS,
    });
    expect(result).toEqual({ outcome: "notReady" });
  });

  test("fails closed when the definition has no approved grant snapshot", async () => {
    // A null `grant_snapshot` column is the "not yet approved" state. The
    // materializer must raise rather than launch a run with an empty grant set.
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({ deploymentRow, assetRow, grantSnapshot: null }),
      principalKeyStore: stubPrincipalKeyStore,
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
      principalKeyStore: stubPrincipalKeyStore,
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
