import { describe, test, expect } from "bun:test";

import { createInMemoryGrantStore } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";
import type { GrantWalkSnapshot } from "@intx/types";
import {
  workflowDefinitionVersion as workflowDefinitionVersionTable,
  workflowRun as workflowRunTable,
} from "@intx/db/schema";
import type { PrincipalKeyStore } from "@intx/db";

import {
  createMailTriggeredRunGrantsMaterializer,
  deriveMailAcceptGrantRows,
  type MailAcceptGrantContext,
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
const SENDER_PRINCIPAL_ID = "prn_sender";
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

// The authenticated sender's own authority over `secret:other`. Held by the
// SENDER principal, so `collectGrants(SENDER_PRINCIPAL_ID, ...)` returns it. Its
// origin is not `invoker`, so it is delegatable when the definition's
// invoker-sourced requirement resolves against it.
function invokerGrant(): GrantRule {
  return {
    id: "grant-sender-other",
    resource: "secret:other",
    action: "use",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: SENDER_PRINCIPAL_ID,
  };
}

// The sender identity the mail seam resolves and threads into every
// materialize call below. A run binds this principal as its invoker.
const senderArgs = {
  senderPrincipalId: SENDER_PRINCIPAL_ID,
  senderTenantId: TENANT_ID,
} as const;

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
      ...senderArgs,
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
        ...senderArgs,
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
      // Both the creator and the sender's invoker grant are held, so staging
      // succeeds and the run reaches the under-lock revalidation the test
      // exercises -- the lock, not an insufficient-authority reject, is what
      // rejects here.
      grantStore: createInMemoryGrantStore([creatorGrant(), invokerGrant()]),
    });

    await expect(
      materialize({
        agentAddress: WORKFLOW_ADDRESS,
        runId: WORKFLOW_ADDRESS,
        ...senderArgs,
      }),
    ).resolves.toMatchObject({
      outcome: "rejected",
      status: 409,
      code: "workflow_run_terminal",
    });
  });

  test("stages the tool grant, the creator requirement, and the invoker requirement bound to the sender", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({ deploymentRow, assetRow, grantSnapshot: snapshot() }),
      principalKeyStore: stubPrincipalKeyStore,
      grantStore: createInMemoryGrantStore([creatorGrant(), invokerGrant()]),
    });

    const result = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: WORKFLOW_ADDRESS,
      ...senderArgs,
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
    // The invoker-sourced requirement now materializes against the sender's
    // collected grants -- the mail sender is bound as the run's invoker.
    expect(resources).toContain("secret:other/use");
    // The materialized invoker grant carries invoker origin and the 24h TTL the
    // shared machinery stamps; the creator/runtime grants do not expire.
    const invokerRow = result.stepGrants.find(
      (g) => g.resource === "secret:other",
    );
    expect(invokerRow?.origin).toBe("invoker");
    expect(invokerRow?.expiresAt).not.toBeNull();
    // Every reserved grant is principal-scoped on the run principal.
    for (const g of result.stepGrants) {
      expect(g.roleId).toBeNull();
      expect(g.principalId).not.toBeNull();
    }
  });

  test("binds an invoker-only requirement against the sender's grants", async () => {
    // No creator requirement; only an invoker-sourced one. It resolves against
    // the sender's collected grants, so the run launches with the invoker grant
    // bound -- previously the invoker requirement was stripped and never
    // materialized.
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: invokerOnlySnapshot(),
      }),
      principalKeyStore: stubPrincipalKeyStore,
      grantStore: createInMemoryGrantStore([invokerGrant()]),
    });
    const result = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: WORKFLOW_ADDRESS,
      ...senderArgs,
    });
    if (result.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${result.outcome}`);
    }
    const resources = result.stepGrants
      .map((g) => `${g.resource}/${g.action}`)
      .sort();
    // The tool grant and the invoker requirement both materialize.
    expect(resources).toEqual(["secret:other/use", "tool:read_file/invoke"]);
  });

  test("fails an invoker requirement closed when the sender does not resolve", async () => {
    // An unresolvable/ambiguous sender threads null ids: no invoker grants are
    // collected, so an invoker-sourced requirement fails closed rather than
    // launching under-authorized.
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        grantSnapshot: invokerOnlySnapshot(),
      }),
      principalKeyStore: stubPrincipalKeyStore,
      grantStore: createInMemoryGrantStore([invokerGrant()]),
    });
    await expect(
      materialize({
        agentAddress: WORKFLOW_ADDRESS,
        runId: WORKFLOW_ADDRESS,
        senderPrincipalId: null,
        senderTenantId: null,
      }),
    ).resolves.toMatchObject({
      outcome: "rejected",
      status: 403,
      code: "insufficient_grants",
    });
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
        ...senderArgs,
      }),
    ).rejects.toThrow(/no approved grant snapshot/);
  });
});

describe("deriveMailAcceptGrantRows", () => {
  const CTX: MailAcceptGrantContext = {
    invokerPrincipalId: "prn_invoker",
    definitionId: "wfd_self",
    tenantId: "tenant-1",
    runPrincipalId: "prn_run",
  };
  const NOW = new Date("2026-01-01T00:00:00.000Z");

  // Build a snapshot whose single step carries the given `mail.accept:*`
  // markers plus an inert non-mail grant, so the derivation must ignore
  // `tool:`/`effect:` rows.
  function mailSnapshot(markers: string[]): GrantWalkSnapshot {
    return {
      perStep: [
        {
          stepId: "work",
          grants: ["tool:read_file", ...markers],
          grantEffects: { "tool:read_file": "allow" },
        },
      ],
      grantRequirements: [],
    };
  }

  function resources(markers: string[], ctx = CTX): string[] {
    return deriveMailAcceptGrantRows(mailSnapshot(markers), ctx, NOW)
      .map((row) => row.resource)
      .sort();
  }

  test("resolves invoker/self/tenant relation markers to concrete coordinates", () => {
    expect(
      resources([
        "mail.accept:invoker",
        "mail.accept:self",
        "mail.accept:tenant",
      ]),
    ).toEqual([
      "mail.accept:definition:wfd_self",
      "mail.accept:principal:prn_invoker",
      "mail.accept:tenant:tenant-1",
    ]);
  });

  test("passes explicit concrete coordinates through unchanged", () => {
    expect(
      resources([
        "mail.accept:principal:prn_explicit",
        "mail.accept:definition:wfd_explicit",
      ]),
    ).toEqual([
      "mail.accept:definition:wfd_explicit",
      "mail.accept:principal:prn_explicit",
    ]);
  });

  test("skips parent/child/correspondent markers without error", () => {
    expect(
      resources([
        "mail.accept:parent",
        "mail.accept:child",
        "mail.accept:correspondent",
      ]),
    ).toEqual([]);
  });

  test("emits no invoker row when no invoker principal resolved", () => {
    expect(
      resources(["mail.accept:invoker", "mail.accept:tenant"], {
        ...CTX,
        invokerPrincipalId: null,
      }),
    ).toEqual(["mail.accept:tenant:tenant-1"]);
  });

  test("deduplicates a relation that resolves to an explicit coordinate", () => {
    // `self` resolves to the definition coordinate an explicit entry already
    // names, and the markers repeat across nothing here -- one row survives.
    expect(
      resources(["mail.accept:self", "mail.accept:definition:wfd_self"]),
    ).toEqual(["mail.accept:definition:wfd_self"]);
  });

  test("deduplicates a marker repeated across steps", () => {
    const snapshot: GrantWalkSnapshot = {
      perStep: [
        { stepId: "a", grants: ["mail.accept:tenant"], grantEffects: {} },
        { stepId: "b", grants: ["mail.accept:tenant"], grantEffects: {} },
      ],
      grantRequirements: [],
    };
    const rows = deriveMailAcceptGrantRows(snapshot, CTX, NOW);
    expect(rows).toHaveLength(1);
    expect(rows[0]?.resource).toBe("mail.accept:tenant:tenant-1");
  });

  test("stamps each row as a creator-origin accept/allow grant on the run principal", () => {
    const rows = deriveMailAcceptGrantRows(
      mailSnapshot(["mail.accept:tenant"]),
      CTX,
      NOW,
    );
    expect(rows).toHaveLength(1);
    const row = rows[0];
    expect(row).toMatchObject({
      action: "accept",
      effect: "allow",
      origin: "creator",
      conditions: null,
      expiresAt: null,
      principalId: "prn_run",
      tenantId: "tenant-1",
    });
    expect(row?.createdAt).toEqual(NOW);
    expect(row?.updatedAt).toEqual(NOW);
  });

  test("emits no rows for a snapshot with no mail.accept markers", () => {
    expect(resources([])).toEqual([]);
  });

  test("throws on an unrecognized mail.accept marker", () => {
    expect(() =>
      deriveMailAcceptGrantRows(mailSnapshot(["mail.accept:bogus"]), CTX, NOW),
    ).toThrow(/unrecognized mail.accept marker/);
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
      grantStore: createInMemoryGrantStore([creatorGrant(), invokerGrant()]),
    });

    const first = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: RUN_ID,
      ...senderArgs,
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
      ...senderArgs,
    });
    if (second.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${second.outcome}`);
    }
    expect(reads.count).toBe(1);
  });
});
