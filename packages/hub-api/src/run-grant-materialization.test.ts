import { describe, test, expect } from "bun:test";

import { createInMemoryGrantStore } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";
import type { GrantEffect } from "@intx/types";
import type { AssetService } from "@intx/hub-sessions";
import { workflowRun as workflowRunTable } from "@intx/db/schema";
import {
  createDefaultDirectorRegistry,
  defineAgent,
  type AnnotatedToolFactory,
  type BaseEnv,
  type ToolDeclaration,
} from "@intx/agent";
import { action, defineWorkflow, step } from "@intx/workflow/definition";
import { flattenWalkToSurface, walkCapabilities } from "@intx/workflow-deploy";

import {
  createMailTriggeredRunGrantsMaterializer,
  deriveRunRuntimeGrantRows,
} from "./run-grant-materialization";

const TENANT_ID = "tenant-1";
const ASSET_ID = "asset-wf";
const CREATOR_PRINCIPAL_ID = "prn_creator";
const WORKFLOW_ADDRESS = "run_wf1@tenant.example";

// A one-step workflow whose agent declares one tool, plus a creator-sourced
// and an invoker-sourced grant requirement. The walk yields a `tool:` runtime
// grant; the creator requirement resolves against the creator's grants; the
// invoker requirement must be OMITTED on the mail path.
function workflowJson(): string {
  return JSON.stringify({
    id: "wf_mail",
    triggers: [{ type: "mail", to: WORKFLOW_ADDRESS }],
    stepOrder: ["work"],
    steps: {
      work: {
        kind: "step",
        id: "work",
        agent: {
          id: "worker",
          systemPrompt: "do work",
          toolFactories: [{ id: "fac", definitions: [{ name: "read_file" }] }],
          capabilities: [],
          inference: { sources: [{ provider: "anthropic", model: "m" }] },
        },
        after: [],
      },
    },
    grantRequirements: [
      { resource: "secret:vault", action: "use", source: "creator" },
      { resource: "secret:other", action: "use", source: "invoker" },
    ],
  });
}

// A DB stand-in for the deployment lookup and first-run reservation. It
// reports no pre-existing run principal, so each test exercises the winning
// reservation path.
function mockDb(opts: {
  deploymentRow:
    | { id: string; tenantId: string; definitionAssetId: string }
    | undefined;
  assetRow: unknown;
  topLevelRunStatus?: "running" | "completed" | "failed" | "cancelled" | null;
  lockedRunStatus?: "running" | "completed" | "failed" | "cancelled";
}) {
  // The materializer resolves the deployment's anchor run and its definition in
  // one inner-joined select keyed by address; model that read shape here.
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
            Object.assign(
              Promise.resolve(
                table === workflowRunTable && opts.deploymentRow
                  ? joined
                    ? [
                        {
                          anchorRunId: opts.deploymentRow.id,
                          tenantId: opts.deploymentRow.tenantId,
                          definitionId: `wfd_${opts.deploymentRow.id}`,
                          definitionAssetId:
                            opts.deploymentRow.definitionAssetId,
                          anchorStatus: "running",
                          topLevelRunStatus: opts.topLevelRunStatus ?? null,
                        },
                      ]
                    : [{ status: "running" }]
                  : [],
              ),
              {
                for: () =>
                  Promise.resolve(
                    table === workflowRunTable && opts.deploymentRow
                      ? [{ status: opts.lockedRunStatus ?? "running" }]
                      : [],
                  ),
              },
            ),
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

function mockAssetService(json: string): AssetService {
  function notImpl(name: string): never {
    throw new Error(`mock: assetService.${name} not implemented`);
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only readAssetBlob is exercised by the materializer
  return {
    createAsset: () => notImpl("createAsset"),
    populateAsset: () => notImpl("populateAsset"),
    readAssetBlob: async () => new TextEncoder().encode(json),
  } as unknown as AssetService;
}

// A mutable, read-counting asset service. `setJson` swaps the blob returned
// under the same asset id, modeling a mutated asset; `reads` records how many
// times the materializer actually read the blob (the only path to a capability
// walk), so a stable count across triggers proves no per-run re-read or re-walk.
type CountingAssetService = AssetService & {
  reads: number;
  setJson: (json: string) => void;
};

function countingAssetService(initialJson: string): CountingAssetService {
  function notImpl(name: string): never {
    throw new Error(`mock: assetService.${name} not implemented`);
  }
  let json = initialJson;
  const svc = {
    reads: 0,
    setJson(next: string) {
      json = next;
    },
    createAsset: () => notImpl("createAsset"),
    populateAsset: () => notImpl("populateAsset"),
    readAssetBlob: async () => {
      svc.reads += 1;
      return new TextEncoder().encode(json);
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only readAssetBlob is exercised by the materializer
  return svc as unknown as CountingAssetService;
}

// The same workflow rewritten under a stable asset id: its agent now declares a
// DIFFERENT tool, so a live re-walk would surface `tool:write_file` and drop
// `tool:read_file`. A run bound to the frozen approved walk must ignore this.
function mutatedWorkflowJson(): string {
  return JSON.stringify({
    id: "wf_mail",
    triggers: [{ type: "mail", to: WORKFLOW_ADDRESS }],
    stepOrder: ["work"],
    steps: {
      work: {
        kind: "step",
        id: "work",
        agent: {
          id: "worker",
          systemPrompt: "do work",
          toolFactories: [{ id: "fac", definitions: [{ name: "write_file" }] }],
          capabilities: [],
          inference: { sources: [{ provider: "anthropic", model: "m" }] },
        },
        after: [],
      },
    },
    grantRequirements: [
      { resource: "secret:vault", action: "use", source: "creator" },
      { resource: "secret:other", action: "use", source: "invoker" },
    ],
  });
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
      db: mockDb({ deploymentRow: undefined, assetRow }),
      assetService: mockAssetService(workflowJson()),
      grantStore: createInMemoryGrantStore([creatorGrant()]),
    });
    const result = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: WORKFLOW_ADDRESS,
    });
    expect(result.outcome).toBe("skip");
  });

  test("rejects mail for a terminal stable run before loading its definition", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        topLevelRunStatus: "completed",
      }),
      assetService: {
        ...mockAssetService(workflowJson()),
        readAssetBlob: async () => {
          throw new Error("terminal run must not load its definition");
        },
      },
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

  test("revalidates under lock before reserving a run that became terminal", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({
        deploymentRow,
        assetRow,
        topLevelRunStatus: null,
        lockedRunStatus: "failed",
      }),
      assetService: mockAssetService(workflowJson()),
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
      db: mockDb({ deploymentRow, assetRow }),
      assetService: mockAssetService(workflowJson()),
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
    // The walk's tool grant and the resolved creator requirement are present.
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
    // No creator grant held: the creator requirement would fail closed. But
    // the invoker requirement is filtered out before staging, so a definition
    // with ONLY an invoker requirement still launches.
    const invokerOnlyJson = JSON.stringify({
      id: "wf_invoker_only",
      triggers: [{ type: "mail", to: WORKFLOW_ADDRESS }],
      stepOrder: ["work"],
      steps: {
        work: {
          kind: "step",
          id: "work",
          agent: {
            id: "worker",
            systemPrompt: "do work",
            toolFactories: [
              { id: "fac", definitions: [{ name: "read_file" }] },
            ],
            capabilities: [],
            inference: { sources: [{ provider: "anthropic", model: "m" }] },
          },
          after: [],
        },
      },
      grantRequirements: [
        { resource: "secret:other", action: "use", source: "invoker" },
      ],
    });
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({ deploymentRow, assetRow }),
      assetService: mockAssetService(invokerOnlyJson),
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
    // Only the walk's tool grant survives; the invoker requirement is omitted
    // and the creator requirement is absent, so the run launches with the tool.
    expect(resources).toEqual(["tool:read_file/invoke"]);
  });
});

describe("createMailTriggeredRunGrantsMaterializer frozen basis", () => {
  test("does not re-read or re-walk the definition per run", async () => {
    const assets = countingAssetService(workflowJson());
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({ deploymentRow, assetRow }),
      assetService: assets,
      grantStore: createInMemoryGrantStore([creatorGrant()]),
    });

    const first = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: RUN_ID,
    });
    if (first.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${first.outcome}`);
    }
    // The first trigger reads and walks the approved definition exactly once.
    expect(assets.reads).toBe(1);

    // A second trigger of the SAME deployment consumes the closure-cached
    // frozen basis: the asset blob is never read again, and the capability
    // walk -- which can only run over a freshly read definition -- therefore
    // does not run again either.
    const second = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: RUN_ID,
    });
    if (second.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${second.outcome}`);
    }
    expect(assets.reads).toBe(1);
  });

  test("a mutated asset blob under a stable id cannot change run grants", async () => {
    const assets = countingAssetService(workflowJson());
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({ deploymentRow, assetRow }),
      assetService: assets,
      grantStore: createInMemoryGrantStore([creatorGrant()]),
    });

    const first = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: RUN_ID,
    });
    if (first.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${first.outcome}`);
    }
    const firstResources = first.stepGrants
      .map((g) => `${g.resource}/${g.action}`)
      .sort();
    expect(firstResources).toContain("tool:read_file/invoke");

    // Rewrite the asset blob under the SAME asset id to a definition whose
    // live re-walk would surface a different tool grant and drop the original.
    assets.setJson(mutatedWorkflowJson());

    const second = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: RUN_ID,
    });
    if (second.outcome !== "materialized") {
      throw new Error(`expected materialized, got ${second.outcome}`);
    }
    const secondResources = second.stepGrants
      .map((g) => `${g.resource}/${g.action}`)
      .sort();
    // The run's grants are the deploy-approved set, unchanged by the mutation:
    // the smuggled-in tool never appears, and the original is still present.
    expect(secondResources).toEqual(firstResources);
    expect(secondResources).toContain("tool:read_file/invoke");
    expect(secondResources).not.toContain("tool:write_file/invoke");
  });
});

function makeFactory(
  id: string,
  definitions: readonly ToolDeclaration[],
): AnnotatedToolFactory<BaseEnv> {
  const factory = (_env: BaseEnv) => ({
    definitions: [],
    run: () =>
      Promise.resolve({ callId: "", content: "", isError: false as const }),
  });
  return Object.assign(factory, {
    id,
    requires: [] as readonly string[],
    definitions,
  });
}

describe("flattenWalkToSurface agrees with deriveRunRuntimeGrantRows", () => {
  // The equality is the whole point of extracting the shared core: a child
  // workflow's stored pinned surface (built via flattenWalkToSurface) must
  // equal the runtime ceiling the run-grant materializer emits from the same
  // walk. This pins the two producers together across the
  // workflow-deploy/hub-api boundary so neither can drift.
  test("same resources and effects on a walk with ask, allow, and effect grants", () => {
    const registry = createDefaultDirectorRegistry();
    const agent = defineAgent({
      id: "ag_equal",
      systemPrompt: "gated + ungated tools",
      tools: [
        makeFactory("@intx/tools-posix/sidecar-bundle", [
          { name: "run_shell", approval: "ask" },
          { name: "list_dir" },
        ]),
      ],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "m" }] },
    });
    const workflow = defineWorkflow({
      id: "wf_equal",
      trigger: { type: "manual" },
      steps: {
        run: step({ agent }),
        commit: action({
          handler: "commit",
          effect: { requires: ["git:commit"] },
          after: ["run"],
        }),
      },
    });

    const walk = walkCapabilities(workflow, registry);
    const surface = flattenWalkToSurface(walk);
    const rows = deriveRunRuntimeGrantRows(
      walk,
      "tnt",
      "prn",
      new Date("2020-01-01T00:00:00Z"),
    );

    const rowEffects: Record<string, GrantEffect> = {};
    for (const row of rows) {
      rowEffects[row.resource] = row.effect;
    }
    expect(surface.grants).toEqual(rows.map((r) => r.resource).sort());
    expect(surface.grantEffects).toEqual(rowEffects);
  });
});
