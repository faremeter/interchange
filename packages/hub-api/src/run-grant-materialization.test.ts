import { describe, test, expect } from "bun:test";

import { createInMemoryGrantStore } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";
import type { AssetService } from "@intx/hub-sessions";

import { createMailTriggeredRunGrantsMaterializer } from "./run-grant-materialization";

const TENANT_ID = "tenant-1";
const ASSET_ID = "asset-wf";
const CREATOR_PRINCIPAL_ID = "prn_creator";
const WORKFLOW_ADDRESS = "ins_dep_wf1@tenant.example";

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

// A DB stand-in exercising only the reads STAGING performs: the deployment
// lookup and the workflow asset lookup. Staging writes nothing, so the
// commit path (a real transaction with a guard select and inserts) is left
// to the DB-backed test; this mock deliberately does not model it.
function mockDb(opts: { deploymentRow: unknown; assetRow: unknown }) {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return {
    query: {
      workflowDeployment: { findFirst: async () => opts.deploymentRow },
      asset: { findFirst: async () => opts.assetRow },
    },
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
    attachAsset: () => notImpl("attachAsset"),
    listAgentAssets: () => notImpl("listAgentAssets"),
    readAssetBlob: async () => new TextEncoder().encode(json),
  } as unknown as AssetService;
}

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
      runId: "<mail-run-1@tenant.example>",
    });
    expect(result.outcome).toBe("skip");
  });

  test("stages the tool grant and the creator requirement, omitting the invoker one", async () => {
    const materialize = createMailTriggeredRunGrantsMaterializer({
      db: mockDb({ deploymentRow, assetRow }),
      assetService: mockAssetService(workflowJson()),
      grantStore: createInMemoryGrantStore([creatorGrant()]),
    });

    const result = await materialize({
      agentAddress: WORKFLOW_ADDRESS,
      runId: "<mail-run-1@tenant.example>",
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
    // Every staged grant is principal-scoped on the run principal, and the
    // commit is deferred (a callable the caller invokes after delivery).
    expect(typeof result.commit).toBe("function");
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
      runId: "<mail-run-2@tenant.example>",
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
