import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal, seedTenants } from "@intx/test-harness/seed";
import { workflowDefinition, workflowDefinitionVersion } from "@intx/db/schema";
import {
  resolveChildWorkflowSurface,
  ChildWorkflowSelfReferenceError,
  ChildWorkflowNotFoundError,
  ChildWorkflowKindError,
  ChildWorkflowNotApprovedError,
  type AssetService,
} from "@intx/hub-sessions";
import {
  childWorkflow,
  defineWorkflow,
  sleep,
  step,
  type WorkflowDefinition,
} from "@intx/workflow/definition";
import { computeLiveDefinitionHash, projectLiveToInert } from "@intx/workflow";
import { defineAgent } from "@intx/agent";
import type { ApprovedGrantSurface } from "@intx/types";

const TENANT = "tnt_root";
const OTHER_TENANT = "tnt_other";
const CREATOR = "prn_creator";
const PARENT_ASSET = "ast_parent";

/** A mock asset service that serves canned workflow.json blobs by asset id and
 * records which assets were read, so a test can assert the fold never touches a
 * grandchild's asset. */
function mockAssetService(blobs: Record<string, string>): {
  service: AssetService;
  reads: string[];
} {
  const reads: string[] = [];
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only readAssetBlob is exercised by the fold
  const service = {
    readAssetBlob: async (params: { assetId: string; path: string }) => {
      reads.push(params.assetId);
      const json = blobs[params.assetId];
      if (json === undefined) {
        throw new Error(`mock: no blob for asset ${params.assetId}`);
      }
      return new TextEncoder().encode(json);
    },
  } as unknown as AssetService;
  return { service, reads };
}

/** A minimal child definition with no agent step. */
function childDefinition(id: string): WorkflowDefinition {
  return defineWorkflow({
    id,
    trigger: { type: "manual" },
    steps: { wait: sleep({ duration: 1 }) },
  });
}

/** A child whose step carries an agent, so its inert workflow.json exercises
 * the agent-bearing projection -- the shape `computeLiveDefinitionHash` would
 * throw on and `computeWireDefinitionHash` must hash. */
function agentChildDefinition(id: string): WorkflowDefinition {
  return defineWorkflow({
    id,
    trigger: { type: "manual" },
    steps: {
      run: step({
        agent: defineAgent({
          id: `${id}-agent`,
          systemPrompt: "s",
          tools: [],
          capabilities: [],
          inference: { sources: [{ provider: "anthropic", model: "m" }] },
        }),
      }),
    },
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "resolveChildWorkflowSurface (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT }, { id: OTHER_TENANT }]);
      await seedPrincipal(h.db, {
        id: CREATOR,
        tenantId: TENANT,
        kind: "user",
        refId: "creator",
      });
    });

    /**
     * Seed a deployed, approved child: its workflow asset, a definition row
     * keyed by the content hash of `def`, and a version stamped with `surface`.
     * Returns the child's `workflow.json` blob so the caller wires the mock
     * asset service.
     */
    async function seedApprovedChild(args: {
      assetId: string;
      definitionId: string;
      def: WorkflowDefinition;
      surface: ApprovedGrantSurface | null;
      tenantId?: string;
      version?: string;
      currentVersion?: string;
    }): Promise<string> {
      const tenantId = args.tenantId ?? TENANT;
      const version = args.version ?? "1";
      await seedAsset(h.db, {
        id: args.assetId,
        tenantId,
        kind: "workflow",
        name: args.assetId,
        creatorPrincipalId: CREATOR,
      });
      // The stored wire hash keys on the live definition's projection; the
      // blob is the inert projection that production writes to workflow.json,
      // so the fold's `computeWireDefinitionHash(blob)` reproduces this hash.
      const wireHash = await computeLiveDefinitionHash(args.def);
      await h.db.insert(workflowDefinition).values({
        id: args.definitionId,
        tenantId,
        name: args.assetId,
        assetId: args.assetId,
        wireHash,
        currentVersion: args.currentVersion ?? version,
      });
      await h.db.insert(workflowDefinitionVersion).values({
        id: `wdv_${args.definitionId}_${version}`,
        definitionId: args.definitionId,
        version,
        approvedGrantSurface: args.surface,
      });
      return JSON.stringify(projectLiveToInert(args.def));
    }

    function parentWith(refs: string[]): WorkflowDefinition {
      const steps: Record<string, ReturnType<typeof childWorkflow>> = {};
      refs.forEach((ref, i) => {
        steps[`c${String(i)}`] = childWorkflow({ definitionRef: ref });
      });
      return defineWorkflow({
        id: "parent",
        trigger: { type: "manual" },
        steps,
      });
    }

    test("folds a resolved child's stored surface, landing on the stored row", async () => {
      const def = childDefinition("child");
      const surface: ApprovedGrantSurface = {
        grants: ["tool:search"],
        grantEffects: { "tool:search": "ask" },
      };
      const blob = await seedApprovedChild({
        assetId: "ast_child",
        definitionId: "wfd_child",
        def,
        surface,
      });
      const { service } = mockAssetService({ ast_child: blob });

      const folded = await resolveChildWorkflowSurface(
        {
          definition: parentWith(["ast_child"]),
          deployingAssetId: PARENT_ASSET,
          tenantId: TENANT,
        },
        { db: h.db, assetService: service },
      );
      expect(folded).toEqual(surface);
    });

    test("resolves an agent-bearing child from its inert workflow.json", async () => {
      // The representative case: a child with an agent step, whose workflow.json
      // is the inert projection. Resolution must hash that projection with
      // computeWireDefinitionHash and land on the stored row -- re-projecting it
      // would throw.
      const def = agentChildDefinition("agent-child");
      // Grants are stored sorted; the fold returns the surface sorted too.
      const surface: ApprovedGrantSurface = {
        grants: ["inference.source:anthropic:m", "tool:do"],
        grantEffects: { "tool:do": "ask" },
      };
      const blob = await seedApprovedChild({
        assetId: "ast_child",
        definitionId: "wfd_child",
        def,
        surface,
      });
      const { service } = mockAssetService({ ast_child: blob });

      const folded = await resolveChildWorkflowSurface(
        {
          definition: parentWith(["ast_child"]),
          deployingAssetId: PARENT_ASSET,
          tenantId: TENANT,
        },
        { db: h.db, assetService: service },
      );
      expect(folded).toEqual(surface);
    });

    test("reads the child's stored surface transitively, never a grandchild", async () => {
      // The child's stored surface already contains a grandchild's grant. The
      // parent must union it by reading the child's surface alone -- it must
      // never resolve the grandchild.
      const def = childDefinition("child");
      const surface: ApprovedGrantSurface = {
        grants: ["tool:child", "tool:grandchild"],
        grantEffects: { "tool:child": "allow", "tool:grandchild": "ask" },
      };
      const blob = await seedApprovedChild({
        assetId: "ast_child",
        definitionId: "wfd_child",
        def,
        surface,
      });
      const { service, reads } = mockAssetService({ ast_child: blob });

      const folded = await resolveChildWorkflowSurface(
        {
          definition: parentWith(["ast_child"]),
          deployingAssetId: PARENT_ASSET,
          tenantId: TENANT,
        },
        { db: h.db, assetService: service },
      );
      expect(folded.grants).toEqual(["tool:child", "tool:grandchild"]);
      expect(folded.grantEffects["tool:grandchild"]).toBe("ask");
      expect(reads).toEqual(["ast_child"]);
    });

    test("reads the surface at current_version, not the literal 1", async () => {
      const def = childDefinition("child");
      const v2Surface: ApprovedGrantSurface = {
        grants: ["tool:v2"],
        grantEffects: {},
      };
      const blob = await seedApprovedChild({
        assetId: "ast_child",
        definitionId: "wfd_child",
        def,
        surface: v2Surface,
        version: "2",
        currentVersion: "2",
      });
      // A version "1" exists too, with a different surface; reading it would
      // fold the wrong grants.
      await h.db.insert(workflowDefinitionVersion).values({
        id: "wdv_child_1",
        definitionId: "wfd_child",
        version: "1",
        approvedGrantSurface: { grants: ["tool:v1"], grantEffects: {} },
      });
      const { service } = mockAssetService({ ast_child: blob });

      const folded = await resolveChildWorkflowSurface(
        {
          definition: parentWith(["ast_child"]),
          deployingAssetId: PARENT_ASSET,
          tenantId: TENANT,
        },
        { db: h.db, assetService: service },
      );
      expect(folded.grants).toEqual(["tool:v2"]);
    });

    test("unions multiple children and folds a repeated ref once", async () => {
      const defA = childDefinition("child-a");
      const defB = childDefinition("child-b");
      const blobA = await seedApprovedChild({
        assetId: "ast_a",
        definitionId: "wfd_a",
        def: defA,
        surface: { grants: ["tool:a"], grantEffects: { "tool:a": "allow" } },
      });
      const blobB = await seedApprovedChild({
        assetId: "ast_b",
        definitionId: "wfd_b",
        def: defB,
        surface: { grants: ["tool:b"], grantEffects: { "tool:b": "ask" } },
      });
      const { service, reads } = mockAssetService({
        ast_a: blobA,
        ast_b: blobB,
      });

      const folded = await resolveChildWorkflowSurface(
        {
          definition: parentWith(["ast_a", "ast_b", "ast_a"]),
          deployingAssetId: PARENT_ASSET,
          tenantId: TENANT,
        },
        { db: h.db, assetService: service },
      );
      expect(folded).toEqual({
        grants: ["tool:a", "tool:b"],
        grantEffects: { "tool:a": "allow", "tool:b": "ask" },
      });
      // ast_a appears twice in the parent but is resolved once.
      expect(reads.filter((id) => id === "ast_a")).toHaveLength(1);
    });

    test("yields an empty surface for a parent with no childWorkflow steps", async () => {
      const { service, reads } = mockAssetService({});
      const folded = await resolveChildWorkflowSurface(
        {
          definition: defineWorkflow({
            id: "parent",
            trigger: { type: "manual" },
            steps: { wait: sleep({ duration: 1 }) },
          }),
          deployingAssetId: PARENT_ASSET,
          tenantId: TENANT,
        },
        { db: h.db, assetService: service },
      );
      expect(folded).toEqual({ grants: [], grantEffects: {} });
      expect(reads).toEqual([]);
    });

    test("rejects a self-reference before any blob read", async () => {
      const { service, reads } = mockAssetService({});
      await expect(
        resolveChildWorkflowSurface(
          {
            definition: parentWith([PARENT_ASSET]),
            deployingAssetId: PARENT_ASSET,
            tenantId: TENANT,
          },
          { db: h.db, assetService: service },
        ),
      ).rejects.toBeInstanceOf(ChildWorkflowSelfReferenceError);
      expect(reads).toEqual([]);
    });

    test("rejects a cross-tenant ref without reading its blob", async () => {
      const def = childDefinition("child");
      // The child exists, but in another tenant.
      const blob = await seedApprovedChild({
        assetId: "ast_child",
        definitionId: "wfd_child",
        def,
        surface: { grants: ["tool:x"], grantEffects: {} },
        tenantId: OTHER_TENANT,
      });
      const { service, reads } = mockAssetService({ ast_child: blob });
      await expect(
        resolveChildWorkflowSurface(
          {
            definition: parentWith(["ast_child"]),
            deployingAssetId: PARENT_ASSET,
            tenantId: TENANT,
          },
          { db: h.db, assetService: service },
        ),
      ).rejects.toBeInstanceOf(ChildWorkflowNotFoundError);
      expect(reads).toEqual([]);
    });

    test("rejects a ref to a non-workflow asset", async () => {
      await seedAsset(h.db, {
        id: "ast_skill",
        tenantId: TENANT,
        kind: "skill",
        name: "a-skill",
        creatorPrincipalId: CREATOR,
      });
      const { service } = mockAssetService({});
      await expect(
        resolveChildWorkflowSurface(
          {
            definition: parentWith(["ast_skill"]),
            deployingAssetId: PARENT_ASSET,
            tenantId: TENANT,
          },
          { db: h.db, assetService: service },
        ),
      ).rejects.toBeInstanceOf(ChildWorkflowKindError);
    });

    test("rejects a child that resolves but carries no approved surface", async () => {
      const def = childDefinition("child");
      const blob = await seedApprovedChild({
        assetId: "ast_child",
        definitionId: "wfd_child",
        def,
        surface: null,
      });
      const { service } = mockAssetService({ ast_child: blob });
      await expect(
        resolveChildWorkflowSurface(
          {
            definition: parentWith(["ast_child"]),
            deployingAssetId: PARENT_ASSET,
            tenantId: TENANT,
          },
          { db: h.db, assetService: service },
        ),
      ).rejects.toBeInstanceOf(ChildWorkflowNotApprovedError);
    });

    test("rejects a child whose content does not match any stored definition", async () => {
      // The asset exists and is workflow-kind, but no definition row is keyed
      // by the blob's content hash -- the child was never approved.
      await seedAsset(h.db, {
        id: "ast_child",
        tenantId: TENANT,
        kind: "workflow",
        name: "child",
        creatorPrincipalId: CREATOR,
      });
      const { service } = mockAssetService({
        ast_child: JSON.stringify(childDefinition("child")),
      });
      await expect(
        resolveChildWorkflowSurface(
          {
            definition: parentWith(["ast_child"]),
            deployingAssetId: PARENT_ASSET,
            tenantId: TENANT,
          },
          { db: h.db, assetService: service },
        ),
      ).rejects.toBeInstanceOf(ChildWorkflowNotApprovedError);
    });
  },
);
