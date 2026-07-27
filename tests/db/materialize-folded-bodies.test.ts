import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { type } from "arktype";
import { eq } from "drizzle-orm";

import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import { createGrantStore, resolveInferencePreferences } from "@intx/db";
import {
  agent,
  agentVersion,
  asset,
  workflowDefinition,
} from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedCredential,
  seedGrant,
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedPrincipal,
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";
import {
  createAgentRepoStore,
  createAssetService,
  MaterializeError,
  materializeFoldedBodies,
  runBackfill,
  WORKFLOW_JSON_PATH,
  workflowDefinitionEnvelopeSchema,
} from "@intx/hub-sessions";
import {
  deriveDeploymentAddress,
  synthesizeFoldedWorkflow,
} from "@intx/workflow-deploy";

// Materialization (phase 2 of the agent fold) freezes each folded definition's
// synthesized workflow.json into a `workflow`-kind asset and points the
// definition at it, so the body survives the agent table's retirement. This
// drives the real materializer against a real asset repo (over a temp data
// dir), proving: the frozen body is byte-faithful to an independent synthesis
// from the agent's columns; it satisfies the hydrate envelope contract; the run
// is idempotent and recovers a partial prior run; and an ambiguous model
// resolution fails loud into the manifest rather than freezing a lossy body.

const TENANT = "tnt_root";
const DOMAIN = `${TENANT}.example.test`;
const CREATOR = "prn_creator";

const tempDirs: string[] = [];

describe.skipIf(!harnessDbEnvAvailable())(
  "materialize folded bodies (real DB)",
  () => {
    let h: TestDb;
    let signingKey: KeyPair;
    let assetService: ReturnType<typeof createAssetService>;

    beforeAll(async () => {
      h = await createTestDb();
      signingKey = await generateKeyPair();
      const dataDir = await fs.promises.mkdtemp(
        path.join(os.tmpdir(), "materialize-folded-"),
      );
      tempDirs.push(dataDir);
      const agentRepoStore = createAgentRepoStore({ dataDir, signingKey });
      assetService = createAssetService({
        db: h.db,
        repoStore: agentRepoStore.repoStore,
      });
    });

    afterAll(async () => {
      await h.close();
      for (const d of tempDirs) {
        await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
          // Best-effort cleanup; a leaked temp dir is not a test failure.
        });
      }
    });

    beforeEach(async () => {
      await h.reset();
    });

    // A credential-backed "opus" offering plus a creator authorized to use
    // credentials, so a deployable agent's model requirements resolve to a
    // single source. Mirrors `seedResolvableBase` in the db-backfill suite.
    async function seedResolvableBase(): Promise<void> {
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedProvider(h.db, {
        id: "prv_x",
        tenantId: TENANT,
        name: "prv-x",
      });
      await seedCredential(h.db, {
        id: "cred_a",
        tenantId: TENANT,
        providerId: "prv_x",
        name: "cred-a",
        secret: "sk-anthropic",
      });
      await seedModel(h.db, {
        id: "mdl_opus",
        tenantId: TENANT,
        canonicalName: "opus",
      });
      await seedModelProvider(h.db, {
        id: "mpv_anthropic",
        tenantId: TENANT,
        name: "anthropic",
        credentialId: "cred_a",
      });
      await seedModelOffering(h.db, {
        id: "mof_a",
        tenantId: TENANT,
        modelId: "mdl_opus",
        providerId: "mpv_anthropic",
        priority: 0,
        capabilities: [],
      });
      await seedPrincipal(h.db, { id: CREATOR, tenantId: TENANT });
      await seedGrant(h.db, {
        id: "grt_cred",
        tenantId: TENANT,
        resource: "credential:*",
        action: "use",
        principalId: CREATOR,
        effect: "allow",
        origin: "creator",
      });
    }

    async function insertAgent(opts: {
      id: string;
      systemPrompt: string | null;
      modelRequirements: unknown;
      name?: string;
      description?: string;
      toolPackages?: unknown;
      grantRequirements?: unknown;
    }): Promise<void> {
      await h.db.insert(agent).values({
        id: opts.id,
        tenantId: TENANT,
        creatorPrincipalId: CREATOR,
        name: opts.name ?? opts.id,
        description: opts.description ?? null,
        systemPrompt: opts.systemPrompt,
        modelRequirements: opts.modelRequirements,
        toolPackages: opts.toolPackages ?? [],
        grantRequirements: opts.grantRequirements ?? null,
      });
      await h.db.insert(agentVersion).values({
        id: `agv_${opts.id}_1`,
        agentId: opts.id,
        version: "1",
        status: "active",
      });
    }

    async function foldedDefinition(agentId: string): Promise<{
      id: string;
      assetId: string | null;
      grantRequirements: unknown;
    }> {
      const rows = await h.db
        .select()
        .from(workflowDefinition)
        .where(eq(workflowDefinition.originAgentId, agentId));
      const def = rows[0];
      if (def === undefined) {
        throw new Error(`no folded definition for ${agentId}`);
      }
      return {
        id: def.id,
        assetId: def.assetId,
        grantRequirements: def.grantRequirements,
      };
    }

    async function readWorkflowJson(assetId: string): Promise<string> {
      const bytes = await assetService.readAssetBlob({
        assetId,
        path: WORKFLOW_JSON_PATH,
      });
      return new TextDecoder().decode(bytes);
    }

    test("freezes a body byte-faithful to an independent synthesis", async () => {
      await seedResolvableBase();
      const toolPackages = [{ name: "left-pad", version: "^1.0.0" }];
      await insertAgent({
        id: "agt_ok",
        systemPrompt: "You are helpful.",
        modelRequirements: [{ model: "opus" }],
        name: "helper-bot",
        description: "a helper",
        toolPackages,
      });
      await runBackfill(h.db);

      const summary = await materializeFoldedBodies(h.db, assetService);
      expect(summary.bodiesMaterialized).toBe(1);
      expect(summary.bodiesSkipped).toBe(0);

      const def = await foldedDefinition("agt_ok");
      expect(def.assetId).not.toBeNull();
      const assetId = def.assetId;
      if (assetId === null) throw new Error("expected asset_id");

      // The asset is a workflow-kind row named deterministically off the agent
      // id, carrying the human name as its display name.
      const assetRows = await h.db
        .select()
        .from(asset)
        .where(eq(asset.id, assetId));
      const assetRow = assetRows[0];
      if (assetRow === undefined) throw new Error("expected asset row");
      expect(assetRow.kind).toBe("workflow");
      expect(assetRow.name).toBe("folded-agt-ok");
      expect(assetRow.displayName).toBe("helper-bot");
      expect(assetRow.tenantId).toBe(TENANT);

      const bodyText = await readWorkflowJson(assetId);

      // Hydrate contract: the frozen workflow.json parses and passes the
      // envelope schema the deploy-time hydrate validates against.
      const parsed = workflowDefinitionEnvelopeSchema(JSON.parse(bodyText));
      expect(parsed instanceof type.errors).toBe(false);

      // Byte fidelity: the frozen body equals an independent synthesis built
      // from the agent's own columns -- proving materialization captured the
      // system prompt, tool pins, inference preferences, description, and
      // trigger address without drift.
      const creatorGrants = await createGrantStore(h.db).collectGrantsInChain(
        CREATOR,
        TENANT,
      );
      const inferencePreferences = await resolveInferencePreferences(
        h.db,
        TENANT,
        [{ model: "opus" }],
        creatorGrants,
      );
      const expected = JSON.stringify(
        synthesizeFoldedWorkflow({
          workflowId: "wf_agt_ok",
          mailAddress: deriveDeploymentAddress({
            deploymentId: "agt_ok",
            deploymentDomain: DOMAIN,
          }),
          systemPrompt: "You are helpful.",
          description: "a helper",
          inferencePreferences,
          toolPackagePins: toolPackages,
        }),
        null,
        2,
      );
      expect(bodyText).toBe(expected);

      // Explicit field checks (guarding against a shared reconstruction bug):
      // the verbatim system prompt is present, and the trigger routes to the
      // definition's stable deployment address, not a per-launch instance one.
      expect(bodyText).toContain("You are helpful.");
      expect(bodyText).toContain(`"to": "ins_agt_ok@${DOMAIN}"`);
    });

    test("carries grant requirements into the envelope matching the row", async () => {
      await seedResolvableBase();
      const grantRequirements = [
        { source: "creator", resource: "mail:acme", action: "send" },
      ];
      await insertAgent({
        id: "agt_gr",
        systemPrompt: "You are helpful.",
        modelRequirements: [{ model: "opus" }],
        grantRequirements,
      });
      await runBackfill(h.db);
      await materializeFoldedBodies(h.db, assetService);

      const def = await foldedDefinition("agt_gr");
      if (def.assetId === null) throw new Error("expected asset_id");
      const envelope = workflowDefinitionEnvelopeSchema(
        JSON.parse(await readWorkflowJson(def.assetId)),
      );
      if (envelope instanceof type.errors) {
        throw new Error(`envelope failed validation: ${envelope.summary}`);
      }
      // The frozen envelope's grant requirements match the definition row's
      // column: the two copies are written from one source and must not drift.
      expect(def.grantRequirements).toEqual(envelope.grantRequirements);
      // And both captured the agent's requirement, not an empty set.
      expect(JSON.stringify(def.grantRequirements)).toContain("mail:acme");
    });

    test("is idempotent and reuses the asset after a partial prior run", async () => {
      await seedResolvableBase();
      await insertAgent({
        id: "agt_idem",
        systemPrompt: "You are helpful.",
        modelRequirements: [{ model: "opus" }],
      });
      await runBackfill(h.db);

      const first = await materializeFoldedBodies(h.db, assetService);
      expect(first.bodiesMaterialized).toBe(1);
      const def = await foldedDefinition("agt_idem");
      const assetId = def.assetId;
      if (assetId === null) throw new Error("expected asset_id");

      // A clean re-run skips the already-materialized definition.
      const second = await materializeFoldedBodies(h.db, assetService);
      expect(second.bodiesMaterialized).toBe(0);
      expect(second.bodiesSkipped).toBe(1);

      // Simulate a crash between the asset write and the asset_id bind: the
      // asset and its repo exist, but the definition points at nothing.
      await h.db
        .update(workflowDefinition)
        .set({ assetId: null })
        .where(eq(workflowDefinition.id, def.id));

      const recovered = await materializeFoldedBodies(h.db, assetService);
      expect(recovered.bodiesMaterialized).toBe(1);

      // The recovery reuses the same asset by name -- no orphan second asset.
      const rebound = await foldedDefinition("agt_idem");
      expect(rebound.assetId).toBe(assetId);
      const workflowAssets = await h.db
        .select()
        .from(asset)
        .where(eq(asset.name, "folded-agt-idem"));
      expect(workflowAssets).toHaveLength(1);
    });

    test("fails loud when the model resolution is not injective", async () => {
      await seedResolvableBase();
      await insertAgent({
        id: "agt_ambiguous",
        systemPrompt: "You are helpful.",
        modelRequirements: [{ model: "opus" }],
      });
      // Fold while the resolution is still injective (one provider offers
      // "opus"), so the definition lands with a null asset_id.
      await runBackfill(h.db);

      // The catalog then gains a second anthropic-plugin provider with its own
      // authorized credential, also offering "opus". The agent's model now
      // resolves to two sources that both project to (anthropic, opus) -- an
      // ambiguous collapse the frozen body could not distinguish once the agent
      // columns are dropped. Materialization must catch this at the cliff.
      await seedCredential(h.db, {
        id: "cred_b",
        tenantId: TENANT,
        providerId: "prv_x",
        name: "cred-b",
        secret: "sk-anthropic-2",
      });
      await seedModelProvider(h.db, {
        id: "mpv_anthropic_2",
        tenantId: TENANT,
        name: "anthropic-2",
        credentialId: "cred_b",
      });
      await seedModelOffering(h.db, {
        id: "mof_b",
        tenantId: TENANT,
        modelId: "mdl_opus",
        providerId: "mpv_anthropic_2",
        priority: 1,
        capabilities: [],
      });

      let err: unknown;
      try {
        await materializeFoldedBodies(h.db, assetService);
      } catch (e) {
        err = e;
      }
      if (!(err instanceof MaterializeError)) {
        throw new Error(`expected MaterializeError, got ${String(err)}`);
      }
      const manifest = err.unmaterializable;
      expect(manifest).toHaveLength(1);
      expect(manifest[0]?.agentId).toBe("agt_ambiguous");
      expect(manifest[0]?.reason).toMatch(/not injective/);

      // The definition stays unmaterialized -- no lossy body was frozen.
      const def = await foldedDefinition("agt_ambiguous");
      expect(def.assetId).toBeNull();
    });
  },
);
