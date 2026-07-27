import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

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
  agent,
  agentVersion,
  asset,
  grant,
  workflowDefinition,
  workflowDefinitionVersion,
} from "@intx/db/schema";
import { BackfillPreflightError, runBackfill } from "@intx/hub-sessions";

describe.skipIf(!harnessDbEnvAvailable())("db-backfill (real DB)", () => {
  let h: TestDb;

  beforeAll(async () => {
    h = await createTestDb();
  });

  afterAll(async () => {
    await h.close();
  });

  beforeEach(async () => {
    await h.reset();
  });

  // A credential-backed "opus" offering plus a creator principal authorized to
  // use credentials, so a deployable agent's model requirements resolve to a
  // source. Mirrors `seedBase` in the model-source-resolution suite.
  async function seedResolvableBase(): Promise<void> {
    await seedTenants(h.db, [{ id: "tnt_root" }]);
    await seedProvider(h.db, {
      id: "prv_x",
      tenantId: "tnt_root",
      name: "prv-x",
    });
    await seedCredential(h.db, {
      id: "cred_a",
      tenantId: "tnt_root",
      providerId: "prv_x",
      name: "cred-a",
      secret: "sk-anthropic",
    });
    await seedModel(h.db, {
      id: "mdl_opus",
      tenantId: "tnt_root",
      canonicalName: "opus",
    });
    await seedModelProvider(h.db, {
      id: "mpv_anthropic",
      tenantId: "tnt_root",
      name: "anthropic",
      credentialId: "cred_a",
    });
    await seedModelOffering(h.db, {
      id: "mof_a",
      tenantId: "tnt_root",
      modelId: "mdl_opus",
      providerId: "mpv_anthropic",
      priority: 0,
      capabilities: [],
    });
    await seedPrincipal(h.db, { id: "prn_creator", tenantId: "tnt_root" });
    await seedGrant(h.db, {
      id: "grt_cred",
      tenantId: "tnt_root",
      resource: "credential:*",
      action: "use",
      principalId: "prn_creator",
      effect: "allow",
      origin: "creator",
    });
  }

  async function insertAgent(opts: {
    id: string;
    systemPrompt: string | null;
    modelRequirements: unknown;
    name?: string;
    versions?: { version: string; status?: "active" | "inactive" | "failed" }[];
  }): Promise<void> {
    await h.db.insert(agent).values({
      id: opts.id,
      tenantId: "tnt_root",
      creatorPrincipalId: "prn_creator",
      name: opts.name ?? opts.id,
      systemPrompt: opts.systemPrompt,
      modelRequirements: opts.modelRequirements,
    });
    for (const v of opts.versions ?? []) {
      await h.db.insert(agentVersion).values({
        id: `agv_${opts.id}_${v.version}`,
        agentId: opts.id,
        version: v.version,
        status: v.status ?? "active",
      });
    }
  }

  test("folds a deployable agent into a definition mirroring its versions", async () => {
    await seedResolvableBase();
    await insertAgent({
      id: "agt_ok",
      systemPrompt: "You are helpful.",
      modelRequirements: [{ model: "opus" }],
      name: "helper-bot",
      versions: [
        { version: "1", status: "inactive" },
        { version: "2", status: "active" },
      ],
    });

    const summary = await runBackfill(h.db);
    expect(summary.agentsFolded).toBe(1);
    expect(summary.agentsSkipped).toBe(0);

    const defs = await h.db
      .select()
      .from(workflowDefinition)
      .where(eq(workflowDefinition.originAgentId, "agt_ok"));
    expect(defs).toHaveLength(1);
    const def = defs[0];
    if (def === undefined) throw new Error("expected a definition");
    expect(def.assetId).toBeNull();
    expect(def.name).toBe("helper-bot");
    expect(def.tenantId).toBe("tnt_root");
    expect(def.creatorPrincipalId).toBe("prn_creator");
    // The agent's model requirements are mirrored onto the definition, so a
    // folded launch resolves its sources without the agent row.
    expect(def.modelRequirements).toEqual([{ model: "opus" }]);

    const versions = await h.db
      .select()
      .from(workflowDefinitionVersion)
      .where(eq(workflowDefinitionVersion.definitionId, def.id));
    expect(versions.map((v) => v.version).sort()).toEqual(["1", "2"]);
    const v1 = versions.find((v) => v.version === "1");
    if (v1 === undefined) throw new Error("expected version 1");
    expect(v1.status).toBe("inactive");
  });

  test("is idempotent: a second run folds nothing and leaves one definition", async () => {
    await seedResolvableBase();
    await insertAgent({
      id: "agt_ok",
      systemPrompt: "p",
      modelRequirements: [{ model: "opus" }],
      versions: [{ version: "1" }],
    });

    await runBackfill(h.db);
    const second = await runBackfill(h.db);
    expect(second.agentsFolded).toBe(0);
    expect(second.agentsSkipped).toBe(1);

    const defs = await h.db
      .select()
      .from(workflowDefinition)
      .where(eq(workflowDefinition.originAgentId, "agt_ok"));
    expect(defs).toHaveLength(1);
  });

  test("preflight aborts with a manifest and writes nothing when any agent is undeployable", async () => {
    await seedResolvableBase();
    await insertAgent({
      id: "agt_ok",
      systemPrompt: "p",
      modelRequirements: [{ model: "opus" }],
      versions: [{ version: "1" }],
    });
    // Null prompt: resolves a source, but the synthesizer throws.
    await insertAgent({
      id: "agt_noprompt",
      systemPrompt: null,
      modelRequirements: [{ model: "opus" }],
    });
    // No requirements: the resolver throws before synthesis.
    await insertAgent({
      id: "agt_noreq",
      systemPrompt: "p",
      modelRequirements: null,
    });

    let caught: unknown;
    try {
      await runBackfill(h.db);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(BackfillPreflightError);
    if (!(caught instanceof BackfillPreflightError)) {
      throw new Error("expected a BackfillPreflightError");
    }
    expect(caught.undeployable.map((u) => u.agentId).sort()).toEqual([
      "agt_noprompt",
      "agt_noreq",
    ]);

    // Nothing was written -- not even for the deployable agent.
    const defs = await h.db.select().from(workflowDefinition);
    expect(defs).toHaveLength(0);
  });

  test("folds a native workflow asset into an asset-backed definition", async () => {
    await seedResolvableBase();
    await h.db.insert(asset).values({
      id: "ast_wf",
      tenantId: "tnt_root",
      kind: "workflow",
      name: "my-workflow",
      creatorPrincipalId: "prn_creator",
    });

    const summary = await runBackfill(h.db);
    expect(summary.workflowAssetsFolded).toBe(1);
    expect(summary.workflowAssetsSkipped).toBe(0);

    const defs = await h.db
      .select()
      .from(workflowDefinition)
      .where(eq(workflowDefinition.assetId, "ast_wf"));
    expect(defs).toHaveLength(1);
    const def = defs[0];
    if (def === undefined) throw new Error("expected a definition");
    expect(def.originAgentId).toBeNull();

    const versions = await h.db
      .select()
      .from(workflowDefinitionVersion)
      .where(eq(workflowDefinitionVersion.definitionId, def.id));
    expect(versions).toHaveLength(1);
    expect(versions[0]?.version).toBe("1");
  });

  test("a re-run folds only the newly-added agents", async () => {
    await seedResolvableBase();
    await insertAgent({
      id: "agt_first",
      systemPrompt: "p",
      modelRequirements: [{ model: "opus" }],
      versions: [{ version: "1" }],
    });
    expect((await runBackfill(h.db)).agentsFolded).toBe(1);

    await insertAgent({
      id: "agt_second",
      systemPrompt: "p",
      modelRequirements: [{ model: "opus" }],
      versions: [{ version: "1" }],
    });
    const second = await runBackfill(h.db);
    expect(second.agentsFolded).toBe(1);
    expect(second.agentsSkipped).toBe(1);

    const originIds = (await h.db.select().from(workflowDefinition)).map(
      (d) => d.originAgentId,
    );
    expect(originIds.sort()).toEqual(["agt_first", "agt_second"]);
  });

  test("a re-run is not blocked by an already-folded agent whose creator lost authorization", async () => {
    await seedResolvableBase();
    // A separately-authorized creator owns the first agent.
    await seedPrincipal(h.db, { id: "prn_creator2", tenantId: "tnt_root" });
    await seedGrant(h.db, {
      id: "grt_cred2",
      tenantId: "tnt_root",
      resource: "credential:*",
      action: "use",
      principalId: "prn_creator2",
      effect: "allow",
      origin: "creator",
    });
    await h.db.insert(agent).values({
      id: "agt_folded",
      tenantId: "tnt_root",
      creatorPrincipalId: "prn_creator2",
      name: "agt_folded",
      systemPrompt: "p",
      modelRequirements: [{ model: "opus" }],
    });
    await h.db.insert(agentVersion).values({
      id: "agv_folded_1",
      agentId: "agt_folded",
      version: "1",
      status: "active",
    });
    expect((await runBackfill(h.db)).agentsFolded).toBe(1);

    // The first agent's creator loses credential authorization, so re-folding
    // it would now fail the preflight -- but it is already folded and out of
    // the preflight's scope, so the run proceeds.
    await h.db.delete(grant).where(eq(grant.id, "grt_cred2"));

    await insertAgent({
      id: "agt_new",
      systemPrompt: "p",
      modelRequirements: [{ model: "opus" }],
      versions: [{ version: "1" }],
    });
    const result = await runBackfill(h.db);
    expect(result.agentsFolded).toBe(1);
    expect(result.agentsSkipped).toBe(1);
  });
});
