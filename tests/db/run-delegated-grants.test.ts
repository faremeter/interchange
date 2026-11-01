import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { asc, eq } from "drizzle-orm";

import { createGrantStore } from "@intx/db";
import {
  approval,
  asset,
  grant,
  principal,
  principalRole,
  role,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import type { GrantRequirement, GrantWalkSnapshot } from "@intx/types";
import {
  findDelegatedAnchorRunIds,
  lockTenantRunDelegatedGrants,
  rederiveRunDelegatedGrants,
} from "@intx/hub-api";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedGrant,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
  seedWorkflowRunLaunchSpec,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const CREATOR = "prn_creator";
const DEPLOYER = "prn_deployer";
const INVOKER_GRANT_TTL_MS = 24 * 60 * 60 * 1000;

const VAULT_CREATOR: GrantRequirement = {
  resource: "secret:vault",
  action: "use",
  source: "creator",
};

describe.skipIf(!harnessDbEnvAvailable())(
  "rederiveRunDelegatedGrants (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT }, { id: "tnt_other" }]);
      for (const id of [CREATOR, DEPLOYER]) {
        await seedPrincipal(h.db, {
          id,
          tenantId: TENANT,
          kind: "user",
          refId: `${id}-user`,
        });
      }
    });

    // Seed a live deployment whose top-level run committed its grants: the
    // anchor run, its run principal, and the frozen snapshot in its launch
    // spec. Returns the run principal id.
    async function seedDeployment(opts: {
      anchorRunId: string;
      requirements: GrantRequirement[];
      perStep?: GrantWalkSnapshot["perStep"];
      creator?: string | null;
      deployer?: string;
      tenantId?: string;
      status?: "deployed" | "running" | "completed";
      principalCreatedAt?: Date;
    }): Promise<string> {
      const tenantId = opts.tenantId ?? TENANT;
      const assetId = `ast_${opts.anchorRunId}`;
      const definitionId = `wfd_${opts.anchorRunId}`;
      const runPrincipalId = `prn_${opts.anchorRunId}`;
      await seedAsset(h.db, {
        id: assetId,
        tenantId,
        kind: "workflow",
        name: assetId,
        creatorPrincipalId: opts.creator === undefined ? CREATOR : opts.creator,
      });
      await h.db.insert(workflowDefinition).values({
        id: definitionId,
        tenantId,
        name: definitionId,
        assetId,
      });
      await h.db.insert(principal).values({
        id: runPrincipalId,
        tenantId,
        kind: "workflow",
        refId: opts.anchorRunId,
        status: "active",
        ...(opts.principalCreatedAt !== undefined
          ? { createdAt: opts.principalCreatedAt }
          : {}),
      });
      await seedWorkflowRun(h.db, {
        id: opts.anchorRunId,
        tenantId,
        anchorRunId: opts.anchorRunId,
        definitionId,
        principalId: runPrincipalId,
        address: `${opts.anchorRunId}@tenant.example`,
        status: opts.status ?? "running",
      });
      await seedWorkflowRunLaunchSpec(h.db, {
        anchorRunId: opts.anchorRunId,
        sourceAuthorityPrincipalId: opts.deployer ?? DEPLOYER,
        grantSnapshot: {
          perStep: opts.perStep ?? [],
          grantRequirements: opts.requirements,
        },
      });
      return runPrincipalId;
    }

    async function rederive(
      principalIds: string[],
      anchorRunIds: string[] = [],
      tenantId = TENANT,
    ) {
      return h.db.transaction((tx) =>
        rederiveRunDelegatedGrants(tx, {
          grantStoreFor: createGrantStore,
          tenantId,
          principalIds,
          anchorRunIds,
          now: new Date(),
        }),
      );
    }

    async function runGrants(principalId: string) {
      return h.db
        .select()
        .from(grant)
        .where(eq(grant.principalId, principalId))
        .orderBy(asc(grant.resource), asc(grant.id));
    }

    function summary(rows: Awaited<ReturnType<typeof runGrants>>) {
      return rows
        .map((r) => `${r.origin} ${r.resource}/${r.action} ${r.effect}`)
        .sort();
    }

    test("a revoked creator grant removes the run's row and reports the run", async () => {
      const run = await seedDeployment({
        anchorRunId: "run_a",
        requirements: [VAULT_CREATOR],
      });
      await seedGrant(h.db, {
        id: "grt_run_vault",
        tenantId: TENANT,
        principalId: run,
        resource: "secret:vault",
        action: "use",
        origin: "creator",
      });

      const changed = await rederive([CREATOR]);

      expect(changed).toEqual([
        { anchorRunId: "run_a", address: "run_a@tenant.example" },
      ]);
      expect(await runGrants(run)).toEqual([]);
    });

    test("a regained creator grant adds the row back", async () => {
      const run = await seedDeployment({
        anchorRunId: "run_a",
        requirements: [VAULT_CREATOR],
      });
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId: TENANT,
        principalId: CREATOR,
        resource: "secret:*",
        action: "use",
      });

      await rederive([CREATOR]);

      const rows = await runGrants(run);
      expect(summary(rows)).toEqual(["creator secret:vault/use allow"]);
      expect(rows[0]?.expiresAt).toBeNull();
    });

    test("a held grant leaves the run's row and its id as they are", async () => {
      const run = await seedDeployment({
        anchorRunId: "run_a",
        requirements: [VAULT_CREATOR],
      });
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId: TENANT,
        principalId: CREATOR,
        resource: "secret:vault",
        action: "use",
      });
      await seedGrant(h.db, {
        id: "grt_run_vault",
        tenantId: TENANT,
        principalId: run,
        resource: "secret:vault",
        action: "use",
        origin: "creator",
      });

      expect(await rederive([CREATOR])).toEqual([]);
      expect((await runGrants(run)).map((r) => r.id)).toEqual([
        "grt_run_vault",
      ]);
    });

    test("a run row matches its requirement whatever order its conditions' keys are stored in", async () => {
      const conditions = { zeta: 1, a: { yy: 2, b: 3 } };
      const run = await seedDeployment({
        anchorRunId: "run_a",
        requirements: [{ ...VAULT_CREATOR, conditions }],
      });
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId: TENANT,
        principalId: CREATOR,
        resource: "secret:vault",
        action: "use",
      });
      await seedGrant(h.db, {
        id: "grt_run_vault",
        tenantId: TENANT,
        principalId: run,
        resource: "secret:vault",
        action: "use",
        origin: "creator",
        conditions,
      });

      expect(await rederive([CREATOR])).toEqual([]);
      expect((await runGrants(run)).map((r) => r.id)).toEqual([
        "grt_run_vault",
      ]);

      await h.db.delete(grant).where(eq(grant.id, "grt_creator_vault"));
      await rederive([CREATOR]);
      expect(await runGrants(run)).toEqual([]);
    });

    test("a requirement in the mail.accept namespace leaves the run's accept row alone", async () => {
      const run = await seedDeployment({
        anchorRunId: "run_a",
        requirements: [
          {
            resource: "mail.accept:asset:ast_run_a",
            action: "accept",
            source: "creator",
          },
        ],
      });
      await seedGrant(h.db, {
        id: "grt_run_accept",
        tenantId: TENANT,
        principalId: run,
        resource: "mail.accept:asset:ast_run_a",
        action: "accept",
        origin: "creator",
      });

      expect(await rederive([CREATOR])).toEqual([]);
      expect(await runGrants(run)).toHaveLength(1);
    });

    const SELF_READ: GrantRequirement = {
      resource: "workflow-run:self",
      action: "read",
      source: "creator",
    };

    test("a self-run requirement held by the creator leaves the bound row alone", async () => {
      const run = await seedDeployment({
        anchorRunId: "run_s",
        requirements: [SELF_READ],
      });
      await seedGrant(h.db, {
        id: "grt_creator_runs",
        tenantId: TENANT,
        principalId: CREATOR,
        resource: "workflow-run:*",
        action: "read",
      });
      await seedGrant(h.db, {
        id: "grt_run_self",
        tenantId: TENANT,
        principalId: run,
        resource: "workflow-run:run_s",
        action: "read",
        origin: "creator",
      });

      expect(await rederive([CREATOR])).toEqual([]);
      expect((await runGrants(run)).map((r) => r.id)).toEqual(["grt_run_self"]);
    });

    test("a self-run requirement the creator lost removes the bound row", async () => {
      const run = await seedDeployment({
        anchorRunId: "run_s",
        requirements: [SELF_READ],
      });
      await seedGrant(h.db, {
        id: "grt_run_self",
        tenantId: TENANT,
        principalId: run,
        resource: "workflow-run:run_s",
        action: "read",
        origin: "creator",
      });

      expect((await rederive([CREATOR])).map((r) => r.anchorRunId)).toEqual([
        "run_s",
      ]);
      expect(await runGrants(run)).toEqual([]);
    });

    describe("through a role", () => {
      async function seedRoleWithVault() {
        await h.db
          .insert(role)
          .values({ id: "rol_vault", tenantId: TENANT, name: "vault" });
        await h.db
          .insert(principalRole)
          .values({ principalId: CREATOR, roleId: "rol_vault" });
        await seedGrant(h.db, {
          id: "grt_role_vault",
          tenantId: TENANT,
          roleId: "rol_vault",
          resource: "secret:vault",
          action: "use",
          origin: "role",
        });
      }

      test("a removed role membership removes the run's row", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_CREATOR],
        });
        await seedRoleWithVault();
        await rederive([CREATOR]);
        expect(summary(await runGrants(run))).toEqual([
          "creator secret:vault/use allow",
        ]);

        await h.db
          .delete(principalRole)
          .where(eq(principalRole.principalId, CREATOR));
        await rederive([CREATOR]);

        expect(await runGrants(run)).toEqual([]);
      });

      test("a deleted role grant removes the run's row", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_CREATOR],
        });
        await seedRoleWithVault();
        await rederive([CREATOR]);

        await h.db.delete(grant).where(eq(grant.id, "grt_role_vault"));
        await rederive([CREATOR]);

        expect(await runGrants(run)).toEqual([]);
      });
    });

    describe("invoker rows", () => {
      const VAULT_INVOKER: GrantRequirement = {
        resource: "secret:vault",
        action: "use",
        source: "invoker",
      };

      async function grantDeployerVault() {
        await seedGrant(h.db, {
          id: "grt_deployer_vault",
          tenantId: TENANT,
          principalId: DEPLOYER,
          resource: "secret:vault",
          action: "use",
        });
      }

      test("a re-added invoker row keeps the first trigger's expiry", async () => {
        const createdAt = new Date(Date.now() - 60 * 60 * 1000);
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_INVOKER],
          principalCreatedAt: createdAt,
        });
        await grantDeployerVault();

        await rederive([DEPLOYER]);

        const rows = await runGrants(run);
        expect(summary(rows)).toEqual(["invoker secret:vault/use allow"]);
        expect(rows[0]?.expiresAt?.getTime()).toBe(
          createdAt.getTime() + INVOKER_GRANT_TTL_MS,
        );
      });

      test("an invoker row whose first-trigger expiry has passed is not added", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_INVOKER],
          principalCreatedAt: new Date(Date.now() - 2 * INVOKER_GRANT_TTL_MS),
        });
        await grantDeployerVault();

        expect(await rederive([DEPLOYER])).toEqual([]);
        expect(await runGrants(run)).toEqual([]);
      });

      test("a deactivated deployer's invoker rows lapse", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_INVOKER],
        });
        await grantDeployerVault();
        await rederive([DEPLOYER]);
        expect(await runGrants(run)).toHaveLength(1);

        await h.db
          .update(principal)
          .set({ status: "deactivated" })
          .where(eq(principal.id, DEPLOYER));
        await rederive([DEPLOYER]);

        expect(await runGrants(run)).toEqual([]);
      });
    });

    describe("floors and standing decisions", () => {
      const ASK_TOOL: GrantRequirement = {
        resource: "tool:search",
        action: "invoke",
        source: "creator",
        effect: "ask",
        conditions: { tool: "pkg" },
      };

      async function seedToolRow(
        run: string,
        effect: "allow" | "deny" | "ask",
      ) {
        await seedGrant(h.db, {
          id: `grt_run_search_${effect}`,
          tenantId: TENANT,
          principalId: run,
          resource: "tool:search",
          action: "invoke",
          origin: "creator",
          effect,
          conditions: { tool: "pkg" },
        });
      }

      async function seedStandingDecision(
        status: "approved" | "rejected",
        resolvedAt = new Date(),
      ) {
        await h.db.insert(approval).values({
          id: `apv_${status}`,
          tenantId: TENANT,
          anchorRunId: "run_a",
          runId: "run_a",
          agentAddress: "run_a@tenant.example",
          correlationId: `cor_${status}`,
          toolDefinition: {
            name: "search",
            description: "search",
            inputSchema: {},
          },
          toolArguments: {},
          scope: "always",
          status,
          resolvedAt,
        });
      }

      test("a regained ask row gets the run's latest standing decision", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_TOOL],
        });
        await seedStandingDecision("rejected", new Date(Date.now() - 1000));
        await seedStandingDecision("approved");
        await seedGrant(h.db, {
          id: "grt_creator_search",
          tenantId: TENANT,
          principalId: CREATOR,
          resource: "tool:*",
          action: "invoke",
        });

        await rederive([CREATOR]);

        expect(summary(await runGrants(run))).toEqual([
          "creator tool:search/invoke allow",
        ]);
      });

      test("a lapsed requirement keeps its ask and deny rows", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_TOOL],
        });
        await seedToolRow(run, "ask");
        await seedToolRow(run, "deny");

        expect(await rederive([CREATOR])).toEqual([]);
        expect(summary(await runGrants(run))).toEqual([
          "creator tool:search/invoke ask",
          "creator tool:search/invoke deny",
        ]);
      });

      test("a lapsed requirement loses an allow that a standing approval made", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_TOOL],
        });
        await seedToolRow(run, "allow");
        await seedStandingDecision("approved");

        await rederive([CREATOR]);

        expect(await runGrants(run)).toEqual([]);
      });

      test("a regained ask row gets the run's standing decision back", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_TOOL],
        });
        await seedStandingDecision("rejected");
        await seedGrant(h.db, {
          id: "grt_creator_search",
          tenantId: TENANT,
          principalId: CREATOR,
          resource: "tool:*",
          action: "invoke",
        });

        await rederive([CREATOR]);

        expect(summary(await runGrants(run))).toEqual([
          "creator tool:search/invoke deny",
        ]);
      });

      test("a requirement on a runtime tool row's key is left alone", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [
            { resource: "tool:read_file", action: "invoke", source: "creator" },
          ],
          perStep: [
            {
              stepId: "work",
              grants: ["tool:read_file"],
              grantEffects: { "tool:read_file": "allow" },
            },
          ],
        });
        await seedGrant(h.db, {
          id: "grt_run_read_file",
          tenantId: TENANT,
          principalId: run,
          resource: "tool:read_file",
          action: "invoke",
          origin: "creator",
        });

        expect(await rederive([CREATOR])).toEqual([]);
        expect(await runGrants(run)).toHaveLength(1);
      });
    });

    test("an ended run is not changed", async () => {
      const run = await seedDeployment({
        anchorRunId: "run_a",
        requirements: [VAULT_CREATOR],
        status: "completed",
      });
      await seedGrant(h.db, {
        id: "grt_run_vault",
        tenantId: TENANT,
        principalId: run,
        resource: "secret:vault",
        action: "use",
        origin: "creator",
      });

      expect(await rederive([CREATOR])).toEqual([]);
      expect(await runGrants(run)).toHaveLength(1);
    });

    test("an ended deployment is not a candidate anchor", async () => {
      await seedDeployment({
        anchorRunId: "run_deployed",
        requirements: [VAULT_CREATOR],
        status: "deployed",
      });
      await seedDeployment({
        anchorRunId: "run_ended",
        requirements: [VAULT_CREATOR],
        status: "completed",
      });

      expect(
        await findDelegatedAnchorRunIds(h.db, TENANT, [CREATOR, DEPLOYER]),
      ).toEqual(["run_deployed"]);
    });

    test("a run with a stop requested still loses the row", async () => {
      const run = await seedDeployment({
        anchorRunId: "run_a",
        requirements: [VAULT_CREATOR],
      });
      await seedGrant(h.db, {
        id: "grt_run_vault",
        tenantId: TENANT,
        principalId: run,
        resource: "secret:vault",
        action: "use",
        origin: "creator",
      });
      await h.db
        .update(workflowRun)
        .set({ cancellationRequestedAt: new Date() })
        .where(eq(workflowRun.id, "run_a"));

      await rederive([CREATOR]);

      expect(await runGrants(run)).toEqual([]);
    });

    test("a deployment's run re-derives the deployments it deployed", async () => {
      // run_b's run principal deployed run_c. Both delegate from the creator:
      // run_b directly, run_c through run_b's invoker authority. run_c sorts
      // first, so it is re-derived again after run_b changes.
      const parent = await seedDeployment({
        anchorRunId: "run_b",
        requirements: [VAULT_CREATOR],
      });
      const child = await seedDeployment({
        anchorRunId: "run_a_child",
        requirements: [
          VAULT_CREATOR,
          { resource: "secret:vault", action: "use", source: "invoker" },
        ],
        deployer: parent,
      });
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId: TENANT,
        principalId: CREATOR,
        resource: "secret:vault",
        action: "use",
      });
      await rederive([CREATOR]);
      expect(summary(await runGrants(child))).toEqual([
        "creator secret:vault/use allow",
        "invoker secret:vault/use allow",
      ]);

      await h.db.delete(grant).where(eq(grant.id, "grt_creator_vault"));
      const changed = await rederive([CREATOR]);

      expect(changed.map((r) => r.anchorRunId).sort()).toEqual([
        "run_a_child",
        "run_b",
      ]);
      expect(await runGrants(parent)).toEqual([]);
      expect(await runGrants(child)).toEqual([]);
    });

    test("a deployment's run re-derives the deployments whose asset it created", async () => {
      // run_b's run principal created run_a_made's asset, so run_a_made
      // delegates its creator requirement from run_b's rows. run_a_made sorts
      // first, so it is re-derived again after run_b changes.
      const maker = await seedDeployment({
        anchorRunId: "run_b",
        requirements: [VAULT_CREATOR],
      });
      const made = await seedDeployment({
        anchorRunId: "run_a_made",
        requirements: [VAULT_CREATOR],
        creator: maker,
      });
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId: TENANT,
        principalId: CREATOR,
        resource: "secret:vault",
        action: "use",
      });
      await rederive([CREATOR]);
      expect(summary(await runGrants(made))).toEqual([
        "creator secret:vault/use allow",
      ]);

      await h.db.delete(grant).where(eq(grant.id, "grt_creator_vault"));
      const changed = await rederive([CREATOR]);

      expect(changed.map((r) => r.anchorRunId).sort()).toEqual([
        "run_a_made",
        "run_b",
      ]);
      expect(await runGrants(maker)).toEqual([]);
      expect(await runGrants(made)).toEqual([]);
    });

    test("a seeded anchor is re-derived after its creator is deleted", async () => {
      const run = await seedDeployment({
        anchorRunId: "run_a",
        requirements: [VAULT_CREATOR],
      });
      await seedGrant(h.db, {
        id: "grt_run_vault",
        tenantId: TENANT,
        principalId: run,
        resource: "secret:vault",
        action: "use",
        origin: "creator",
      });
      await h.db
        .update(asset)
        .set({ creatorPrincipalId: null })
        .where(eq(asset.id, "ast_run_a"));

      expect(await rederive([CREATOR])).toEqual([]);
      await rederive([CREATOR], ["run_a"]);

      expect(await runGrants(run)).toEqual([]);
    });

    test("another tenant's runs are not changed", async () => {
      await seedPrincipal(h.db, {
        id: "prn_other_creator",
        tenantId: "tnt_other",
        kind: "user",
        refId: "other-creator",
      });
      const run = await seedDeployment({
        anchorRunId: "run_other",
        tenantId: "tnt_other",
        creator: "prn_other_creator",
        requirements: [VAULT_CREATOR],
      });
      await seedGrant(h.db, {
        id: "grt_run_vault",
        tenantId: "tnt_other",
        principalId: run,
        resource: "secret:vault",
        action: "use",
        origin: "creator",
      });

      expect(await rederive(["prn_other_creator"])).toEqual([]);
      expect(await runGrants(run)).toHaveLength(1);
      expect(
        await findDelegatedAnchorRunIds(h.db, TENANT, ["prn_other_creator"]),
      ).toEqual([]);
    });

    test("two concurrent changes in one tenant both apply", async () => {
      const run = await seedDeployment({
        anchorRunId: "run_a",
        requirements: [VAULT_CREATOR],
      });
      for (const id of ["grt_creator_vault", "grt_creator_vault_2"]) {
        await seedGrant(h.db, {
          id,
          tenantId: TENANT,
          principalId: CREATOR,
          resource: "secret:vault",
          action: "use",
        });
      }
      await seedGrant(h.db, {
        id: "grt_run_vault",
        tenantId: TENANT,
        principalId: run,
        resource: "secret:vault",
        action: "use",
        origin: "creator",
      });

      const change = (grantId: string) =>
        h.db.transaction(async (tx) => {
          await lockTenantRunDelegatedGrants(tx, TENANT);
          await tx.delete(grant).where(eq(grant.id, grantId));
          return rederiveRunDelegatedGrants(tx, {
            grantStoreFor: createGrantStore,
            tenantId: TENANT,
            principalIds: [CREATOR],
            anchorRunIds: [],
            now: new Date(),
          });
        });
      await Promise.all([
        change("grt_creator_vault"),
        change("grt_creator_vault_2"),
      ]);

      expect(await runGrants(run)).toEqual([]);
    });
  },
);
