import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { asc, eq, sql } from "drizzle-orm";

import { authorize, createInMemoryGrantStore } from "@intx/authz";
import {
  createApprovalStore,
  createGrantStore,
  createSignalCorrelationStore,
  createWorkflowRunDispatchStore,
} from "@intx/db";
import {
  approval,
  asset,
  grant,
  principal,
  principalRole,
  role,
  sidecarAllocation,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import type { GrantRequirement, GrantWalkSnapshot } from "@intx/types";
import {
  createRunGrantsLoader,
  findDelegatedAnchorRunIds,
  lockTenantRunDelegatedGrants,
  rederiveRunDelegatedGrants,
  resolveApproval,
  sweepLapsedDeployerGrants,
} from "@intx/hub-api";
import { createSidecarRouter } from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAllocation,
  seedAsset,
  seedGrant,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
  seedWorkflowRunLaunchSpec,
} from "@intx/test-harness/seed";
import {
  createCredentialsBackedAuthorize,
  createGrantEvaluator,
  createSnapshotGrantsResolver,
} from "@intx/workflow-host/local";

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
      // Seed the launch spec with no grant snapshot, so a re-derivation of a
      // run that holds rows fails.
      noSnapshot?: true;
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
        grantSnapshot:
          opts.noSnapshot === true
            ? null
            : {
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

    const inHours = (n: number) => new Date(Date.now() + n * 3_600_000);

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

    // The requirement has the same key as the run's staged send row, and the
    // creator holds no send authority, so only the namespace skip keeps the
    // row from being adopted and deleted.
    test("a requirement in the mail.send namespace leaves the run's send row alone", async () => {
      const run = await seedDeployment({
        anchorRunId: "run_m",
        requirements: [
          { resource: "mail.send:*:*", action: "send", source: "creator" },
        ],
      });
      await seedGrant(h.db, {
        id: "grt_run_send",
        tenantId: TENANT,
        principalId: run,
        resource: "mail.send:*:*",
        action: "send",
        origin: "creator",
      });

      expect(await rederive([CREATOR])).toEqual([]);
      expect((await runGrants(run)).map((r) => r.id)).toEqual(["grt_run_send"]);
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
      // Re-derivation keys a row by its conditions without evaluating them,
      // so any conditions body serves here, even this `tool` one that the
      // probe gate would refuse on a declared requirement.
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

      test("a regained ask row that a standing always makes allow takes the allow expiry", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_TOOL],
        });
        await seedStandingDecision("approved");
        const until = inHours(2);
        await seedGrant(h.db, {
          id: "grt_creator_tools",
          tenantId: TENANT,
          principalId: CREATOR,
          resource: "tool:*",
          action: "invoke",
          expiresAt: until,
        });

        await rederive([CREATOR]);

        const rows = await runGrants(run);
        expect(summary(rows)).toEqual(["creator tool:search/invoke allow"]);
        expect(rows[0]?.expiresAt).toEqual(until);
      });

      test("a regained ask row that a standing reject makes deny keeps the floor's null expiry", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_TOOL],
        });
        await seedStandingDecision("rejected");
        await seedGrant(h.db, {
          id: "grt_creator_tools",
          tenantId: TENANT,
          principalId: CREATOR,
          resource: "tool:*",
          action: "invoke",
          expiresAt: inHours(2),
        });

        await rederive([CREATOR]);

        const rows = await runGrants(run);
        expect(summary(rows)).toEqual(["creator tool:search/invoke deny"]);
        expect(rows[0]?.expiresAt).toBeNull();
      });
    });

    describe("a standing approval resolution re-derives the run", () => {
      const ASK_SEARCH: GrantRequirement = {
        resource: "tool:search",
        action: "invoke",
        source: "creator",
        effect: "ask",
      };
      const APPROVER = "prn_approver";

      async function seedPendingApproval(
        run: string,
        expiresAt: Date | null = null,
        origin: "creator" | "invoker" = "creator",
      ) {
        await seedGrant(h.db, {
          id: "grt_run_search",
          tenantId: TENANT,
          principalId: run,
          resource: "tool:search",
          action: "invoke",
          origin,
          effect: "ask",
          expiresAt,
        });
        await createApprovalStore(h.db).create({
          id: "apr_search",
          tenantId: TENANT,
          anchorRunId: "run_a",
          runId: "run_a",
          agentAddress: "run_a@tenant.example",
          correlationId: "cor_search",
          toolDefinition: {
            name: "search",
            description: "search",
            inputSchema: { type: "object" },
          },
          toolArguments: {},
          timeoutAt: new Date(Date.now() + 3_600_000),
        });
        await createSignalCorrelationStore(h.db).register({
          correlationId: "cor_search",
          tenantId: TENANT,
          anchorRunId: "run_a",
          agentAddress: "run_a@tenant.example",
          runId: "run_a",
          signalName: "approval:cor_search",
          kind: "approval",
        });
      }

      // Give run_a's deployment a sidecar allocation, so a resolve takes the
      // provisioned path: it locks the allocation and the run rows and
      // enqueues the decision for durable dispatch.
      const ALLOCATION_A = {
        tenantId: TENANT,
        anchorRunId: "run_a",
        sidecarId: "sidecar_a",
        allocationId: "alloc_a",
        generation: 1,
      };

      function resolve(
        status: "approved" | "rejected",
        scope: "once" | "always",
        pushed: string[] = [],
        enqueued: string[] = [],
      ) {
        const dispatchStore = createWorkflowRunDispatchStore(h.db);
        const router = createSidecarRouter({
          withExecutableWorkflowRun: async (_target, send) => send(),
          authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
          resolveSidecarBindings: async () => [],
          validateSidecarIdentity: async () => true,
        });
        return resolveApproval(
          {
            db: h.db,
            sidecarRouter: {
              ...router,
              sendSignalDeliver: async () => {
                /* the run is not connected in this test */
              },
              sendRunGrants: async (address) => {
                pushed.push(address);
                return true;
              },
            },
            grantStore: createInMemoryGrantStore([
              {
                id: "grt_approver",
                resource: "approval:*",
                action: "resolve",
                effect: "allow",
                origin: "system",
                conditions: null,
                expiresAt: null,
                roleId: null,
                principalId: APPROVER,
              },
            ]),
            workflowDispatchService: {
              enqueueSignal: (args, tx) => {
                enqueued.push(args.signal.signalId);
                return dispatchStore.enqueueSignal(args, tx);
              },
              wake: () => {
                /* nothing waits on the dispatch in this test */
              },
            },
            readRunLifecycles: async () => ({
              topLevel: "live",
              target: "live",
            }),
            grantStoreFor: createGrantStore,
            conditionRegistry: {},
            approvalStore: createApprovalStore(h.db),
            signalCorrelationStore: createSignalCorrelationStore(h.db),
          },
          {
            approvalId: "apr_search",
            tenantId: TENANT,
            principalId: APPROVER,
            consumer: undefined,
            status,
            scope,
            decisionPayload: { outcome: status },
          },
        );
      }

      test("approve-always on a held requirement gives the allow row the creator's expiry", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_SEARCH],
        });
        const until = inHours(2);
        await seedGrant(h.db, {
          id: "grt_creator_tools",
          tenantId: TENANT,
          principalId: CREATOR,
          resource: "tool:*",
          action: "invoke",
          expiresAt: until,
        });
        await seedPendingApproval(run);
        const pushed: string[] = [];

        const outcome = await resolve("approved", "always", pushed);

        expect(outcome.kind).toBe("resolved");
        const rows = await runGrants(run);
        expect(summary(rows)).toEqual(["creator tool:search/invoke allow"]);
        expect(rows[0]?.expiresAt).toEqual(until);
        expect(pushed).toEqual(["run_a@tenant.example"]);
      });

      test("approve-always on a lapsed requirement removes the row", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_SEARCH],
        });
        await seedPendingApproval(run);

        expect((await resolve("approved", "always")).kind).toBe("resolved");

        expect(await runGrants(run)).toEqual([]);
      });

      test("reject-always turns the row into a deny floor with no expiry", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_SEARCH],
        });
        await seedGrant(h.db, {
          id: "grt_creator_tools",
          tenantId: TENANT,
          principalId: CREATOR,
          resource: "tool:*",
          action: "invoke",
          expiresAt: inHours(2),
        });
        // An expiry the floor does not have, so the re-derivation must set it.
        await seedPendingApproval(run, inHours(1));

        expect((await resolve("rejected", "always")).kind).toBe("resolved");

        const rows = await runGrants(run);
        expect(summary(rows)).toEqual(["creator tool:search/invoke deny"]);
        expect(rows[0]?.expiresAt).toBeNull();
      });

      test("a standing resolution re-derives the deployments the run deployed", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_SEARCH],
        });
        await seedPendingApproval(run);
        const deployed = await seedDeployment({
          anchorRunId: "run_b",
          requirements: [
            { resource: "tool:search", action: "invoke", source: "invoker" },
          ],
          deployer: run,
        });
        await seedGrant(h.db, {
          id: "grt_b_search",
          tenantId: TENANT,
          principalId: deployed,
          resource: "tool:search",
          action: "invoke",
          origin: "invoker",
          expiresAt: inHours(20),
        });

        expect((await resolve("rejected", "always")).kind).toBe("resolved");

        expect(summary(await runGrants(run))).toEqual([
          "creator tool:search/invoke deny",
        ]);
        expect(await runGrants(deployed)).toEqual([]);
      });

      test("approve-always on a provisioned deployment enqueues the decision and gives the allow row the creator's expiry", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_SEARCH],
        });
        await seedAllocation(h.db, ALLOCATION_A);
        const until = inHours(2);
        await seedGrant(h.db, {
          id: "grt_creator_tools",
          tenantId: TENANT,
          principalId: CREATOR,
          resource: "tool:*",
          action: "invoke",
          expiresAt: until,
        });
        await seedPendingApproval(run);
        const enqueued: string[] = [];

        const outcome = await resolve("approved", "always", [], enqueued);

        expect(outcome.kind).toBe("resolved");
        expect(enqueued).toHaveLength(1);
        const rows = await runGrants(run);
        expect(summary(rows)).toEqual(["creator tool:search/invoke allow"]);
        expect(rows[0]?.expiresAt).toEqual(until);
      });

      test("a standing provisioned resolution waits on the tenant lock before it locks the allocation", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_SEARCH],
        });
        await seedAllocation(h.db, ALLOCATION_A);
        await seedPendingApproval(run);
        const locked = Promise.withResolvers<number>();
        const release = Promise.withResolvers<undefined>();
        const holder = h.db.transaction(async (tx) => {
          await lockTenantRunDelegatedGrants(tx, TENANT);
          const [backend] = await tx.execute(
            sql`select pg_backend_pid() as pid`,
          );
          const pid = backend?.["pid"];
          if (typeof pid !== "number") throw new Error("Missing backend PID");
          locked.resolve(pid);
          await release.promise;
        });
        void holder.catch(locked.reject);
        try {
          const holderPid = await locked.promise;
          let settled = false;
          const resolved = resolve("approved", "always").finally(() => {
            settled = true;
          });
          // The resolve either waits on the holder's lock or finishes.
          // Postgres exposes lock waits through its activity view only.
          let blocked = false;
          while (!settled && !blocked) {
            const waiting = await h.db.execute(
              sql`select pid from pg_stat_activity where ${holderPid} = any(pg_blocking_pids(pid))`,
            );
            blocked = typeof waiting[0]?.["pid"] === "number";
            if (!blocked) {
              await new Promise<void>((done) => setImmediate(done));
            }
          }
          expect(blocked).toBe(true);
          // While the resolve waits on the tenant lock, it holds no lock on
          // the allocation: another transaction takes it without waiting.
          const allocation = await h.db.transaction((tx) =>
            tx
              .select({ id: sidecarAllocation.id })
              .from(sidecarAllocation)
              .where(eq(sidecarAllocation.anchorRunId, "run_a"))
              .for("update", { noWait: true }),
          );
          expect(allocation).toEqual([{ id: "alloc_a" }]);
          release.resolve(undefined);
          await holder;
          expect((await resolved).kind).toBe("resolved");
        } finally {
          release.resolve(undefined);
        }
      });

      test("approve-always on an invoker ask row after the 24-hour limit removes the row", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [{ ...ASK_SEARCH, source: "invoker" }],
          principalCreatedAt: new Date(
            Date.now() - INVOKER_GRANT_TTL_MS - 60_000,
          ),
        });
        await seedGrant(h.db, {
          id: "grt_deployer_tools",
          tenantId: TENANT,
          principalId: DEPLOYER,
          resource: "tool:*",
          action: "invoke",
        });
        await seedPendingApproval(run, null, "invoker");

        expect((await resolve("approved", "always")).kind).toBe("resolved");

        expect(await runGrants(run)).toEqual([]);
      });

      test("a once resolution does not wait on the tenant lock", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [ASK_SEARCH],
        });
        await seedPendingApproval(run);
        const locked = Promise.withResolvers<number>();
        const release = Promise.withResolvers<undefined>();
        const holder = h.db.transaction(async (tx) => {
          await lockTenantRunDelegatedGrants(tx, TENANT);
          const [backend] = await tx.execute(
            sql`select pg_backend_pid() as pid`,
          );
          const pid = backend?.["pid"];
          if (typeof pid !== "number") throw new Error("Missing backend PID");
          locked.resolve(pid);
          await release.promise;
        });
        void holder.catch(locked.reject);
        try {
          const holderPid = await locked.promise;
          let settled = false;
          const resolved = resolve("approved", "once").finally(() => {
            settled = true;
          });
          // The resolve either finishes or waits on the holder's lock. Postgres
          // exposes lock waits through its activity view only.
          let blocked = false;
          while (!settled && !blocked) {
            const waiting = await h.db.execute(
              sql`select pid from pg_stat_activity where ${holderPid} = any(pg_blocking_pids(pid))`,
            );
            blocked = typeof waiting[0]?.["pid"] === "number";
            if (!blocked) {
              await new Promise<void>((done) => setImmediate(done));
            }
          }
          expect(blocked).toBe(false);
          expect((await resolved).kind).toBe("resolved");
        } finally {
          release.resolve(undefined);
          await holder;
        }
        expect(summary(await runGrants(run))).toEqual([
          "creator tool:search/invoke ask",
        ]);
      });
    });

    describe("row expiry", () => {
      async function seedCreatorGrant(expiresAt: Date | null) {
        await seedGrant(h.db, {
          id: "grt_creator_vault",
          tenantId: TENANT,
          principalId: CREATOR,
          resource: "secret:*",
          action: "use",
          expiresAt,
        });
      }

      async function seedRunRow(
        run: string,
        opts: {
          id?: string;
          expiresAt: Date | null;
          effect?: "allow" | "deny" | "ask";
        },
      ) {
        await seedGrant(h.db, {
          id: opts.id ?? "grt_run_vault",
          tenantId: TENANT,
          principalId: run,
          resource: "secret:vault",
          action: "use",
          origin: "creator",
          effect: opts.effect ?? "allow",
          expiresAt: opts.expiresAt,
        });
      }

      async function setGrantExpiry(id: string, expiresAt: Date | null) {
        await h.db.update(grant).set({ expiresAt }).where(eq(grant.id, id));
      }

      async function hubAllows(run: string) {
        const result = await authorize(
          createGrantStore(h.db),
          run,
          TENANT,
          "secret:vault",
          "use",
          { conditions: "pessimistic" },
        );
        return result.effect === "allow";
      }

      test("a creator grant's expiry becomes the run row's expiry, and the hub skips the row after it", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_CREATOR],
        });
        const until = inHours(2);
        await seedCreatorGrant(until);

        await rederive([CREATOR]);

        const [row] = await runGrants(run);
        expect(row?.expiresAt).toEqual(until);
        expect(await hubAllows(run)).toBe(true);

        await setGrantExpiry(row?.id ?? "", inHours(-1));
        expect(await hubAllows(run)).toBe(false);
      });

      test("the grants the hub sends the run carry the expiry, and the child does not allow after it", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_CREATOR],
        });
        await seedCreatorGrant(inHours(2));
        await rederive([CREATOR]);
        const loadRunGrants = createRunGrantsLoader({ db: h.db });

        async function childAllows() {
          const stepGrants = await loadRunGrants("run_a");
          const grants: unknown = JSON.parse(JSON.stringify(stepGrants));
          if (!Array.isArray(grants)) throw new Error("expected an array");
          const childAuthorize = createCredentialsBackedAuthorize(
            createSnapshotGrantsResolver(
              {
                current: {
                  steps: [
                    {
                      stepId: "work",
                      grants,
                      contentHash: "",
                    },
                  ],
                },
              },
              "test child",
            ),
            createGrantEvaluator(),
          );
          const result = await childAuthorize("secret:vault", "use", {
            stepId: "work",
            attempt: 1,
            runId: "run_a",
          });
          return result.effect === "allow";
        }

        expect(await childAllows()).toBe(true);
        const [row] = await runGrants(run);
        await setGrantExpiry(row?.id ?? "", inHours(-1));
        expect(await childAllows()).toBe(false);
      });

      test("a creator grant renewed after it expired gives the row back and removes the expired row", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_CREATOR],
        });
        await seedCreatorGrant(inHours(-1));
        await seedRunRow(run, { expiresAt: inHours(-1) });
        const until = inHours(4);
        await setGrantExpiry("grt_creator_vault", until);

        const changed = await rederive([CREATOR]);

        expect(changed).toEqual([
          { anchorRunId: "run_a", address: "run_a@tenant.example" },
        ]);
        const rows = await runGrants(run);
        expect(rows).toHaveLength(1);
        expect(rows[0]?.id).not.toBe("grt_run_vault");
        expect(rows[0]?.expiresAt).toEqual(until);
        expect(await hubAllows(run)).toBe(true);
      });

      test("a creator grant whose expiry moves gives the run's row the new expiry and keeps its id", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_CREATOR],
        });
        const until = inHours(5);
        await seedCreatorGrant(until);
        await seedRunRow(run, { expiresAt: inHours(1) });

        expect(await rederive([CREATOR])).toHaveLength(1);

        const rows = await runGrants(run);
        expect(rows.map((r) => r.id)).toEqual(["grt_run_vault"]);
        expect(rows[0]?.expiresAt).toEqual(until);
      });

      test("a second re-derivation with no delegator change changes nothing", async () => {
        await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_CREATOR],
        });
        await seedCreatorGrant(inHours(2));

        expect(await rederive([CREATOR])).toHaveLength(1);
        expect(await rederive([CREATOR])).toEqual([]);
      });

      test("deleting an expired row of a lapsed requirement does not count as a change", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_CREATOR],
        });
        await seedRunRow(run, { expiresAt: inHours(-1) });

        expect(await rederive([CREATOR])).toEqual([]);
        expect(await runGrants(run)).toEqual([]);
      });

      test("a role grant's expiry ends the row, and its renewal gives the row back", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_CREATOR],
        });
        await h.db
          .insert(role)
          .values({ id: "rol_vault", tenantId: TENANT, name: "vault" });
        await h.db
          .insert(principalRole)
          .values({ principalId: CREATOR, roleId: "rol_vault" });
        const until = inHours(2);
        await seedGrant(h.db, {
          id: "grt_role_vault",
          tenantId: TENANT,
          roleId: "rol_vault",
          resource: "secret:vault",
          action: "use",
          origin: "role",
          expiresAt: until,
        });

        await rederive([CREATOR]);
        const [row] = await runGrants(run);
        expect(row?.expiresAt).toEqual(until);

        await setGrantExpiry(row?.id ?? "", inHours(-1));
        await setGrantExpiry("grt_role_vault", inHours(-1));
        expect(await hubAllows(run)).toBe(false);

        const renewed = inHours(6);
        await setGrantExpiry("grt_role_vault", renewed);
        await rederive([CREATOR]);
        const rows = await runGrants(run);
        expect(rows).toHaveLength(1);
        expect(rows[0]?.expiresAt).toEqual(renewed);
        expect(await hubAllows(run)).toBe(true);
      });

      test("an invoker row ends at the deployer grant's expiry when it comes before the TTL", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [{ ...VAULT_CREATOR, source: "invoker" }],
        });
        const until = inHours(1);
        await seedGrant(h.db, {
          id: "grt_deployer_vault",
          tenantId: TENANT,
          principalId: DEPLOYER,
          resource: "secret:vault",
          action: "use",
          expiresAt: until,
        });

        await rederive([DEPLOYER]);

        const rows = await runGrants(run);
        expect(summary(rows)).toEqual(["invoker secret:vault/use allow"]);
        expect(rows[0]?.expiresAt).toEqual(until);
      });

      test("an operator deny on a held requirement gets the creator floor's null expiry", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_CREATOR],
        });
        await seedCreatorGrant(inHours(2));
        await seedRunRow(run, { expiresAt: inHours(2), effect: "deny" });

        await rederive([CREATOR]);

        const rows = await runGrants(run);
        expect(summary(rows)).toEqual(["creator secret:vault/use deny"]);
        expect(rows[0]?.expiresAt).toBeNull();
      });

      test("a held invoker allow row whose first-trigger TTL has passed is deleted", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [{ ...VAULT_CREATOR, source: "invoker" }],
          principalCreatedAt: new Date(
            Date.now() - INVOKER_GRANT_TTL_MS - 60_000,
          ),
        });
        await seedGrant(h.db, {
          id: "grt_deployer_vault",
          tenantId: TENANT,
          principalId: DEPLOYER,
          resource: "secret:vault",
          action: "use",
        });
        // An approval turned the row into `allow` after the TTL passed, so it
        // still carries the floor's lack of expiry.
        await seedGrant(h.db, {
          id: "grt_run_vault",
          tenantId: TENANT,
          principalId: run,
          resource: "secret:vault",
          action: "use",
          origin: "invoker",
        });

        expect(await rederive([DEPLOYER])).toHaveLength(1);

        expect(await runGrants(run)).toEqual([]);
      });

      test("an invoker ask floor has no expiry, so it outlasts the first-trigger TTL", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [
            { ...VAULT_CREATOR, source: "invoker", effect: "ask" },
          ],
        });
        await seedGrant(h.db, {
          id: "grt_deployer_vault",
          tenantId: TENANT,
          principalId: DEPLOYER,
          resource: "secret:vault",
          action: "use",
        });
        await seedGrant(h.db, {
          id: "grt_run_vault",
          tenantId: TENANT,
          principalId: run,
          resource: "secret:vault",
          action: "use",
          origin: "invoker",
          effect: "ask",
          expiresAt: inHours(1),
        });

        await rederive([DEPLOYER]);

        const rows = await runGrants(run);
        expect(summary(rows)).toEqual(["invoker secret:vault/use ask"]);
        expect(rows[0]?.expiresAt).toBeNull();
      });

      test("an operator deny on a lapsed requirement gets the creator floor's null expiry", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [VAULT_CREATOR],
        });
        await seedRunRow(run, { expiresAt: inHours(2), effect: "deny" });

        await rederive([CREATOR]);

        const rows = await runGrants(run);
        expect(summary(rows)).toEqual(["creator secret:vault/use deny"]);
        expect(rows[0]?.expiresAt).toBeNull();
      });

      test("an approval that changes the row's effect while a re-derivation waits keeps its own expiry", async () => {
        const run = await seedDeployment({
          anchorRunId: "run_a",
          requirements: [{ ...VAULT_CREATOR, effect: "ask" }],
        });
        await seedCreatorGrant(inHours(5));
        const rowExpiry = inHours(2);
        await seedRunRow(run, { expiresAt: rowExpiry, effect: "ask" });

        // The approval holds the row's lock after it changes the effect, so
        // the re-derivation reads `ask` and waits to write the floor expiry.
        const locked = Promise.withResolvers<number>();
        const release = Promise.withResolvers<undefined>();
        const approval = h.db.transaction(async (tx) => {
          await tx
            .update(grant)
            .set({ effect: "allow" })
            .where(eq(grant.id, "grt_run_vault"));
          const [backend] = await tx.execute(
            sql`select pg_backend_pid() as pid`,
          );
          const pid = backend?.["pid"];
          if (typeof pid !== "number") throw new Error("Missing backend PID");
          locked.resolve(pid);
          await release.promise;
        });
        void approval.catch(locked.reject);
        try {
          const approvalPid = await locked.promise;
          const rederived = rederive([CREATOR]);
          // Postgres exposes lock waits through its activity view only.
          for (;;) {
            const waiting = await h.db.execute(
              sql`select pid from pg_stat_activity where ${approvalPid} = any(pg_blocking_pids(pid))`,
            );
            if (typeof waiting[0]?.["pid"] === "number") break;
            await new Promise<void>((resolve) => setImmediate(resolve));
          }
          release.resolve(undefined);
          await approval;

          expect(await rederived).toEqual([]);
        } finally {
          release.resolve(undefined);
        }
        const rows = await runGrants(run);
        expect(summary(rows)).toEqual(["creator secret:vault/use allow"]);
        expect(rows[0]?.expiresAt).toEqual(rowExpiry);
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

    describe("the lapsed deployer sweep", () => {
      // The creator holds no `secret:db` grant, so a re-derivation removes
      // the creator row of a run that requires it. A run the sweep does not
      // select keeps that row.
      const SWEPT_REQUIREMENTS: GrantRequirement[] = [
        { resource: "secret:vault", action: "use", source: "invoker" },
        { resource: "secret:db", action: "use", source: "creator" },
      ];

      beforeEach(async () => {
        await seedGrant(h.db, {
          id: "grt_deployer_vault",
          tenantId: TENANT,
          principalId: DEPLOYER,
          resource: "secret:vault",
          action: "use",
        });
      });

      async function sweep() {
        const sent: string[] = [];
        await sweepLapsedDeployerGrants(
          {
            db: h.db,
            grantStoreFor: createGrantStore,
            sidecarRouter: {
              sendRunGrants: async (address) => {
                sent.push(address);
                return true;
              },
            },
          },
          new Date(),
        );
        return sent;
      }

      async function setPrincipalStatus(
        id: string,
        status: "active" | "suspended" | "deactivated",
      ) {
        await h.db
          .update(principal)
          .set({ status })
          .where(eq(principal.id, id));
      }

      // A deployment that holds an unexpired invoker row and a creator row.
      async function seedSweptDeployment(
        opts: Omit<Parameters<typeof seedDeployment>[0], "requirements">,
      ) {
        const tenantId = opts.tenantId ?? TENANT;
        const run = await seedDeployment({
          ...opts,
          requirements: SWEPT_REQUIREMENTS,
        });
        await seedGrant(h.db, {
          id: `grt_${opts.anchorRunId}_vault`,
          tenantId,
          principalId: run,
          resource: "secret:vault",
          action: "use",
          origin: "invoker",
          expiresAt: new Date(Date.now() + 60_000),
        });
        await seedGrant(h.db, {
          id: `grt_${opts.anchorRunId}_db`,
          tenantId,
          principalId: run,
          resource: "secret:db",
          action: "use",
          origin: "creator",
        });
        return run;
      }

      const UNCHANGED = [
        "creator secret:db/use allow",
        "invoker secret:vault/use allow",
      ];

      test("a run deployed by an ended run loses its invoker row once", async () => {
        // run_deployed was deployed by run_deployer's run principal and holds
        // an invoker row delegated from it.
        const deployerRun = await seedDeployment({
          anchorRunId: "run_deployer",
          requirements: [],
        });
        await seedGrant(h.db, {
          id: "grt_deployer_run_vault",
          tenantId: TENANT,
          principalId: deployerRun,
          resource: "secret:vault",
          action: "use",
          origin: "creator",
        });
        await setPrincipalStatus(deployerRun, "deactivated");
        const deployed = await seedDeployment({
          anchorRunId: "run_deployed",
          requirements: [
            { resource: "secret:vault", action: "use", source: "invoker" },
          ],
          deployer: deployerRun,
        });
        await seedGrant(h.db, {
          id: "grt_deployed_vault",
          tenantId: TENANT,
          principalId: deployed,
          resource: "secret:vault",
          action: "use",
          origin: "invoker",
          expiresAt: new Date(Date.now() + 60_000),
        });

        expect(await sweep()).toEqual(["run_deployed@tenant.example"]);
        expect(await runGrants(deployed)).toEqual([]);
        expect(await sweep()).toEqual([]);
      });

      test("a run whose deployer is active is not selected", async () => {
        const run = await seedSweptDeployment({ anchorRunId: "run_a" });

        expect(await sweep()).toEqual([]);
        expect(summary(await runGrants(run))).toEqual(UNCHANGED);
      });

      test("a run whose invoker row has expired is not selected", async () => {
        await setPrincipalStatus(DEPLOYER, "deactivated");
        const run = await seedSweptDeployment({ anchorRunId: "run_a" });
        await h.db
          .update(grant)
          .set({ expiresAt: new Date(Date.now() - 1_000) })
          .where(eq(grant.id, "grt_run_a_vault"));

        expect(await sweep()).toEqual([]);
        expect(summary(await runGrants(run))).toEqual(UNCHANGED);
      });

      test("an ended run is not selected", async () => {
        await setPrincipalStatus(DEPLOYER, "deactivated");
        const run = await seedSweptDeployment({
          anchorRunId: "run_a",
          status: "completed",
        });

        expect(await sweep()).toEqual([]);
        expect(summary(await runGrants(run))).toEqual(UNCHANGED);
      });

      test("a deployer in another tenant counts as not active", async () => {
        await seedPrincipal(h.db, {
          id: "prn_foreign",
          tenantId: "tnt_other",
          kind: "user",
          refId: "foreign-user",
        });
        const run = await seedSweptDeployment({
          anchorRunId: "run_a",
          deployer: "prn_foreign",
        });

        expect(await sweep()).toEqual(["run_a@tenant.example"]);
        expect(await runGrants(run)).toEqual([]);
      });

      test("a suspended deployer counts as not active", async () => {
        await setPrincipalStatus(DEPLOYER, "suspended");
        const run = await seedSweptDeployment({ anchorRunId: "run_a" });

        expect(await sweep()).toEqual(["run_a@tenant.example"]);
        expect(await runGrants(run)).toEqual([]);
      });

      test("one tenant's failure does not stop another tenant", async () => {
        await setPrincipalStatus(DEPLOYER, "deactivated");
        await seedSweptDeployment({ anchorRunId: "run_bad", noSnapshot: true });
        await seedPrincipal(h.db, {
          id: "prn_other_deployer",
          tenantId: "tnt_other",
          kind: "user",
          refId: "other-deployer",
          status: "deactivated",
        });
        const other = await seedSweptDeployment({
          anchorRunId: "run_other",
          tenantId: "tnt_other",
          creator: null,
          deployer: "prn_other_deployer",
        });

        expect(await sweep()).toEqual(["run_other@tenant.example"]);
        expect(await runGrants(other)).toEqual([]);
      });

      test("one broken anchor does not stop another run of its tenant", async () => {
        await setPrincipalStatus(DEPLOYER, "deactivated");
        const bad = await seedSweptDeployment({
          anchorRunId: "run_a_bad",
          noSnapshot: true,
        });
        const good = await seedSweptDeployment({ anchorRunId: "run_b_good" });

        expect(await sweep()).toEqual(["run_b_good@tenant.example"]);
        expect(await runGrants(good)).toEqual([]);
        expect(summary(await runGrants(bad))).toEqual(UNCHANGED);
      });
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
