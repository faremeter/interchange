import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq, sql } from "drizzle-orm";

import {
  createGrantStore,
  createPrincipalKeyStore,
  createRunTokenStore,
  resolveSenderPrincipal,
} from "@intx/db";
import {
  grant,
  principal,
  runToken,
  workflowDefinition,
  workflowDefinitionVersion,
  workflowRun,
} from "@intx/db/schema";
import type { GrantWalkSnapshot } from "@intx/types";
import { mailAcceptResource, type MailAcceptCoordinate } from "@intx/authz";
import {
  createCorrespondentGrantMinter,
  createMailTriggeredRunGrantsMaterializer,
} from "@intx/hub-api";
import { createRunCredentialsLoader } from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  seedAsset,
  seedGrant,
  seedPrincipal,
  seedTenants,
  seedWorkflowRunLaunchSpec,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const ASSET = "ast";
const DEFINITION = "wfd_real";
const DEPLOYMENT = "run_real";
const WORKFLOW_ADDRESS = "run_real@tenant.example";
// The capability walk emits this grant for the workflow's mail trigger. A
// snapshot without it belongs to a workflow that mail does not start.
const MAIL_TRIGGER_GRANT = `mail.address:${WORKFLOW_ADDRESS}`;
const CREATOR = "prn_creator";
const RUN_ID = "<mail-run-1@tenant.example>";
const HASH = "a".repeat(64);

// The deploy-approved grant-walk snapshot a source-ref deployment persists at
// approval: one `tool:read_file` runtime grant, a `mail.accept:tenant` accept
// marker so the mail-transport admission gate admits a same-tenant sender, plus
// a creator-sourced requirement. The mail path materializes grants from THIS,
// never from a workflow.json blob.
function snapshot(creatorRequirementResource: string): GrantWalkSnapshot {
  return {
    perStep: [
      {
        stepId: "work",
        grants: ["tool:read_file", MAIL_TRIGGER_GRANT, "mail.accept:tenant"],
        grantEffects: { "tool:read_file": "allow" },
      },
    ],
    grantRequirements: [
      {
        resource: creatorRequirementResource,
        action: "use",
        source: "creator",
      },
    ],
  };
}

// The sender the admission gate matches against the run's accept-policy. A
// same-tenant sender's `tenant` coordinate matches the `mail.accept:tenant`
// marker the snapshot declares, so the gate admits.
const ADMITTED_SENDER: {
  senderCoordinates: MailAcceptCoordinate[] | null;
} = {
  senderCoordinates: [{ coordType: "tenant", id: TENANT }],
};

describe.skipIf(!harnessDbEnvAvailable())(
  "createMailTriggeredRunGrantsMaterializer (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedPrincipal(h.db, {
        id: CREATOR,
        tenantId: TENANT,
        kind: "user",
        refId: "creator-user",
      });
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: ASSET,
        creatorPrincipalId: CREATOR,
      });
      // The deployment's first-class definition and its anchor run: the
      // materializer resolves the run's asset and definition off the anchor,
      // keyed by the deployment address.
      await h.db.insert(workflowDefinition).values({
        id: DEFINITION,
        tenantId: TENANT,
        name: DEFINITION,
        assetId: ASSET,
      });
      await h.db.insert(workflowRun).values({
        id: DEPLOYMENT,
        tenantId: TENANT,
        anchorRunId: DEPLOYMENT,
        definitionId: DEFINITION,
        address: WORKFLOW_ADDRESS,
        status: "running",
        publicKey: "test-public-key",
      });
      // The creator holds the grant its creator-sourced requirement demands,
      // so the happy path resolves it. The rejection test overrides the
      // requirement to a resource the creator does NOT hold.
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId: TENANT,
        principalId: CREATOR,
        resource: "secret:vault",
        action: "use",
        effect: "allow",
        origin: "creator",
      });
    });

    // Freeze a grant-walk snapshot into the deployment's launch spec, the way a
    // deploy does. The mail materializer reads grants from here.
    async function seedFrozenSnapshot(grantSnapshot: GrantWalkSnapshot | null) {
      await seedWorkflowRunLaunchSpec(h.db, {
        anchorRunId: DEPLOYMENT,
        sourceAuthorityPrincipalId: CREATOR,
        grantSnapshot,
      });
    }

    async function materializeOnce(
      runId: string,
      sender: {
        senderCoordinates: MailAcceptCoordinate[] | null;
      } = ADMITTED_SENDER,
    ): ReturnType<ReturnType<typeof createMailTriggeredRunGrantsMaterializer>> {
      const materialize = createMailTriggeredRunGrantsMaterializer({
        db: h.db,
        principalKeyStore: createPrincipalKeyStore({
          db: h.db,
          cipher: createTestCredentialCipher(),
        }),
        runTokenStore: createRunTokenStore({
          db: h.db,
          cipher: createTestCredentialCipher(),
        }),
        grantStore: createGrantStore(h.db),
      });
      return materialize({ agentAddress: WORKFLOW_ADDRESS, runId, ...sender });
    }

    test("deployments of one definition keep their own frozen snapshots", async () => {
      // Each deployment's launch spec holds the snapshot it was deployed with.
      const second = "run_second";
      await h.db.insert(workflowRun).values({
        id: second,
        tenantId: TENANT,
        anchorRunId: second,
        definitionId: DEFINITION,
        address: "run_second@tenant.example",
        status: "running",
        publicKey: "test-public-key",
      });
      const writeSnapshot: GrantWalkSnapshot = {
        ...snapshot("secret:vault"),
        perStep: [
          {
            stepId: "work",
            grants: [
              "tool:write_file",
              "mail.address:run_second@tenant.example",
              "mail.accept:tenant",
            ],
            grantEffects: { "tool:write_file": "allow" },
          },
        ],
      };
      for (const [anchorRunId, grantSnapshot] of [
        [DEPLOYMENT, snapshot("secret:vault")],
        [second, writeSnapshot],
      ] as const) {
        await seedWorkflowRunLaunchSpec(h.db, {
          anchorRunId,
          sourceAuthorityPrincipalId: CREATOR,
          grantSnapshot,
        });
      }
      // One materializer, so a cache keyed by definition would serve the
      // first deployment's snapshot to the second.
      const materialize = createMailTriggeredRunGrantsMaterializer({
        db: h.db,
        principalKeyStore: createPrincipalKeyStore({
          db: h.db,
          cipher: createTestCredentialCipher(),
        }),
        runTokenStore: createRunTokenStore({
          db: h.db,
          cipher: createTestCredentialCipher(),
        }),
        grantStore: createGrantStore(h.db),
      });
      const runs = [
        { address: WORKFLOW_ADDRESS, runId: RUN_ID },
        { address: "run_second@tenant.example", runId: "<mail-run-2@t>" },
      ];
      const resources: string[][] = [];
      for (const { address, runId } of runs) {
        const result = await materialize({
          agentAddress: address,
          runId,
          ...ADMITTED_SENDER,
        });
        if (result.outcome !== "materialized") {
          throw new Error(`expected materialized, got ${result.outcome}`);
        }
        const [runPrincipal] = await h.db
          .select()
          .from(principal)
          .where(eq(principal.refId, runId));
        const grants = await h.db
          .select()
          .from(grant)
          .where(eq(grant.principalId, runPrincipal?.id ?? ""));
        resources.push(grants.map((g) => g.resource));
      }
      expect(resources[0]).toContain("tool:read_file");
      expect(resources[0]).not.toContain("tool:write_file");
      expect(resources[1]).toContain("tool:write_file");
      expect(resources[1]).not.toContain("tool:read_file");
    });

    test("derives a run's grants from the persisted snapshot and commits once", async () => {
      // A source-ref deployment persists no workflow.json blob; its run derives
      // grants purely from the frozen snapshot.
      await seedFrozenSnapshot(snapshot("secret:vault"));
      const result = await materializeOnce(RUN_ID);
      if (result.outcome !== "materialized") {
        throw new Error(`expected materialized, got ${result.outcome}`);
      }

      // Exactly one workflow-kind principal keyed on the runId.
      const principals = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, RUN_ID));
      expect(principals).toHaveLength(1);
      expect(principals[0]?.kind).toBe("workflow");
      const runPrincipalId = principals[0]?.id;

      // Exactly one run row whose deployment_id is the SEEDED deployment id
      // -- the real anchor run's id, NOT the address-derived substrate slug.
      // The row inserts at all only because that real id was written: the slug
      // is not a workflow_run.id, so the NOT NULL foreign key would reject it.
      const runs = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, RUN_ID));
      expect(runs).toHaveLength(1);
      expect(runs[0]?.anchorRunId).toBe(DEPLOYMENT);
      expect(runs[0]?.principalId).toBe(runPrincipalId);
      expect(runs[0]?.status).toBe("running");
      // The committed run anchors on the deployment's definition -- the one the
      // anchor run carries.
      expect(runs[0]?.definitionId).toBe(DEFINITION);

      // Every staged grant persisted and FK-resolves to the run principal. The
      // tool grant comes from the snapshot; the creator requirement resolves
      // against the creator's seeded grant.
      const grants = await h.db
        .select()
        .from(grant)
        .where(eq(grant.principalId, runPrincipalId ?? ""));
      expect(grants.length).toBe(result.stepGrants.length);
      const resources = grants.map((g) => `${g.resource}/${g.action}`).sort();
      expect(resources).toContain("tool:read_file/invoke");
      expect(resources).toContain("secret:vault/use");
    });

    test("commits a credential use grant from the anchor bindings", async () => {
      await seedFrozenSnapshot(snapshot("secret:vault"));
      const consumer = "tool:bundle-id";
      await h.db
        .update(workflowRun)
        .set({
          credentialRefs: {
            credentialIds: ["cred-1", "inference-only"],
            bindings: [
              { handle: "api", credentialId: "cred-1", consumer },
              { handle: "other", credentialId: "cred-1", consumer },
            ],
          },
        })
        .where(eq(workflowRun.id, DEPLOYMENT));

      const runId = "<mail-run-cred@tenant.example>";
      const result = await materializeOnce(runId);
      if (result.outcome !== "materialized") {
        throw new Error(`expected materialized, got ${result.outcome}`);
      }

      const principals = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, runId));
      const runPrincipalId = principals[0]?.id;
      const grants = await h.db
        .select()
        .from(grant)
        .where(eq(grant.principalId, runPrincipalId ?? ""));
      const credentialGrants = grants.filter((row) =>
        row.resource.startsWith("credential:"),
      );
      expect(credentialGrants).toHaveLength(1);
      expect(credentialGrants[0]).toMatchObject({
        resource: "credential:cred-1",
        action: "use",
        effect: "allow",
        origin: "system",
        conditions: { tool: consumer },
        expiresAt: null,
      });
    });

    test("mints one hub token per hub consumer and delivers each at the hub origin", async () => {
      await seedFrozenSnapshot(snapshot("secret:vault"));
      const consumers = ["action:caller", "tool:@acme/hub"];
      const origin = "https://hub.example.test";
      await h.db
        .update(workflowRun)
        .set({
          credentialRefs: {
            credentialIds: [],
            bindings: [],
            hubConsumers: consumers,
          },
        })
        .where(eq(workflowRun.id, DEPLOYMENT));

      const runId = "<mail-run-hub@tenant.example>";
      const result = await materializeOnce(runId);
      if (result.outcome !== "materialized") {
        throw new Error(`expected materialized, got ${result.outcome}`);
      }
      for (const consumer of consumers) {
        expect(
          result.stepGrants.find(
            (g) => g.resource === `credential:hub:${consumer}`,
          ),
        ).toMatchObject({ action: "use", conditions: { tool: consumer } });
      }

      const [runPrincipal] = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, runId));
      const runPrincipalId = runPrincipal?.id ?? "";
      const tokensOfRun = () =>
        h.db
          .select({ id: runToken.id, consumer: runToken.consumer })
          .from(runToken)
          .where(eq(runToken.principalId, runPrincipalId))
          .orderBy(runToken.consumer);
      const minted = await tokensOfRun();
      expect(minted.map((t) => t.consumer)).toEqual(consumers);

      // A redelivered trigger keeps the tokens the run already holds.
      await materializeOnce(runId);
      expect(await tokensOfRun()).toEqual(minted);

      const runTokenStore = createRunTokenStore({
        db: h.db,
        cipher: createTestCredentialCipher(),
      });
      const issued = new Map(
        (await runTokenStore.loadDelivery(runPrincipalId)).map((t) => [
          t.consumer,
          t.token,
        ]),
      );
      const issuedTo = (consumer: string) => {
        const token = issued.get(consumer);
        if (token === undefined) throw new Error(`no token for ${consumer}`);
        return token;
      };
      const delivery = await createRunCredentialsLoader({
        db: h.db,
        runTokenStore,
        hubApiOrigin: origin,
      })(runId);
      expect(delivery?.bindings).toEqual(
        consumers.map((consumer) => ({
          handle: "interchange.hub",
          credentialId: `hub:${consumer}`,
          consumer,
        })),
      );
      // Each consumer receives its own minted token, pinned to the hub.
      expect(delivery?.materials).toEqual(
        consumers.map((consumer) => ({
          credentialId: `hub:${consumer}`,
          providerKey: "http",
          origin,
          secret: issuedTo(consumer),
        })),
      );
      expect(new Set(issued.values()).size).toBe(consumers.length);
    });

    test("does not commit grants before credential resolution is recorded", async () => {
      await seedFrozenSnapshot(snapshot("secret:vault"));
      await h.db
        .update(workflowRun)
        .set({ publicKey: null })
        .where(eq(workflowRun.id, DEPLOYMENT));

      const runId = "<mail-run-unresolved@tenant.example>";
      const result = await materializeOnce(runId);
      expect(result).toEqual({ outcome: "notReady" });

      const principals = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, runId));
      expect(principals).toHaveLength(0);
    });

    test.each(["a null snapshot", "no launch spec"])(
      "fails closed when the deployment has %s",
      async (state) => {
        // The shared version row carries a snapshot, but a later approval may
        // have written it, so the materializer must not fall back to it.
        await h.db.insert(workflowDefinitionVersion).values({
          id: `wdv_${DEFINITION}`,
          definitionId: DEFINITION,
          version: "1",
          status: "active",
          approvedWireHash: HASH,
          grantSnapshot: snapshot("secret:vault"),
        });
        if (state === "a null snapshot") await seedFrozenSnapshot(null);
        await expect(materializeOnce(RUN_ID)).rejects.toThrow(
          /no approved grant snapshot/,
        );

        // Nothing was reserved: no run principal, run row, or run grants.
        const principals = await h.db
          .select()
          .from(principal)
          .where(eq(principal.refId, RUN_ID));
        expect(principals).toHaveLength(0);
        const runs = await h.db
          .select()
          .from(workflowRun)
          .where(eq(workflowRun.id, RUN_ID));
        expect(runs).toHaveLength(0);
      },
    );

    test("throws when the anchor run's definition has no asset", async () => {
      // A native workflow definition names its asset; a null asset leaves the
      // materializer with no creator to resolve creator grants against, so it
      // throws loudly before deriving any grants.
      await seedFrozenSnapshot(snapshot("secret:vault"));
      await h.db.insert(workflowDefinition).values({
        id: "wfd_null",
        tenantId: TENANT,
        name: "no-asset",
        assetId: null,
      });
      await h.db
        .update(workflowRun)
        .set({ definitionId: "wfd_null" })
        .where(eq(workflowRun.id, DEPLOYMENT));

      await expect(materializeOnce(RUN_ID)).rejects.toThrow(
        /definition has no asset/,
      );
    });

    test("a redelivery with the same runId neither throws nor duplicates", async () => {
      await seedFrozenSnapshot(snapshot("secret:vault"));
      const first = await materializeOnce(RUN_ID);
      if (first.outcome !== "materialized") {
        throw new Error(`expected materialized, got ${first.outcome}`);
      }

      const firstGrants = await h.db.select().from(grant);
      const firstGrantCount = firstGrants.length;
      expect(firstGrantCount).toBeGreaterThan(0);

      // Second delivery of the SAME runId: the deterministic principal id and
      // the in-transaction guard make the reservation a true no-op. A fresh
      // random principal id would break the principal foreign key on redelivery
      // (the conflict-noop principal insert would leave the new id unwritten
      // while the grant rows referenced it), and re-running the grant inserts
      // would duplicate rows -- both of which the assertions below rule out.
      const second = await materializeOnce(RUN_ID);
      if (second.outcome !== "materialized") {
        throw new Error(`expected materialized, got ${second.outcome}`);
      }

      // The two materializations derive the SAME run principal id from the shared
      // runId, confirming the derivation is deterministic. A random id would
      // differ here and leave the persisted grant rows referencing an id the
      // conflict-noop principal insert never wrote.
      const firstPrincipalIds = new Set(
        first.stepGrants.map((g) => g.principalId),
      );
      const secondPrincipalIds = new Set(
        second.stepGrants.map((g) => g.principalId),
      );
      expect(firstPrincipalIds.size).toBe(1);
      expect(secondPrincipalIds).toEqual(firstPrincipalIds);

      const principals = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, RUN_ID));
      expect(principals).toHaveLength(1);
      // The single persisted principal is exactly the id both materializations
      // derived, so the grant rows' FK resolves against it.
      expect(firstPrincipalIds.has(principals[0]?.id ?? "")).toBe(true);

      const runs = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, RUN_ID));
      expect(runs).toHaveLength(1);

      const grants = await h.db.select().from(grant);
      expect(grants.length).toBe(firstGrantCount);
    });

    // A run that deploys a workflow through its run token is the deployment's
    // deployer, so the child's invoker requirements resolve against the run's
    // own grants. A run triggers its child by mail, so this path decides them.
    const PARENT_RUN = "prn_parent_run";
    const DEPLOYING_CONSUMER = "tool:@acme/deployer";

    async function seedRunDeployer(
      grants: { effect: "allow" | "deny"; conditions?: { tool: string } }[],
    ) {
      await seedPrincipal(h.db, {
        id: PARENT_RUN,
        tenantId: TENANT,
        kind: "workflow",
        refId: "run_parent",
      });
      for (const [i, g] of grants.entries()) {
        await seedGrant(h.db, {
          id: `grt_parent_${String(i)}`,
          tenantId: TENANT,
          principalId: PARENT_RUN,
          resource: "asset:a1",
          action: "read",
          origin: "system",
          ...g,
        });
      }
      await seedWorkflowRunLaunchSpec(h.db, {
        anchorRunId: DEPLOYMENT,
        sourceAuthorityPrincipalId: PARENT_RUN,
        grantSnapshot: {
          ...snapshot("secret:vault"),
          grantRequirements: [
            { resource: "asset:a1", action: "read", source: "invoker" },
          ],
        },
      });
    }

    test("a deploying run's grant reaches its child as an invoker grant", async () => {
      await seedRunDeployer([{ effect: "allow" }]);
      const result = await materializeOnce(RUN_ID);
      if (result.outcome !== "materialized") {
        throw new Error(`expected materialized, got ${result.outcome}`);
      }
      const [child] = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, RUN_ID));
      const delegated = await h.db
        .select()
        .from(grant)
        .where(eq(grant.principalId, child?.id ?? ""));
      const invoker = delegated.find((g) => g.resource === "asset:a1");
      expect(invoker).toMatchObject({
        action: "read",
        effect: "allow",
        origin: "invoker",
        conditions: null,
      });
      expect(invoker?.expiresAt).not.toBeNull();
    });

    test("a deploying run's consumer-scoped deny refuses its child's invoker requirement", async () => {
      // On a request from the deploying consumer, the scoped deny wins over
      // the unscoped allow. Delegation has no request, so the deny applies.
      await seedRunDeployer([
        { effect: "allow" },
        { effect: "deny", conditions: { tool: DEPLOYING_CONSUMER } },
      ]);
      const result = await materializeOnce(RUN_ID);
      expect(result).toMatchObject({
        outcome: "rejected",
        status: 403,
        code: "insufficient_grants",
      });
      const principals = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, RUN_ID));
      expect(principals).toHaveLength(0);
    });

    test("a creator shortfall rejects and writes zero rows", async () => {
      // The creator-sourced requirement names a resource the creator does not
      // hold, so staging rejects (403). The rejection is RETURNED (not
      // thrown), and because authorization fails before reservation, NOTHING
      // is written -- no orphaned principal, run, or grant rows.
      await seedFrozenSnapshot(snapshot("secret:locked"));
      const result = await materializeOnce(RUN_ID);
      expect(result.outcome).toBe("rejected");
      if (result.outcome === "rejected") {
        expect(result.status).toBe(403);
      }

      // No creator-requirement rejection reaches reservation, so the tables
      // carry no run principal, run row, or run grants.
      const principals = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, RUN_ID));
      expect(principals).toHaveLength(0);

      const runs = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, RUN_ID));
      expect(runs).toHaveLength(0);

      // The only grant present is the creator's seeded grant; no run grants
      // were written.
      const runGrants = await h.db
        .select()
        .from(grant)
        .where(eq(grant.resource, "tool:read_file"));
      expect(runGrants).toHaveLength(0);
    });

    test("an unadmitted sender is dropped and writes zero rows", async () => {
      // The definition accepts only its own tenant; a sender whose coordinates
      // name a DIFFERENT tenant is not admitted. The start gate rejects before
      // commit, so no run principal, run row, or run grants are written.
      await seedFrozenSnapshot(snapshot("secret:vault"));
      const result = await materializeOnce(RUN_ID, {
        senderCoordinates: [
          { coordType: "principal", id: "prn_outsider" },
          { coordType: "tenant", id: "tnt_other" },
        ],
      });
      expect(result.outcome).toBe("rejected");
      if (result.outcome === "rejected") {
        expect(result.status).toBe(403);
        expect(result.code).toBe("mail_admission_denied");
      }

      const principals = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, RUN_ID));
      expect(principals).toHaveLength(0);
      const runs = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, RUN_ID));
      expect(runs).toHaveLength(0);
      const runGrants = await h.db
        .select()
        .from(grant)
        .where(eq(grant.resource, "tool:read_file"));
      expect(runGrants).toHaveLength(0);
    });

    test("refuses relayed mail to a run of a workflow with no mail trigger and no mailAccept", async () => {
      // An HTTP-launched run: a committed run principal, a snapshot with a
      // `tool:mail_send` grant and no mail trigger, and no accept rule. The run
      // has an address, so mail reaches the gate, and the gate refuses it.
      await seedFrozenSnapshot({
        perStep: [
          {
            stepId: "work",
            grants: ["tool:mail_send"],
            grantEffects: { "tool:mail_send": "allow" },
          },
        ],
        grantRequirements: [],
      });
      await seedPrincipal(h.db, {
        id: "prn_http_run",
        tenantId: TENANT,
        kind: "workflow",
        refId: DEPLOYMENT,
      });
      await h.db
        .update(workflowRun)
        .set({ principalId: "prn_http_run" })
        .where(eq(workflowRun.id, DEPLOYMENT));
      await seedGrant(h.db, {
        id: "grt_http_run_tool",
        tenantId: TENANT,
        principalId: "prn_http_run",
        resource: "tool:mail_send",
        action: "invoke",
        effect: "allow",
        origin: "creator",
      });

      const result = await materializeOnce(DEPLOYMENT, {
        senderCoordinates: [
          { coordType: "principal", id: CREATOR },
          { coordType: "tenant", id: TENANT },
        ],
      });

      expect(result.outcome).toBe("rejected");
      if (result.outcome === "rejected") {
        expect(result.code).toBe("mail_admission_denied");
      }
    });

    describe("a workflow with no mail trigger", () => {
      // The snapshot of `trigger: manual, mailAccept: { invoker: true }`: no
      // `mail.address:` grant, only the invoker accept marker.
      const manualSnapshot: GrantWalkSnapshot = {
        perStep: [
          {
            stepId: "work",
            grants: ["tool:read_file", "mail.accept:invoker"],
            grantEffects: { "tool:read_file": "allow" },
          },
        ],
        grantRequirements: [],
      };

      test("refuses a first fire by mail, even from a sender the rules name", async () => {
        await seedTenants(h.db, [{ id: "tnt_other" }]);
        await seedFrozenSnapshot(manualSnapshot);

        const result = await materializeOnce(DEPLOYMENT, {
          senderCoordinates: [
            { coordType: "principal", id: "prn_outsider" },
            { coordType: "tenant", id: "tnt_other" },
          ],
        });

        expect(result.outcome).toBe("rejected");
        if (result.outcome === "rejected") {
          expect(result.code).toBe("mail_admission_denied");
        }
        const runPrincipals = await h.db
          .select({ id: principal.id })
          .from(principal)
          .where(eq(principal.refId, DEPLOYMENT));
        expect(runPrincipals).toHaveLength(0);
      });

      test("admits mail to the run that its own trigger started", async () => {
        // The run's own trigger committed its principal and its accept rule:
        // here the caller that started it, the tenant's creator.
        await seedFrozenSnapshot(manualSnapshot);
        await seedPrincipal(h.db, {
          id: "prn_manual_run",
          tenantId: TENANT,
          kind: "workflow",
          refId: DEPLOYMENT,
        });
        await h.db
          .update(workflowRun)
          .set({ principalId: "prn_manual_run" })
          .where(eq(workflowRun.id, DEPLOYMENT));
        await seedGrant(h.db, {
          id: "grt_manual_run_invoker",
          tenantId: TENANT,
          principalId: "prn_manual_run",
          resource: mailAcceptResource("principal", CREATOR),
          action: "accept",
          effect: "allow",
          origin: "creator",
        });

        const result = await materializeOnce(DEPLOYMENT, {
          senderCoordinates: [
            { coordType: "principal", id: CREATOR },
            { coordType: "tenant", id: TENANT },
          ],
        });

        expect(result.outcome).toBe("materialized");
      });
    });

    describe("invoker authority is the deployer's, not the mail sender's", () => {
      // An invoker-sourced requirement, which resolves against the
      // deployment's deployer whoever sends the mail.
      const invokerSnapshot: GrantWalkSnapshot = {
        perStep: [
          {
            stepId: "work",
            grants: [
              "tool:read_file",
              MAIL_TRIGGER_GRANT,
              "mail.accept:invoker",
              "mail.accept:tenant",
            ],
            grantEffects: { "tool:read_file": "allow" },
          },
        ],
        grantRequirements: [
          { resource: "secret:vault", action: "use", source: "invoker" },
        ],
      };

      async function seedSender(id: string, tenantId: string) {
        await seedPrincipal(h.db, {
          id,
          tenantId,
          kind: "user",
          refId: `${id}-user`,
        });
        await seedGrant(h.db, {
          id: `grt_${id}_star`,
          tenantId,
          principalId: id,
          resource: "*",
          action: "*",
          effect: "allow",
          origin: "role",
        });
      }

      async function invokerRows() {
        const rows = await h.db
          .select()
          .from(grant)
          .where(eq(grant.resource, "secret:vault"));
        return rows.filter((row) => row.origin === "invoker");
      }

      const insider = {
        senderCoordinates: [
          { coordType: "principal" as const, id: "prn_insider" },
          { coordType: "tenant" as const, id: TENANT },
        ],
      };

      test("the deployer's grants satisfy an invoker requirement the sender cannot", async () => {
        // The deployer (the creator here) holds `secret:vault`; the sender
        // holds nothing.
        await seedPrincipal(h.db, {
          id: "prn_insider",
          tenantId: TENANT,
          kind: "user",
          refId: "prn_insider-user",
        });
        await seedFrozenSnapshot(invokerSnapshot);

        const result = await materializeOnce(RUN_ID, insider);

        expect(result.outcome).toBe("materialized");
        const rows = await invokerRows();
        expect(rows).toHaveLength(1);
        expect(rows[0]?.tenantId).toBe(TENANT);
      });

      test("a sender's grants do not satisfy an invoker requirement the deployer lacks", async () => {
        // The sender holds `*`; the deployer holds nothing.
        await seedSender("prn_insider", TENANT);
        await seedPrincipal(h.db, {
          id: "prn_bare_deployer",
          tenantId: TENANT,
          kind: "user",
          refId: "prn_bare_deployer-user",
        });
        await seedWorkflowRunLaunchSpec(h.db, {
          anchorRunId: DEPLOYMENT,
          sourceAuthorityPrincipalId: "prn_bare_deployer",
          grantSnapshot: invokerSnapshot,
        });

        const result = await materializeOnce(RUN_ID, insider);

        expect(result.outcome).toBe("rejected");
        if (result.outcome === "rejected") {
          expect(result.status).toBe(403);
        }
        expect(await invokerRows()).toHaveLength(0);
      });

      test("a sender the rules refuse is refused before its invoker requirement resolves", async () => {
        // The rules admit only the run's tenant, and the sender is in another
        // tenant and holds nothing in the run's tenant. The sender gate runs
        // first, so the refusal is `mail_admission_denied` and not the
        // requirement shortfall, and no grant is read for the sender.
        await seedTenants(h.db, [{ id: "tnt_other" }]);
        await seedPrincipal(h.db, {
          id: "prn_outsider",
          tenantId: "tnt_other",
          kind: "user",
          refId: "prn_outsider-user",
        });
        await seedFrozenSnapshot({
          perStep: [
            {
              stepId: "work",
              grants: [
                "tool:read_file",
                MAIL_TRIGGER_GRANT,
                "mail.accept:tenant",
              ],
              grantEffects: { "tool:read_file": "allow" },
            },
          ],
          grantRequirements: [
            { resource: "secret:vault", action: "use", source: "invoker" },
          ],
        });
        const grantStore = createGrantStore(h.db);
        let grantReads = 0;
        const countingGrantStore: typeof grantStore = {
          ...grantStore,
          collectGrants: (principalId, tenantId) => {
            grantReads += 1;
            return grantStore.collectGrants(principalId, tenantId);
          },
        };
        const materialize = createMailTriggeredRunGrantsMaterializer({
          runTokenStore: createRunTokenStore({
            db: h.db,
            cipher: createTestCredentialCipher(),
          }),
          db: h.db,
          principalKeyStore: createPrincipalKeyStore({
            db: h.db,
            cipher: createTestCredentialCipher(),
          }),
          grantStore: countingGrantStore,
        });

        const result = await materialize({
          agentAddress: WORKFLOW_ADDRESS,
          runId: RUN_ID,
          senderCoordinates: [
            { coordType: "principal", id: "prn_outsider" },
            { coordType: "tenant", id: "tnt_other" },
          ],
        });

        expect(result.outcome).toBe("rejected");
        if (result.outcome === "rejected") {
          expect(result.code).toBe("mail_admission_denied");
        }
        expect(grantReads).toBe(0);
      });

      // A rule set that names only the invoker, so only the deployer may
      // fire the run.
      const invokerOnlyRules: GrantWalkSnapshot = {
        perStep: [
          {
            stepId: "work",
            grants: [MAIL_TRIGGER_GRANT, "mail.accept:invoker"],
            grantEffects: {},
          },
        ],
        grantRequirements: [],
      };

      test("an invoker-only rule refuses the first mail from a sender who is not the deployer", async () => {
        await seedTenants(h.db, [{ id: "tnt_other" }]);
        await seedPrincipal(h.db, {
          id: "prn_stranger",
          tenantId: "tnt_other",
          kind: "user",
          refId: "usr_stranger",
        });
        await seedFrozenSnapshot(invokerOnlyRules);

        const result = await materializeOnce(RUN_ID, {
          senderCoordinates: [
            { coordType: "principal", id: "prn_stranger" },
            { coordType: "tenant", id: "tnt_other" },
          ],
        });

        expect(result.outcome).toBe("rejected");
        if (result.outcome === "rejected") {
          expect(result.code).toBe("mail_admission_denied");
        }
      });

      test("an invoker-only rule admits the first mail from the deployer", async () => {
        // The frozen snapshot's launch spec records the creator as deployer.
        await seedFrozenSnapshot(invokerOnlyRules);

        const result = await materializeOnce(RUN_ID, {
          senderCoordinates: [
            { coordType: "principal", id: CREATOR },
            { coordType: "tenant", id: TENANT },
          ],
        });

        expect(result.outcome).toBe("materialized");
      });

      test("an unadmitted sender is refused before the deployer's absence is reported", async () => {
        // The deployment's deployer is gone, and it has an invoker
        // requirement. The stranger is refused as unadmitted, not told that
        // the deployment has no deployer.
        await seedTenants(h.db, [{ id: "tnt_other" }]);
        await seedPrincipal(h.db, {
          id: "prn_gone",
          tenantId: TENANT,
          kind: "user",
          refId: "prn_gone-user",
          status: "deactivated",
        });
        await seedWorkflowRunLaunchSpec(h.db, {
          anchorRunId: DEPLOYMENT,
          sourceAuthorityPrincipalId: "prn_gone",
          grantSnapshot: {
            ...invokerOnlyRules,
            grantRequirements: [
              { resource: "secret:vault", action: "use", source: "invoker" },
            ],
          },
        });

        const result = await materializeOnce(RUN_ID, {
          senderCoordinates: [
            { coordType: "principal", id: "prn_stranger" },
            { coordType: "tenant", id: "tnt_other" },
          ],
        });

        expect(result).toMatchObject({
          outcome: "rejected",
          code: "mail_admission_denied",
        });
      });

      test("an inactive deployer emits no invoker row", async () => {
        await seedPrincipal(h.db, {
          id: "prn_gone",
          tenantId: TENANT,
          kind: "user",
          refId: "prn_gone-user",
          status: "deactivated",
        });
        await seedWorkflowRunLaunchSpec(h.db, {
          anchorRunId: DEPLOYMENT,
          sourceAuthorityPrincipalId: "prn_gone",
          grantSnapshot: {
            perStep: [
              {
                stepId: "work",
                grants: [
                  MAIL_TRIGGER_GRANT,
                  "mail.accept:invoker",
                  "mail.accept:tenant",
                ],
                grantEffects: {},
              },
            ],
            grantRequirements: [],
          },
        });

        const result = await materializeOnce(RUN_ID);

        if (result.outcome !== "materialized") {
          throw new Error(`expected materialized, got ${result.outcome}`);
        }
        expect(
          result.stepGrants.filter((g) =>
            g.resource.startsWith("mail.accept:principal:"),
          ),
        ).toEqual([]);
      });
    });

    describe("two senders racing the first fire", () => {
      const WINNER = "prn_winner";
      const LOSER = "prn_loser";

      function senderOf(principalId: string) {
        return {
          senderCoordinates: [
            { coordType: "principal" as const, id: principalId },
            { coordType: "tenant" as const, id: TENANT },
          ],
        };
      }

      async function waitForBlocked(blockerPid: number) {
        // Postgres exposes lock waits through its activity view only.
        for (;;) {
          const waiting = await h.db.execute(
            sql`select pid from pg_stat_activity where ${blockerPid} = any(pg_blocking_pids(pid))`,
          );
          const pid = waiting[0]?.["pid"];
          if (typeof pid === "number") return pid;
          await new Promise<void>((resolve) => setImmediate(resolve));
        }
      }

      // Hold the anchor lock while the winner and then the loser pass their
      // staged first-fire admission and queue on it, so the winner commits the
      // run first and the loser reaches the commit with the run already live.
      async function raceFirstFire(accept: string[]) {
        await seedFrozenSnapshot({
          perStep: [
            {
              stepId: "work",
              grants: [MAIL_TRIGGER_GRANT, ...accept],
              grantEffects: {},
            },
          ],
          grantRequirements: [],
        });
        const locked = Promise.withResolvers<number>();
        const release = Promise.withResolvers<undefined>();
        const blocker = h.db.transaction(async (tx) => {
          await tx
            .select({ id: workflowRun.id })
            .from(workflowRun)
            .where(eq(workflowRun.id, DEPLOYMENT))
            .for("update");
          const [backend] = await tx.execute(
            sql`select pg_backend_pid() as pid`,
          );
          const pid = backend?.["pid"];
          if (typeof pid !== "number") throw new Error("Missing backend PID");
          locked.resolve(pid);
          await release.promise;
        });
        void blocker.catch(locked.reject);
        try {
          const blockerPid = await locked.promise;
          const winner = materializeOnce(RUN_ID, senderOf(WINNER));
          const winnerPid = await waitForBlocked(blockerPid);
          const loser = materializeOnce(RUN_ID, senderOf(LOSER));
          // A second waiter on a row lock queues behind the first waiter.
          await waitForBlocked(winnerPid);
          release.resolve(undefined);
          await blocker;
          return { winner: await winner, loser: await loser };
        } finally {
          release.resolve(undefined);
        }
      }

      test("gates the loser on the committed run even while credentials are unready", async () => {
        // The rules name the loser, so it passes the staged first-fire check
        // and queues on the anchor lock.
        await seedFrozenSnapshot({
          perStep: [
            {
              stepId: "work",
              grants: [
                MAIL_TRIGGER_GRANT,
                mailAcceptResource("principal", LOSER),
              ],
              grantEffects: {},
            },
          ],
          grantRequirements: [],
        });
        // The blocker plays the winner: under the anchor lock it commits the
        // run principal with the winner's accept-grant, and it clears the
        // anchor's key so credential resolution is not ready afterwards. The
        // loser must be gated as a delivery to the live run, not parked.
        const locked = Promise.withResolvers<number>();
        const release = Promise.withResolvers<undefined>();
        const blocker = h.db.transaction(async (tx) => {
          await tx
            .select({ id: workflowRun.id })
            .from(workflowRun)
            .where(eq(workflowRun.id, DEPLOYMENT))
            .for("update");
          await tx.insert(principal).values({
            id: "prn_committed_run",
            tenantId: TENANT,
            kind: "workflow",
            refId: RUN_ID,
            status: "active",
          });
          await tx.insert(grant).values({
            id: "grt_winner_accept",
            tenantId: TENANT,
            principalId: "prn_committed_run",
            resource: mailAcceptResource("principal", WINNER),
            action: "accept",
            effect: "allow",
            origin: "creator",
          });
          await tx
            .update(workflowRun)
            .set({ publicKey: null })
            .where(eq(workflowRun.id, DEPLOYMENT));
          const [backend] = await tx.execute(
            sql`select pg_backend_pid() as pid`,
          );
          const pid = backend?.["pid"];
          if (typeof pid !== "number") throw new Error("Missing backend PID");
          locked.resolve(pid);
          await release.promise;
        });
        void blocker.catch(locked.reject);
        try {
          const blockerPid = await locked.promise;
          const loser = materializeOnce(RUN_ID, senderOf(LOSER));
          await waitForBlocked(blockerPid);
          release.resolve(undefined);
          await blocker;
          const result = await loser;

          expect(result.outcome).toBe("rejected");
          if (result.outcome === "rejected") {
            expect(result.code).toBe("mail_admission_denied");
          }
        } finally {
          release.resolve(undefined);
          await blocker.catch(() => undefined);
        }
      });

      test("admits the loser when the committed run accepts it", async () => {
        const { winner, loser } = await raceFirstFire(["mail.accept:tenant"]);

        expect(winner.outcome).toBe("materialized");
        expect(loser.outcome).toBe("materialized");
      });
    });

    describe("createCorrespondentGrantMinter (send-seam)", () => {
      const SENDER_PRINCIPAL = "prn_sender_run";
      const RECIPIENT_RUN_ID = "run_recipient";
      const RECIPIENT_ADDRESS = "run_recipient@tenant.example";
      const RECIPIENT_PRINCIPAL = "prn_recipient";
      const CORRESPONDENT_RESOURCE = mailAcceptResource(
        "principal",
        RECIPIENT_PRINCIPAL,
      );

      // A frozen snapshot that authored the `correspondent` relation: the deploy
      // marker is present, so the send-time mint is authorized.
      function correspondentSnapshot(): GrantWalkSnapshot {
        return {
          perStep: [
            {
              stepId: "work",
              grants: ["mail.accept:correspondent"],
              grantEffects: {},
            },
          ],
          grantRequirements: [],
        };
      }

      // Give the seeded deployment run a principal and a live/terminal status,
      // optionally with a cancellation request or deadline: the send-seam mint
      // hangs the correspondent grant off the SENDING run's principal and only
      // mints for an executable run.
      async function makeSenderRun(
        status: "running" | "completed",
        overrides: { cancellationRequestedAt?: Date; expiresAt?: Date } = {},
      ) {
        await seedPrincipal(h.db, {
          id: SENDER_PRINCIPAL,
          tenantId: TENANT,
          kind: "workflow",
          refId: DEPLOYMENT,
        });
        await h.db
          .update(workflowRun)
          .set({ principalId: SENDER_PRINCIPAL, status, ...overrides })
          .where(eq(workflowRun.id, DEPLOYMENT));
      }

      async function senderRunStatus() {
        const [row] = await h.db
          .select({ status: workflowRun.status })
          .from(workflowRun)
          .where(eq(workflowRun.id, DEPLOYMENT));
        return row?.status;
      }

      // A recipient run that resolves to a durable principal: `resolveSenderKey`
      // reads its `public_key`/`principal_id` off the run row, so the reply's
      // coordinate the gate later matches is exactly this principal.
      async function seedRecipientRun() {
        await seedPrincipal(h.db, {
          id: RECIPIENT_PRINCIPAL,
          tenantId: TENANT,
          kind: "workflow",
          refId: RECIPIENT_RUN_ID,
        });
        await h.db.insert(workflowRun).values({
          id: RECIPIENT_RUN_ID,
          tenantId: TENANT,
          anchorRunId: RECIPIENT_RUN_ID,
          definitionId: DEFINITION,
          address: RECIPIENT_ADDRESS,
          status: "running",
          principalId: RECIPIENT_PRINCIPAL,
          publicKey: "ab".repeat(32),
        });
      }

      function mint() {
        return createCorrespondentGrantMinter({
          db: h.db,
          principalKeyStore: createPrincipalKeyStore({
            db: h.db,
            cipher: createTestCredentialCipher(),
          }),
        });
      }

      async function correspondentRows() {
        return h.db
          .select()
          .from(grant)
          .where(eq(grant.resource, CORRESPONDENT_RESOURCE));
      }

      test("mints a correspondent accept-grant on the sending run when the definition authored correspondent", async () => {
        await seedFrozenSnapshot(correspondentSnapshot());
        await makeSenderRun("running");
        await seedRecipientRun();

        await mint()({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });

        const rows = await correspondentRows();
        expect(rows).toHaveLength(1);
        expect(rows[0]?.principalId).toBe(SENDER_PRINCIPAL);
        expect(rows[0]?.action).toBe("accept");
        expect(rows[0]?.effect).toBe("allow");
        expect(rows[0]?.origin).toBe("creator");
      });

      test("admits the reply to a run with no mail trigger and refuses another sender", async () => {
        // An HTTP-launched run that sends with mail_send: its snapshot has no
        // mail trigger and authors only `correspondent`.
        await seedFrozenSnapshot({
          perStep: [
            {
              stepId: "work",
              grants: ["tool:mail_send", "mail.accept:correspondent"],
              grantEffects: { "tool:mail_send": "allow" },
            },
          ],
          grantRequirements: [],
        });
        await makeSenderRun("running");
        await seedRecipientRun();

        await mint()({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });

        const reply = await materializeOnce(DEPLOYMENT, {
          senderCoordinates: [
            { coordType: "principal", id: RECIPIENT_PRINCIPAL },
            { coordType: "asset", id: ASSET },
            { coordType: "tenant", id: TENANT },
          ],
        });
        expect(reply.outcome).toBe("materialized");

        const stranger = await materializeOnce(DEPLOYMENT, {
          senderCoordinates: [
            { coordType: "principal", id: CREATOR },
            { coordType: "tenant", id: TENANT },
          ],
        });
        expect(stranger.outcome).toBe("rejected");
        if (stranger.outcome === "rejected") {
          expect(stranger.code).toBe("mail_admission_denied");
        }
      });

      test("mints nothing when the sending definition did not author correspondent", async () => {
        // The base snapshot declares `mail.accept:tenant`, never correspondent.
        await seedFrozenSnapshot(snapshot("secret:vault"));
        await makeSenderRun("running");
        await seedRecipientRun();

        await mint()({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });

        expect(await correspondentRows()).toHaveLength(0);
      });

      test("mints nothing when the sending run is terminal", async () => {
        await seedFrozenSnapshot(correspondentSnapshot());
        await makeSenderRun("completed");
        await seedRecipientRun();

        await mint()({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });

        expect(await correspondentRows()).toHaveLength(0);
      });

      test("mints nothing when the sending run has a cancellation request", async () => {
        await seedFrozenSnapshot(correspondentSnapshot());
        await makeSenderRun("running", { cancellationRequestedAt: new Date() });
        await seedRecipientRun();
        expect(await senderRunStatus()).toBe("running");

        await mint()({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });

        expect(await correspondentRows()).toHaveLength(0);
      });

      test("mints nothing when the sending run's deadline has passed", async () => {
        await seedFrozenSnapshot(correspondentSnapshot());
        await makeSenderRun("running", {
          expiresAt: new Date(Date.now() - 3_600_000),
        });
        await seedRecipientRun();
        expect(await senderRunStatus()).toBe("running");

        await mint()({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });

        expect(await correspondentRows()).toHaveLength(0);
      });

      test("mints when the sending run's deadline is in the future", async () => {
        await seedFrozenSnapshot(correspondentSnapshot());
        await makeSenderRun("running", {
          expiresAt: new Date(Date.now() + 3_600_000),
        });
        await seedRecipientRun();

        await mint()({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });

        expect(await correspondentRows()).toHaveLength(1);
      });

      test("is idempotent: a repeat mail to the same recipient does not accrue a second row", async () => {
        await seedFrozenSnapshot(correspondentSnapshot());
        await makeSenderRun("running");
        await seedRecipientRun();

        const minter = mint();
        await minter({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });
        await minter({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });

        expect(await correspondentRows()).toHaveLength(1);
      });

      test("mints past an expired grant on the same coordinate and admits the reply", async () => {
        await seedFrozenSnapshot({
          perStep: [
            {
              stepId: "work",
              grants: ["tool:mail_send", "mail.accept:correspondent"],
              grantEffects: { "tool:mail_send": "allow" },
            },
          ],
          grantRequirements: [],
        });
        await makeSenderRun("running");
        await seedRecipientRun();
        await h.db.insert(grant).values({
          id: "grt_expired_allow",
          tenantId: TENANT,
          principalId: SENDER_PRINCIPAL,
          resource: CORRESPONDENT_RESOURCE,
          action: "accept",
          effect: "allow",
          origin: "creator",
          expiresAt: new Date(Date.now() - 60_000),
        });

        await mint()({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });

        expect(await correspondentRows()).toHaveLength(2);
        const reply = await materializeOnce(DEPLOYMENT, {
          senderCoordinates: [
            { coordType: "principal", id: RECIPIENT_PRINCIPAL },
            { coordType: "asset", id: ASSET },
            { coordType: "tenant", id: TENANT },
          ],
        });
        expect(reply.outcome).toBe("materialized");
      });

      test("mints for a fired recipient whose signing key is cleared", async () => {
        await seedFrozenSnapshot(correspondentSnapshot());
        await makeSenderRun("running");
        await seedRecipientRun();
        // A sidecar replacement clears the recorded key but keeps the principal.
        await h.db
          .update(workflowRun)
          .set({ publicKey: null })
          .where(eq(workflowRun.id, RECIPIENT_RUN_ID));

        await mint()({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });

        const rows = await correspondentRows();
        expect(rows).toHaveLength(1);
        expect(rows[0]?.principalId).toBe(SENDER_PRINCIPAL);
      });

      test("mints nothing when the recipient does not resolve to a principal", async () => {
        await seedFrozenSnapshot(correspondentSnapshot());
        await makeSenderRun("running");
        // No recipient run seeded: the address resolves to no durable principal.

        await mint()({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS],
        });

        expect(await correspondentRows()).toHaveLength(0);
      });

      test("mints a grant for each recipient of one outbound mail in a single call", async () => {
        await seedFrozenSnapshot(correspondentSnapshot());
        await makeSenderRun("running");
        await seedRecipientRun();
        // A second resolvable recipient run: the fan-out mints one grant per
        // recipient under a single lock on the sending run.
        const RECIPIENT2_RUN_ID = "run_recipient2";
        const RECIPIENT2_ADDRESS = "run_recipient2@tenant.example";
        const RECIPIENT2_PRINCIPAL = "prn_recipient2";
        await seedPrincipal(h.db, {
          id: RECIPIENT2_PRINCIPAL,
          tenantId: TENANT,
          kind: "workflow",
          refId: RECIPIENT2_RUN_ID,
        });
        await h.db.insert(workflowRun).values({
          id: RECIPIENT2_RUN_ID,
          tenantId: TENANT,
          anchorRunId: RECIPIENT2_RUN_ID,
          definitionId: DEFINITION,
          address: RECIPIENT2_ADDRESS,
          status: "running",
          principalId: RECIPIENT2_PRINCIPAL,
          publicKey: "cd".repeat(32),
        });

        await mint()({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [RECIPIENT_ADDRESS, RECIPIENT2_ADDRESS],
        });

        expect(await correspondentRows()).toHaveLength(1);
        const rows2 = await h.db
          .select()
          .from(grant)
          .where(
            eq(
              grant.resource,
              mailAcceptResource("principal", RECIPIENT2_PRINCIPAL),
            ),
          );
        expect(rows2).toHaveLength(1);
        expect(rows2[0]?.principalId).toBe(SENDER_PRINCIPAL);
      });

      test("a recipient that fails to resolve does not cost a co-recipient its grant", async () => {
        await seedFrozenSnapshot(correspondentSnapshot());
        await makeSenderRun("running");
        const keys = createPrincipalKeyStore({
          db: h.db,
          cipher: createTestCredentialCipher(),
        });
        // A resolvable user recipient with a signing key.
        await seedPrincipal(h.db, {
          id: "prn_bob",
          tenantId: TENANT,
          kind: "user",
          refId: "usr_bob",
          status: "active",
        });
        await keys.generate("prn_bob");
        // An ambiguous user recipient: two case variants of one user id and
        // no canonical row, so its resolution throws.
        for (const [id, refId] of [
          ["prn_v1", "Usr_Alice"],
          ["prn_v2", "USR_ALICE"],
        ] as const) {
          await seedPrincipal(h.db, {
            id,
            tenantId: TENANT,
            kind: "user",
            refId,
            status: "active",
          });
          await keys.generate(id);
        }

        await createCorrespondentGrantMinter({
          db: h.db,
          principalKeyStore: keys,
        })({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [
            "usr_alice@tnt.example.test",
            "usr_bob@tnt.example.test",
          ],
        });

        const rows = await h.db
          .select()
          .from(grant)
          .where(
            eq(grant.resource, mailAcceptResource("principal", "prn_bob")),
          );
        expect(rows).toHaveLength(1);
        expect(rows[0]?.principalId).toBe(SENDER_PRINCIPAL);
      });

      test("a SQL fault on one recipient does not cost a co-recipient its grant", async () => {
        await seedFrozenSnapshot(correspondentSnapshot());
        await makeSenderRun("running");
        const keys = createPrincipalKeyStore({
          db: h.db,
          cipher: createTestCredentialCipher(),
        });
        for (const [id, refId] of [
          ["prn_carol", "usr_carol"],
          ["prn_bob", "usr_bob"],
        ] as const) {
          await seedPrincipal(h.db, {
            id,
            tenantId: TENANT,
            kind: "user",
            refId,
            status: "active",
          });
          await keys.generate(id);
        }
        // A statement error inside the minter's transaction, unlike a throw
        // in application code, aborts that transaction in Postgres. Only a
        // savepoint per recipient keeps the co-recipient's insert alive.
        const faultingKeys: typeof keys = {
          ...keys,
          async getPublicKey(principalId, tx) {
            if (principalId === "prn_carol") {
              await (tx ?? h.db).execute(sql`select 1 / 0`);
            }
            return keys.getPublicKey(principalId, tx);
          },
        };

        await createCorrespondentGrantMinter({
          db: h.db,
          principalKeyStore: faultingKeys,
        })({
          senderAddress: WORKFLOW_ADDRESS,
          recipientAddresses: [
            "usr_carol@tnt.example.test",
            "usr_bob@tnt.example.test",
          ],
        });

        const rows = await h.db
          .select({ resource: grant.resource })
          .from(grant)
          .where(eq(grant.principalId, SENDER_PRINCIPAL));
        expect(
          rows
            .map((row) => row.resource)
            .filter((resource) =>
              resource.startsWith("mail.accept:principal:"),
            ),
        ).toEqual([mailAcceptResource("principal", "prn_bob")]);
      });

      describe("a recipient deployment that has not fired yet", () => {
        const FRESH_DEFINITION = "wfd_fresh";
        const FRESH_RUN_ID = "run_fresh";
        const FRESH_ADDRESS = "run_fresh@tenant.example";

        // A deployed, acked anchor with no principal, which the sending run
        // deployed: its first fire mints the principal, and its own snapshot
        // admits mail from its deployer, the invoker.
        async function seedFreshRecipient(
          overrides: {
            cancellationRequestedAt?: Date;
            publicKey?: string | null;
          } = {},
        ) {
          await h.db.insert(workflowDefinition).values({
            id: FRESH_DEFINITION,
            tenantId: TENANT,
            name: FRESH_DEFINITION,
            assetId: ASSET,
          });
          await h.db.insert(workflowRun).values({
            id: FRESH_RUN_ID,
            tenantId: TENANT,
            anchorRunId: FRESH_RUN_ID,
            definitionId: FRESH_DEFINITION,
            address: FRESH_ADDRESS,
            status: "deployed",
            publicKey: "ef".repeat(32),
            ...overrides,
          });
          await seedWorkflowRunLaunchSpec(h.db, {
            anchorRunId: FRESH_RUN_ID,
            sourceAuthorityPrincipalId: SENDER_PRINCIPAL,
            grantSnapshot: {
              perStep: [
                {
                  stepId: "work",
                  grants: [
                    `mail.address:${FRESH_ADDRESS}`,
                    "mail.accept:invoker",
                  ],
                  grantEffects: {},
                },
              ],
              grantRequirements: [],
            },
          });
        }

        function keyStore() {
          return createPrincipalKeyStore({
            db: h.db,
            cipher: createTestCredentialCipher(),
          });
        }

        function materializer() {
          return createMailTriggeredRunGrantsMaterializer({
            runTokenStore: createRunTokenStore({
              db: h.db,
              cipher: createTestCredentialCipher(),
            }),
            db: h.db,
            principalKeyStore: keyStore(),
            grantStore: createGrantStore(h.db),
          });
        }

        async function senderArgs(address: string) {
          const resolved = await resolveSenderPrincipal(
            h.db,
            keyStore(),
            address,
          );
          if (resolved === null) throw new Error(`${address} did not resolve`);
          return {
            senderCoordinates: resolved.coordinates,
          };
        }

        // The send-seam order: the sender mints, then its mail first-fires the
        // recipient, then the recipient replies to the sender.
        async function firstContactThenReply() {
          await mint()({
            senderAddress: WORKFLOW_ADDRESS,
            recipientAddresses: [FRESH_ADDRESS],
          });
          const fired = await materializer()({
            agentAddress: FRESH_ADDRESS,
            runId: FRESH_RUN_ID,
            ...(await senderArgs(WORKFLOW_ADDRESS)),
          });
          expect(fired.outcome).toBe("materialized");
          return materializer()({
            agentAddress: WORKFLOW_ADDRESS,
            runId: DEPLOYMENT,
            ...(await senderArgs(FRESH_ADDRESS)),
          });
        }

        async function senderAcceptRows() {
          const rows = await h.db
            .select({ resource: grant.resource })
            .from(grant)
            .where(eq(grant.principalId, SENDER_PRINCIPAL));
          return rows
            .map((row) => row.resource)
            .filter((resource) =>
              resource.startsWith("mail.accept:principal:"),
            );
        }

        test("admits the reply to a first contact", async () => {
          await seedFrozenSnapshot(correspondentSnapshot());
          await makeSenderRun("running");
          await seedFreshRecipient();

          const reply = await firstContactThenReply();

          const [fresh] = await h.db
            .select({ principalId: workflowRun.principalId })
            .from(workflowRun)
            .where(eq(workflowRun.id, FRESH_RUN_ID));
          expect(fresh?.principalId).not.toBeNull();
          expect(await senderAcceptRows()).toEqual([
            mailAcceptResource("principal", fresh?.principalId ?? ""),
          ]);
          expect(reply.outcome).toBe("materialized");
        });

        test("refuses the reply when the sender did not author correspondent", async () => {
          await seedFrozenSnapshot(snapshot("secret:vault"));
          await makeSenderRun("running");
          await seedFreshRecipient();

          const reply = await firstContactThenReply();

          expect(await senderAcceptRows()).toEqual([]);
          expect(reply.outcome).toBe("rejected");
          if (reply.outcome === "rejected") {
            expect(reply.code).toBe("mail_admission_denied");
          }
        });

        test("mints for a deployment that has not acked its deploy yet", async () => {
          await seedFrozenSnapshot(correspondentSnapshot());
          await makeSenderRun("running");
          await seedFreshRecipient({ publicKey: null });

          await mint()({
            senderAddress: WORKFLOW_ADDRESS,
            recipientAddresses: [FRESH_ADDRESS],
          });

          expect(await senderAcceptRows()).toHaveLength(1);
        });

        test("mints nothing for a deployment that can no longer fire", async () => {
          await seedFrozenSnapshot(correspondentSnapshot());
          await makeSenderRun("running");
          await seedFreshRecipient({ cancellationRequestedAt: new Date() });

          await mint()({
            senderAddress: WORKFLOW_ADDRESS,
            recipientAddresses: [FRESH_ADDRESS],
          });

          expect(await senderAcceptRows()).toEqual([]);
        });
      });
    });
  },
);
