// Creator-derived grant materialization at the trigger route.
//
// A workflow may declare a CREATOR-sourced grant requirement: at trigger
// time the run's authority for that resource is materialized against the
// workflow ASSET's creator, not the triggerer. The run gets the grant only
// if the creator actually holds it; otherwise the trigger is rejected
// fail-closed. This test drives that resolution through the production
// `POST /workflows/:deploymentId/mail` route against a real migrated schema
// and a real DB-backed grant store:
//
//   - POSITIVE: the workflow declares `grantRequirements: [{ source:
//     "creator", resource: "secret:vault", action: "use" }]`. The asset's
//     creator holds a backing `secret:vault`/`use` grant, so
//     `resolveGrantMaterialization` resolves it and commits a run grant row
//     with `origin="creator"`. The run principal, run row, and the
//     creator-materialized grant all land in Postgres.
//   - NEGATIVE: the same requirement, but the creator holds NO backing
//     grant. The route rejects `403 insufficient_grants` and commits
//     nothing -- no run principal, no run row, no grant rows. This is the
//     discriminator: the creator gate actually gates.
//
// FALLBACK (stated honestly). GAP-3 was scoped as an A-mails-B federation:
// deployment A sends mail to B, and B's run materializes via the real hub
// path. That is NOT reproducible in this harness: the deploy-flow fixture's
// inbound-mail path runs against a `fakeDb` and injects grants directly
// (`fireMailTrigger` -> `sendRunGrants`), and never wires the real
// `createMailTriggeredRunGrantsMaterializer`, so B's run would never
// materialize through the real creator-resolution path. Additionally, A's
// workflow could only mail B from an action step, which this host does not
// execute. So this test collapses "A mails B" into driving B's `/mail`
// route directly through the real `createApp` seam with a creator-declared
// requirement -- which exercises the SAME `resolveGrantMaterialization`
// against B's asset creator that a federated trigger would. The A->B mail
// hop is not exercised; the creator-derived materialization it would feed
// is.
//
// SCOPE. As with the other reachable-half tests, the run's CONSUME side is
// not exercised: this host does not execute the run's steps, so the
// materialized grant's runtime authorization is out of reach. The
// derivation, materialization, and DB commit (or fail-closed rejection) are
// what this asserts.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { and, eq } from "drizzle-orm";
import { type } from "arktype";

import { createGrantStore } from "@intx/db";
import {
  grant as grantTable,
  principal as principalTable,
  tenant as tenantTable,
  workflowDeployment as workflowDeploymentTable,
  workflowRun as workflowRunTable,
} from "@intx/db/schema";
import { createSSHSignature, generateKeyPair } from "@intx/crypto";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  DEFAULT_ASSET_REF,
  WORKFLOW_JSON_PATH,
  createAssetService,
  createRepoStore,
  workflowAuthorize,
  workflowKindHandler,
  type AuthorizeFn,
  type EventCollectorRegistry,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import { createSidecarEmitter } from "@intx/hub-sessions";
import type { KeyPair } from "@intx/types/runtime";
import type { GrantRequirement } from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedGrant, seedPrincipal } from "@intx/test-harness/seed";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import { defineAgent } from "@intx/agent";
import { deriveDeploymentAddress } from "@intx/workflow-deploy";

const DOMAIN = "federation.example.test";
const TENANT_ID = "tnt_mail_federation";
const CALLER_USER_ID = "usr_federation_caller";
const CALLER_PRINCIPAL_ID = "prn_federation_caller";
const CREATOR_PRINCIPAL_ID = "prn_federation_creator";

// The creator-declared requirement B's workflow carries.
const CREATOR_CAP_RESOURCE = "secret:vault";
const CREATOR_CAP_ACTION = "use";

const CREATOR_REQUIREMENT: GrantRequirement = {
  source: "creator",
  resource: CREATOR_CAP_RESOURCE,
  action: CREATOR_CAP_ACTION,
};

// B's workflow: a single agent step, plus a creator-sourced grant
// requirement. An agent step keeps the definition hydrate-able without an
// action host; the requirement is what the trigger route resolves against
// the asset's creator.
function bWorkflow(id: string, address: string): WorkflowDefinition {
  const agent = defineAgent({
    id: "agent-federation-b",
    systemPrompt: "You are deployment B's step agent.",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
  });
  return defineWorkflow({
    id,
    trigger: { type: "mail", to: address },
    steps: { b: step({ agent }) },
    grantRequirements: [CREATOR_REQUIREMENT],
  });
}

const TriggerResponse = type({
  deploymentId: "string",
  address: "string",
  messageId: "string",
});

// The structured error body the route returns on rejection.
const ErrorBody = type({
  error: type({ code: "string", message: "string" }),
});

let h: TestDb;
let signingKey: KeyPair;
const tempDirs: string[] = [];

function createMockGetSession(userId: string): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "caller@example.com",
      emailVerified: true,
      name: "Caller",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_federation",
      userId,
      token: "tok_federation",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`mail-federation mock: ${name} not implemented`);
}

function createMockSessionService(): SessionService {
  return {
    stageWorkflowStep: () => notImpl("stageWorkflowStep"),
    deployInstanceAtHead: () => notImpl("deployInstanceAtHead"),
    deployWorkflowDefinition: () => notImpl("deployWorkflowDefinition"),
    deploySingleStepAtHead: () => notImpl("deploySingleStepAtHead"),
    sendUserMessage: () => notImpl("sendUserMessage"),
    endSession: () => notImpl("endSession"),
  };
}

function createMockEventCollectors(): EventCollectorRegistry {
  return {
    create: () => notImpl("create"),
    dispatch: () => notImpl("dispatch"),
    abandon: () => notImpl("abandon"),
    has: () => false,
    getStatus: () => undefined,
    getAccumulatedText: () => undefined,
    getCurrentTurnId: () => undefined,
    getLastTurnId: () => undefined,
  };
}

// A sidecar router whose routing surface accepts the trigger's send. The
// positive case reaches `sendRunGrants`/`routeMail` after materialization,
// so both must return true; the negative case rejects (403) before it ever
// routes, so its calls never fire.
function createRoutingSidecarRouter(): SidecarRouter {
  function notImplRouter(name: string): never {
    throw new Error(`mail-federation mock: sidecarRouter.${name} not used`);
  }
  return {
    handleOpen: () => notImplRouter("handleOpen"),
    handleMessage: () => notImplRouter("handleMessage"),
    handleClose: () => notImplRouter("handleClose"),
    routeMail: () => true,
    sendRunGrants: () => true,
    sendAgentDeploy: () => notImplRouter("sendAgentDeploy"),
    sendAgentUndeploy: () => notImplRouter("sendAgentUndeploy"),
    sendSourcesUpdate: () => notImplRouter("sendSourcesUpdate"),
    sendPack: () => notImplRouter("sendPack"),
    sendProvisionStep: () => notImplRouter("sendProvisionStep"),
    bindStepRoute: () => notImplRouter("bindStepRoute"),
    unbindStepRoute: () => notImplRouter("unbindStepRoute"),
    sendSyncRequest: () => notImplRouter("sendSyncRequest"),
    sendSignalDeliver: () => notImplRouter("sendSignalDeliver"),
    sendDrain: () => notImplRouter("sendDrain"),
    subscribeAgent: () => notImplRouter("subscribeAgent"),
    dispatchAgentEvent: () => undefined,
    getConnectedSidecars: () => [],
    getRoutableAddresses: () => [],
    getConnectorState: () => null,
    events: createSidecarEmitter(),
  };
}

async function createWorkflowRepoStore(): Promise<RepoStore> {
  const dataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "mail-federation-"),
  );
  tempDirs.push(dataDir);
  const signer = async (payload: string) =>
    createSSHSignature(payload, signingKey.privateKey, signingKey.publicKey);
  const authorize: AuthorizeFn = (principal, repoId, ref, act) => {
    if (repoId.kind === "workflow") {
      return workflowAuthorize(principal, repoId, ref, act);
    }
    return { allowed: false, reason: `no authorize for ${repoId.kind}` };
  };
  return createRepoStore({
    dataDir,
    signingKey,
    handlers: { workflow: workflowKindHandler },
    authorize,
    signingCallback: () => signer,
  });
}

// The trigger route derives the deployment address as
// `ins_<deploymentId>@<tenant.domain>`; the routing router accepts any
// address, so the derived value only needs to be well-formed.

describe.skipIf(!harnessDbEnvAvailable())(
  "creator-derived grant materialization at the trigger route",
  () => {
    let repoStore: RepoStore;
    let assetService: ReturnType<typeof createAssetService>;

    beforeAll(async () => {
      signingKey = await generateKeyPair();
      h = await createTestDb();
      repoStore = await createWorkflowRepoStore();
      assetService = createAssetService({ db: h.db, repoStore });
    });

    afterAll(async () => {
      await h.close();
      for (const d of tempDirs.splice(0)) {
        await fs.promises.rm(d, { recursive: true, force: true });
      }
    });

    beforeEach(async () => {
      await h.reset();
    });

    // Seed the shared tenancy and the workflow asset (populated with B's
    // `workflow.json`), returning the wired app. `creatorHoldsGrant`
    // decides whether the creator holds the backing `secret:vault`/`use`
    // grant.
    async function setup(opts: {
      deploymentId: string;
      creatorHoldsGrant: boolean;
    }): Promise<ReturnType<typeof createApp>> {
      const address = deriveDeploymentAddress({
        deploymentId: opts.deploymentId,
        deploymentDomain: DOMAIN,
      });
      const assetId = `ast_${opts.deploymentId}`;

      await h.db.insert(tenantTable).values({
        id: TENANT_ID,
        name: TENANT_ID,
        slug: TENANT_ID,
        domain: DOMAIN,
        parentId: null,
      });
      await seedPrincipal(h.db, {
        id: CALLER_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
        refId: CALLER_USER_ID,
        status: "active",
      });
      await seedPrincipal(h.db, {
        id: CREATOR_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
        refId: "usr_federation_creator_ref",
        status: "active",
      });
      await seedAsset(h.db, {
        id: assetId,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: `federation-wf-${opts.deploymentId}`,
        creatorPrincipalId: CREATOR_PRINCIPAL_ID,
      });
      // The caller's grant to invoke the trigger route.
      await seedGrant(h.db, {
        id: `grant-caller-manage-${opts.deploymentId}`,
        tenantId: TENANT_ID,
        resource: `workflow-run:${opts.deploymentId}`,
        action: "manage",
        effect: "allow",
        origin: "system",
        principalId: CALLER_PRINCIPAL_ID,
      });
      // The creator's backing grant, present only in the positive case.
      if (opts.creatorHoldsGrant) {
        await seedGrant(h.db, {
          id: `grant-creator-vault-${opts.deploymentId}`,
          tenantId: TENANT_ID,
          resource: CREATOR_CAP_RESOURCE,
          action: CREATOR_CAP_ACTION,
          effect: "allow",
          origin: "creator",
          principalId: CREATOR_PRINCIPAL_ID,
        });
      }
      await h.db.insert(workflowDeploymentTable).values({
        id: opts.deploymentId,
        tenantId: TENANT_ID,
        definitionAssetId: assetId,
        address,
        publicKey: null,
        status: "deployed",
      });

      await repoStore.initRepo({ kind: "workflow", id: assetId });
      await assetService.populateAsset({
        assetId,
        ref: DEFAULT_ASSET_REF,
        principal: { kind: "hub" },
        tree: {
          files: {
            [WORKFLOW_JSON_PATH]: JSON.stringify(
              bWorkflow(`wf_${opts.deploymentId}`, address),
            ),
          },
          message: "seed workflow.json",
        },
      });

      return createApp({
        getSession: createMockGetSession(CALLER_USER_ID),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore: createGrantStore(h.db),
        sidecarRouter: createRoutingSidecarRouter(),
        sessionService: createMockSessionService(),
        eventCollectors: createMockEventCollectors(),
        assetService,
        repoStore,
        maxTarballBytes: 10_000_000,
      });
    }

    test("materializes the creator grant when the creator holds it", async () => {
      const deploymentId = "federation-b-positive-1";
      const app = await setup({ deploymentId, creatorHoldsGrant: true });

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/workflows/${deploymentId}/mail`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ content: "kick off B" }),
        },
      );
      if (res.status !== 202) {
        const body: unknown = await res.json();
        throw new Error(
          `expected 202, got ${String(res.status)}: ${JSON.stringify(body)}`,
        );
      }
      const json = TriggerResponse.assert(await res.json());
      const runId = json.messageId;

      // The run principal and run row committed.
      const principals = await h.db
        .select()
        .from(principalTable)
        .where(
          and(
            eq(principalTable.tenantId, TENANT_ID),
            eq(principalTable.kind, "workflow"),
            eq(principalTable.refId, runId),
          ),
        );
      expect(principals).toHaveLength(1);
      const runPrincipal = principals[0];
      if (runPrincipal === undefined) throw new Error("unreachable");

      const runs = await h.db
        .select()
        .from(workflowRunTable)
        .where(eq(workflowRunTable.id, runId));
      expect(runs).toHaveLength(1);

      // The creator-materialized grant committed on the run principal,
      // carrying `origin="creator"` and the requirement's resource/action.
      const grants = await h.db
        .select()
        .from(grantTable)
        .where(eq(grantTable.principalId, runPrincipal.id));
      const creatorGrant = grants.find(
        (g) =>
          g.resource === CREATOR_CAP_RESOURCE &&
          g.action === CREATOR_CAP_ACTION,
      );
      if (creatorGrant === undefined) {
        throw new Error(
          `no creator-materialized ${CREATOR_CAP_RESOURCE}/${CREATOR_CAP_ACTION} grant; got ${JSON.stringify(
            grants.map((g) => `${g.resource}/${g.action}`),
          )}`,
        );
      }
      expect(creatorGrant.origin).toBe("creator");
      expect(creatorGrant.effect).toBe("allow");
    });

    test("rejects 403 and commits nothing when the creator lacks the grant", async () => {
      const deploymentId = "federation-b-negative-1";
      const app = await setup({ deploymentId, creatorHoldsGrant: false });

      const res = await app.request(
        `/api/tenants/${TENANT_ID}/workflows/${deploymentId}/mail`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ content: "kick off B" }),
        },
      );
      expect(res.status).toBe(403);
      const body = ErrorBody.assert(await res.json());
      expect(body.error.code).toBe("insufficient_grants");

      // Nothing committed: no run principal, no run row, no grant rows on a
      // run principal. The route rejects before `commitRunGrants` runs.
      const runPrincipals = await h.db
        .select()
        .from(principalTable)
        .where(
          and(
            eq(principalTable.tenantId, TENANT_ID),
            eq(principalTable.kind, "workflow"),
          ),
        );
      expect(runPrincipals).toHaveLength(0);

      const runs = await h.db.select().from(workflowRunTable);
      expect(runs).toHaveLength(0);

      // The only grants present are the seeded caller manage grant; no run
      // grant row was committed.
      const vaultGrants = await h.db
        .select()
        .from(grantTable)
        .where(eq(grantTable.resource, CREATOR_CAP_RESOURCE));
      expect(vaultGrants).toHaveLength(0);
    });
  },
);
