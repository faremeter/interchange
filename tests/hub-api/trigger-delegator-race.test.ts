import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { type } from "arktype";
import { and, eq, sql } from "drizzle-orm";

import { createApp, type GetSession } from "@intx/hub-api";
import {
  createEventCollectorRegistry,
  createSidecarRouter,
  type AssetService,
  type RepoStore,
  type SessionService,
} from "@intx/hub-sessions";
import { grant, principal, workflowDefinition } from "@intx/db/schema";
import type { GrantRequirement } from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedGrant,
  seedPrincipal,
  seedWorkflowRun,
  seedWorkflowRunLaunchSpec,
} from "@intx/test-harness/seed";

// The HTTP trigger route stages a run's first grants under the deployment's
// anchor lock. A delegator change that holds that lock and commits while the
// trigger waits must be visible to the trigger: a revoked creator grant
// refuses the requirement, and no row is committed.

function mockGetSession(userId: string): GetSession {
  const now = new Date("2026-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "owner@example.com",
      emailVerified: true,
      name: "Owner",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_test",
      userId,
      token: "tok_test",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`mock: ${name} not implemented`);
}

function mockSessionService(): SessionService {
  return {
    stageWorkflowStep: () => notImpl("stageWorkflowStep"),
    endSession: () => notImpl("endSession"),
  };
}

// The workflow routes mount only with an asset service and a repo store. The
// trigger reads the run's durable lifecycle through the repo store, and a
// repo with no committed reads is a run that has not started.
function stubAssetService(): AssetService {
  return {
    createAsset: () => notImpl("createAsset"),
    populateAsset: () => notImpl("populateAsset"),
    readAssetBlob: () => notImpl("readAssetBlob"),
    listAssetBlobs: () => notImpl("listAssetBlobs"),
  };
}

function stubRepoStore(): RepoStore {
  const unused = () => Promise.reject(new Error("stub repoStore not wired"));
  return {
    initRepo: unused,
    writeTree: unused,
    writeTreePreservingPrefix: unused,
    writeTreeDelta: unused,
    receivePack: unused,
    createPack: unused,
    commitPackedTip: () => notImpl("commitPackedTip"),
    forgetPackedTip: () => notImpl("forgetPackedTip"),
    resolveRef: unused,
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: () => notImpl("getRepoDir"),
    openCommittedReads: async () => null,
    openCommittedReadsAtCommit: unused,
    subscribe: () => notImpl("subscribe"),
  };
}

const CREATOR = "prn_creator";
const DEPLOYER = "prn_deployer";
const ANCHOR = "run_first";
const VAULT = { resource: "secret:vault", action: "use" } as const;

describe.skipIf(!harnessDbEnvAvailable())(
  "the HTTP trigger and a delegator change on its anchor lock (real DB)",
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
    });

    // Create the tenant through the route, so the caller is its owner, then
    // seed a deployment that has not fired: an anchor run with no run
    // principal and a frozen snapshot that holds `requirements`.
    async function setup(requirements: GrantRequirement[]) {
      const router = createSidecarRouter({
        withExecutableWorkflowRun: async (_target, send) => send(),
        authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
        resolveSidecarBindings: async () => [],
        validateSidecarIdentity: async () => true,
      });
      const app = createApp({
        getSession: mockGetSession("usr_owner"),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        sidecarRouter: router,
        sessionService: mockSessionService(),
        eventCollectors: createEventCollectorRegistry({ db: h.db }),
        assetService: stubAssetService(),
        repoStore: stubRepoStore(),
        maxTarballBytes: 10_000_000,
      });
      const res = await app.request("/api/tenants", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "Acme", slug: "acme" }),
      });
      expect(res.status).toBe(201);
      const { id: tenantId } = type({ id: "string" }).assert(await res.json());

      for (const id of [CREATOR, DEPLOYER]) {
        await seedPrincipal(h.db, {
          id,
          tenantId,
          kind: "user",
          refId: `${id}-user`,
        });
      }
      await seedAsset(h.db, {
        id: "ast_first",
        tenantId,
        kind: "workflow",
        name: "ast_first",
        creatorPrincipalId: CREATOR,
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_first",
        tenantId,
        name: "wfd_first",
        assetId: "ast_first",
      });
      await seedWorkflowRun(h.db, {
        id: ANCHOR,
        tenantId,
        anchorRunId: ANCHOR,
        definitionId: "wfd_first",
        address: `${ANCHOR}@acme.example`,
      });
      await seedWorkflowRunLaunchSpec(h.db, {
        anchorRunId: ANCHOR,
        sourceAuthorityPrincipalId: DEPLOYER,
        grantSnapshot: { perStep: [], grantRequirements: requirements },
      });
      return { app, tenantId };
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

    // Hold the anchor lock, send the trigger and wait until it queues on the
    // lock, then apply `change` in the lock-holding transaction and commit
    // it. The trigger must see the change.
    async function triggerAcross(
      app: Awaited<ReturnType<typeof setup>>["app"],
      tenantId: string,
      change: (
        tx: Parameters<Parameters<typeof h.db.transaction>[0]>[0],
      ) => Promise<void>,
    ) {
      const locked = Promise.withResolvers<number>();
      const release = Promise.withResolvers<undefined>();
      const blocker = h.db.transaction(async (tx) => {
        await tx.execute(
          sql`select id from workflow_run where id = ${ANCHOR} for update`,
        );
        const [backend] = await tx.execute(sql`select pg_backend_pid() as pid`);
        const pid = backend?.["pid"];
        if (typeof pid !== "number") throw new Error("Missing backend PID");
        locked.resolve(pid);
        await release.promise;
        await change(tx);
      });
      void blocker.catch(locked.reject);
      try {
        const blockerPid = await locked.promise;
        const fire = app.request(
          `/api/tenants/${tenantId}/workflows/${ANCHOR}/mail`,
          {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ content: "hi" }),
          },
        );
        await waitForBlocked(blockerPid);
        release.resolve(undefined);
        await blocker;
        return await fire;
      } finally {
        release.resolve(undefined);
      }
    }

    // The grant rows held by the tenant's workflow run principals.
    async function runGrantCount(tenantId: string) {
      const rows = await h.db
        .select({ id: grant.id })
        .from(grant)
        .innerJoin(principal, eq(principal.id, grant.principalId))
        .where(
          and(eq(principal.tenantId, tenantId), eq(principal.kind, "workflow")),
        );
      return rows.length;
    }

    test("a creator grant revoked during the wait refuses the trigger and commits no row", async () => {
      const { app, tenantId } = await setup([{ ...VAULT, source: "creator" }]);
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId,
        principalId: CREATOR,
        ...VAULT,
      });

      const res = await triggerAcross(app, tenantId, async (tx) => {
        await tx.delete(grant).where(eq(grant.id, "grt_creator_vault"));
      });

      expect(res.status).toBe(403);
      expect(await res.json()).toMatchObject({
        error: { code: "insufficient_grants" },
      });
      expect(await runGrantCount(tenantId)).toBe(0);
    });

    test("a deployer deactivated during the wait refuses the trigger and commits no row", async () => {
      const { app, tenantId } = await setup([{ ...VAULT, source: "invoker" }]);
      await seedGrant(h.db, {
        id: "grt_deployer_vault",
        tenantId,
        principalId: DEPLOYER,
        ...VAULT,
      });

      const res = await triggerAcross(app, tenantId, async (tx) => {
        await tx
          .update(principal)
          .set({ status: "deactivated" })
          .where(eq(principal.id, DEPLOYER));
      });

      expect(res.status).toBe(409);
      expect(await res.json()).toMatchObject({
        error: { code: "deployer_unavailable" },
      });
      expect(await runGrantCount(tenantId)).toBe(0);
    });
  },
);
