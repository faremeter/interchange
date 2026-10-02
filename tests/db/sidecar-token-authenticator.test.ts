import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { sha256 } from "@intx/crypto";
import {
  sidecar,
  sidecarAllocation,
  workflowProbe,
  workflowRun,
} from "@intx/db/schema";
import {
  createSidecarCredentialResolver,
  createSidecarTokenAuthenticator,
} from "@intx/hub-sessions";
import { eq } from "drizzle-orm";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT_ID = "tnt-sidecar-auth";

// The mock-DB unit test proves the authenticator's control flow, but it
// never exercises the real `bytea` lookup: the token hash is written to
// and read back from Postgres through the customType encoder, and the
// query matches on that stored digest. A flipped byte on the write, a
// mangled encode/decode, or a `bytea` equality that Postgres does not
// evaluate the way drizzle builds it would all pass the mock and fail
// here. These cases drive the shipped `createSidecarTokenAuthenticator`
// against a real migrated schema to defend that write->read round-trip
// and the token-derived identity property.
describe.skipIf(!harnessDbEnvAvailable())(
  "sidecar token authenticator (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT_ID }]);
    });

    // Seed a sidecar identity the same way provisioning does: store the
    // SHA-256 digest of the token as the `bytea` hash. `url` is not read
    // by the authenticator; a placeholder satisfies its NOT NULL.
    async function seedSidecar(opts: {
      id: string;
      token: string;
      allocated?: boolean;
    }): Promise<{
      kind: "allocated";
      sidecarId: string;
      allocationId: string;
      tenantId: string;
      anchorRunId: string;
      workflowRunAddress: string;
      generation: number;
    }> {
      const anchorRunId = `run-${opts.id}`;
      const workflowRunAddress = `${opts.id}@sidecar-auth.example.test`;
      await h.db.insert(sidecar).values({
        id: opts.id,
        url: "ws://dev-sidecar",
        tokenHashSha256: await sha256(opts.token),
      });
      if (opts.allocated !== false) {
        await seedWorkflowRun(h.db, {
          id: anchorRunId,
          anchorRunId,
          tenantId: TENANT_ID,
          address: workflowRunAddress,
          status: "deployed",
        });
        await h.db.insert(sidecarAllocation).values({
          id: `allocation-${opts.id}`,
          anchorRunId,
          tenantId: TENANT_ID,
          provisionerId: "test",
          provisionerApiVersion: 1,
          provisionerBindingFingerprint: "test:v1",
          sidecarId: opts.id,
          status: "allocated",
          generation: 1,
          ensureAcceptedGeneration: 1,
        });
      }
      return {
        kind: "allocated",
        sidecarId: opts.id,
        allocationId: `allocation-${opts.id}`,
        tenantId: TENANT_ID,
        anchorRunId,
        workflowRunAddress,
        generation: 1,
      };
    }

    test("resolves a valid token to the seeded sidecar's identity", async () => {
      const token = "sidecar-secret";
      await seedSidecar({ id: "sc-1", token });
      const authenticate = createSidecarTokenAuthenticator({ db: h.db });

      const identity = await authenticate({ sidecarId: "sc-1", token });

      expect(identity).toEqual({ sidecarId: "sc-1" });
    });

    test("rejects a wrong token with null", async () => {
      await seedSidecar({ id: "sc-1", token: "sidecar-secret" });
      const authenticate = createSidecarTokenAuthenticator({ db: h.db });

      const identity = await authenticate({
        sidecarId: "sc-1",
        token: "wrong-secret",
      });

      expect(identity).toBeNull();
    });

    test("rejects an empty token with null", async () => {
      await seedSidecar({ id: "sc-1", token: "sidecar-secret" });
      const authenticate = createSidecarTokenAuthenticator({ db: h.db });

      const identity = await authenticate({ sidecarId: "sc-1", token: "" });

      // `null` here comes from the lookup missing, not an input guard: the
      // authenticator hashes `""` and finds no row carrying `sha256("")`.
      // There is no empty-token validation to short-circuit the query.
      expect(identity).toBeNull();
    });

    test("resolves an allocated token without a current allocation to no bindings", async () => {
      const token = "stale-allocated-secret";
      await seedSidecar({
        id: "sc-replaced",
        token,
        allocated: false,
      });
      const resolver = createSidecarCredentialResolver({ db: h.db });

      expect(await resolver.resolve(token)).toEqual({
        sidecarId: "sc-replaced",
      });
      expect(await resolver.resolveBindings("sc-replaced")).toEqual([]);
    });

    test("derives identity from the token, not a spoofed claimed sidecarId", async () => {
      const token = "sidecar-secret";
      await seedSidecar({ id: "sc-real", token });
      const authenticate = createSidecarTokenAuthenticator({ db: h.db });

      const identity = await authenticate({ sidecarId: "sc-spoofed", token });

      expect(identity).toEqual({ sidecarId: "sc-real" });
    });

    test("selects the matching row by hash among several sidecars", async () => {
      // With more than one sidecar present, each token must resolve to its
      // own row. A single-row mock cannot distinguish "matched by hash" from
      // "returned the only row"; a populated table proves the `bytea`
      // equality actually keys the lookup on the presented token's digest.
      const tokenA = "sidecar-secret-a";
      const tokenB = "sidecar-secret-b";
      await seedSidecar({ id: "sc-a", token: tokenA });
      await seedSidecar({ id: "sc-b", token: tokenB });
      const authenticate = createSidecarTokenAuthenticator({ db: h.db });

      expect(await authenticate({ sidecarId: "sc-a", token: tokenA })).toEqual({
        sidecarId: "sc-a",
      });
      expect(await authenticate({ sidecarId: "sc-b", token: tokenB })).toEqual({
        sidecarId: "sc-b",
      });
    });

    test("resolves a probe hosted beside an allocation", async () => {
      const token = "shared-secret";
      const allocated = await seedSidecar({ id: "sc-shared", token });
      await seedAsset(h.db, {
        id: "asset-shared",
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "shared-workflow",
      });
      await h.db.insert(workflowProbe).values({
        id: "probe-shared",
        tenantId: TENANT_ID,
        definitionAssetId: "asset-shared",
        source: { kind: "registry", registry: "npmjs" },
        entry: "./workflow.js",
        status: "probing",
        provisionerId: "test",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "test:v1",
        sidecarId: "sc-shared",
      });
      const resolver = createSidecarCredentialResolver({ db: h.db });

      expect(await resolver.resolve(token)).toEqual({ sidecarId: "sc-shared" });
      expect(await resolver.resolveBindings("sc-shared")).toEqual([
        allocated,
        {
          kind: "probe",
          sidecarId: "sc-shared",
          allocationId: "probe-shared",
          tenantId: TENANT_ID,
          generation: 0,
        },
      ]);
    });

    test("leaves out an allocation that is no longer active", async () => {
      const token = "released-secret";
      await seedSidecar({ id: "sc-released", token });
      await h.db
        .update(sidecarAllocation)
        .set({ status: "released" })
        .where(eq(sidecarAllocation.sidecarId, "sc-released"));
      const resolver = createSidecarCredentialResolver({ db: h.db });

      expect(await resolver.resolve(token)).toEqual({
        sidecarId: "sc-released",
      });
      expect(await resolver.resolveBindings("sc-released")).toEqual([]);
    });

    test("reclaims only a deployment whose first deploy completed", async () => {
      const binding = await seedSidecar({ id: "sc-reclaim", token: "t" });
      const resolver = createSidecarCredentialResolver({ db: h.db });

      expect(await resolver.isCurrent(binding, "routing")).toBe(true);
      expect(await resolver.isCurrent(binding, "reclaim")).toBe(false);

      await h.db
        .update(workflowRun)
        .set({ publicKey: "public-key" })
        .where(eq(workflowRun.id, binding.anchorRunId));
      expect(await resolver.isCurrent(binding, "reclaim")).toBe(true);

      await h.db
        .update(sidecarAllocation)
        .set({ initializationLeaseId: "uncertain-attempt" })
        .where(eq(sidecarAllocation.sidecarId, "sc-reclaim"));
      expect(await resolver.isCurrent(binding, "reclaim")).toBe(false);
    });

    test("reclaims no copy of a run the Hub already ended", async () => {
      const binding = await seedSidecar({ id: "sc-ended", token: "t" });
      const resolver = createSidecarCredentialResolver({ db: h.db });
      await h.db
        .update(workflowRun)
        .set({ publicKey: "public-key", cancellationRequestedAt: new Date() })
        .where(eq(workflowRun.id, binding.anchorRunId));
      // A run still cancelling keeps its copy, which records the cancel.
      expect(await resolver.isCurrent(binding, "reclaim")).toBe(true);

      await h.db
        .update(workflowRun)
        .set({ status: "failed", endedAt: new Date() })
        .where(eq(workflowRun.id, binding.anchorRunId));
      expect(await resolver.isCurrent(binding, "reclaim")).toBe(false);
      // The Hub can still reach it to stop or undeploy it.
      expect(await resolver.isCurrent(binding, "routing")).toBe(true);
    });
  },
);
