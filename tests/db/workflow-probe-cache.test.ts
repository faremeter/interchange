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
  createWorkflowDefinitionArtifactStore,
  loadDeploymentGrantSnapshot,
  createWorkflowRunLaunchSpecStore,
} from "@intx/db";
import {
  workflowDefinitionArtifact,
  workflowDefinitionRevision,
  workflowDefinitionVersion,
} from "@intx/db/schema";
import { installAndApproveWorkflowDefinition } from "@intx/hub-sessions";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { WorkflowProbeResultFrame } from "@intx/types/sidecar";
import { createApprovalSet } from "@intx/workflow-deploy";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT_ID = "tnt-probe-cache";
const ASSET_ID = "ast-probe-cache";
const SOURCE = {
  kind: "asset",
  assetId: ASSET_ID,
  package: { format: "source", commitSha: "1".repeat(40) },
} as const;
const PROJECTION = { id: "cached", triggers: [], stepOrder: [], steps: {} };
type ProbeResult = Omit<WorkflowProbeResultFrame, "type" | "requestId">;
const LEASE = { leaseMs: 60_000, renewMs: 15_000, waitMs: 10 };

describe.skipIf(!harnessDbEnvAvailable())(
  "workflow probe cache (real DB)",
  () => {
    let h: TestDb;
    let result: ProbeResult;
    beforeAll(async () => {
      h = await createTestDb();
      result = {
        projection: PROJECTION,
        wireHash: await computeWireDefinitionHash(PROJECTION),
        grants: [],
        grantWalkSnapshot: { perStep: [], grantRequirements: [] },
      };
    });
    afterAll(async () => {
      await h.close();
    });
    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedAsset(h.db, {
        id: ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "cached",
      });
    });

    function installArgs(
      sendProbe: (probe: { closure: unknown }) => Promise<ProbeResult>,
    ) {
      return {
        db: h.db,
        assetId: ASSET_ID,
        source: SOURCE,
        entry: "./workflow.ts",
        assetCommitSha: SOURCE.package.commitSha,
        approvals: { kind: "approve-probed" } as const,
        router: { sendProbe },
        reads: {
          readBlob: async () =>
            new TextEncoder().encode(
              JSON.stringify({
                name: "cached",
                version: "1.0.0",
                dependencies: {},
              }),
            ),
          listDir: async () => [],
          treeOid: async () => "2".repeat(40),
        },
        registryName: "npm",
        registryConfig: { url: "https://registry.example.test" },
        resolveAttachment: async () => ({
          pack: new Uint8Array(),
          ref: "refs/heads/main",
          commitSha: SOURCE.package.commitSha,
        }),
        cache: {
          store: createWorkflowDefinitionArtifactStore(h.db),
          tenantId: TENANT_ID,
          registryKey: "npm",
          lease: LEASE,
        },
      };
    }

    test("reuses a completed probe through a new store and skips resolution and asset delivery", async () => {
      let probes = 0;
      const sendProbe = async () => {
        probes++;
        return result;
      };
      const first = await installAndApproveWorkflowDefinition(
        installArgs(sendProbe),
      );
      const next = installArgs(sendProbe);
      next.reads.readBlob = async () => {
        throw new Error(
          "A frozen revision must not resolve dependencies again",
        );
      };
      next.resolveAttachment = async () => {
        throw new Error("A cache hit must not build probe packs");
      };
      const second = await installAndApproveWorkflowDefinition(next);
      expect(probes).toBe(1);
      expect(second).toEqual(first);
      expect(first.artifactId).toBeDefined();
    });

    test("concurrent requests share one probe", async () => {
      const started = Promise.withResolvers<undefined>();
      const finish = Promise.withResolvers<undefined>();
      const waiting = Promise.withResolvers<undefined>();
      let probes = 0;
      const args = installArgs(async () => {
        probes++;
        started.resolve(undefined);
        await finish.promise;
        return result;
      });
      const first = installAndApproveWorkflowDefinition(args);
      await started.promise;
      const other = installArgs(args.router.sendProbe);
      const claim = other.cache.store.claim;
      other.cache.store.claim = async (input) => {
        const acquired = await claim(input);
        if (acquired === "held") waiting.resolve(undefined);
        return acquired;
      };
      const second = installAndApproveWorkflowDefinition(other);
      try {
        await waiting.promise;
        finish.resolve(undefined);
        const [a, b] = await Promise.all([first, second]);
        expect(a.artifactId).toBe(b.artifactId);
        expect(probes).toBe(1);
      } finally {
        finish.resolve(undefined);
        await Promise.allSettled([first, second]);
      }
    });

    test("waiters retry once and probe in parallel after the owner's probe fails", async () => {
      const ownerStarted = Promise.withResolvers<undefined>();
      const ownerFail = Promise.withResolvers<undefined>();
      const retriesStarted = Promise.withResolvers<undefined>();
      let probes = 0;
      const owner = installAndApproveWorkflowDefinition(
        installArgs(async () => {
          probes++;
          ownerStarted.resolve(undefined);
          await ownerFail.promise;
          throw new Error("probe failed");
        }),
      );
      await ownerStarted.promise;
      // Both retries block until each has started, so neither can read the
      // other's published result instead of probing.
      const waiters = [0, 1].map(() => {
        const waiting = Promise.withResolvers<undefined>();
        const args = installArgs(async () => {
          probes++;
          if (probes === 3) retriesStarted.resolve(undefined);
          await retriesStarted.promise;
          return result;
        });
        const claim = args.cache.store.claim;
        args.cache.store.claim = async (input) => {
          const acquired = await claim(input);
          if (acquired === "held") waiting.resolve(undefined);
          return acquired;
        };
        return {
          waiting: waiting.promise,
          install: installAndApproveWorkflowDefinition(args),
        };
      });
      try {
        await Promise.all(waiters.map((waiter) => waiter.waiting));
        ownerFail.resolve(undefined);
        await expect(owner).rejects.toThrow("probe failed");
        const retried = await Promise.all(
          waiters.map((waiter) => waiter.install),
        );
        expect(probes).toBe(3);
        expect(retried.every((r) => r.approval.ok)).toBe(true);
        // One waiter claimed the retry and published it; the other probed
        // without queueing behind it.
        expect(retried.filter((r) => r.artifactId !== undefined)).toHaveLength(
          1,
        );
        const [artifact] = await h.db.select().from(workflowDefinitionArtifact);
        expect(artifact?.result).toEqual(result);
      } finally {
        ownerFail.resolve(undefined);
        retriesStarted.resolve(undefined);
        await Promise.allSettled([
          owner,
          ...waiters.map((waiter) => waiter.install),
        ]);
      }
    });

    test("a transient renewal failure does not give up the lease", async () => {
      const renewed = Promise.withResolvers<undefined>();
      const args = installArgs(async () => {
        await renewed.promise;
        return result;
      });
      args.cache.lease = { ...LEASE, renewMs: 1 };
      const renew = args.cache.store.renew;
      let renewals = 0;
      args.cache.store.renew = async (...renewArgs) => {
        renewals++;
        if (renewals === 1) throw new Error("database unavailable");
        const owned = await renew(...renewArgs);
        if (renewals === 3) renewed.resolve(undefined);
        return owned;
      };
      const deployed = await installAndApproveWorkflowDefinition(args);
      expect(renewals).toBeGreaterThanOrEqual(3);
      expect(deployed.artifactId).toBeDefined();
    });

    test("a crashed owner's expired claim is taken over and published", async () => {
      const crashed = installArgs(async () => {
        throw new Error("Hub process died");
      });
      // A crashed process never abandons its claim.
      crashed.cache.store.abandon = async () => undefined;
      await expect(
        installAndApproveWorkflowDefinition(crashed),
      ).rejects.toThrow("Hub process died");
      await h.db
        .update(workflowDefinitionArtifact)
        .set({ leaseExpiresAt: sql`clock_timestamp() - interval '1 second'` });
      let probes = 0;
      const replacement = await installAndApproveWorkflowDefinition(
        installArgs(async () => {
          probes++;
          return result;
        }),
      );
      expect(probes).toBe(1);
      expect(replacement.artifactId).toBeDefined();
      const [artifact] = await h.db.select().from(workflowDefinitionArtifact);
      expect(artifact?.result).toEqual(result);
    });

    test("a validated probe still deploys when its artifact cannot be published", async () => {
      const deployed = await installAndApproveWorkflowDefinition(
        installArgs(async () => {
          await h.db
            .update(workflowDefinitionArtifact)
            .set({ leaseOwner: "successor" });
          return result;
        }),
      );
      expect(deployed.approval.ok).toBe(true);
      expect(deployed.artifactId).toBeUndefined();
      const [artifact] = await h.db.select().from(workflowDefinitionArtifact);
      expect(artifact?.leaseOwner).toBe("successor");
      expect(artifact?.result).toBeNull();
    });

    test.each(["source", "entry", "registry", "tenant", "asset"])(
      "probes again when %s changes",
      async (changed) => {
        let probes = 0;
        const sendProbe = async () => {
          probes++;
          return result;
        };
        const first = await installAndApproveWorkflowDefinition(
          installArgs(sendProbe),
        );
        if (changed === "tenant") {
          await seedTenants(h.db, [{ id: "tnt-probe-cache-other" }]);
        }
        if (changed === "asset") {
          await seedAsset(h.db, {
            id: "ast-probe-cache-other",
            tenantId: TENANT_ID,
            kind: "workflow",
            name: "cached-other",
          });
        }
        const args = installArgs(sendProbe);
        const second = await installAndApproveWorkflowDefinition({
          ...args,
          ...(changed === "asset" ? { assetId: "ast-probe-cache-other" } : {}),
          ...(changed === "source"
            ? {
                source: {
                  ...SOURCE,
                  package: { ...SOURCE.package, commitSha: "3".repeat(40) },
                },
                assetCommitSha: "3".repeat(40),
              }
            : {}),
          ...(changed === "entry" ? { entry: "./other.ts" } : {}),
          cache: {
            ...args.cache,
            ...(changed === "registry"
              ? { registryKey: "other-registry" }
              : {}),
            ...(changed === "tenant"
              ? { tenantId: "tnt-probe-cache-other" }
              : {}),
          },
        });
        expect(probes).toBe(2);
        expect(second.artifactId).not.toBe(first.artifactId);
      },
    );

    test("an older cache version probes again without changing dependencies", async () => {
      let probes = 0;
      const sendProbe = async () => {
        probes++;
        return result;
      };
      const first = await installAndApproveWorkflowDefinition(
        installArgs(sendProbe),
      );
      const [artifact] = await h.db.select().from(workflowDefinitionArtifact);
      if (artifact === undefined) throw new Error("No artifact row");
      const previousId = await computeWireDefinitionHash({
        schemaVersion: 1,
        revisionId: artifact.revisionId,
      });
      await h.db
        .update(workflowDefinitionArtifact)
        .set({ id: previousId })
        .where(eq(workflowDefinitionArtifact.id, artifact.id));
      const args = installArgs(sendProbe);
      args.reads.readBlob = async () => {
        throw new Error("Cache upgrades must not change dependencies");
      };
      const second = await installAndApproveWorkflowDefinition(args);
      expect(probes).toBe(2);
      expect(second.closure).toEqual(first.closure);
      expect(second.artifactId).toBe(first.artifactId);
      expect(second.artifactId).not.toBe(previousId);
    });

    test.each(["transport", "validation"])(
      "does not cache a %s failure and resolves dependencies again",
      async (failure) => {
        const args = installArgs(async () => {
          if (failure === "transport") throw new Error("probe failed");
          return { ...result, wireHash: "invalid" };
        });
        const outcome = await installAndApproveWorkflowDefinition(args).then(
          (installed) => installed.approval.ok,
          () => "threw",
        );
        expect(outcome).toBe(failure === "transport" ? "threw" : false);
        expect(
          await h.db.select().from(workflowDefinitionArtifact),
        ).toHaveLength(0);
        expect(
          await h.db.select().from(workflowDefinitionRevision),
        ).toHaveLength(0);
        const retryArgs = installArgs(async () => result);
        let resolutions = 0;
        const readBlob = retryArgs.reads.readBlob;
        retryArgs.reads.readBlob = async () => {
          resolutions++;
          return readBlob();
        };
        const retry = await installAndApproveWorkflowDefinition(retryArgs);
        expect(retry.approval.ok).toBe(true);
        expect(resolutions).toBeGreaterThan(0);
      },
    );

    test("a failed probe keeps a revision that an older cache version validated", async () => {
      const first = await installAndApproveWorkflowDefinition(
        installArgs(async () => result),
      );
      const [artifact] = await h.db.select().from(workflowDefinitionArtifact);
      if (artifact === undefined) throw new Error("No artifact row");
      await h.db
        .update(workflowDefinitionArtifact)
        .set({
          id: await computeWireDefinitionHash({
            schemaVersion: 1,
            revisionId: artifact.revisionId,
          }),
        })
        .where(eq(workflowDefinitionArtifact.id, artifact.id));
      await expect(
        installAndApproveWorkflowDefinition(
          installArgs(async () => {
            throw new Error("probe failed");
          }),
        ),
      ).rejects.toThrow("probe failed");
      const args = installArgs(async () => result);
      args.reads.readBlob = async () => {
        throw new Error("A validated revision must keep its dependencies");
      };
      const second = await installAndApproveWorkflowDefinition(args);
      expect(second.closure).toEqual(first.closure);
    });

    test("a waiter resolves dependencies again after the owner's failed probe deletes the revision", async () => {
      const ownerStarted = Promise.withResolvers<undefined>();
      const ownerFail = Promise.withResolvers<undefined>();
      const owner = installAndApproveWorkflowDefinition(
        installArgs(async () => {
          ownerStarted.resolve(undefined);
          await ownerFail.promise;
          throw new Error("probe failed");
        }),
      );
      const ownerSettled = owner.catch(() => undefined);
      await ownerStarted.promise;
      const waiting = Promise.withResolvers<undefined>();
      const args = installArgs(async () => result);
      let resolutions = 0;
      const readBlob = args.reads.readBlob;
      args.reads.readBlob = async () => {
        resolutions++;
        return readBlob();
      };
      const claims: string[] = [];
      const claim = args.cache.store.claim;
      args.cache.store.claim = async (input) => {
        // Claim again only once the owner has deleted its claim and revision.
        if (claims.length > 0) await ownerSettled;
        const outcome = await claim(input);
        claims.push(outcome);
        if (outcome === "held") waiting.resolve(undefined);
        return outcome;
      };
      const waiter = installAndApproveWorkflowDefinition(args);
      try {
        await waiting.promise;
        expect(resolutions).toBe(0);
        ownerFail.resolve(undefined);
        await expect(owner).rejects.toThrow("probe failed");
        const retried = await waiter;
        expect(claims).toEqual(["held", "revision_missing", "claimed"]);
        expect(resolutions).toBeGreaterThan(0);
        expect(retried.artifactId).toBeDefined();
      } finally {
        ownerFail.resolve(undefined);
        await Promise.allSettled([owner, waiter]);
      }
    });

    test("a request that loses the resolution race probes the stored closure", async () => {
      const first = installArgs(async () => {
        throw new Error("probe failed");
      });
      first.reads.treeOid = async () => "4".repeat(40);
      // Keep the revision as if another request had just stored it.
      first.cache.store.deleteRevision = async () => false;
      await expect(installAndApproveWorkflowDefinition(first)).rejects.toThrow(
        "probe failed",
      );
      const [stored] = await h.db.select().from(workflowDefinitionRevision);
      if (stored === undefined) throw new Error("No revision row");

      const probed: unknown[] = [];
      const late = installArgs(async (probe) => {
        probed.push(probe.closure);
        return result;
      });
      late.cache.store.hasRevision = async () => false;
      const resolved: unknown[] = [];
      const createRevision = late.cache.store.createRevision;
      late.cache.store.createRevision = async (row) => {
        resolved.push(row.closure);
        await createRevision(row);
      };
      const deployed = await installAndApproveWorkflowDefinition(late);
      expect(resolved).toHaveLength(1);
      expect(resolved[0]).not.toEqual(stored.closure);
      expect(probed).toEqual([stored.closure]);
      expect(stored.closure).toEqual(deployed.closure);
    });

    test("a cache hit still checks the current approval policy", async () => {
      const restricted = { ...result, grants: ["tool:fetch"] };
      await installAndApproveWorkflowDefinition(
        installArgs(async () => restricted),
      );
      const args = installArgs(async () => {
        throw new Error("Must use cached probe");
      });
      const refused = await installAndApproveWorkflowDefinition({
        ...args,
        approvals: createApprovalSet([]),
      });
      expect(refused.approval).toMatchObject({
        ok: false,
        reason: "grants_not_approved",
      });
    });

    test("expired owners cannot publish or remove a successor's result", async () => {
      const completed = await installAndApproveWorkflowDefinition(
        installArgs(async () => result),
      );
      if (completed.artifactId === undefined) throw new Error("No artifact");
      const [artifact] = await h.db.select().from(workflowDefinitionArtifact);
      if (artifact === undefined) throw new Error("No artifact row");
      const store = createWorkflowDefinitionArtifactStore(h.db);
      const claim = {
        id: "claim-under-test",
        revisionId: artifact.revisionId,
        leaseMs: 60_000,
      };
      expect(await store.claim({ ...claim, owner: "old" })).toBe("claimed");
      await h.db
        .update(workflowDefinitionArtifact)
        .set({ leaseExpiresAt: sql`clock_timestamp() - interval '1 second'` })
        .where(eq(workflowDefinitionArtifact.id, claim.id));
      expect(await store.claim({ ...claim, owner: "new" })).toBe("claimed");
      expect(await store.renew(claim.id, "old", 60_000)).toBe(false);
      expect(await store.complete(claim.id, "old", result)).toBe(false);
      await store.abandon(claim.id, "old");
      expect(await store.complete(claim.id, "new", result)).toBe(true);
      expect((await store.get(claim.id))?.result).toEqual(result);
      expect(await store.claim({ ...claim, owner: "third" })).toBe("held");
    });

    test("deployment grants remain bound to their artifact after a newer probe changes the shared definition", async () => {
      const first = await installAndApproveWorkflowDefinition(
        installArgs(async () => result),
      );
      if (!first.approval.ok || first.artifactId === undefined)
        throw new Error("No approved artifact");
      await seedPrincipal(h.db, {
        id: "prn-cache",
        tenantId: TENANT_ID,
        kind: "user",
      });
      await seedWorkflowRun(h.db, {
        id: "run-cache",
        tenantId: TENANT_ID,
        definitionId: first.approval.definitionId,
        anchorRunId: "run-cache",
      });
      await createWorkflowRunLaunchSpecStore(h.db).create({
        anchorRunId: "run-cache",
        sessionId: "ses-cache",
        deploymentDomain: "cache.test",
        sourceAuthorityPrincipalId: "prn-cache",
        definitionArtifactId: first.artifactId,
        grantSnapshot: first.grantSnapshot,
        frozenApprovalBundle: {
          source: SOURCE,
          entry: "./workflow.ts",
          projection: first.projection,
          closure: first.closure,
          approvedWireHash: result.wireHash,
          approvedGrants: [],
        },
        sourceOfferingIds: ["offering"],
        defaultSourceOfferingId: "offering",
        deployContent: {},
      });
      const changed = {
        perStep: [
          { stepId: "different", grants: ["tool:other"], grantEffects: {} },
        ],
        grantRequirements: [],
      };
      await h.db
        .update(workflowDefinitionVersion)
        .set({ grantSnapshot: changed })
        .where(
          eq(
            workflowDefinitionVersion.definitionId,
            first.approval.definitionId,
          ),
        );
      expect(await loadDeploymentGrantSnapshot(h.db, "run-cache")).toEqual(
        result.grantWalkSnapshot,
      );
    });
  },
);
