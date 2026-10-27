import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import { createSidecarAllocationStore } from "@intx/db";
import { sidecarAllocation } from "@intx/db/schema";
import {
  createAgentRepoStore,
  createSessionService,
  createSidecarRouter,
  DEFAULT_ASSET_REF,
  type InstallAndApproveResult,
} from "@intx/hub-sessions";
import { runGC } from "@intx/storage-isogit/node";
import type { HarnessConfig } from "@intx/types/runtime";
import type { AgentDeployFrame } from "@intx/types/sidecar";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";
import { deriveRunAddress } from "@intx/workflow-deploy";
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

import { buildSyntheticNpmPackageTarball } from "../hub-agent/lib/synthetic-npm-package-tarball";

const TENANT_ID = "tnt-pinned-asset-deploy";
const ASSET_ID = "ast-pinned-asset-deploy";
const ANCHOR_RUN_ID = "run_0123456789abcdef0123456789abcdef";
const DEPLOYMENT_DOMAIN = "workflow.interchange";
const DEPLOY_ADDRESS = deriveRunAddress({
  runId: ANCHOR_RUN_ID,
  domain: DEPLOYMENT_DOMAIN,
});
const ALLOC_ID = "alloc-pinned-asset-deploy";
const ALLOC_GENERATION = 1;
const PROJECTION = { id: "pinned", triggers: [], stepOrder: [], steps: {} };
const CONFIG: HarnessConfig = {
  sessionId: "ses-pinned-asset-deploy",
  agentId: ANCHOR_RUN_ID,
  tenantId: TENANT_ID,
  principalId: "prin-pinned-asset-deploy",
  agentAddress: DEPLOY_ADDRESS,
  systemPrompt: "",
  tools: [],
  grants: [],
  sources: [],
  defaultSource: "",
};
const HUB = { kind: "hub" } as const;

function sourceTree(workflow: string, message: string) {
  return {
    files: {
      "package.json": JSON.stringify({
        name: "pinned",
        version: "1.0.0",
        interchange: { workflow: "./workflow.ts" },
      }),
      "workflow.ts": workflow,
    },
    message,
  };
}

async function registryTree(moduleSource: string, message: string) {
  const tarball = await buildSyntheticNpmPackageTarball({
    packageName: "@wf/pinned",
    version: "1.0.0",
    moduleFilename: "workflow.mjs",
    moduleSource,
    manifest: { interchange: { workflow: "./workflow.mjs" } },
  });
  return {
    files: { [`tarballs/${tarball.tarballFilename}`]: tarball.bytes },
    message,
  };
}

// Records the asset commit each probe received and captures the deploy frame
// instead of sending it.
function createCapturingRouter() {
  const router = createSidecarRouter({
    withExecutableWorkflowRun: async (_target, send) => send(),
    authenticateSidecar: async () => null,
    validateSidecarIdentity: async () => false,
    resolveSidecarBindings: async () => [],
  });
  const probedAssets: string[] = [];
  router.sendProbeToAllocation = async (_target, args) => {
    probedAssets.push(...(args.assets ?? []).map((a) => a.commitSha));
    return {
      projection: PROJECTION,
      wireHash: await computeWireDefinitionHash(PROJECTION),
      grants: [],
      grantWalkSnapshot: { perStep: [], grantRequirements: [] },
    };
  };
  const captured: { workflow?: AgentDeployFrame["workflow"] } = {};
  router.sendAgentDeployToAllocation = async (
    _target,
    _address,
    _config,
    workflow,
  ) => {
    captured.workflow = workflow;
    throw new Error("Deploy frame captured");
  };
  return { router, probedAssets, captured };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "pinned asset deploy (real DB)",
  () => {
    let h: TestDb;
    let dataDir: string;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      dataDir = await fs.promises.mkdtemp(
        path.join(os.tmpdir(), "pinned-asset-deploy-"),
      );
    });

    afterEach(async () => {
      await fs.promises.rm(dataDir, { recursive: true, force: true });
    });

    async function createHarness(registryURL = "https://registry.test") {
      const agentRepoStore = createAgentRepoStore({
        dataDir,
        signingKey: await generateKeyPair(),
      });
      const capture = createCapturingRouter();
      const service = createSessionService({
        loadRunCredentials: async () => null,
        db: h.db,
        sidecarRouter: capture.router,
        sidecarAllocationRouter: capture.router,
        agentRepoStore,
        toolPackageRegistries: {
          httpRegistries: new Map([["npmjs", { url: registryURL }]]),
          defaultRegistry: "npmjs",
        },
      });
      return { repoStore: agentRepoStore.repoStore, service, ...capture };
    }

    // Deploy a prepared approval onto a claimed allocation. The router throws
    // once it has captured the frame.
    async function deployPrepared(
      service: ReturnType<typeof createSessionService>,
      source: WorkflowDefinitionAssetSource,
      approved: InstallAndApproveResult,
    ) {
      if (!approved.approval.ok) throw new Error("Probe was not approved");
      await seedWorkflowRun(h.db, {
        id: ANCHOR_RUN_ID,
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        definitionId: approved.approval.definitionId,
        address: DEPLOY_ADDRESS,
        status: "deployed",
      });
      await h.db.insert(sidecarAllocation).values({
        id: ALLOC_ID,
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "provisioner-pinned",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "fp-pinned",
        adapterManifestDigest: "1".repeat(64),
        maxDisconnectedMs: 900_000,
        status: "allocated",
        generation: ALLOC_GENERATION,
        ensureAcceptedGeneration: ALLOC_GENERATION,
      });
      const allocations = createSidecarAllocationStore(h.db);
      await allocations.wakeReconciliation(ALLOC_ID, ALLOC_GENERATION);
      const leaseId = "pinned-attempt";
      await allocations.claimNextReconcilable({
        leaseId,
        leaseDurationMs: 60_000,
      });
      await expect(
        service.deployPreparedCodeSourcedWorkflow({
          tenantId: TENANT_ID,
          anchorRunId: ANCHOR_RUN_ID,
          deploymentDomain: DEPLOYMENT_DOMAIN,
          agentAddress: DEPLOY_ADDRESS,
          source,
          approved,
          config: CONFIG,
          allocationTarget: {
            allocationId: ALLOC_ID,
            generation: ALLOC_GENERATION,
          },
          reconciliation: {
            leaseId,
            signal: new AbortController().signal,
          },
        }),
      ).rejects.toThrow("Deploy frame captured");
    }

    const probeTarget = async () => ({
      allocationId: "alloc-probe",
      generation: 1,
    });

    test("a branch replaced and collected between probe and deploy still ships the probed commit", async () => {
      await seedAsset(h.db, {
        id: ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "pinned",
      });
      const { repoStore, service, probedAssets, captured } =
        await createHarness();
      const repoId = { kind: "workflow", id: ASSET_ID } as const;
      const probed = await repoStore.writeTree(
        HUB,
        repoId,
        DEFAULT_ASSET_REF,
        sourceTree("probed", "probed"),
      );
      const source = {
        kind: "asset",
        assetId: ASSET_ID,
        package: { format: "source", commitSha: probed.commitSha },
      } as const;
      const approved = await service.installAndApproveWorkflowSource({
        tenantId: TENANT_ID,
        source,
        entry: "./workflow.ts",
        definitionAssetId: ASSET_ID,
        getProbeTarget: probeTarget,
      });
      expect(probedAssets).toEqual([probed.commitSha]);

      const replacement = await repoStore.writeTree(
        HUB,
        repoId,
        DEFAULT_ASSET_REF,
        sourceTree("replacement", "replacement"),
      );
      await runGC(repoStore.getRepoDir(repoId), { retention: "tip-only" });
      await deployPrepared(service, source, approved);

      expect(await repoStore.resolveRef(HUB, repoId, DEFAULT_ASSET_REF)).toBe(
        replacement.commitSha,
      );
      expect(
        captured.workflow?.assets?.map((asset) => asset.commitSha),
      ).toEqual([probed.commitSha]);
    });

    test("a tarball registry branch replaced and collected between probe and deploy still ships the probed commit", async () => {
      await seedAsset(h.db, {
        id: ASSET_ID,
        tenantId: TENANT_ID,
        kind: "package-registry",
        name: "pinned",
      });
      const { repoStore, service, probedAssets, captured } =
        await createHarness();
      const repoId = { kind: "package-registry", id: ASSET_ID } as const;
      await repoStore.initRepo(repoId);
      const probed = await repoStore.writeTree(
        HUB,
        repoId,
        DEFAULT_ASSET_REF,
        await registryTree('export const v = "probed";', "probed"),
      );
      const source = {
        kind: "asset",
        assetId: ASSET_ID,
        package: { format: "tarball" },
      } as const;
      const approved = await service.installAndApproveWorkflowSource({
        tenantId: TENANT_ID,
        source,
        entry: "./workflow.mjs",
        pin: "@wf/pinned@^1.0.0",
        definitionAssetId: ASSET_ID,
        getProbeTarget: probeTarget,
      });
      expect(probedAssets).toEqual([probed.commitSha]);

      const replacement = await repoStore.writeTree(
        HUB,
        repoId,
        DEFAULT_ASSET_REF,
        await registryTree('export const v = "replacement";', "replacement"),
      );
      await runGC(repoStore.getRepoDir(repoId), { retention: "tip-only" });
      await deployPrepared(service, source, approved);

      expect(await repoStore.resolveRef(HUB, repoId, DEFAULT_ASSET_REF)).toBe(
        replacement.commitSha,
      );
      expect(
        captured.workflow?.assets?.map((asset) => asset.commitSha),
      ).toEqual([probed.commitSha]);
    });

    // Launch specs written before the approved commit was recorded carry no
    // assetCommitSha.
    test("an older source launch spec ships the commit its source names", async () => {
      await seedAsset(h.db, {
        id: ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "pinned",
      });
      const { repoStore, service, captured } = await createHarness();
      const repoId = { kind: "workflow", id: ASSET_ID } as const;
      const probed = await repoStore.writeTree(
        HUB,
        repoId,
        DEFAULT_ASSET_REF,
        sourceTree("probed", "probed"),
      );
      const source = {
        kind: "asset",
        assetId: ASSET_ID,
        package: { format: "source", commitSha: probed.commitSha },
      } as const;
      const { assetCommitSha: _assetCommitSha, ...legacy } =
        await service.installAndApproveWorkflowSource({
          tenantId: TENANT_ID,
          source,
          entry: "./workflow.ts",
          definitionAssetId: ASSET_ID,
          getProbeTarget: probeTarget,
        });
      await repoStore.writeTree(
        HUB,
        repoId,
        DEFAULT_ASSET_REF,
        sourceTree("replacement", "replacement"),
      );
      await deployPrepared(service, source, legacy);

      expect(
        captured.workflow?.assets?.map((asset) => asset.commitSha),
      ).toEqual([probed.commitSha]);
    });

    test("an older tarball launch spec ships the branch tip", async () => {
      await seedAsset(h.db, {
        id: ASSET_ID,
        tenantId: TENANT_ID,
        kind: "package-registry",
        name: "pinned",
      });
      const { repoStore, service, captured } = await createHarness();
      const repoId = { kind: "package-registry", id: ASSET_ID } as const;
      await repoStore.initRepo(repoId);
      await repoStore.writeTree(
        HUB,
        repoId,
        DEFAULT_ASSET_REF,
        await registryTree('export const v = "probed";', "probed"),
      );
      const source = {
        kind: "asset",
        assetId: ASSET_ID,
        package: { format: "tarball" },
      } as const;
      const { assetCommitSha: _assetCommitSha, ...legacy } =
        await service.installAndApproveWorkflowSource({
          tenantId: TENANT_ID,
          source,
          entry: "./workflow.mjs",
          pin: "@wf/pinned@^1.0.0",
          definitionAssetId: ASSET_ID,
          getProbeTarget: probeTarget,
        });
      const tip = await repoStore.writeTree(
        HUB,
        repoId,
        DEFAULT_ASSET_REF,
        await registryTree('export const v = "probed";', "unrelated push"),
      );
      await deployPrepared(service, source, legacy);

      expect(
        captured.workflow?.assets?.map((asset) => asset.commitSha),
      ).toEqual([tip.commitSha]);
    });

    // A source install resolves its dependencies against the default registry,
    // so the cache key must change with that registry's URL.
    test("a cached source install probes again when the registry URL changes", async () => {
      await seedAsset(h.db, {
        id: ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "pinned",
      });
      const first = await createHarness();
      const probed = await first.repoStore.writeTree(
        HUB,
        { kind: "workflow", id: ASSET_ID },
        DEFAULT_ASSET_REF,
        sourceTree("probed", "probed"),
      );
      const install = (service: ReturnType<typeof createSessionService>) =>
        service.installAndApproveWorkflowSource({
          tenantId: TENANT_ID,
          source: {
            kind: "asset",
            assetId: ASSET_ID,
            package: { format: "source", commitSha: probed.commitSha },
          },
          entry: "./workflow.ts",
          definitionAssetId: ASSET_ID,
          getProbeTarget: probeTarget,
        });

      const reused = await install(first.service);
      expect((await install(first.service)).artifactId).toBe(reused.artifactId);
      expect(first.probedAssets).toHaveLength(1);

      const moved = await createHarness("https://moved-registry.test");
      const reprobed = await install(moved.service);
      expect(moved.probedAssets).toHaveLength(1);
      expect(reprobed.artifactId).toBeDefined();
      expect(reprobed.artifactId).not.toBe(reused.artifactId);
    });
  },
);
