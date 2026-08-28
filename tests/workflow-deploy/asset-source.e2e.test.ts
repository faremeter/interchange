// Asset-sourced end-to-end: install a code-sourced workflow package published
// as a tarball inside a hub `package-registry` ASSET, probe it, approve+freeze
// it, deploy it BY SOURCE-REF, and assert the deployed workflow runs to
// completion.
//
// This is the asset analog of the registry walking skeleton: it drives the same
// two production entrypoints (installAndApproveWorkflowDefinition,
// deployCodeSourcedWorkflow) but sources the definition's bytes from a hub asset
// instead of an npm registry, so the hub delivers the asset inline on the probe
// and deploy frames and the sidecar checks it out into its durable
// per-deployment source store rather than fetching tarballs over HTTP. The
// durable store's restore-from-disk path (re-materializing the closure on a
// sidecar restart from the pin alone) is unit-covered by
// `apps/sidecar/src/workflow-host-wiring-source-assets.test.ts`.

import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { dirname } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { and, eq } from "drizzle-orm";
import * as tar from "tar";

import {
  DEFAULT_ASSET_REF,
  deployCodeSourcedWorkflow,
  installAndApproveWorkflowDefinition,
  type RepoId,
} from "@intx/hub-sessions";
import {
  workflowDefinitionVersion as workflowDefinitionVersionTable,
  workflowRun as workflowRunTable,
  tenant as tenantTable,
} from "@intx/db/schema";
import type { HarnessConfig } from "@intx/types/runtime";
import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";
import { generateId } from "@intx/hub-common";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";

import {
  SESSION_ID,
  fireMailTrigger,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = generateId("workflowRun");
const STEP_ID = "run";
const WORKFLOW_RUN_REF = "refs/heads/main";

const PACKAGE_NAME = "@wf/asset-skeleton";
const PACKAGE_VERSION = "1.0.0";
const PACKAGE_BASENAME = "asset-skeleton";
const WORKFLOW_ENTRY = "./workflow.mjs";

const TENANT_ID = "tnt_asset_skeleton";
const CALLER_PRINCIPAL_ID = "prn_asset_skeleton_creator";
// The `workflow`-kind asset the frozen definition projects a first-class
// workflow_definition over -- distinct from the `package-registry` asset that
// holds the definition's bytes.
const DEFINITION_ASSET_ID = "ast_asset_skeleton_wf";
// The `package-registry` asset the definition is SOURCED from: it holds the
// workflow package tarball under `tarballs/`.
const SOURCE_ASSET_ID = "ast_asset_skeleton_registry";

const HUB_PRINCIPAL = { kind: "hub" } as const;

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

const repoRoot = path.resolve(import.meta.dir, "..", "..");

const workflowEntrySource = `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const agent = defineAgent({
  id: "asset-skeleton-agent",
  systemPrompt: "You are the asset-skeleton single-step agent.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: "wf_asset_skeleton",
  trigger: { type: "mail", to: ${JSON.stringify(deploymentMailAddress)} },
  steps: {
    ${STEP_ID}: step({ agent }),
  },
});
`;

interface WorkflowPackageFixture {
  bytes: Uint8Array;
  tarballFilename: string;
}

// Bundle the entry into one self-contained `.mjs` (its `@intx/*` imports inlined
// to source) so the sidecar-materialized closure evaluates it with no bare
// import left to resolve. Mirrors the walking-skeleton fixture builder.
async function bundleWorkflowEntry(
  scratchDir: string,
  entrySource: string,
): Promise<string> {
  const entrySrcPath = path.join(scratchDir, "asset-workflow-entry-src.ts");
  await fs.writeFile(entrySrcPath, entrySource);

  const built = await Bun.build({
    entrypoints: [entrySrcPath],
    target: "bun",
    format: "esm",
    throw: true,
    plugins: [
      {
        name: "resolve-intx-to-source",
        setup(build) {
          build.onResolve({ filter: /^@intx\// }, (args) => {
            const fromDir = args.importer.startsWith(`${repoRoot}${path.sep}`)
              ? dirname(args.importer)
              : repoRoot;
            return { path: Bun.resolveSync(args.path, fromDir) };
          });
        },
      },
    ],
  });

  const artifact = built.outputs[0];
  if (artifact === undefined) {
    throw new Error("bundleWorkflowEntry: Bun.build produced no output");
  }
  const code = await artifact.text();
  if (code.includes("@intx/")) {
    throw new Error(
      "bundleWorkflowEntry: bundle still carries a bare @intx import",
    );
  }
  return code;
}

async function buildWorkflowPackageFixture(
  scratchDir: string,
): Promise<WorkflowPackageFixture> {
  const workflowJs = await bundleWorkflowEntry(scratchDir, workflowEntrySource);

  const packageDir = path.join(scratchDir, "package");
  await fs.mkdir(packageDir, { recursive: true });
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: PACKAGE_NAME,
      version: PACKAGE_VERSION,
      interchange: { workflow: WORKFLOW_ENTRY },
    }),
  );
  await fs.writeFile(path.join(packageDir, "workflow.mjs"), workflowJs);

  const tarballPath = path.join(scratchDir, "out.tgz");
  await tar.create({ cwd: scratchDir, gzip: true, file: tarballPath }, [
    "package",
  ]);
  const bytes = await fs.readFile(tarballPath);
  return {
    bytes,
    tarballFilename: `${PACKAGE_BASENAME}-${PACKAGE_VERSION}.tgz`,
  };
}

let env: DeployFlowEnv;
let h: TestDb;
let scratchDir: string;
let fixture: WorkflowPackageFixture;
// The seeded blob bytes, keyed by asset-root-relative path, backing the install
// call's `readBlob`/`listBlobs` closures (the hub reads the packument by
// synthesizing it from the asset's tarballs; no HTTP).
let blobsByPath: Map<string, Uint8Array>;

describe.skipIf(!harnessDbEnvAvailable())("asset-sourced workflow e2e", () => {
  beforeAll(async () => {
    scratchDir = await fs.mkdtemp(path.join(os.tmpdir(), "asset-skeleton-"));
    fixture = await buildWorkflowPackageFixture(scratchDir);

    h = await createTestDb();
    await h.db.insert(tenantTable).values({
      id: TENANT_ID,
      name: TENANT_ID,
      slug: TENANT_ID,
      domain: DEPLOYMENT_DOMAIN,
      parentId: null,
    });
    await seedPrincipal(h.db, {
      id: CALLER_PRINCIPAL_ID,
      tenantId: TENANT_ID,
      kind: "user",
    });

    // The sidecar needs no registry env: an asset-sourced deploy delivers the
    // asset inline on the frame and the sidecar checks it out.
    env = await startDeployFlowEnv({});

    // Seed the package-registry source asset: write the workflow tarball under
    // `tarballs/` in a hub `package-registry` git repo. This is the byte source
    // the install resolves against and the deploy delivers to the sidecar.
    const blobPath = `tarballs/${fixture.tarballFilename}`;
    blobsByPath = new Map([[blobPath, fixture.bytes]]);
    const sourceRepoId: RepoId = {
      kind: "package-registry",
      id: SOURCE_ASSET_ID,
    };
    await env.hub.agentRepoStore.repoStore.initRepo(sourceRepoId);
    await env.hub.agentRepoStore.repoStore.writeTree(
      HUB_PRINCIPAL,
      sourceRepoId,
      DEFAULT_ASSET_REF,
      {
        files: { [blobPath]: fixture.bytes },
        message: "Seed asset-skeleton workflow tarball",
      },
    );
  });

  afterAll(async () => {
    if (env !== undefined) await env.teardown();
    if (h !== undefined) await h.close();
    if (scratchDir !== undefined) {
      await fs.rm(scratchDir, { recursive: true, force: true });
    }
  });

  test("install -> approve -> deploy-by-source-ref -> run-to-completion", async () => {
    await seedAsset(h.db, {
      id: DEFINITION_ASSET_ID,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: "asset-skeleton-wf",
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });

    const source: WorkflowDefinitionAssetSource = {
      kind: "asset",
      assetId: SOURCE_ASSET_ID,
      package: { format: "tarball" },
    };

    // Read the seeded tarball bytes; `AssetRegistrySource` synthesizes the
    // packument from these (no HTTP on the hub side).
    const readBlob = async (blobPath: string): Promise<Uint8Array> => {
      const bytes = blobsByPath.get(blobPath);
      if (bytes === undefined) {
        throw new Error(`asset e2e: no blob at ${blobPath}`);
      }
      return bytes;
    };
    const listBlobs = async (dir: string): Promise<string[]> => {
      if (dir !== "tarballs") return [];
      return [...blobsByPath.keys()].map((p) => p.replace(/^tarballs\//, ""));
    };

    // The git pack the sidecar checks the source asset out from. `createPack`
    // packs the tip commit + tree of the package-registry repo, exactly what
    // `applyAssetPack` consumes.
    const sourceRepoId: RepoId = {
      kind: "package-registry",
      id: SOURCE_ASSET_ID,
    };
    const resolveAttachment = async (
      assetId: string,
    ): Promise<{ pack: Uint8Array; ref: string; commitSha: string }> => {
      if (assetId !== SOURCE_ASSET_ID) {
        throw new Error(`asset e2e: unexpected attachment request ${assetId}`);
      }
      const commitSha = await env.hub.agentRepoStore.repoStore.resolveRef(
        HUB_PRINCIPAL,
        sourceRepoId,
        DEFAULT_ASSET_REF,
      );
      if (commitSha === null) {
        throw new Error("asset e2e: source asset has no commit");
      }
      const { pack, ref } = await env.hub.agentRepoStore.repoStore.createPack(
        HUB_PRINCIPAL,
        sourceRepoId,
        DEFAULT_ASSET_REF,
      );
      return { pack, ref, commitSha };
    };

    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    // 1) Install + probe + gate + freeze against the source asset.
    const approved = await installAndApproveWorkflowDefinition({
      source,
      pin: `${PACKAGE_NAME}@${PACKAGE_VERSION}`,
      entry: WORKFLOW_ENTRY,
      assetId: DEFINITION_ASSET_ID,
      approvals: operatorApprovals,
      router: env.hub.router,
      db: h.db,
      readBlob,
      listBlobs,
      resolveAttachment,
    });

    if (!approved.approval.ok) {
      throw new Error(
        `install/approve gate did not approve (reason: ${approved.approval.reason}): ` +
          `${JSON.stringify(approved.approval)}\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(approved.projection.id).toBe("wf_asset_skeleton");
    expect(approved.projection.stepOrder).toEqual([STEP_ID]);
    // The closure's single top-level entry is asset-sourced.
    expect(approved.closure.topLevel).toEqual([
      { name: PACKAGE_NAME, version: PACKAGE_VERSION },
    ]);
    const topEntry = approved.closure.entries.find(
      (e) => e.name === PACKAGE_NAME,
    );
    expect(topEntry?.source.kind).toBe("asset");

    const versionRow = await h.db
      .select({
        approvedWireHash: workflowDefinitionVersionTable.approvedWireHash,
      })
      .from(workflowDefinitionVersionTable)
      .where(
        and(
          eq(
            workflowDefinitionVersionTable.definitionId,
            approved.approval.definitionId,
          ),
          eq(workflowDefinitionVersionTable.version, "1"),
        ),
      )
      .limit(1)
      .then((rows) => rows[0]);
    expect(versionRow?.approvedWireHash).toBe(
      approved.approval.approvedWireHash,
    );

    // 2) Deploy by source-ref: the frame carries the source asset inline.
    const inferenceSource = {
      id: "anthropic:mock-model",
      provider: "anthropic",
      baseURL: `http://localhost:${String(env.inference.server.port)}`,
      apiKey: "sk-mock",
      model: "mock-model",
    };
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: DEPLOYMENT_ID,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the definition)",
      tools: [],
      grants: [],
      sources: [inferenceSource],
      defaultSource: "anthropic:mock-model",
    };

    const deployResult = await deployCodeSourcedWorkflow({
      approved,
      source,
      resolveAttachment,
      sidecarRouter: env.hub.router,
      agentAddress: deploymentMailAddress,
      config,
      sources: { [STEP_ID]: [inferenceSource] },
      db: h.db,
      tenantId: TENANT_ID,
      anchorRunId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });
    expect(deployResult.publicKey.length).toBeGreaterThan(0);

    const anchorRow = await h.db
      .select({ address: workflowRunTable.address })
      .from(workflowRunTable)
      .where(eq(workflowRunTable.id, DEPLOYMENT_ID))
      .limit(1)
      .then((rows) => rows[0]);
    expect(anchorRow?.address).toBe(deploymentMailAddress);

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveDeploymentId(deploymentMailAddress),
    };
    env.registerDeployment({
      anchorRunId: DEPLOYMENT_ID,
      workflowDefinition: {
        id: approved.projection.id,
        triggers: [{ type: "mail", to: deploymentMailAddress }],
        steps: {},
        stepOrder: [...approved.projection.stepOrder],
      },
      workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: deploymentMailAddress,
    });

    await waitFor(
      () =>
        env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );

    // 3) Fire the mail trigger and assert the run reaches RunCompleted. This is
    // the load-bearing assertion: it proves asset-sourced closure resolution,
    // the probe's inline asset delivery + host-side checkout, the hub gate +
    // freeze, the source-ref deploy carrying the asset inline, the sidecar's
    // durable-store checkout + frozen-closure apply, and the child re-verify all
    // composed into one run.
    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<asset-skeleton-e2e@integration.interchange>",
    });
    const runId = await waitForFirstRunId(env, workflowRunRepoId, {
      timeoutMs: 30_000,
      diagnostics: env.sidecarDiagnostics,
    });
    const terminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      runId,
      { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
    );
    if (terminal.type !== "RunCompleted") {
      throw new Error(
        `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(terminal.body)}\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(terminal.type).toBe("RunCompleted");
  }, 90_000);
});
