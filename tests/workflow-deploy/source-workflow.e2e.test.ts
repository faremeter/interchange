// Source-sourced end-to-end: install a code-sourced workflow package that lives
// as RAW SOURCE (a git subtree at a pinned commit) inside a hub asset, probe it,
// approve+freeze it, deploy it BY SOURCE-REF, run it to completion, then kill the
// sidecar, restart it against the same data directory, and assert the deployment
// restores to routable from the durable indexed-`.git` store alone (no frame
// re-delivered).
//
// This is the source analog of `asset-source.e2e.test.ts`. Where that test
// sources the definition's bytes from a published tarball, this one sources them
// from the asset's git tree at a pinned commit: the hub resolves the closure by
// reading the tree, freezes each package as a `format:"source"` entry keyed by
// its git tree oid, delivers the asset's git pack inline, and the sidecar indexes
// the pack into a retained `.git` and checks the pinned subtree out of it.
//
// The restore leg is the load-bearing durable-store assertion: boot-time restore
// re-materializes the source closure from the retained `.git` (derived from the
// pin alone, so nothing is re-delivered), running the full subtree checkout +
// tree-oid re-verify before it re-registers the address. A missing or corrupt
// git store fails the restore closed, so the address re-routing proves the
// checkout succeeded from disk.

import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { and, eq } from "drizzle-orm";

import {
  DEFAULT_ASSET_REF,
  committedReadsToSourceTree,
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
  startSidecarSubprocess,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { bundleWorkflowEntry } from "../hub-agent/lib/bundle-workflow-entry";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = generateId("workflowRun");
const STEP_ID = "run";
const WORKFLOW_RUN_REF = "refs/heads/main";

const PACKAGE_NAME = "@wf/source-skeleton";
const PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "./workflow.mjs";

const TENANT_ID = "tnt_source_skeleton";
const CALLER_PRINCIPAL_ID = "prn_source_skeleton_creator";
// The `workflow`-kind asset the frozen definition projects a workflow_definition
// over -- distinct from the asset that holds the definition's source.
const DEFINITION_ASSET_ID = "ast_source_skeleton_wf";
// The asset the definition is SOURCED from: it holds the workflow package as
// raw source (package.json + workflow.mjs) at the repo root.
const SOURCE_ASSET_ID = "ast_source_skeleton_src";

const HUB_PRINCIPAL = { kind: "hub" } as const;

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

const workflowEntrySource = `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const agent = defineAgent({
  id: "source-skeleton-agent",
  systemPrompt: "You are the source-skeleton single-step agent.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: "wf_source_skeleton",
  trigger: { type: "mail", to: ${JSON.stringify(deploymentMailAddress)} },
  steps: {
    ${STEP_ID}: step({ agent }),
  },
});
`;

let env: DeployFlowEnv;
let h: TestDb;
let scratchDir: string;
let restartedSidecar: SidecarHandle | undefined;
const restartTempDirs: string[] = [];
// The pinned commit the definition's source is frozen at, resolved once the
// source repo is seeded.
let sourceCommitSha: string;

// The source asset is a `workflow`-kind repo in its CODEBASE shape: a
// `package.json` declaring `interchange.workflow` plus arbitrary source files.
// (The `package-registry` kind only accepts `tarballs/`, so it cannot hold raw
// source.)
const sourceRepoId: RepoId = {
  kind: "workflow",
  id: SOURCE_ASSET_ID,
};

// Resolve the asset's git pack for delivery: the sidecar indexes it and checks
// the pinned subtree out of it. `createPack` packs the tip commit + tree.
const resolveAttachment = async (
  assetId: string,
): Promise<{ pack: Uint8Array; ref: string; commitSha: string }> => {
  if (assetId !== SOURCE_ASSET_ID) {
    throw new Error(`source e2e: unexpected attachment request ${assetId}`);
  }
  const commitSha = await env.hub.agentRepoStore.repoStore.resolveRef(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  if (commitSha === null) {
    throw new Error("source e2e: source asset has no commit");
  }
  const { pack, ref } = await env.hub.agentRepoStore.repoStore.createPack(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  return { pack, ref, commitSha };
};

describe.skipIf(!harnessDbEnvAvailable())("source-sourced workflow e2e", () => {
  beforeAll(async () => {
    scratchDir = await fs.mkdtemp(path.join(os.tmpdir(), "source-skeleton-"));
    const workflowJs = await bundleWorkflowEntry(
      scratchDir,
      workflowEntrySource,
    );

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

    // No registry env: a source deploy delivers the asset inline on the frame
    // and the sidecar checks the subtree out of the indexed pack.
    env = await startDeployFlowEnv({});

    // Seed the source asset: write the workflow package as RAW SOURCE at the
    // repo root (package.json declaring interchange.workflow + the bundled
    // entry). This is the tree the install resolves the closure from and the
    // deploy delivers as a git pack.
    await env.hub.agentRepoStore.repoStore.initRepo(sourceRepoId);
    const writeResult = await env.hub.agentRepoStore.repoStore.writeTree(
      HUB_PRINCIPAL,
      sourceRepoId,
      DEFAULT_ASSET_REF,
      {
        files: {
          "package.json": JSON.stringify({
            name: PACKAGE_NAME,
            version: PACKAGE_VERSION,
            interchange: { workflow: WORKFLOW_ENTRY },
          }),
          "workflow.mjs": workflowJs,
        },
        message: "Seed source-skeleton workflow package",
      },
    );
    sourceCommitSha = writeResult.commitSha;
  });

  afterAll(async () => {
    if (restartedSidecar !== undefined) {
      restartedSidecar.proc.kill();
      await restartedSidecar.proc.exited;
    }
    for (const dir of restartTempDirs.splice(0)) {
      await fs.rm(dir, { recursive: true, force: true });
    }
    if (env !== undefined) await env.teardown();
    if (h !== undefined) await h.close();
    if (scratchDir !== undefined) {
      await fs.rm(scratchDir, { recursive: true, force: true });
    }
  });

  test("install -> deploy-by-source-ref -> run, then restart restores from the durable git store", async () => {
    await seedAsset(h.db, {
      id: DEFINITION_ASSET_ID,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: "source-skeleton-wf",
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });

    const source: WorkflowDefinitionAssetSource = {
      kind: "asset",
      assetId: SOURCE_ASSET_ID,
      package: { format: "source", commitSha: sourceCommitSha },
    };

    // Git-tree reads pinned to the source commit, adapted from the repo store's
    // committed-reads handle. The closure resolver reads package.json and the
    // subtree oid through this.
    const committed =
      await env.hub.agentRepoStore.repoStore.openCommittedReadsAtCommit(
        HUB_PRINCIPAL,
        sourceRepoId,
        sourceCommitSha,
      );
    if (committed === null) {
      throw new Error("source e2e: could not open committed reads at commit");
    }
    const reads = committedReadsToSourceTree(committed);

    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    // 1) Install + probe + gate + freeze against the source asset.
    const approved = await installAndApproveWorkflowDefinition({
      source,
      entry: WORKFLOW_ENTRY,
      assetId: DEFINITION_ASSET_ID,
      approvals: operatorApprovals,
      router: env.hub.router,
      db: h.db,
      reads,
      // The sidecar's default registry map is keyed "npmjs"; external deps this
      // workflow had (it has none) would be stamped with this name so the
      // sidecar could resolve them. Kept consistent with that map.
      registryName: "npmjs",
      registryConfig: { url: "https://registry.test" },
      resolveAttachment,
    });

    if (!approved.approval.ok) {
      throw new Error(
        `install/approve gate did not approve (reason: ${approved.approval.reason}): ` +
          `${JSON.stringify(approved.approval)}\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(approved.projection.id).toBe("wf_source_skeleton");
    expect(approved.projection.stepOrder).toEqual([STEP_ID]);
    // The closure's single top-level entry is a source-format asset entry.
    expect(approved.closure.topLevel).toEqual([
      { name: PACKAGE_NAME, version: PACKAGE_VERSION },
    ]);
    const topEntry = approved.closure.entries.find(
      (e) => e.name === PACKAGE_NAME,
    );
    expect(topEntry?.source.kind).toBe("asset");
    if (topEntry?.source.kind !== "asset") {
      throw new Error("source e2e: top entry is not asset-sourced");
    }
    expect(topEntry.source.package.format).toBe("source");

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

    // 3) Fire the mail trigger and assert the run reaches RunCompleted. This
    // proves source closure resolution, the probe's inline asset delivery +
    // host-side subtree checkout, the hub gate + freeze, the source-ref deploy
    // carrying the asset inline, the sidecar's durable-store git index + subtree
    // checkout + frozen-closure apply, and the child re-verify all composed.
    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<source-skeleton-e2e@integration.interchange>",
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

    // 4) Restore leg: kill the sidecar and bring a fresh one up against the SAME
    // data directory. Boot-time restore re-materializes the source closure from
    // the durable indexed-`.git` store ALONE -- the hub re-delivers no asset
    // frame -- so the deployment becoming routable again is the load-bearing
    // restore assertion. Restore runs the full re-materialization
    // (`resolveDeploymentAssetMounts` asserts the durable git store is present,
    // then the loader checks the pinned subtree out of it and re-verifies its
    // tree oid) BEFORE it re-registers the address, so an absent or corrupt git
    // store fails the restore closed and the address never re-routes.
    //
    // Re-triggering the run is deliberately NOT asserted here: a mail-triggered
    // deployment hosts a single run keyed to its address, and it is already
    // terminal from step 3, so a fresh trigger is rejected. Routability after a
    // from-disk restore is the strongest assertion the model supports.
    const restoredDataDir = env.sidecar.dataDir;
    const hubPort = env.hub.server.port;
    if (hubPort === undefined) {
      throw new Error("source e2e: hub.server.port is undefined after kill");
    }
    env.sidecar.proc.kill();
    await env.sidecar.proc.exited;

    restartedSidecar = await startSidecarSubprocess({
      hubPort,
      registerTempDir: (dir) => {
        restartTempDirs.push(dir);
      },
      extraEnv: { SIDECAR_DATA_DIR: restoredDataDir },
    });

    await waitFor(
      () =>
        env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      {
        timeoutMs: 30_000,
        diagnostics: () =>
          `${env.sidecarDiagnostics()}\nrestored sidecar stderr:\n${restartedSidecar?.stderr.slice(-60).join("") ?? "<none>"}`,
      },
    );
  }, 180_000);
});
