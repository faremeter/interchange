// Negative-path source end-to-end: a source workflow installs and freezes
// cleanly, but the pack DELIVERED at deploy is tampered (a byte flipped), so the
// sidecar's pack index / subtree checkout fails its integrity check. The deploy
// must be rejected and the deployment address must NEVER become routable.
//
// This is the load-bearing reject-path proof for the source feature: every other
// source e2e asserts a run REACHES completion, so none of them would catch a
// regression that silently stopped rejecting a bad deploy. Here the frozen
// definition is valid (install succeeds), and only the delivered bytes are
// corrupted, so the failure is exactly the eval-boundary integrity guard the
// feature depends on -- proven end to end through the real subprocess sidecar.

import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { dirname } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import {
  DEFAULT_ASSET_REF,
  committedReadsToSourceTree,
  deployCodeSourcedWorkflow,
  installAndApproveWorkflowDefinition,
  type RepoId,
} from "@intx/hub-sessions";
import { tenant as tenantTable } from "@intx/db/schema";
import type { HarnessConfig } from "@intx/types/runtime";
import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";
import { generateId } from "@intx/hub-common";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { createTestDb, type TestDb } from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";

import {
  SESSION_ID,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = generateId("workflowRun");
const STEP_ID = "run";

const PACKAGE_NAME = "@wf/tamper-skeleton";
const PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "./workflow.mjs";

const TENANT_ID = "tnt_source_tamper";
const CALLER_PRINCIPAL_ID = "prn_source_tamper_creator";
const DEFINITION_ASSET_ID = "ast_source_tamper_wf";
const SOURCE_ASSET_ID = "ast_source_tamper_src";

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
  id: "tamper-skeleton-agent",
  systemPrompt: "You are the tamper-skeleton single-step agent.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: "wf_tamper_skeleton",
  trigger: { type: "mail", to: ${JSON.stringify(deploymentMailAddress)} },
  steps: {
    ${STEP_ID}: step({ agent }),
  },
});
`;

async function bundleWorkflowEntry(scratchDir: string): Promise<string> {
  const entrySrcPath = path.join(scratchDir, "tamper-entry-src.ts");
  await fs.writeFile(entrySrcPath, workflowEntrySource);
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
  return artifact.text();
}

let env: DeployFlowEnv;
let h: TestDb;
let scratchDir: string;
let sourceCommitSha: string;
// Flip the delivered pack once the (clean) install has frozen the definition, so
// only the DEPLOY delivery is tampered -- the install must resolve the genuine
// closure first.
let tamperDelivery = false;

const sourceRepoId: RepoId = { kind: "workflow", id: SOURCE_ASSET_ID };

const resolveAttachment = async (
  assetId: string,
): Promise<{ pack: Uint8Array; ref: string; commitSha: string }> => {
  if (assetId !== SOURCE_ASSET_ID) {
    throw new Error(`tamper e2e: unexpected attachment request ${assetId}`);
  }
  const commitSha = await env.hub.agentRepoStore.repoStore.resolveRef(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  if (commitSha === null) {
    throw new Error("tamper e2e: source asset has no commit");
  }
  const { pack, ref } = await env.hub.agentRepoStore.repoStore.createPack(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  if (tamperDelivery) {
    // Flip a byte in the object region (past the 12-byte header) so the pack's
    // trailing checksum no longer matches and its objects no longer hash to
    // their oids: indexing / checkout fails its integrity check.
    const bad = new Uint8Array(pack);
    const at = Math.floor(bad.length / 2);
    bad[at] = (bad[at] ?? 0) ^ 0xff;
    return { pack: bad, ref, commitSha };
  }
  return { pack, ref, commitSha };
};

describe("tampered source-workflow deploy is rejected", () => {
  beforeAll(async () => {
    scratchDir = await fs.mkdtemp(path.join(os.tmpdir(), "source-tamper-"));
    const workflowJs = await bundleWorkflowEntry(scratchDir);

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

    env = await startDeployFlowEnv({});

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
        message: "Seed tamper-skeleton workflow package",
      },
    );
    sourceCommitSha = writeResult.commitSha;
  });

  afterAll(async () => {
    if (env !== undefined) await env.teardown();
    if (h !== undefined) await h.close();
    if (scratchDir !== undefined) {
      await fs.rm(scratchDir, { recursive: true, force: true });
    }
  });

  test("a byte-tampered delivery fails the deploy and never routes", async () => {
    await seedAsset(h.db, {
      id: DEFINITION_ASSET_ID,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: "source-tamper-wf",
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });

    const source: WorkflowDefinitionAssetSource = {
      kind: "asset",
      assetId: SOURCE_ASSET_ID,
      package: { format: "source", commitSha: sourceCommitSha },
    };

    const committed =
      await env.hub.agentRepoStore.repoStore.openCommittedReadsAtCommit(
        HUB_PRINCIPAL,
        sourceRepoId,
        sourceCommitSha,
      );
    if (committed === null) {
      throw new Error("tamper e2e: could not open committed reads at commit");
    }

    const approvals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    // 1) Install cleanly: the genuine closure is resolved and frozen.
    const approved = await installAndApproveWorkflowDefinition({
      source,
      entry: WORKFLOW_ENTRY,
      assetId: DEFINITION_ASSET_ID,
      approvals,
      router: env.hub.router,
      db: h.db,
      reads: committedReadsToSourceTree(committed),
      registryName: "npmjs",
      registryConfig: { url: "https://registry.test" },
      resolveAttachment,
    });
    if (!approved.approval.ok) {
      throw new Error(
        `tamper e2e: install did not approve: ${JSON.stringify(approved.approval)}`,
      );
    }

    // 2) Deploy with a TAMPERED delivery.
    tamperDelivery = true;
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

    let deployRejected = false;
    try {
      await deployCodeSourcedWorkflow({
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
    } catch {
      deployRejected = true;
    }

    // Whether the deploy call rejected outright or acked, the tampered source
    // must never become routable: materialization failed, so the sidecar never
    // reported the address as deployed.
    let becameRoutable = false;
    try {
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 8_000 },
      );
      becameRoutable = true;
    } catch {
      // Expected: the address never became routable within the window.
    }
    expect(becameRoutable).toBe(false);

    // Positive signal that the failure is the tamper, not a vacuously
    // non-routing harness: the deploy call itself rejected when the sidecar's
    // integrity check failed on the tampered pack. (A CLEAN source deploy on
    // this same harness DOES become routable -- see source-workflow.e2e.)
    expect(deployRejected).toBe(true);
  }, 120_000);
});
