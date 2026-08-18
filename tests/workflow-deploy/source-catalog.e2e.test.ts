// catalog: end-to-end: a monorepo whose workflow member depends on a
// workspace-local sibling through pnpm/bun's `catalog:` protocol, resolved
// against the root package.json `catalog` object. The hub expands the bare
// `catalog:` specifier to the catalog's range, classifies the dependency as a
// workspace member, and freezes BOTH members as source entries; a run reaching
// completion proves the catalog-resolved dependency materialized and was
// importable at evaluation time.
//
// `catalog:` resolution is otherwise only unit-tested; this exercises it through
// install -> deploy-by-source-ref -> run against the real subprocess sidecar.

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
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import { createTestDb, type TestDb } from "@intx/test-harness/db-harness";
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

const APP_PACKAGE_NAME = "@wf/app";
const LIB_PACKAGE_NAME = "@wf/lib";
const PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "./workflow.mjs";

const TENANT_ID = "tnt_source_catalog";
const CALLER_PRINCIPAL_ID = "prn_source_catalog_creator";
const DEFINITION_ASSET_ID = "ast_source_catalog_wf";
const SOURCE_ASSET_ID = "ast_source_catalog_src";

const HUB_PRINCIPAL = { kind: "hub" } as const;

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

const repoRoot = path.resolve(import.meta.dir, "..", "..");

// `@wf/lib`'s only export, used as the agent's system prompt so evaluating
// `@wf/app` requires the catalog-resolved `@wf/lib` to have materialized.
const libSource = `export const LIB_SYSTEM_PROMPT = "You are the catalog-resolved workspace agent.";\n`;

const workflowEntrySource = `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { LIB_SYSTEM_PROMPT } from "@wf/lib";

const agent = defineAgent({
  id: "catalog-agent",
  systemPrompt: LIB_SYSTEM_PROMPT,
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: "wf_catalog",
  trigger: { type: "mail", to: ${JSON.stringify(deploymentMailAddress)} },
  steps: {
    ${STEP_ID}: step({ agent }),
  },
});
`;

async function bundleWorkflowEntry(scratchDir: string): Promise<string> {
  const entrySrcPath = path.join(scratchDir, "catalog-entry-src.ts");
  await fs.writeFile(entrySrcPath, workflowEntrySource);
  const built = await Bun.build({
    entrypoints: [entrySrcPath],
    target: "bun",
    format: "esm",
    throw: true,
    external: [LIB_PACKAGE_NAME],
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
  if (!code.includes(LIB_PACKAGE_NAME)) {
    throw new Error(
      "bundleWorkflowEntry: bundle dropped the bare @wf/lib import",
    );
  }
  return code;
}

let env: DeployFlowEnv;
let h: TestDb;
let scratchDir: string;
let sourceCommitSha: string;

const sourceRepoId: RepoId = { kind: "workflow", id: SOURCE_ASSET_ID };

const resolveAttachment = async (
  assetId: string,
): Promise<{ pack: Uint8Array; ref: string; commitSha: string }> => {
  if (assetId !== SOURCE_ASSET_ID) {
    throw new Error(`catalog e2e: unexpected attachment request ${assetId}`);
  }
  const commitSha = await env.hub.agentRepoStore.repoStore.resolveRef(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  if (commitSha === null) {
    throw new Error("catalog e2e: source asset has no commit");
  }
  const { pack, ref } = await env.hub.agentRepoStore.repoStore.createPack(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  return { pack, ref, commitSha };
};

describe("catalog: source-workflow e2e", () => {
  beforeAll(async () => {
    scratchDir = await fs.mkdtemp(path.join(os.tmpdir(), "source-catalog-"));
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

    // Seed the monorepo: a private root declaring a `catalog` for @wf/lib, the
    // workflow member @wf/app depending on @wf/lib via `catalog:`, and @wf/lib.
    await env.hub.agentRepoStore.repoStore.initRepo(sourceRepoId);
    const writeResult = await env.hub.agentRepoStore.repoStore.writeTree(
      HUB_PRINCIPAL,
      sourceRepoId,
      DEFAULT_ASSET_REF,
      {
        files: {
          "package.json": JSON.stringify({
            name: "@wf/catalog-root",
            private: true,
            workspaces: ["packages/*"],
            catalog: { [LIB_PACKAGE_NAME]: "*" },
          }),
          "packages/app/package.json": JSON.stringify({
            name: APP_PACKAGE_NAME,
            version: PACKAGE_VERSION,
            type: "module",
            interchange: { workflow: WORKFLOW_ENTRY },
            dependencies: { [LIB_PACKAGE_NAME]: "catalog:" },
          }),
          "packages/app/workflow.mjs": workflowJs,
          "packages/lib/package.json": JSON.stringify({
            name: LIB_PACKAGE_NAME,
            version: PACKAGE_VERSION,
            type: "module",
            exports: "./index.mjs",
          }),
          "packages/lib/index.mjs": libSource,
        },
        message: "Seed catalog-resolved monorepo source",
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

  test("resolves a catalog: dependency and runs the workflow to completion", async () => {
    await seedAsset(h.db, {
      id: DEFINITION_ASSET_ID,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: "source-catalog-wf",
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });

    const source: WorkflowDefinitionAssetSource = {
      kind: "asset",
      assetId: SOURCE_ASSET_ID,
      package: {
        format: "source",
        commitSha: sourceCommitSha,
        packageName: APP_PACKAGE_NAME,
      },
    };

    const committed =
      await env.hub.agentRepoStore.repoStore.openCommittedReadsAtCommit(
        HUB_PRINCIPAL,
        sourceRepoId,
        sourceCommitSha,
      );
    if (committed === null) {
      throw new Error("catalog e2e: could not open committed reads at commit");
    }

    const approvals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

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
        `catalog e2e: install did not approve (reason: ${approved.approval.reason}): ${JSON.stringify(approved.approval)}\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(approved.projection.id).toBe("wf_catalog");
    // Both members are source entries: @wf/lib resolved through `catalog:`.
    const byName = new Map(approved.closure.entries.map((e) => [e.name, e]));
    for (const memberName of [APP_PACKAGE_NAME, LIB_PACKAGE_NAME]) {
      const entry = byName.get(memberName);
      if (entry?.source.kind !== "asset") {
        throw new Error(`catalog e2e: ${memberName} is not asset-sourced`);
      }
      expect(entry.source.package.format).toBe("source");
    }

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

    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<source-catalog-e2e@integration.interchange>",
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
        `catalog e2e: expected RunCompleted, got ${terminal.type}: ${JSON.stringify(terminal.body)}\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(terminal.type).toBe("RunCompleted");
  }, 180_000);
});
