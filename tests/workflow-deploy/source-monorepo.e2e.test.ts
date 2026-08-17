// Monorepo source-workflow end-to-end: install a code-sourced workflow that is
// ONE member of a bun/npm workspaces monorepo -- `@wf/app` (the workflow)
// depending on `@wf/lib` (a workspace-local member) via `workspace:*` -- living
// as raw source at a pinned commit inside a hub `workflow`-kind asset. Probe it,
// approve+freeze it, deploy it BY SOURCE-REF, then kill the sidecar, restart it
// against the same data directory, and assert the deployment restores to
// routable from the durable indexed-`.git` store alone.
//
// This exercises the monorepo-specific paths the single-package source e2e does
// not: the hub enumerates the workspace members from the root's globs and
// freezes BOTH `@wf/app` and `@wf/lib` as `format:"source"` closure entries; the
// `workflow`-kind push accepts the monorepo root; and the sidecar checks both
// members out of the delivered pack, so `@wf/app`'s bundled entry can import
// `@wf/lib` at evaluation time. The workflow's agent system prompt IS the value
// exported by `@wf/lib`, so a run reaching completion proves the workspace-local
// dependency resolved from the materialized closure.

import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { dirname } from "node:path";

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
import { createTestDb, type TestDb } from "@intx/test-harness/db-harness";
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

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = generateId("workflowRun");
const STEP_ID = "run";
const WORKFLOW_RUN_REF = "refs/heads/main";

const APP_PACKAGE_NAME = "@wf/app";
const APP_PACKAGE_VERSION = "1.0.0";
const LIB_PACKAGE_NAME = "@wf/lib";
const LIB_PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "./workflow.mjs";

const TENANT_ID = "tnt_source_monorepo";
const CALLER_PRINCIPAL_ID = "prn_source_monorepo_creator";
const DEFINITION_ASSET_ID = "ast_source_monorepo_wf";
// The `workflow`-kind asset holding the monorepo source (root + members).
const SOURCE_ASSET_ID = "ast_source_monorepo_src";

const HUB_PRINCIPAL = { kind: "hub" } as const;

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

const repoRoot = path.resolve(import.meta.dir, "..", "..");

// `@wf/lib`'s only export -- the workflow's agent uses it as its system prompt,
// so evaluating `@wf/app` requires `@wf/lib` to have resolved.
const libSource = `export const LIB_SYSTEM_PROMPT = "You are the monorepo workspace agent.";\n`;

const workflowEntrySource = `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { LIB_SYSTEM_PROMPT } from "@wf/lib";

const agent = defineAgent({
  id: "monorepo-agent",
  systemPrompt: LIB_SYSTEM_PROMPT,
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: "wf_monorepo",
  trigger: { type: "mail", to: ${JSON.stringify(deploymentMailAddress)} },
  steps: {
    ${STEP_ID}: step({ agent }),
  },
});
`;

// Bundle `@wf/app`'s entry: inline its `@intx/*` imports to source, but keep
// `@wf/lib` a BARE import (marked external) so the sidecar resolves it from the
// materialized workspace member at evaluation time rather than the bundler
// inlining it here.
async function bundleWorkflowEntry(scratchDir: string): Promise<string> {
  const entrySrcPath = path.join(scratchDir, "monorepo-entry-src.ts");
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
  if (code.includes("@intx/")) {
    throw new Error(
      "bundleWorkflowEntry: bundle still carries a bare @intx import",
    );
  }
  if (!code.includes(LIB_PACKAGE_NAME)) {
    throw new Error(
      "bundleWorkflowEntry: bundle dropped the bare @wf/lib import it must keep",
    );
  }
  return code;
}

let env: DeployFlowEnv;
let h: TestDb;
let scratchDir: string;
let restartedSidecar: SidecarHandle | undefined;
const restartTempDirs: string[] = [];
let sourceCommitSha: string;

const sourceRepoId: RepoId = {
  kind: "workflow",
  id: SOURCE_ASSET_ID,
};

const resolveAttachment = async (
  assetId: string,
): Promise<{ pack: Uint8Array; ref: string; commitSha: string }> => {
  if (assetId !== SOURCE_ASSET_ID) {
    throw new Error(`monorepo e2e: unexpected attachment request ${assetId}`);
  }
  const commitSha = await env.hub.agentRepoStore.repoStore.resolveRef(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  if (commitSha === null) {
    throw new Error("monorepo e2e: source asset has no commit");
  }
  const { pack, ref } = await env.hub.agentRepoStore.repoStore.createPack(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  return { pack, ref, commitSha };
};

describe("monorepo source-workflow e2e", () => {
  beforeAll(async () => {
    scratchDir = await fs.mkdtemp(path.join(os.tmpdir(), "source-monorepo-"));
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

    // Seed the monorepo source: a private workspace root plus two members, one
    // the workflow (`@wf/app`) and one its `workspace:*` dependency (`@wf/lib`).
    // The `workflow`-kind push accepts the root; the resolver enumerates both
    // members from `packages/*`.
    await env.hub.agentRepoStore.repoStore.initRepo(sourceRepoId);
    const writeResult = await env.hub.agentRepoStore.repoStore.writeTree(
      HUB_PRINCIPAL,
      sourceRepoId,
      DEFAULT_ASSET_REF,
      {
        files: {
          "package.json": JSON.stringify({
            name: "@wf/monorepo-root",
            private: true,
            workspaces: ["packages/*"],
          }),
          "packages/app/package.json": JSON.stringify({
            name: APP_PACKAGE_NAME,
            version: APP_PACKAGE_VERSION,
            type: "module",
            interchange: { workflow: WORKFLOW_ENTRY },
            dependencies: { [LIB_PACKAGE_NAME]: "workspace:*" },
          }),
          "packages/app/workflow.mjs": workflowJs,
          "packages/lib/package.json": JSON.stringify({
            name: LIB_PACKAGE_NAME,
            version: LIB_PACKAGE_VERSION,
            type: "module",
            exports: "./index.mjs",
          }),
          "packages/lib/index.mjs": libSource,
        },
        message: "Seed monorepo source workflow",
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
      name: "source-monorepo-wf",
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });

    const source: WorkflowDefinitionAssetSource = {
      kind: "asset",
      assetId: SOURCE_ASSET_ID,
      // The workflow lives in one member of the monorepo; select it by name.
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
      throw new Error("monorepo e2e: could not open committed reads at commit");
    }
    const reads = committedReadsToSourceTree(committed);

    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    // 1) Install: enumerate the monorepo members, probe, gate, freeze.
    const approved = await installAndApproveWorkflowDefinition({
      source,
      entry: WORKFLOW_ENTRY,
      assetId: DEFINITION_ASSET_ID,
      approvals: operatorApprovals,
      router: env.hub.router,
      db: h.db,
      reads,
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
    expect(approved.projection.id).toBe("wf_monorepo");
    expect(approved.projection.stepOrder).toEqual([STEP_ID]);
    // The top-level pin is the selected member; the closure carries BOTH members
    // as source entries.
    expect(approved.closure.topLevel).toEqual([
      { name: APP_PACKAGE_NAME, version: APP_PACKAGE_VERSION },
    ]);
    const byName = new Map(approved.closure.entries.map((e) => [e.name, e]));
    for (const memberName of [APP_PACKAGE_NAME, LIB_PACKAGE_NAME]) {
      const entry = byName.get(memberName);
      if (entry?.source.kind !== "asset") {
        throw new Error(`monorepo e2e: ${memberName} is not asset-sourced`);
      }
      expect(entry.source.package.format).toBe("source");
    }

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

    // 2) Deploy by source-ref: the frame carries the monorepo asset inline.
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

    // 3) Fire the trigger and assert the run completes. This proves the monorepo
    // closure resolved BOTH members, the sidecar checked both subtrees out of
    // the delivered pack, and `@wf/app`'s entry resolved its `@wf/lib`
    // workspace-local import at evaluation time.
    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<source-monorepo-e2e@integration.interchange>",
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
    // data directory. Boot-time restore re-materializes BOTH monorepo members
    // from the durable indexed-`.git` store alone, so the deployment re-routing
    // is the load-bearing restore assertion.
    const restoredDataDir = env.sidecar.dataDir;
    const hubPort = env.hub.server.port;
    if (hubPort === undefined) {
      throw new Error("monorepo e2e: hub.server.port is undefined after kill");
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
