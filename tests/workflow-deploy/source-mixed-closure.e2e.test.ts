// Mixed-closure end-to-end: a monorepo workflow member depends on BOTH a
// workspace-local sibling (`@wf/lib` via workspace:*) AND an external npm
// package (`wf-ext-dep`) served from an in-process registry. The install
// resolves the two origins into one closure -- a git-source entry for each
// member and a registry entry for the external dep -- and the sidecar
// materializes both (the members from the checked-out git subtree, the external
// dep by fetching its tarball from the registry) so `@wf/app` can import both at
// evaluation time.
//
// This is the only source e2e that resolves an EXTERNAL registry dependency
// alongside a workspace-local one: the mixed-origin closure and its cross-origin
// node_modules layout are otherwise only unit-tested. The sidecar reaches the
// external registry through `SIDECAR_TOOL_REGISTRIES`, pointed at the in-process
// server this test stands up.

import { promises as fs } from "node:fs";
import { createHash } from "node:crypto";
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
import { createNoopCredentialCipher } from "@intx/crypto";
import { tenant as tenantTable } from "@intx/db/schema";
import type { HarnessConfig } from "@intx/types/runtime";
import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";
import type { Packument, PackumentFetcher } from "@intx/tool-packaging";
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
  buildSyntheticNpmPackageTarball,
  fireMailTrigger,
  seedInferenceCredentials,
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
const EXT_PACKAGE_NAME = "wf-ext-dep";
const EXT_PACKAGE_VERSION = "1.0.0";
const EXT_TARBALL_PATH = `/${EXT_PACKAGE_NAME}/-/${EXT_PACKAGE_NAME}-${EXT_PACKAGE_VERSION}.tgz`;
const REGISTRY_NAME = "ext-reg";
const PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "./workflow.mjs";

const TENANT_ID = "tnt_source_mixed";
const CALLER_PRINCIPAL_ID = "prn_source_mixed_creator";
const DEFINITION_ASSET_ID = "ast_source_mixed_wf";
const SOURCE_ASSET_ID = "ast_source_mixed_src";

const HUB_PRINCIPAL = { kind: "hub" } as const;

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

const repoRoot = path.resolve(import.meta.dir, "..", "..");

const libSource = `export const LIB_PART = "workspace-lib";\n`;
const extModuleSource = `export const EXT_PART = "external-dep";\n`;

const workflowEntrySource = `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { LIB_PART } from "@wf/lib";
import { EXT_PART } from "wf-ext-dep";

const agent = defineAgent({
  id: "mixed-agent",
  systemPrompt: "You are the " + LIB_PART + " + " + EXT_PART + " agent.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: "wf_mixed",
  trigger: { type: "mail", to: ${JSON.stringify(deploymentMailAddress)} },
  steps: {
    ${STEP_ID}: step({ agent }),
  },
});
`;

async function bundleWorkflowEntry(scratchDir: string): Promise<string> {
  const entrySrcPath = path.join(scratchDir, "mixed-entry-src.ts");
  await fs.writeFile(entrySrcPath, workflowEntrySource);
  const built = await Bun.build({
    entrypoints: [entrySrcPath],
    target: "bun",
    format: "esm",
    throw: true,
    external: [LIB_PACKAGE_NAME, EXT_PACKAGE_NAME],
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
  for (const dep of [LIB_PACKAGE_NAME, EXT_PACKAGE_NAME]) {
    if (!code.includes(dep)) {
      throw new Error(`bundleWorkflowEntry: bundle dropped the bare ${dep}`);
    }
  }
  return code;
}

function sri(bytes: Uint8Array): string {
  return `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
}

let env: DeployFlowEnv;
let h: TestDb;
let scratchDir: string;
let sourceCommitSha: string;
let registryServer: ReturnType<typeof Bun.serve> | undefined;
let packument: Packument;
const tarballTempDirs: string[] = [];

const sourceRepoId: RepoId = { kind: "workflow", id: SOURCE_ASSET_ID };

const resolveAttachment = async (
  assetId: string,
): Promise<{ pack: Uint8Array; ref: string; commitSha: string }> => {
  if (assetId !== SOURCE_ASSET_ID) {
    throw new Error(`mixed e2e: unexpected attachment request ${assetId}`);
  }
  const commitSha = await env.hub.agentRepoStore.repoStore.resolveRef(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  if (commitSha === null) {
    throw new Error("mixed e2e: source asset has no commit");
  }
  const { pack, ref } = await env.hub.agentRepoStore.repoStore.createPack(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  return { pack, ref, commitSha };
};

// The install resolves the external dep from this packument (no HTTP on the hub
// side); the sidecar fetches the same packument + tarball from the in-process
// registry at materialize. Both see the same SRI.
const fetchPackument: PackumentFetcher = async (name) => {
  if (name !== EXT_PACKAGE_NAME) {
    throw new Error(`mixed e2e: unexpected packument request ${name}`);
  }
  return packument;
};

describe.skipIf(!harnessDbEnvAvailable())(
  "mixed-closure source-workflow e2e",
  () => {
    beforeAll(async () => {
      scratchDir = await fs.mkdtemp(path.join(os.tmpdir(), "source-mixed-"));
      const workflowJs = await bundleWorkflowEntry(scratchDir);

      // Build the external dep tarball, compute its SRI, and stand up an
      // in-process npm registry that serves its packument + tarball.
      const extBytes = await buildSyntheticNpmPackageTarball(
        (dir) => tarballTempDirs.push(dir),
        {
          packageName: EXT_PACKAGE_NAME,
          version: EXT_PACKAGE_VERSION,
          moduleSource: extModuleSource,
        },
      );
      const extIntegrity = sri(extBytes);

      registryServer = Bun.serve({
        port: 0,
        fetch(req) {
          const url = new URL(req.url);
          if (url.pathname === `/${EXT_PACKAGE_NAME}`) {
            return new Response(JSON.stringify(packument), {
              headers: { "content-type": "application/json" },
            });
          }
          if (url.pathname === EXT_TARBALL_PATH) {
            return new Response(extBytes);
          }
          return new Response("not found", { status: 404 });
        },
      });
      const registryUrl = `http://localhost:${String(registryServer.port)}`;
      packument = {
        name: EXT_PACKAGE_NAME,
        "dist-tags": { latest: EXT_PACKAGE_VERSION },
        versions: {
          [EXT_PACKAGE_VERSION]: {
            name: EXT_PACKAGE_NAME,
            version: EXT_PACKAGE_VERSION,
            dist: {
              tarball: `${registryUrl}${EXT_TARBALL_PATH}`,
              integrity: extIntegrity,
            },
          },
        },
      };

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

      // Point the sidecar's registry map at the in-process registry so it can
      // fetch the external dep's tarball at probe + deploy.
      env = await startDeployFlowEnv({
        sidecarEnv: {
          SIDECAR_TOOL_REGISTRIES: JSON.stringify([
            { name: REGISTRY_NAME, url: registryUrl },
          ]),
        },
      });

      await env.hub.agentRepoStore.repoStore.initRepo(sourceRepoId);
      const writeResult = await env.hub.agentRepoStore.repoStore.writeTree(
        HUB_PRINCIPAL,
        sourceRepoId,
        DEFAULT_ASSET_REF,
        {
          files: {
            "package.json": JSON.stringify({
              name: "@wf/mixed-root",
              private: true,
              workspaces: ["packages/*"],
            }),
            "packages/app/package.json": JSON.stringify({
              name: APP_PACKAGE_NAME,
              version: PACKAGE_VERSION,
              type: "module",
              interchange: { workflow: WORKFLOW_ENTRY },
              dependencies: {
                [LIB_PACKAGE_NAME]: "workspace:*",
                [EXT_PACKAGE_NAME]: `^${EXT_PACKAGE_VERSION}`,
              },
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
          message: "Seed mixed-closure monorepo source",
        },
      );
      sourceCommitSha = writeResult.commitSha;
    });

    afterAll(async () => {
      if (env !== undefined) await env.teardown();
      if (h !== undefined) await h.close();
      if (registryServer !== undefined) await registryServer.stop(true);
      for (const dir of tarballTempDirs.splice(0)) {
        await fs.rm(dir, { recursive: true, force: true });
      }
      if (scratchDir !== undefined) {
        await fs.rm(scratchDir, { recursive: true, force: true });
      }
    });

    test("resolves a workspace-local and an external dep in one closure and runs", async () => {
      await seedAsset(h.db, {
        id: DEFINITION_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "source-mixed-wf",
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
        throw new Error("mixed e2e: could not open committed reads at commit");
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
        router: env.hub.probeRouter,
        db: h.db,
        reads: committedReadsToSourceTree(committed),
        registryName: REGISTRY_NAME,
        registryConfig: {
          url: `http://localhost:${String(registryServer?.port)}`,
        },
        fetchPackument,
        resolveAttachment,
      });
      if (!approved.approval.ok) {
        throw new Error(
          `mixed e2e: install did not approve (reason: ${approved.approval.reason}): ${JSON.stringify(approved.approval)}\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(approved.projection.id).toBe("wf_mixed");

      // The closure carries three entries across two origins: @wf/app and @wf/lib
      // as source, wf-ext-dep as a registry entry.
      const byName = new Map(approved.closure.entries.map((e) => [e.name, e]));
      const appEntry = byName.get(APP_PACKAGE_NAME);
      const libEntry = byName.get(LIB_PACKAGE_NAME);
      const extEntry = byName.get(EXT_PACKAGE_NAME);
      if (
        appEntry?.source.kind !== "asset" ||
        libEntry?.source.kind !== "asset"
      ) {
        throw new Error("mixed e2e: workspace members are not asset-sourced");
      }
      expect(appEntry.source.package.format).toBe("source");
      expect(libEntry.source.package.format).toBe("source");
      if (extEntry?.source.kind !== "registry") {
        throw new Error("mixed e2e: external dep is not a registry entry");
      }
      expect(extEntry.source.registry).toBe(REGISTRY_NAME);

      const inferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        credentialId: "sk-mock",
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

      await seedInferenceCredentials(
        h.db,
        TENANT_ID,
        { [STEP_ID]: [inferenceSource] },
        config,
      );
      env.hub.setPrimaryAllocationIdentity(
        DEPLOYMENT_ID,
        deploymentMailAddress,
      );
      await deployCodeSourcedWorkflow({
        approved,
        source,
        resolveAttachment,
        sidecarAllocationRouter: env.hub.router,
        allocationTarget: {
          allocationId: "allocation-integration-1",
          generation: 1,
        },
        agentAddress: deploymentMailAddress,
        config,
        sources: { [STEP_ID]: [inferenceSource] },
        db: h.db,
        tenantId: TENANT_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        credentialCipher: createNoopCredentialCipher(),
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
        messageId: "<source-mixed-e2e@integration.interchange>",
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
          `mixed e2e: expected RunCompleted, got ${terminal.type}: ${JSON.stringify(terminal.body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(terminal.type).toBe("RunCompleted");
    }, 180_000);
  },
);
