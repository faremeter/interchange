// Credential-bound source-workflow end-to-end: a code-sourced workflow whose
// frozen definition declares a `credentialBindings` entry. On deploy,
// `deployCodeSourcedWorkflow` resolves the binding against the tenant-owned
// credential seeded in the DB, decrypts it through the credential cipher, and
// carries the delivery on the frame; the run reaching completion proves the
// binding resolved and was delivered.
//
// The credential rail (a tool consuming the handle, Gate 2 authorization, the
// mediated-http origin) is proven on the live path by
// single-step-credential-tool. This test proves the SOURCE-path composition:
// that a source workflow's operator-approved credential bindings resolve and
// deliver through the code-sourced deploy, which no other source e2e exercises.

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
import { createNoopCredentialCipher } from "@intx/crypto";
import { generateId } from "@intx/hub-common";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedCredential,
  seedPrincipal,
  seedProvider,
} from "@intx/test-harness/seed";

import {
  SESSION_ID,
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

const PACKAGE_NAME = "@wf/credential-skeleton";
const PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "./workflow.mjs";

const TENANT_ID = "tnt_source_credential";
const CALLER_PRINCIPAL_ID = "prn_source_credential_creator";
const DEFINITION_ASSET_ID = "ast_source_credential_wf";
const SOURCE_ASSET_ID = "ast_source_credential_src";

const PROVIDER_ID = "prv_source_credential";
const PROVIDER_NAME = "test-provider";
const CREDENTIAL_ID = "cred_source_credential";
const CREDENTIAL_NAME = "test-cred";
const CREDENTIAL_HANDLE = "test-handle";

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
  id: "credential-skeleton-agent",
  systemPrompt: "You are the credential-skeleton single-step agent.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: "wf_source_credential",
  trigger: { type: "mail", to: ${JSON.stringify(deploymentMailAddress)} },
  credentialBindings: [
    {
      package: ${JSON.stringify(PACKAGE_NAME)},
      handle: ${JSON.stringify(CREDENTIAL_HANDLE)},
      provider: ${JSON.stringify(PROVIDER_NAME)},
      name: ${JSON.stringify(CREDENTIAL_NAME)},
      locator: "tenant",
    },
  ],
  steps: {
    ${STEP_ID}: step({ agent }),
  },
});
`;

async function bundleWorkflowEntry(scratchDir: string): Promise<string> {
  const entrySrcPath = path.join(scratchDir, "credential-entry-src.ts");
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

const sourceRepoId: RepoId = { kind: "workflow", id: SOURCE_ASSET_ID };

const resolveAttachment = async (
  assetId: string,
): Promise<{ pack: Uint8Array; ref: string; commitSha: string }> => {
  if (assetId !== SOURCE_ASSET_ID) {
    throw new Error(`credential e2e: unexpected attachment request ${assetId}`);
  }
  const commitSha = await env.hub.agentRepoStore.repoStore.resolveRef(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  if (commitSha === null) {
    throw new Error("credential e2e: source asset has no commit");
  }
  const { pack, ref } = await env.hub.agentRepoStore.repoStore.createPack(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  return { pack, ref, commitSha };
};

describe.skipIf(!harnessDbEnvAvailable())(
  "credential-bound source-workflow e2e",
  () => {
    beforeAll(async () => {
      scratchDir = await fs.mkdtemp(
        path.join(os.tmpdir(), "source-credential-"),
      );
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
      // The tenant-owned provider + credential the binding resolves against.
      await seedProvider(h.db, {
        id: PROVIDER_ID,
        tenantId: TENANT_ID,
        name: PROVIDER_NAME,
        // Origin-pinned delivery needs the provider's API base URL.
        apiBaseUrl: "https://api.test-provider.example",
      });
      await seedCredential(h.db, {
        id: CREDENTIAL_ID,
        tenantId: TENANT_ID,
        providerId: PROVIDER_ID,
        name: CREDENTIAL_NAME,
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
          message: "Seed credential-skeleton workflow package",
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

    test("resolves and delivers a credential binding on a source deploy", async () => {
      await seedAsset(h.db, {
        id: DEFINITION_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "source-credential-wf",
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
        throw new Error("credential e2e: could not open committed reads");
      }

      const approvals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        `credential:${CREDENTIAL_HANDLE}`,
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
          `credential e2e: install did not approve (reason: ${approved.approval.reason}): ${JSON.stringify(approved.approval)}\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(approved.projection.id).toBe("wf_source_credential");
      expect(approved.projection.credentialBindings?.length).toBe(1);

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

      // Deploy with a credential cipher: the binding is resolved from the DB and
      // decrypted through it. A no-op cipher passes the seeded material verbatim.
      await seedInferenceCredentials(
        h.db,
        TENANT_ID,
        { [STEP_ID]: [inferenceSource] },
        config,
      );
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
        messageId: "<source-credential-e2e@integration.interchange>",
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
          `credential e2e: expected RunCompleted, got ${terminal.type}: ${JSON.stringify(terminal.body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(terminal.type).toBe("RunCompleted");
    }, 180_000);
  },
);
