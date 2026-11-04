// A tool from a workflow's mounted source asset keeps working after a workflow
// child crash and after a sidecar restart.
//
// The workflow package is a tarball inside a hub `package-registry` asset, so the
// sidecar mounts the asset and lays the closure out of the mount. It runs
//
//   first -> afterCrash -> second -> afterRestart -> third
//
// where each agent step calls a tool that reads the `package.json` beside the
// evaluated module and reports its own pid. The tool result therefore proves
// that the run's grant still authorizes the tool, that the module was evaluated
// from the closure, and which process ran the call. Before
// the restart the test removes the closure's tarball cache, so the restored
// child's closure is rebuilt from the mount.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow child, and real process deaths. Only inference is mocked.

import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { createNoopCredentialCipher } from "@intx/crypto";
import { tenant as tenantTable } from "@intx/db/schema";
import { generateId } from "@intx/hub-common";
import {
  DEFAULT_ASSET_REF,
  deployCodeSourcedWorkflow,
  installAndApproveWorkflowDefinition,
  type RepoId,
} from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  tenantSlugFromId,
} from "@intx/test-harness/seed";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";
import {
  createApprovalSet,
  deriveRunAddress,
  deriveWorkflowRunRepoId,
} from "@intx/workflow-deploy";

import { bundleWorkflowEntry } from "../hub-agent/lib/bundle-workflow-entry";
import {
  SESSION_ID,
  WORKFLOW_RUN_TERMINAL_TYPES,
  fireMailTrigger,
  injectSignal,
  killWorkflowHostChild,
  listWorkflowHostChildren,
  readWorkflowRunEvents,
  seedInferenceCredentials,
  settleWorkflowRunPacks,
  startDeployFlowEnv,
  terminateSidecarSubprocess,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { buildSyntheticNpmPackageTarball } from "../hub-agent/lib/synthetic-npm-package-tarball";
import {
  deriveWireRunGrants,
  toolResultTexts,
} from "./nested-tool-invoke-helpers";
import { crashSidecar, restartSidecar } from "./sleep-restart-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = generateId("workflowRun");
const PACKAGE_NAME = "@wf/asset-tool";
const PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "./workflow.mjs";
const TOOL_NAME = "read_package_identity";
const TOOL_IDENTITY = `${PACKAGE_NAME}@${PACKAGE_VERSION}`;

const TENANT_ID = "tnt_asset_tool_restarts";
const CALLER_PRINCIPAL_ID = "prn_asset_tool_restarts";
const DEFINITION_ASSET_ID = "ast_asset_tool_restarts_wf";
const SOURCE_ASSET_ID = "ast_asset_tool_restarts_registry";

const HUB_PRINCIPAL = { kind: "hub" } as const;
const SOURCE_REPO_ID: RepoId = {
  kind: "package-registry",
  id: SOURCE_ASSET_ID,
};

const mailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

const workflowEntrySource = `
import { promises as fs } from "node:fs";

import { defineAgent, defineTool } from "@intx/agent";
import { awaitSignal, defineWorkflow, step } from "@intx/workflow/definition";

const readPackageIdentity = defineTool({
  id: "@wf/asset-tool/read-package-identity",
  definitions: [{ name: ${JSON.stringify(TOOL_NAME)} }],
  factory: () => ({
    definitions: [
      {
        name: ${JSON.stringify(TOOL_NAME)},
        description: "Read the identity of the package this tool ships in",
        inputSchema: { type: "object", properties: {} },
      },
    ],
    run: async (call) => {
      const manifest = JSON.parse(
        await fs.readFile(new URL("./package.json", import.meta.url), "utf8"),
      );
      return {
        callId: call.id,
        content: manifest.name + "@" + manifest.version + " pid " + process.pid,
      };
    },
  }),
});

const agent = (id) =>
  defineAgent({
    id,
    systemPrompt: "You read the package identity.",
    tools: [readPackageIdentity],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
  });

export const workflow = defineWorkflow({
  id: "wf_asset_tool_restarts",
  trigger: { type: "mail", to: ${JSON.stringify(mailAddress)} },
  steps: {
    first: step({ agent: agent("asset-tool-first") }),
    afterCrash: awaitSignal({ name: "afterCrash", after: ["first"] }),
    second: step({ agent: agent("asset-tool-second"), after: ["afterCrash"] }),
    afterRestart: awaitSignal({ name: "afterRestart", after: ["second"] }),
    third: step({ agent: agent("asset-tool-third"), after: ["afterRestart"] }),
  },
});
`;

let env: DeployFlowEnv;
let h: TestDb;
let scratchDir: string;
let blobs: Map<string, Uint8Array>;
let restarted: SidecarHandle | undefined;
const restartTempDirs: string[] = [];

function requireRoutable(): Promise<void> {
  return waitFor(
    () => env.hub.router.getRoutableAddresses().includes(mailAddress),
    { diagnostics: env.sidecarDiagnostics },
  );
}

// The pid of each tool call so far, asserting the count and the package the
// tool read.
function toolRunPids(expectedRuns: number): number[] {
  const texts = env.inference.requests.flatMap(toolResultTexts);
  expect(texts).toHaveLength(expectedRuns);
  return texts.map((text) => {
    const match = /^(.+) pid (\d+)$/.exec(text);
    if (match === null) throw new Error(`unexpected tool result: ${text}`);
    expect(match[1]).toBe(TOOL_IDENTITY);
    return Number(match[2]);
  });
}

async function hasParkedAt(
  runId: string,
  signalName: string,
): Promise<boolean> {
  const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
  return events.some(
    (e) => e.type === "SignalAwaited" && e.body["signalName"] === signalName,
  );
}

async function hasTerminated(runId: string): Promise<boolean> {
  const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
  return events.some((e) => WORKFLOW_RUN_TERMINAL_TYPES.has(e.type));
}

// `injectSignal` has no delivery ack, and a respawned child rejects signals
// while it starts, so resend until the run moves on. An extra delivery stays
// queued, which is harmless because each gate awaits its own signal name.
async function signalUntil(
  runId: string,
  signalName: string,
  moved: () => Promise<boolean>,
): Promise<void> {
  let lastSentAt = 0;
  await waitFor(
    async () => {
      if (await moved()) return true;
      if (Date.now() - lastSentAt >= 1_000) {
        lastSentAt = Date.now();
        await injectSignal(env, DEPLOYMENT_ID, runId, signalName, {});
      }
      return false;
    },
    { diagnostics: env.sidecarDiagnostics },
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a mounted asset's tool across process deaths",
  () => {
    beforeAll(async () => {
      scratchDir = await fs.mkdtemp(path.join(os.tmpdir(), "asset-tool-"));
      const fixture = await buildSyntheticNpmPackageTarball({
        packageName: PACKAGE_NAME,
        version: PACKAGE_VERSION,
        moduleFilename: path.basename(WORKFLOW_ENTRY),
        moduleSource: await bundleWorkflowEntry(
          scratchDir,
          workflowEntrySource,
        ),
        manifest: { interchange: { workflow: WORKFLOW_ENTRY } },
      });

      h = await createTestDb();
      await h.db.insert(tenantTable).values({
        id: TENANT_ID,
        name: TENANT_ID,
        slug: tenantSlugFromId(TENANT_ID),
        domain: DEPLOYMENT_DOMAIN,
        parentId: null,
      });
      await seedPrincipal(h.db, {
        id: CALLER_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
      });
      await seedAsset(h.db, {
        id: DEFINITION_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "asset-tool-restarts-wf",
        creatorPrincipalId: CALLER_PRINCIPAL_ID,
      });

      // Each step starts a fresh conversation, so this drives the tool once per
      // step.
      env = await startDeployFlowEnv({
        inferenceToolCall: { toolName: TOOL_NAME, input: {} },
        inferenceToolCallEachRun: true,
      });

      const blobPath = `tarballs/${fixture.tarballFilename}`;
      await env.hub.agentRepoStore.repoStore.initRepo(SOURCE_REPO_ID);
      await env.hub.agentRepoStore.repoStore.writeTree(
        HUB_PRINCIPAL,
        SOURCE_REPO_ID,
        DEFAULT_ASSET_REF,
        {
          files: { [blobPath]: fixture.bytes },
          message: "Seed the workflow package tarball",
        },
      );
      blobs = new Map([[blobPath, fixture.bytes]]);
    });

    afterAll(async () => {
      if (restarted !== undefined) {
        await terminateSidecarSubprocess(restarted);
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

    test("the tool runs before a child crash, after it, and after a sidecar restart", async () => {
      const source: WorkflowDefinitionAssetSource = {
        kind: "asset",
        assetId: SOURCE_ASSET_ID,
        package: { format: "tarball" },
      };
      const resolveAttachment = async (
        assetId: string,
      ): Promise<{ pack: Uint8Array; ref: string; commitSha: string }> => {
        if (assetId !== SOURCE_ASSET_ID) {
          throw new Error(`unexpected attachment request ${assetId}`);
        }
        const commitSha = await env.hub.agentRepoStore.repoStore.resolveRef(
          HUB_PRINCIPAL,
          SOURCE_REPO_ID,
          DEFAULT_ASSET_REF,
        );
        if (commitSha === null) {
          throw new Error("the source asset has no commit");
        }
        const { pack, ref } = await env.hub.agentRepoStore.repoStore.createPack(
          HUB_PRINCIPAL,
          SOURCE_REPO_ID,
          DEFAULT_ASSET_REF,
        );
        return { pack, ref, commitSha };
      };

      const approved = await installAndApproveWorkflowDefinition({
        source,
        pin: `${PACKAGE_NAME}@${PACKAGE_VERSION}`,
        entry: WORKFLOW_ENTRY,
        assetId: DEFINITION_ASSET_ID,
        approvals: createApprovalSet([
          "inference.source:anthropic:mock-model",
          "director:@intx/agent/default",
          `mail.address:${mailAddress}`,
          `mail.send:${DEPLOYMENT_DOMAIN}`,
          `tool:${TOOL_NAME}`,
        ]),
        router: env.hub.probeRouter,
        db: h.db,
        readBlob: async (blobPath) => {
          const bytes = blobs.get(blobPath);
          if (bytes === undefined) throw new Error(`no blob at ${blobPath}`);
          return bytes;
        },
        listBlobs: async (dir) =>
          dir === "tarballs"
            ? [...blobs.keys()].map((p) => p.replace(/^tarballs\//, ""))
            : [],
        resolveAttachment,
      });
      if (!approved.approval.ok) {
        throw new Error(
          `the gate did not approve: ${JSON.stringify(approved.approval)}\n${env.sidecarDiagnostics()}`,
        );
      }
      const topEntry = approved.closure.entries.find(
        (e) => e.name === PACKAGE_NAME,
      );
      if (topEntry?.source.kind !== "asset") {
        throw new Error("the workflow package is not asset-sourced");
      }
      expect(topEntry.source.package.format).toBe("tarball");

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        credentialId: "sk-mock",
        model: "mock-model",
      };
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentAddress: mailAddress,
        sources: [inferenceSource],
        defaultSource: inferenceSource.id,
      };
      const sources = {
        first: [inferenceSource],
        afterCrash: [inferenceSource],
        second: [inferenceSource],
        afterRestart: [inferenceSource],
        third: [inferenceSource],
      };
      await seedInferenceCredentials(h.db, TENANT_ID, sources, config);
      env.hub.setPrimaryAllocationIdentity(DEPLOYMENT_ID, mailAddress);
      await deployCodeSourcedWorkflow({
        approved,
        source,
        resolveAttachment,
        sidecarAllocationRouter: env.hub.router,
        allocationTarget: {
          allocationId: "allocation-integration-1",
          generation: 1,
        },
        agentAddress: mailAddress,
        config,
        sources,
        db: h.db,
        tenantId: TENANT_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        credentialCipher: createNoopCredentialCipher(),
      });
      const workflowRunRepoId: RepoId = {
        kind: "workflow-run",
        id: deriveWorkflowRunRepoId(mailAddress),
      };
      env.registerDeployment({
        anchorRunId: DEPLOYMENT_ID,
        workflowDefinition: {
          id: approved.projection.id,
          triggers: [{ type: "mail", to: mailAddress }],
          steps: {},
          stepOrder: [...approved.projection.stepOrder],
        },
        workflowRunRepoId,
        workflowRunRef: "refs/heads/main",
        mailAddress,
      });
      await requireRoutable();

      await fireMailTrigger(env, mailAddress, {
        messageId: "<asset-tool-restarts@integration.interchange>",
        grants: deriveWireRunGrants(approved.grantSnapshot),
      });
      const runId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
      });
      await waitFor(() => hasParkedAt(runId, "afterCrash"), {
        diagnostics: env.sidecarDiagnostics,
      });
      const beforeCrash = toolRunPids(1);

      await settleWorkflowRunPacks(env);
      const killedPids = killWorkflowHostChild(env);
      expect(killedPids).toEqual(expect.arrayContaining(beforeCrash));
      await waitFor(
        () =>
          listWorkflowHostChildren(env).some(
            (pid) => !killedPids.includes(pid),
          ),
        { diagnostics: env.sidecarDiagnostics },
      );
      expect(env.sidecar.proc.exitCode).toBeNull();
      await signalUntil(runId, "afterCrash", () =>
        hasParkedAt(runId, "afterRestart"),
      );
      // A killed pid can linger as a zombie, so being listed is not enough.
      const afterCrash = toolRunPids(2).slice(1);
      expect(killedPids.some((pid) => afterCrash.includes(pid))).toBe(false);
      expect(listWorkflowHostChildren(env)).toEqual(
        expect.arrayContaining(afterCrash),
      );

      const dataDir = await crashSidecar(env, mailAddress);
      await fs.rm(path.join(dataDir, "workflow-definition-closure-cache"), {
        recursive: true,
      });
      restarted = await restartSidecar(env, {
        address: mailAddress,
        dataDir,
        registerTempDir: (dir) => restartTempDirs.push(dir),
      });
      await signalUntil(runId, "afterRestart", () => hasTerminated(runId));

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { diagnostics: env.sidecarDiagnostics },
      );
      expect(terminal.type).toBe("RunCompleted");
      expect(new Set(toolRunPids(3)).size).toBe(3);
    }, 180_000);
  },
);
