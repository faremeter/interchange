// Route-to-run coverage for a workflow deploy sourced from a packed tarball.
//
// The definition is published as an npm tarball into a `package-registry`
// asset, the same substrate tool packages use, and deployed through the
// PRODUCTION `POST /workflows/deployments` route with `format: "tarball"` and a
// `name@range` pin. The tarball is written through the repo store, so the
// package-registry push validator admits it; the route admits the
// package-registry asset; the real session service resolves the pin from the
// asset's blobs and packs the asset for the probe; the deploy delivers it to a
// live sidecar process. The run is then fired by mail and asserted complete,
// so a 201 alone cannot pass this test: the route, the resolvers and the
// sidecar all have to read the same repository.

import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { eq } from "drizzle-orm";

import { createNoopCredentialCipher } from "@intx/crypto";
import { createGrantStore } from "@intx/db";
import {
  tenant as tenantTable,
  workflowRun as workflowRunTable,
} from "@intx/db/schema";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createAssetService,
  createSessionService,
  DEFAULT_ASSET_REF,
  deployCodeSourcedWorkflow,
  workflowSourceRepoKind,
  type EventCollectorRegistry,
  type PreparedWorkflowDeployer,
  type RepoId,
  type SessionService,
  type WorkflowAllocationService,
} from "@intx/hub-sessions";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";
import {
  buildInertProjectionStepSources,
  deriveRunAddress,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedGrant, seedPrincipal } from "@intx/test-harness/seed";

import {
  fireMailTrigger,
  seedInferenceCredentials,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { bundleWorkflowEntry } from "../hub-agent/lib/bundle-workflow-entry";
import {
  buildSyntheticNpmPackageTarball,
  type SyntheticNpmPackageTarball,
} from "../hub-agent/lib/synthetic-npm-package-tarball";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const TENANT_ID = "tnt_tarball_route";
const CALLER_USER_ID = "usr_tarball_route_caller";
const CALLER_PRINCIPAL_ID = "prn_tarball_route_caller";
const SOURCE_ASSET_ID = "ast_tarball_route_registry";
const STEP_ID = "run";
const WORKFLOW_RUN_REF = "refs/heads/main";

const PACKAGE_NAME = "@wf/tarball-route";
const PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "./workflow.mjs";
const PIN = `${PACKAGE_NAME}@^${PACKAGE_VERSION}`;

// The route mints the anchor run id, so the definition cannot carry the
// deployment's real address; the probe gate approves this placeholder and the
// trigger mail is fired at the address the route derives.
const PLACEHOLDER_ADDRESS = `placeholder@${DEPLOYMENT_DOMAIN}`;

const HUB_PRINCIPAL = { kind: "hub" } as const;

const workflowEntrySource = `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const agent = defineAgent({
  id: "tarball-route-agent",
  systemPrompt: "You are the tarball-route single-step agent.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: "wf_tarball_route",
  trigger: { type: "mail", to: ${JSON.stringify(PLACEHOLDER_ADDRESS)} },
  steps: {
    ${STEP_ID}: step({ agent }),
  },
});
`;

async function buildWorkflowTarball(
  scratchDir: string,
): Promise<SyntheticNpmPackageTarball> {
  return buildSyntheticNpmPackageTarball({
    packageName: PACKAGE_NAME,
    version: PACKAGE_VERSION,
    moduleFilename: path.basename(WORKFLOW_ENTRY),
    moduleSource: await bundleWorkflowEntry(scratchDir, workflowEntrySource),
    manifest: { interchange: { workflow: WORKFLOW_ENTRY } },
  });
}

function createMockGetSession(userId: string): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "caller@example.com",
      emailVerified: true,
      name: "Caller",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_tarball_route",
      userId,
      token: "tok_tarball_route",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`tarball-route mock: ${name} not implemented`);
}

function createMockEventCollectors(): EventCollectorRegistry {
  return {
    create: () => notImpl("create"),
    dispatch: () => notImpl("dispatch"),
    abandon: () => notImpl("abandon"),
    has: () => false,
    getStatus: () => undefined,
    getAccumulatedText: () => undefined,
    getLastTurnId: () => undefined,
    getCurrentTurnId: () => undefined,
  };
}

let env: DeployFlowEnv;
let h: TestDb;
let scratchDir: string;
let sessionService: SessionService & PreparedWorkflowDeployer;

describe.skipIf(!harnessDbEnvAvailable())(
  "a tarball-sourced workflow deploys through the real route and runs",
  () => {
    beforeAll(async () => {
      scratchDir = await fs.mkdtemp(path.join(os.tmpdir(), "tarball-route-"));
      const tarball = await buildWorkflowTarball(scratchDir);

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
        refId: CALLER_USER_ID,
        status: "active",
      });
      await seedGrant(h.db, {
        id: "grant-tarball-route-create",
        tenantId: TENANT_ID,
        resource: "workflow:*",
        action: "create",
        effect: "allow",
        origin: "system",
        principalId: CALLER_PRINCIPAL_ID,
      });
      await seedGrant(h.db, {
        id: "grant-tarball-route-asset-read",
        tenantId: TENANT_ID,
        resource: `asset:${SOURCE_ASSET_ID}`,
        action: "read",
        effect: "allow",
        origin: "system",
        principalId: CALLER_PRINCIPAL_ID,
      });

      env = await startDeployFlowEnv({});

      // The tarball goes through the repo store's write path, so the
      // package-registry push validator admits it the way an upload would.
      await seedAsset(h.db, {
        id: SOURCE_ASSET_ID,
        tenantId: TENANT_ID,
        kind: "package-registry",
        name: "tarball-route-registry",
        creatorPrincipalId: CALLER_PRINCIPAL_ID,
      });
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
          files: {
            [`tarballs/${tarball.tarballFilename}`]: tarball.bytes,
          },
          message: "Publish the tarball-route workflow package",
        },
      );

      const assetService = createAssetService({
        db: h.db,
        repoStore: env.hub.agentRepoStore.repoStore,
      });
      sessionService = createSessionService({
        sidecarRouter: env.hub.router,
        sidecarAllocationRouter: env.hub.router,
        agentRepoStore: env.hub.agentRepoStore,
        assetService,
        db: h.db,
        toolPackageRegistries: {
          httpRegistries: new Map([
            ["npmjs", { url: "https://registry.test" }],
          ]),
          defaultRegistry: "npmjs",
        },
      });
    });

    afterAll(async () => {
      if (env !== undefined) await env.teardown();
      if (h !== undefined) await h.close();
      if (scratchDir !== undefined) {
        await fs.rm(scratchDir, { recursive: true, force: true });
      }
    });

    test("accepted, probed, frozen, provisioned, and run to completion", async () => {
      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        credentialId: "sk-mock",
        model: "mock-model",
      };
      let deployed:
        | { anchorRunId: string; agentAddress: string; projectionId: string }
        | undefined;

      const workflowAllocationService: WorkflowAllocationService = {
        async prepareProvisionedDeployment(args) {
          if (args.source.kind !== "asset") {
            throw new Error("tarball-route test requires an asset source");
          }
          const source: WorkflowDefinitionAssetSource = args.source;
          const agentAddress = deriveRunAddress({
            runId: args.anchorRunId,
            domain: args.deploymentDomain,
          });
          const allocationTarget = env.hub.prepareAllocationIdentity(
            args.anchorRunId,
            agentAddress,
          );
          const approved = await sessionService.installAndApproveWorkflowSource(
            {
              source,
              entry: args.entry,
              ...(args.pin !== undefined ? { pin: args.pin } : {}),
              definitionAssetId: args.definitionAssetId,
              allocationTarget,
            },
          );
          if (!approved.approval.ok) {
            throw new Error(
              `tarball-route probe was not approved: ${approved.approval.reason}`,
            );
          }
          const config: HarnessConfig = {
            sessionId: args.sessionId,
            agentId: args.anchorRunId,
            tenantId: args.tenantId,
            principalId: args.sourceAuthorityPrincipalId,
            agentAddress,
            systemPrompt: "",
            tools: [],
            grants: [],
            sources: [inferenceSource],
            defaultSource: inferenceSource.id,
          };
          const sources = buildInertProjectionStepSources({
            projection: approved.projection,
            config,
            operatorApprovals: approved.approval.approvedSurface,
          });
          await seedInferenceCredentials(h.db, args.tenantId, sources, config);

          const repoId: RepoId = {
            kind: workflowSourceRepoKind(source),
            id: source.assetId,
          };
          const resolveAttachment = async (requestedAssetId: string) => {
            if (requestedAssetId !== source.assetId) {
              throw new Error(
                `tarball-route test received unexpected asset ${requestedAssetId}`,
              );
            }
            const commitSha = await env.hub.agentRepoStore.repoStore.resolveRef(
              HUB_PRINCIPAL,
              repoId,
              DEFAULT_ASSET_REF,
            );
            if (commitSha === null) {
              throw new Error(
                `tarball-route source ${requestedAssetId} is empty`,
              );
            }
            const { pack, ref } =
              await env.hub.agentRepoStore.repoStore.createPack(
                HUB_PRINCIPAL,
                repoId,
                DEFAULT_ASSET_REF,
              );
            return { pack, ref, commitSha };
          };
          await deployCodeSourcedWorkflow({
            approved,
            source,
            resolveAttachment,
            sidecarAllocationRouter: env.hub.router,
            allocationTarget,
            agentAddress,
            config,
            sources,
            db: h.db,
            tenantId: args.tenantId,
            anchorRunId: args.anchorRunId,
            deploymentDomain: args.deploymentDomain,
            credentialCipher: createNoopCredentialCipher(),
          });
          deployed = {
            anchorRunId: args.anchorRunId,
            agentAddress,
            projectionId: approved.projection.id,
          };
          return {
            anchorRunId: args.anchorRunId,
            deploymentAddress: agentAddress,
            allocationId: allocationTarget.allocationId,
            status: "pending",
          };
        },
        async deployReadyAllocation() {
          throw new Error("tarball-route test does not reconcile allocations");
        },
      };

      const app = createApp({
        getSession: createMockGetSession(CALLER_USER_ID),
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        grantStore: createGrantStore(h.db),
        sidecarRouter: env.hub.router,
        sessionService,
        workflowAllocationService,
        eventCollectors: createMockEventCollectors(),
        assetService: createAssetService({
          db: h.db,
          repoStore: env.hub.agentRepoStore.repoStore,
        }),
        repoStore: env.hub.agentRepoStore.repoStore,
        maxTarballBytes: 10_000_000,
      });

      const source: WorkflowDefinitionAssetSource = {
        kind: "asset",
        assetId: SOURCE_ASSET_ID,
        package: { format: "tarball" },
      };
      const res = await app.request(
        `/api/tenants/${TENANT_ID}/workflows/deployments`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            source,
            entry: WORKFLOW_ENTRY,
            pin: PIN,
            sourceOfferingIds: [inferenceSource.id],
            defaultSourceOfferingId: inferenceSource.id,
          }),
        },
      );
      if (res.status !== 201) {
        const body: unknown = await res.json();
        throw new Error(
          `expected 201 from the deploy route, got ${String(res.status)}: ${JSON.stringify(body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      if (deployed === undefined) {
        throw new Error("the route returned 201 without deploying");
      }
      const { anchorRunId, agentAddress, projectionId } = deployed;

      const anchorRow = await h.db
        .select({ address: workflowRunTable.address })
        .from(workflowRunTable)
        .where(eq(workflowRunTable.id, anchorRunId))
        .limit(1)
        .then((rows) => rows[0]);
      expect(anchorRow?.address).toBe(agentAddress);

      const workflowRunRepoId: RepoId = {
        kind: "workflow-run",
        id: deriveDeploymentId(agentAddress),
      };
      env.registerDeployment({
        anchorRunId,
        workflowDefinition: {
          id: projectionId,
          triggers: [{ type: "mail", to: agentAddress }],
          steps: {},
          stepOrder: [STEP_ID],
        },
        workflowRunRepoId,
        workflowRunRef: WORKFLOW_RUN_REF,
        mailAddress: agentAddress,
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(agentAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, agentAddress, {
        messageId: "<tarball-route-e2e@integration.interchange>",
      });
      const runId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
      });
      const terminal = await waitForWorkflowRunComplete(
        env,
        anchorRunId,
        runId,
        { diagnostics: env.sidecarDiagnostics },
      );
      if (terminal.type !== "RunCompleted") {
        throw new Error(
          `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(terminal.body)}\n${env.sidecarDiagnostics()}`,
        );
      }
    }, 90_000);
  },
);
