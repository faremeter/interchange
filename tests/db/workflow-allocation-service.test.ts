import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { createEnvKeyCredentialCipher, generateKeyPair } from "@intx/crypto";
import {
  createSidecarAllocationStore,
  createWorkflowRunLaunchSpecStore,
} from "@intx/db";
import { workflowDefinition, workflowRun } from "@intx/db/schema";
import { eq } from "drizzle-orm";
import {
  createAgentRepoStore,
  createSessionService,
  createSidecarPluginRegistry,
  createSidecarRouter,
  createWorkflowAllocationService,
  SessionLaunchError,
  type DeployPreparedCodeSourcedWorkflowParams,
  type InstallAndApproveResult,
  type SidecarAuthIdentity,
  type SidecarProvisioner,
  type WsHandle,
} from "@intx/hub-sessions";
import { credentialAad, type SidecarCapabilityRule } from "@intx/types";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import {
  deriveRunAddress,
  WorkflowDefinitionInvalidError,
} from "@intx/workflow-deploy";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedCredential,
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedPrincipal,
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";

const TENANT_ID = "tnt-workflow-allocation";
const PRINCIPAL_ID = "prn-workflow-allocation";
const ASSET_ID = "ast-workflow-allocation";
const DEFINITION_ID = "wfd-workflow-allocation";
const OFFERING_ID = "mof-workflow-allocation";
const CREDENTIAL_ID = "cred-workflow-allocation";
const CREDENTIAL_SECRET = "secret-only-resolved-at-launch";
const CREDENTIAL_CIPHER = createEnvKeyCredentialCipher(
  new Uint8Array(32).fill(7),
);
const DEPLOY_COMMIT = "c0ffee".padEnd(40, "0");

const provisioner: SidecarProvisioner = {
  id: "test-provisioner",
  apiVersion: 1,
  bindingFingerprint: "test-provisioner:v1",
  capabilities: [],
  async ensure() {
    return { kind: "accepted" };
  },
  async destroy() {
    return { kind: "destroyed" };
  },
};

// A code-sourced deploy: the workflow asset's own git subtree at a pinned
// commit, evaluated at `entry`. Provisioning freezes the probe of THIS
// source on shared capacity, then deploys the frozen bundle to the allocation.
const SOURCE: WorkflowDefinitionSource = {
  kind: "asset",
  assetId: ASSET_ID,
  package: { format: "source", commitSha: DEPLOY_COMMIT },
};
const ENTRY = "./workflow.mjs";

// A minimal, valid frozen approval bundle: the projection and closure are the
// smallest shapes their arktype schemas accept, and the grant set + wire hash
// are inert strings. No credential secret ever enters the bundle -- sources are
// re-resolved from the launch spec's offering ids at deploy time.
function frozenApproval(
  capabilities: readonly SidecarCapabilityRule[] = [],
): InstallAndApproveResult {
  const projection = {
    id: "wf-x",
    triggers: [],
    stepOrder: [],
    steps: {},
    ...(capabilities.length > 0
      ? { sidecarPlacement: { capabilities: [...capabilities] } }
      : {}),
  };
  return {
    approval: {
      ok: true,
      definitionId: DEFINITION_ID,
      approvedWireHash: "a".repeat(64),
      approvedGrants: new Set<string>(),
      projection,
    },
    projection,
    closure: { schemaVersion: "1", topLevel: [], entries: [] },
  };
}

function frozenApprovalWithSource(
  provider: string,
  model: string,
): InstallAndApproveResult {
  const projection = {
    id: "wf-source-mismatch",
    triggers: [],
    stepOrder: ["only"],
    steps: {
      only: {
        kind: "step",
        agent: { modelSources: [{ provider, model }] },
      },
    },
  };
  return {
    approval: {
      ok: true,
      definitionId: DEFINITION_ID,
      approvedWireHash: "a".repeat(64),
      approvedGrants: new Set([`inference.source:${provider}:${model}`]),
      projection,
    },
    projection,
    closure: { schemaVersion: "1", topLevel: [], entries: [] },
  };
}

// The frozen bundle's inert closure. Shared by the deploy-path assertions
// below: the source-ref deploy frame must carry this shape verbatim.
const FROZEN_CLOSURE = { schemaVersion: "1", topLevel: [], entries: [] };
const FROZEN_WIRE_HASH = "a".repeat(64);

const ANCHOR_RUN_ID = "dep-workflow-allocation";
const ALLOCATION_ID = "sal-workflow-allocation";
const SIDECAR_ID = "sc-workflow-allocation";
const SESSION_ID = "ses-workflow-allocation";
const DEPLOYMENT_DOMAIN = `${TENANT_ID}.example.test`;
const WORKFLOW_RUN_ADDRESS = deriveRunAddress({
  runId: ANCHOR_RUN_ID,
  domain: DEPLOYMENT_DOMAIN,
});
// A 32-byte hex Ed25519 hub key; the source-ref deploy frame carries it, so the
// router refuses to emit a deploy without one.
const TEST_HUB_KEY = "ab".repeat(32);
// The supervisor public key the allocated worker acks the deploy with.
const SUPERVISOR_PUBLIC_KEY = "b".repeat(64);

const tick = (): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, 0));

function createMockWs(): WsHandle & { sent: string[]; closed: boolean } {
  return {
    sent: [],
    closed: false,
    send(data: string) {
      this.sent.push(data);
    },
    close() {
      this.closed = true;
    },
  };
}

// Poll the mock socket until the `agent.deploy` frame lands. The deploy path
// awaits several DB round-trips (anchor read, launch-spec read, source
// re-resolution) before it emits the frame, so a single microtask tick is not
// enough; poll on a short interval up to a bounded ceiling instead.
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- the wire frame is JSON off the mock socket; the assertions below narrow the fields they read
async function waitForDeployFrame(ws: { sent: string[] }): Promise<any> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const frame = ws.sent
      .map((raw) => JSON.parse(raw))
      .find((parsed) => parsed.type === "agent.deploy");
    if (frame !== undefined) return frame;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("mock socket never received an agent.deploy frame");
}

describe.skipIf(!harnessDbEnvAvailable())(
  "workflow allocation service (real DB)",
  () => {
    let h: TestDb;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedPrincipal(h.db, {
        id: PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
      });
      await seedAsset(h.db, {
        id: ASSET_ID,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "workflow-allocation",
        creatorPrincipalId: PRINCIPAL_ID,
      });
      await seedProvider(h.db, {
        id: "prv-workflow-allocation",
        tenantId: TENANT_ID,
        name: "credential-provider",
      });
      await seedCredential(h.db, {
        id: CREDENTIAL_ID,
        tenantId: TENANT_ID,
        providerId: "prv-workflow-allocation",
        name: "credential",
        secret: await CREDENTIAL_CIPHER.encrypt(
          CREDENTIAL_SECRET,
          credentialAad(CREDENTIAL_ID, "secret"),
        ),
      });
      await seedModel(h.db, {
        id: "mdl-workflow-allocation",
        tenantId: TENANT_ID,
        canonicalName: "opus",
      });
      await seedModelProvider(h.db, {
        id: "mpv-workflow-allocation",
        tenantId: TENANT_ID,
        name: "anthropic",
        credentialId: CREDENTIAL_ID,
      });
      await seedModelOffering(h.db, {
        id: OFFERING_ID,
        tenantId: TENANT_ID,
        modelId: "mdl-workflow-allocation",
        providerId: "mpv-workflow-allocation",
      });
    });

    test("freezes a secret-free bundle at prepare and deploys it for the accepted generation", async () => {
      const installCalls: {
        source: WorkflowDefinitionSource;
        entry: string;
      }[] = [];
      const deployCalls: DeployPreparedCodeSourcedWorkflowParams[] = [];
      let workflowActive = false;
      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: {
          getDefaultProvisioner: () => provisioner,
          getProvisioner: (id) => (id === provisioner.id ? provisioner : null),
          selectProvisioner: () => ({ ok: true, provisioner }),
        },
        preparedDeployer: {
          // Preparation freezes the definition before provisioning: the
          // real path persists a workflow_definition row and returns its id, so
          // the mock does the same (the anchor row FKs to it).
          installAndApproveWorkflowSource: async (params) => {
            installCalls.push({ source: params.source, entry: params.entry });
            await h.db
              .insert(workflowDefinition)
              .values({
                id: DEFINITION_ID,
                tenantId: TENANT_ID,
                name: "workflow-allocation-def",
              })
              .onConflictDoNothing({ target: workflowDefinition.id });
            return frozenApproval();
          },
          deployPreparedCodeSourcedWorkflow: async (params) => {
            deployCalls.push(params);
            const result = {
              anchorRunId: params.anchorRunId,
              deploymentAddress: params.agentAddress,
              publicKey: "supervisor-public-key",
            };
            await h.db
              .update(workflowRun)
              .set({ publicKey: result.publicKey })
              .where(eq(workflowRun.id, params.anchorRunId));
            return result;
          },
        },
        credentialCipher: CREDENTIAL_CIPHER,
        allocationRouter: {
          isAllocatedWorkflowActive: async () => workflowActive,
        },
        createAllocationId: () => "sal-workflow-allocation",
        now: () => new Date("2026-08-03T12:00:00.000Z"),
      });

      const prepared = await service.prepareProvisionedDeployment({
        tenantId: TENANT_ID,
        anchorRunId: "dep-workflow-allocation",
        deploymentDomain: `${TENANT_ID}.example.test`,
        source: SOURCE,
        entry: ENTRY,
        definitionAssetId: ASSET_ID,
        sessionId: "ses-workflow-allocation",
        sourceAuthorityPrincipalId: PRINCIPAL_ID,
        sourceOfferingIds: [OFFERING_ID],
        defaultSourceOfferingId: OFFERING_ID,
        deployContent: { systemPrompt: "" },
      });

      expect(prepared.status).toBe("pending");
      // The probe/freeze rode shared capacity at prepare time, carrying the
      // caller's source + entry.
      expect(installCalls).toEqual([{ source: SOURCE, entry: ENTRY }]);

      const spec = await createWorkflowRunLaunchSpecStore(h.db).get(
        prepared.anchorRunId,
      );
      expect(spec?.sourceOfferingIds).toEqual([OFFERING_ID]);
      // The frozen bundle is the recovery input; it names the source but holds
      // no resolved inference source, so no credential secret is persisted.
      expect(spec?.frozenApprovalBundle.source).toEqual(SOURCE);
      expect(spec?.frozenApprovalBundle.approvedWireHash).toBe("a".repeat(64));
      expect(JSON.stringify(spec)).not.toContain(CREDENTIAL_SECRET);

      const allocations = createSidecarAllocationStore(h.db);
      const bound = await allocations.bindInitialSidecar({
        allocationId: prepared.allocationId,
        expectedGeneration: 0,
        sidecarId: "sc-workflow-allocation",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        connectDeadline: new Date("2026-08-03T12:02:00.000Z"),
      });
      expect(bound?.generation).toBe(1);
      const allocated = await allocations.markAllocated({
        allocationId: prepared.allocationId,
        generation: 1,
      });
      if (allocated === null) throw new Error("allocation was not accepted");

      await service.deployReadyAllocation(allocated);

      expect(deployCalls).toHaveLength(1);
      expect(deployCalls[0]?.allocationTarget).toEqual({
        allocationId: prepared.allocationId,
        generation: 1,
      });
      expect(deployCalls[0]?.source).toEqual(SOURCE);
      // The inference chain is re-resolved from the launch spec's offering ids
      // at deploy time, decrypting the credential secret only now.
      expect(deployCalls[0]?.config).toMatchObject({
        sessionId: "ses-workflow-allocation",
        principalId: PRINCIPAL_ID,
        defaultSource: OFFERING_ID,
        sources: [
          {
            id: OFFERING_ID,
            credentialId: CREDENTIAL_ID,
            model: "opus",
          },
        ],
      });

      workflowActive = true;
      await expect(
        service.deployReadyAllocation(allocated),
      ).resolves.toBeNull();
      expect(deployCalls).toHaveLength(1);

      await h.db
        .update(workflowRun)
        .set({ publicKey: null })
        .where(eq(workflowRun.id, prepared.anchorRunId));
      const error = await service
        .deployReadyAllocation(allocated)
        .catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(SessionLaunchError);
      if (!(error instanceof SessionLaunchError)) {
        throw new Error("unreachable");
      }
      expect(error.leakedAgent).toBe(true);
      expect(deployCalls).toHaveLength(1);
    });

    test("rejects an invalid source chain before creating deployment state", async () => {
      const anchorRunId = "dep-invalid-source-chain";
      let selectionAttempted = false;
      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: {
          getDefaultProvisioner: () => provisioner,
          getProvisioner: (id) => (id === provisioner.id ? provisioner : null),
          selectProvisioner: () => {
            selectionAttempted = true;
            return { ok: true, provisioner };
          },
        },
        preparedDeployer: {
          installAndApproveWorkflowSource: async () => {
            await h.db.insert(workflowDefinition).values({
              id: DEFINITION_ID,
              tenantId: TENANT_ID,
              name: "workflow-allocation-def",
            });
            return frozenApprovalWithSource("openai", "gpt-5");
          },
          deployPreparedCodeSourcedWorkflow: async () => {
            throw new Error("invalid source chain must not reach deployment");
          },
        },
        credentialCipher: CREDENTIAL_CIPHER,
        allocationRouter: {
          isAllocatedWorkflowActive: async () => false,
        },
        createAllocationId: () => "sal-invalid-source-chain",
      });

      await expect(
        service.prepareProvisionedDeployment({
          tenantId: TENANT_ID,
          anchorRunId,
          deploymentDomain: `${TENANT_ID}.example.test`,
          source: SOURCE,
          entry: ENTRY,
          definitionAssetId: ASSET_ID,
          sessionId: "ses-invalid-source-chain",
          sourceAuthorityPrincipalId: PRINCIPAL_ID,
          sourceOfferingIds: [OFFERING_ID],
          defaultSourceOfferingId: OFFERING_ID,
          deployContent: { systemPrompt: "" },
        }),
      ).rejects.toBeInstanceOf(WorkflowDefinitionInvalidError);

      expect(selectionAttempted).toBe(false);
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, anchorRunId),
        }),
      ).toBeUndefined();
      expect(
        await createSidecarAllocationStore(h.db).findByAnchorRunId(anchorRunId),
      ).toBeNull();
      expect(
        await createWorkflowRunLaunchSpecStore(h.db).get(anchorRunId),
      ).toBeNull();
    });

    test("persists the provisioner selected from workflow capabilities", async () => {
      const containers: SidecarProvisioner = {
        ...provisioner,
        id: "containers",
        bindingFingerprint: "containers:v1",
        capabilities: [{ capability: "platform:ios", state: "blocked" }],
      };
      const ios: SidecarProvisioner = {
        ...provisioner,
        id: "ios",
        bindingFingerprint: "ios:v1",
        capabilities: [{ capability: "platform:ios", state: "available" }],
      };
      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: createSidecarPluginRegistry({
          provisioners: [containers, ios],
          defaultProvisionerId: containers.id,
        }),
        preparedDeployer: {
          installAndApproveWorkflowSource: async () => {
            await h.db.insert(workflowDefinition).values({
              id: DEFINITION_ID,
              tenantId: TENANT_ID,
              name: "workflow-allocation-def",
            });
            return frozenApproval([
              { capability: "platform:ios", effect: "require" },
            ]);
          },
          deployPreparedCodeSourcedWorkflow: async () => {
            throw new Error("allocation is not ready during preparation");
          },
        },
        credentialCipher: CREDENTIAL_CIPHER,
        allocationRouter: {
          isAllocatedWorkflowActive: async () => false,
        },
        createAllocationId: () => "sal-capability-selection",
      });

      await service.prepareProvisionedDeployment({
        tenantId: TENANT_ID,
        anchorRunId: "dep-capability-selection",
        deploymentDomain: `${TENANT_ID}.example.test`,
        source: SOURCE,
        entry: ENTRY,
        definitionAssetId: ASSET_ID,
        sessionId: "ses-capability-selection",
        sourceAuthorityPrincipalId: PRINCIPAL_ID,
        sourceOfferingIds: [OFFERING_ID],
        defaultSourceOfferingId: OFFERING_ID,
        deployContent: { systemPrompt: "" },
      });

      const allocation = await createSidecarAllocationStore(
        h.db,
      ).findByAnchorRunId("dep-capability-selection");
      expect(allocation?.provisionerId).toBe(ios.id);
      expect(allocation?.provisionerBindingFingerprint).toBe(
        ios.bindingFingerprint,
      );
    });

    // === Real allocated-deploy routing =====================================
    //
    // The case above drives a FAKE `deployPreparedCodeSourcedWorkflow` and
    // asserts the deploy CALL's arguments; it never routes the frozen bundle
    // through a real deploy path. The two cases below wire the REAL
    // `deployPreparedCodeSourcedWorkflow` (from `createSessionService`) into the
    // allocation service, and route it through the REAL sidecar allocation
    // router onto a connected (mock) allocated worker. That proves the frozen
    // bundle's source ref + closure + wire hash reach the
    // `sendAgentDeployToAllocation` wire frame intact -- the routing the fake
    // deployer never exercises.

    // A source-ref freeze synthesized on shared capacity. Inserting the
    // definition row mirrors the real freeze (the anchor row FKs to it); the
    // returned bundle carries the source, the inert closure, and the wire hash,
    // but no resolved inference source, so no credential secret is persisted.
    async function freezeSourceBundle(): Promise<InstallAndApproveResult> {
      await h.db
        .insert(workflowDefinition)
        .values({
          id: DEFINITION_ID,
          tenantId: TENANT_ID,
          name: "workflow-allocation-def",
        })
        .onConflictDoNothing({ target: workflowDefinition.id });
      return frozenApproval();
    }

    // Stand up a real sidecar allocation router with a connected allocated
    // worker (a mock socket), plus the real session-service prepared deployer.
    // The worker is fenced and registered at generation 1 so
    // `sendAgentDeployToAllocation` resolves the exact generation.
    async function startAllocatedWorker(): Promise<{
      router: ReturnType<typeof createSidecarRouter>;
      ws: ReturnType<typeof createMockWs>;
      deployPreparedCodeSourcedWorkflow: ReturnType<
        typeof createSessionService
      >["deployPreparedCodeSourcedWorkflow"];
      dataDir: string;
    }> {
      const dataDir = await fs.promises.mkdtemp(
        path.join(os.tmpdir(), "alloc-deploy-"),
      );
      const agentRepoStore = createAgentRepoStore({
        dataDir,
        signingKey: await generateKeyPair(),
      });
      const identity: Extract<SidecarAuthIdentity, { kind: "allocated" }> = {
        kind: "allocated",
        sidecarId: SIDECAR_ID,
        allocationId: ALLOCATION_ID,
        tenantId: TENANT_ID,
        anchorRunId: ANCHOR_RUN_ID,
        workflowRunAddress: WORKFLOW_RUN_ADDRESS,
        generation: 1,
      };
      const router = createSidecarRouter({
        hubPublicKey: TEST_HUB_KEY,
        authenticateSidecar: async () => identity,
        validateSidecarIdentity: async () => true,
        requestTimeoutMs: 5_000,
      });
      const sessionService = createSessionService({
        sidecarRouter: router,
        sidecarAllocationRouter: router,
        agentRepoStore,
        db: h.db,
      });

      router.fenceAllocation(ALLOCATION_ID, 1);
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: SIDECAR_ID,
          token: "token",
          agentAddresses: [],
        }),
      );
      await tick();

      return {
        router,
        ws,
        deployPreparedCodeSourcedWorkflow:
          sessionService.deployPreparedCodeSourcedWorkflow,
        dataDir,
      };
    }

    function makeService(overrides: {
      router: Pick<
        ReturnType<typeof createSidecarRouter>,
        "isAllocatedWorkflowActive"
      >;
      installAndApproveWorkflowSource: () => Promise<InstallAndApproveResult>;
      deployPreparedCodeSourcedWorkflow: ReturnType<
        typeof createSessionService
      >["deployPreparedCodeSourcedWorkflow"];
    }) {
      return createWorkflowAllocationService({
        db: h.db,
        plugins: {
          getDefaultProvisioner: () => provisioner,
          getProvisioner: (id) => (id === provisioner.id ? provisioner : null),
          selectProvisioner: () => ({ ok: true, provisioner }),
        },
        preparedDeployer: {
          installAndApproveWorkflowSource:
            overrides.installAndApproveWorkflowSource,
          deployPreparedCodeSourcedWorkflow:
            overrides.deployPreparedCodeSourcedWorkflow,
        },
        credentialCipher: CREDENTIAL_CIPHER,
        allocationRouter: overrides.router,
        createAllocationId: () => ALLOCATION_ID,
        now: () => new Date("2026-08-03T12:00:00.000Z"),
      });
    }

    const prepareArgs = {
      tenantId: TENANT_ID,
      anchorRunId: ANCHOR_RUN_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      source: SOURCE,
      entry: ENTRY,
      definitionAssetId: ASSET_ID,
      sessionId: SESSION_ID,
      sourceAuthorityPrincipalId: PRINCIPAL_ID,
      sourceOfferingIds: [OFFERING_ID],
      defaultSourceOfferingId: OFFERING_ID,
      deployContent: { systemPrompt: "" },
    } as const;

    async function bindAndAllocate(allocationId: string) {
      const allocations = createSidecarAllocationStore(h.db);
      const bound = await allocations.bindInitialSidecar({
        allocationId,
        expectedGeneration: 0,
        sidecarId: SIDECAR_ID,
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        connectDeadline: new Date("2026-08-03T12:02:00.000Z"),
      });
      if (bound?.generation !== 1) {
        throw new Error("expected generation 1 after binding the sidecar");
      }
      const allocated = await allocations.markAllocated({
        allocationId,
        generation: 1,
      });
      if (allocated === null) throw new Error("allocation was not accepted");
      return allocated;
    }

    // Drive a ready allocation's deploy through the real router: wait for the
    // frame to land on the worker's socket, ack it, and settle the deploy. The
    // deploy promise is always awaited before returning so an in-flight DB
    // query can never dangle into the next test's `h.reset()`.
    async function deployAndAck(
      service: ReturnType<typeof createWorkflowAllocationService>,
      allocated: Parameters<
        ReturnType<
          typeof createWorkflowAllocationService
        >["deployReadyAllocation"]
      >[0],
      router: ReturnType<typeof createSidecarRouter>,
      ws: ReturnType<typeof createMockWs>,
    ) {
      const deployPromise = service.deployReadyAllocation(allocated);
      try {
        const frame = await waitForDeployFrame(ws);
        router.handleMessage(
          ws,
          JSON.stringify({
            type: "agent.deploy.ack",
            agentAddress: WORKFLOW_RUN_ADDRESS,
            publicKey: SUPERVISOR_PUBLIC_KEY,
          }),
        );
        const result = await deployPromise;
        return { frame, result };
      } catch (err) {
        await deployPromise.catch(() => undefined);
        throw err;
      }
    }

    test("routes the frozen source ref and closure to the allocated worker intact", async () => {
      const { router, ws, deployPreparedCodeSourcedWorkflow, dataDir } =
        await startAllocatedWorker();
      try {
        const service = makeService({
          router,
          installAndApproveWorkflowSource: freezeSourceBundle,
          deployPreparedCodeSourcedWorkflow,
        });

        const prepared =
          await service.prepareProvisionedDeployment(prepareArgs);
        expect(prepared.deploymentAddress).toBe(WORKFLOW_RUN_ADDRESS);
        const allocated = await bindAndAllocate(prepared.allocationId);

        const { frame, result } = await deployAndAck(
          service,
          allocated,
          router,
          ws,
        );
        if (result === null) {
          throw new Error("expected a deploy result, not a no-op");
        }

        // The frame reached the allocated worker's socket, addressed to the
        // deployment's own run address.
        expect(frame.type).toBe("agent.deploy");
        expect(frame.agentAddress).toBe(WORKFLOW_RUN_ADDRESS);
        expect(frame.hubPublicKey).toBe(TEST_HUB_KEY);
        // The frozen bundle's source ref + closure + wire hash ride verbatim.
        expect(frame.workflow.sourceRef.source).toEqual(SOURCE);
        expect(frame.workflow.sourceRef.closure).toEqual(FROZEN_CLOSURE);
        expect(frame.workflow.approvedWireHash).toBe(FROZEN_WIRE_HASH);
        // The inference chain is re-resolved from the offering ids at deploy
        // time, decrypting the credential only now.
        expect(frame.config).toMatchObject({
          sessionId: SESSION_ID,
          principalId: PRINCIPAL_ID,
          defaultSource: OFFERING_ID,
          sources: [
            { id: OFFERING_ID, credentialId: CREDENTIAL_ID, model: "opus" },
          ],
        });

        expect(result.publicKey).toBe(SUPERVISOR_PUBLIC_KEY);
        // The ack's key was stamped onto the prepared anchor under the
        // allocation-ownership lock.
        const anchor = await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, ANCHOR_RUN_ID),
          columns: { publicKey: true },
        });
        expect(anchor?.publicKey).toBe(SUPERVISOR_PUBLIC_KEY);
      } finally {
        await fs.promises.rm(dataDir, { recursive: true, force: true });
      }
    });

    test("rehydrates a persisted frozen bundle and re-deploys after a fresh service is constructed", async () => {
      const { router, ws, deployPreparedCodeSourcedWorkflow, dataDir } =
        await startAllocatedWorker();
      try {
        // Prepare (freeze + persist) with one service instance.
        const prepareService = makeService({
          router,
          installAndApproveWorkflowSource: freezeSourceBundle,
          deployPreparedCodeSourcedWorkflow,
        });
        const prepared =
          await prepareService.prepareProvisionedDeployment(prepareArgs);
        const allocated = await bindAndAllocate(prepared.allocationId);

        // Deploy with a FRESH service instance whose freeze step throws: a
        // recovery worker re-deploys from the persisted launch spec alone and
        // never re-probes the source.
        const recoverService = makeService({
          router,
          installAndApproveWorkflowSource: () => {
            throw new Error("recovery deploy must not re-freeze the source");
          },
          deployPreparedCodeSourcedWorkflow,
        });

        const { frame, result } = await deployAndAck(
          recoverService,
          allocated,
          router,
          ws,
        );
        if (result === null) {
          throw new Error("expected a deploy result, not a no-op");
        }

        // The frozen source ref + closure survived the launch-spec round-trip
        // through the DB and reached the allocated deploy frame intact.
        expect(frame.type).toBe("agent.deploy");
        expect(frame.agentAddress).toBe(WORKFLOW_RUN_ADDRESS);
        expect(frame.workflow.sourceRef.source).toEqual(SOURCE);
        expect(frame.workflow.sourceRef.closure).toEqual(FROZEN_CLOSURE);
        expect(frame.workflow.approvedWireHash).toBe(FROZEN_WIRE_HASH);
        expect(result.publicKey).toBe(SUPERVISOR_PUBLIC_KEY);
      } finally {
        await fs.promises.rm(dataDir, { recursive: true, force: true });
      }
    });
  },
);
