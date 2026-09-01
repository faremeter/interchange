import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createEnvKeyCredentialCipher } from "@intx/crypto";
import {
  createSidecarAllocationStore,
  createWorkflowProbeStore,
  createWorkflowRunLaunchSpecStore,
} from "@intx/db";
import {
  workflowDefinition,
  workflowProbe,
  workflowRun,
} from "@intx/db/schema";
import { eq } from "drizzle-orm";
import {
  createSidecarCredentialResolver,
  createSidecarPluginRegistry,
  createSidecarRouter,
  createWorkflowAllocationService,
  type InstallAndApproveWorkflowSourceParams,
  type SidecarProvisioner,
} from "@intx/hub-sessions";
import { credentialAad, type SidecarCapabilityRule } from "@intx/types";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
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
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT_ID = "tnt-workflow-probe";
const PRINCIPAL_ID = "prn-workflow-probe";
const ASSET_ID = "ast-workflow-probe";
const DEFINITION_ID = "wfd-workflow-probe";
const OFFERING_ID = "mof-workflow-probe";
const CREDENTIAL_ID = "cred-workflow-probe";
const CREDENTIAL_SECRET = "probe-test-secret";
const CIPHER = createEnvKeyCredentialCipher(new Uint8Array(32).fill(7));
const SOURCE: WorkflowDefinitionSource = {
  kind: "asset",
  assetId: ASSET_ID,
  package: {
    format: "source",
    commitSha: "c0ffee".padEnd(40, "0"),
  },
};

function projection(capabilities: readonly SidecarCapabilityRule[] = []) {
  return {
    id: "wf-probed",
    triggers: [],
    stepOrder: [],
    steps: {},
    ...(capabilities.length > 0
      ? { sidecarPlacement: { capabilities: [...capabilities] } }
      : {}),
  };
}

function probeResult(capabilities: readonly SidecarCapabilityRule[] = []) {
  return {
    projection: projection(capabilities),
    grants: [] as string[],
    grantWalkSnapshot: { perStep: [], grantRequirements: [] },
    wireHash: "a".repeat(64),
  };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "workflow probe allocation (real DB)",
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
        name: "workflow-probe",
        creatorPrincipalId: PRINCIPAL_ID,
      });
      await seedProvider(h.db, {
        id: "prv-workflow-probe",
        tenantId: TENANT_ID,
        name: "credential-provider",
      });
      await seedCredential(h.db, {
        id: CREDENTIAL_ID,
        tenantId: TENANT_ID,
        providerId: "prv-workflow-probe",
        name: "credential",
        secret: await CIPHER.encrypt(
          CREDENTIAL_SECRET,
          credentialAad(CREDENTIAL_ID, "secret"),
        ),
      });
      await seedModel(h.db, {
        id: "mdl-workflow-probe",
        tenantId: TENANT_ID,
        canonicalName: "opus",
      });
      await seedModelProvider(h.db, {
        id: "mpv-workflow-probe",
        tenantId: TENANT_ID,
        name: "anthropic",
        credentialId: CREDENTIAL_ID,
      });
      await seedModelOffering(h.db, {
        id: OFFERING_ID,
        tenantId: TENANT_ID,
        modelId: "mdl-workflow-probe",
        providerId: "mpv-workflow-probe",
      });
    });

    function makeProvisioner(args: {
      id: string;
      capabilities?: SidecarProvisioner["capabilities"];
      ensureCalls: unknown[];
      destroyCalls: unknown[];
    }): SidecarProvisioner {
      return {
        id: args.id,
        apiVersion: 1,
        bindingFingerprint: `${args.id}:v1`,
        capabilities: args.capabilities ?? [],
        async ensure(request) {
          args.ensureCalls.push(request);
          return { kind: "accepted", externalRef: `${args.id}-external` };
        },
        async destroy(request) {
          args.destroyCalls.push(request);
          return { kind: "destroyed" };
        },
      };
    }

    async function freeze(
      params: InstallAndApproveWorkflowSourceParams,
      capabilities: readonly SidecarCapabilityRule[] = [],
    ) {
      const result = probeResult(capabilities);
      await params.onProbeResult?.(result);
      await h.db
        .insert(workflowDefinition)
        .values({
          id: DEFINITION_ID,
          tenantId: TENANT_ID,
          name: "workflow-probe-definition",
        })
        .onConflictDoNothing({ target: workflowDefinition.id });
      return {
        approval: {
          ok: true as const,
          definitionId: DEFINITION_ID,
          approvedWireHash: result.wireHash,
          approvedGrants: new Set<string>(),
          projection: result.projection,
        },
        projection: result.projection,
        closure: { schemaVersion: "1" as const, topLevel: [], entries: [] },
      };
    }

    function prepareArgs(anchorRunId: string) {
      return {
        tenantId: TENANT_ID,
        anchorRunId,
        deploymentDomain: `${TENANT_ID}.example.test`,
        source: SOURCE,
        entry: "./workflow.mjs",
        definitionAssetId: ASSET_ID,
        sessionId: `ses-${anchorRunId}`,
        sourceAuthorityPrincipalId: PRINCIPAL_ID,
        sourceOfferingIds: [OFFERING_ID],
        defaultSourceOfferingId: OFFERING_ID,
        deployContent: { systemPrompt: "" },
      } as const;
    }

    test("persists a probe result and adopts matching provisioned capacity", async () => {
      const ensureCalls: unknown[] = [];
      const destroyCalls: unknown[] = [];
      const disconnectCalls: unknown[] = [];
      const provisioner = makeProvisioner({
        id: "sandbox",
        ensureCalls,
        destroyCalls,
      });
      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: createSidecarPluginRegistry({
          provisioners: [provisioner],
        }),
        preparedDeployer: {
          installAndApproveWorkflowSource: (params) => freeze(params),
          deployPreparedCodeSourcedWorkflow: async (params) => ({
            anchorRunId: params.anchorRunId,
            deploymentAddress: params.agentAddress,
            publicKey: "public-key",
          }),
        },
        credentialCipher: CIPHER,
        allocationRouter: {
          fenceAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          disconnectAllocation: (target) => disconnectCalls.push(target),
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        createAllocationId: () => "sal-probe-adopted",
        createSidecarId: () => "sc-probe-adopted",
        createToken: () => "probe-token",
        now: () => new Date("2026-08-31T12:00:00.000Z"),
      });

      const prepared = await service.prepareProvisionedDeployment(
        prepareArgs("run-probe-adopted"),
      );

      expect(prepared.allocationId).toBe("sal-probe-adopted");
      expect(ensureCalls).toHaveLength(1);
      expect(destroyCalls).toHaveLength(0);
      expect(disconnectCalls).toEqual([
        { allocationId: "sal-probe-adopted", generation: 0 },
      ]);
      const probe = await h.db.query.workflowProbe.findFirst({
        where: eq(workflowProbe.id, "sal-probe-adopted"),
      });
      expect(probe).toMatchObject({
        status: "succeeded",
        provisionerId: "sandbox",
        sidecarId: "sc-probe-adopted",
      });
      expect(probe?.result).toEqual(probeResult());
      expect(probe === undefined || !("sourceOfferingIds" in probe)).toBe(true);

      const allocation = await createSidecarAllocationStore(
        h.db,
      ).findByAnchorRunId("run-probe-adopted");
      expect(allocation).toMatchObject({
        id: "sal-probe-adopted",
        status: "allocated",
        ensureAcceptedGeneration: 0,
        sidecarId: "sc-probe-adopted",
      });
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, "run-probe-adopted"),
        }),
      ).toMatchObject({ status: "deployed", definitionId: DEFINITION_ID });
      expect(
        await createWorkflowRunLaunchSpecStore(h.db).get("run-probe-adopted"),
      ).toMatchObject({
        sourceOfferingIds: [OFFERING_ID],
        frozenApprovalBundle: { source: SOURCE },
      });
    });

    test("commits the anchor before deploying a ready allocation", async () => {
      const provisioner = makeProvisioner({
        id: "anchor-ordering",
        ensureCalls: [],
        destroyCalls: [],
      });
      let anchorVisibleDuringDeploy = false;
      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: createSidecarPluginRegistry({ provisioners: [provisioner] }),
        preparedDeployer: {
          installAndApproveWorkflowSource: (params) => freeze(params),
          deployPreparedCodeSourcedWorkflow: async (params) => {
            anchorVisibleDuringDeploy =
              (await h.db.query.workflowRun.findFirst({
                where: eq(workflowRun.id, params.anchorRunId),
              })) !== undefined;
            return {
              anchorRunId: params.anchorRunId,
              deploymentAddress: params.agentAddress,
              publicKey: "public-key",
            };
          },
        },
        credentialCipher: CIPHER,
        allocationRouter: {
          fenceAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          disconnectAllocation: () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        createAllocationId: () => "sal-anchor-ordering",
        createSidecarId: () => "sc-anchor-ordering",
        createToken: () => "anchor-ordering-token",
      });
      const prepared = await service.prepareProvisionedDeployment(
        prepareArgs("run-anchor-ordering"),
      );
      const allocation = await createSidecarAllocationStore(
        h.db,
      ).findByAnchorRunId(prepared.anchorRunId);
      if (allocation === null) throw new Error("expected adopted allocation");

      await service.deployReadyAllocation(allocation);

      expect(anchorVisibleDuringDeploy).toBe(true);
    });

    test("reauthenticates adopted probe capacity as its allocation", async () => {
      const credentialResolver = createSidecarCredentialResolver({ db: h.db });
      const router = createSidecarRouter({
        authenticateSidecar: async ({ token }) =>
          credentialResolver.resolve(token),
        validateSidecarIdentity: credentialResolver.isCurrent,
        requestTimeoutMs: 500,
      });
      let probeWs:
        | {
            sent: string[];
            closed: boolean;
            send(data: string): void;
            close(): void;
          }
        | undefined;
      let issuedToken: string | undefined;
      const provisioner: SidecarProvisioner = {
        id: "adoption-auth",
        apiVersion: 1,
        bindingFingerprint: "adoption-auth:v1",
        capabilities: [],
        async ensure(request) {
          issuedToken = request.token;
          probeWs = {
            sent: [],
            closed: false,
            send(data) {
              this.sent.push(data);
            },
            close() {
              this.closed = true;
            },
          };
          router.handleOpen(probeWs);
          router.handleMessage(
            probeWs,
            JSON.stringify({
              type: "register",
              sidecarId: request.sidecarId,
              token: request.token,
              agentAddresses: [],
            }),
          );
          return { kind: "accepted" };
        },
        async destroy() {
          return { kind: "destroyed" };
        },
      };
      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: createSidecarPluginRegistry({ provisioners: [provisioner] }),
        preparedDeployer: {
          installAndApproveWorkflowSource: (params) => freeze(params),
          deployPreparedCodeSourcedWorkflow: async (params) => ({
            anchorRunId: params.anchorRunId,
            deploymentAddress: params.agentAddress,
            publicKey: "public-key",
          }),
        },
        credentialCipher: CIPHER,
        allocationRouter: router,
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        createAllocationId: () => "sal-adoption-auth",
        createSidecarId: () => "sc-adoption-auth",
        createToken: () => "adoption-auth-token",
        connectTimeoutMs: 500,
      });

      const prepared = await service.prepareProvisionedDeployment(
        prepareArgs("run-adoption-auth"),
      );

      expect(probeWs?.closed).toBe(true);
      if (issuedToken === undefined) throw new Error("expected issued token");
      const reconnected = {
        sent: [] as string[],
        closed: false,
        send(data: string) {
          this.sent.push(data);
        },
        close() {
          this.closed = true;
        },
      };
      router.handleOpen(reconnected);
      router.handleMessage(
        reconnected,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-adoption-auth",
          token: issuedToken,
          agentAddresses: [prepared.deploymentAddress],
        }),
      );
      await router.waitForAllocatedSidecar(
        { allocationId: prepared.allocationId, generation: 0 },
        500,
      );

      expect(reconnected.closed).toBe(false);
      expect(router.getRoutableAddresses()).toContain(
        prepared.deploymentAddress,
      );
      expect(await credentialResolver.resolve(issuedToken)).toMatchObject({
        kind: "allocated",
        allocationId: prepared.allocationId,
        anchorRunId: prepared.anchorRunId,
        workflowRunAddress: prepared.deploymentAddress,
        generation: 0,
      });
    });

    test("releases adopted probe capacity when deployment persistence fails", async () => {
      const anchorRunId = "run-probe-persistence-failure";
      await seedWorkflowRun(h.db, { id: anchorRunId, tenantId: TENANT_ID });
      const ensureCalls: unknown[] = [];
      const destroyCalls: unknown[] = [];
      const disconnectCalls: unknown[] = [];
      const provisioner = makeProvisioner({
        id: "sandbox",
        ensureCalls,
        destroyCalls,
      });
      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: createSidecarPluginRegistry({
          provisioners: [provisioner],
        }),
        preparedDeployer: {
          installAndApproveWorkflowSource: (params) => freeze(params),
          deployPreparedCodeSourcedWorkflow: async (params) => ({
            anchorRunId: params.anchorRunId,
            deploymentAddress: params.agentAddress,
            publicKey: "public-key",
          }),
        },
        credentialCipher: CIPHER,
        allocationRouter: {
          fenceAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          disconnectAllocation: (target) => disconnectCalls.push(target),
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        createAllocationId: () => "sal-probe-persistence-failure",
        createSidecarId: () => "sc-probe-persistence-failure",
        createToken: () => "probe-token",
      });

      await expect(
        service.prepareProvisionedDeployment(prepareArgs(anchorRunId)),
      ).rejects.toThrow();

      expect(ensureCalls).toHaveLength(1);
      expect(destroyCalls).toHaveLength(1);
      expect(disconnectCalls).toEqual([
        { allocationId: "sal-probe-persistence-failure", generation: 0 },
      ]);
      expect(
        await h.db.query.workflowProbe.findFirst({
          where: eq(workflowProbe.id, "sal-probe-persistence-failure"),
        }),
      ).toMatchObject({ status: "failed" });
      expect(
        await createSidecarAllocationStore(h.db).findByAnchorRunId(anchorRunId),
      ).toBeNull();
    });

    test("retries releasing probe cleanup without restarting the Hub", async () => {
      const anchorRunId = "run-probe-cleanup-retry";
      const ensureCalls: unknown[] = [];
      const destroyCalls: unknown[] = [];
      let destroyAttempts = 0;
      const probeProvisioner: SidecarProvisioner = {
        id: "retry-cleanup-probe",
        apiVersion: 1,
        bindingFingerprint: "retry-cleanup-probe:v1",
        capabilities: [{ capability: "platform:ios", state: "blocked" }],
        async ensure(request) {
          ensureCalls.push(request);
          return { kind: "accepted", externalRef: "retry-cleanup-external" };
        },
        async destroy(request) {
          destroyCalls.push(request);
          destroyAttempts += 1;
          if (destroyAttempts === 1) {
            throw new Error("transient probe cleanup failure");
          }
          return { kind: "destroyed" };
        },
      };
      const deploymentProvisioner = makeProvisioner({
        id: "retry-cleanup-deployment",
        capabilities: [{ capability: "platform:ios", state: "available" }],
        ensureCalls: [],
        destroyCalls: [],
      });
      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: createSidecarPluginRegistry({
          provisioners: [probeProvisioner, deploymentProvisioner],
        }),
        preparedDeployer: {
          installAndApproveWorkflowSource: (params) =>
            freeze(params, [{ capability: "platform:ios", effect: "require" }]),
          deployPreparedCodeSourcedWorkflow: async (params) => ({
            anchorRunId: params.anchorRunId,
            deploymentAddress: params.agentAddress,
            publicKey: "public-key",
          }),
        },
        credentialCipher: CIPHER,
        allocationRouter: {
          fenceAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          disconnectAllocation: () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        createAllocationId: () => "sal-probe-cleanup-retry",
        createSidecarId: () => "sc-probe-cleanup-retry",
        createToken: () => "probe-token",
      });

      await expect(
        service.prepareProvisionedDeployment(prepareArgs(anchorRunId)),
      ).rejects.toThrow();

      expect(ensureCalls).toHaveLength(1);
      expect(destroyCalls).toHaveLength(1);
      expect(
        await h.db.query.workflowProbe.findFirst({
          where: eq(workflowProbe.id, "sal-probe-cleanup-retry"),
        }),
      ).toMatchObject({ status: "releasing", failureCode: null });

      if (service.reconcileReleasingProbes === undefined) {
        throw new Error("workflow probe cleanup reconciliation is unavailable");
      }
      await service.reconcileReleasingProbes();

      expect(destroyCalls).toHaveLength(2);
      expect(
        await h.db.query.workflowProbe.findFirst({
          where: eq(workflowProbe.id, "sal-probe-cleanup-retry"),
        }),
      ).toMatchObject({ status: "succeeded" });
      expect(
        await createSidecarAllocationStore(h.db).findByAnchorRunId(anchorRunId),
      ).toBeNull();
    });

    test("starts with failed probe cleanup pending for reconciliation", async () => {
      const destroyCalls: unknown[] = [];
      let destroyAttempts = 0;
      const provisioner: SidecarProvisioner = {
        id: "startup-cleanup",
        apiVersion: 1,
        bindingFingerprint: "startup-cleanup:v1",
        capabilities: [],
        async ensure() {
          return { kind: "accepted" };
        },
        async destroy(request) {
          destroyCalls.push(request);
          destroyAttempts += 1;
          if (destroyAttempts === 1) {
            throw new Error("startup cleanup unavailable");
          }
          return { kind: "destroyed" };
        },
      };
      const probeStore = createWorkflowProbeStore(h.db);
      await probeStore.create({
        id: "sal-startup-cleanup",
        tenantId: TENANT_ID,
        definitionAssetId: ASSET_ID,
        source: SOURCE,
        entry: "./workflow.mjs",
        provisionerId: provisioner.id,
        provisionerApiVersion: provisioner.apiVersion,
        provisionerBindingFingerprint: provisioner.bindingFingerprint,
      });
      await probeStore.bindSidecar({
        probeId: "sal-startup-cleanup",
        sidecarId: "sc-startup-cleanup",
        tokenHashSha256: new Uint8Array(32).fill(9),
      });

      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: createSidecarPluginRegistry({ provisioners: [provisioner] }),
        preparedDeployer: {
          installAndApproveWorkflowSource: async () => {
            throw new Error("not reached");
          },
          deployPreparedCodeSourcedWorkflow: async () => {
            throw new Error("not reached");
          },
        },
        credentialCipher: CIPHER,
        allocationRouter: {
          fenceAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          disconnectAllocation: () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
      });

      if (service.initialize === undefined) {
        throw new Error("workflow probe startup cleanup is unavailable");
      }
      await service.initialize();

      expect(destroyCalls).toHaveLength(1);
      expect(
        await h.db.query.workflowProbe.findFirst({
          where: eq(workflowProbe.id, "sal-startup-cleanup"),
        }),
      ).toMatchObject({
        status: "releasing",
        failureCode: "probe_interrupted",
      });

      if (service.reconcileReleasingProbes === undefined) {
        throw new Error("workflow probe cleanup reconciliation is unavailable");
      }
      await service.reconcileReleasingProbes();

      expect(destroyCalls).toHaveLength(2);
      expect(
        await h.db.query.workflowProbe.findFirst({
          where: eq(workflowProbe.id, "sal-startup-cleanup"),
        }),
      ).toMatchObject({
        status: "failed",
        failureCode: "probe_interrupted",
      });
    });

    test("releases probe capacity when the workflow selects another provisioner", async () => {
      const sandboxEnsure: unknown[] = [];
      const sandboxDestroy: unknown[] = [];
      const workerEnsure: unknown[] = [];
      const workerDestroy: unknown[] = [];
      const sandbox = makeProvisioner({
        id: "sandbox",
        capabilities: [{ capability: "platform:ios", state: "blocked" }],
        ensureCalls: sandboxEnsure,
        destroyCalls: sandboxDestroy,
      });
      const worker = makeProvisioner({
        id: "ios-worker",
        capabilities: [{ capability: "platform:ios", state: "available" }],
        ensureCalls: workerEnsure,
        destroyCalls: workerDestroy,
      });
      const ids = ["sal-probe-released", "sal-workflow-pending"];
      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: createSidecarPluginRegistry({
          provisioners: [sandbox, worker],
        }),
        preparedDeployer: {
          installAndApproveWorkflowSource: (params) =>
            freeze(params, [{ capability: "platform:ios", effect: "require" }]),
          deployPreparedCodeSourcedWorkflow: async () => {
            throw new Error("pending allocation is not ready");
          },
        },
        credentialCipher: CIPHER,
        allocationRouter: {
          fenceAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          disconnectAllocation: () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        createAllocationId: () => {
          const id = ids.shift();
          if (id === undefined) throw new Error("unexpected allocation id");
          return id;
        },
        createSidecarId: () => "sc-probe-released",
        createToken: () => "probe-token",
      });

      const prepared = await service.prepareProvisionedDeployment(
        prepareArgs("run-probe-released"),
      );

      expect(prepared.allocationId).toBe("sal-workflow-pending");
      expect(sandboxEnsure).toHaveLength(1);
      expect(sandboxDestroy).toHaveLength(1);
      expect(workerEnsure).toHaveLength(0);
      expect(workerDestroy).toHaveLength(0);
      expect(
        await h.db.query.workflowProbe.findFirst({
          where: eq(workflowProbe.id, "sal-probe-released"),
        }),
      ).toMatchObject({ status: "succeeded" });
      expect(
        await createSidecarAllocationStore(h.db).findByAnchorRunId(
          "run-probe-released",
        ),
      ).toMatchObject({
        id: "sal-workflow-pending",
        status: "pending",
        provisionerId: "ios-worker",
      });
    });

    test("selects probe capacity with Hub-configured capability rules", async () => {
      const generalEnsure: unknown[] = [];
      const generalDestroy: unknown[] = [];
      const sandboxEnsure: unknown[] = [];
      const sandboxDestroy: unknown[] = [];
      const general = makeProvisioner({
        id: "general",
        ensureCalls: generalEnsure,
        destroyCalls: generalDestroy,
      });
      const sandbox = makeProvisioner({
        id: "probe-sandbox",
        capabilities: [
          { capability: "isolation:workload", state: "available" },
        ],
        ensureCalls: sandboxEnsure,
        destroyCalls: sandboxDestroy,
      });
      const ids = ["sal-isolated-probe", "sal-general-workflow"];
      const service = createWorkflowAllocationService({
        db: h.db,
        plugins: createSidecarPluginRegistry({
          provisioners: [general, sandbox],
        }),
        preparedDeployer: {
          installAndApproveWorkflowSource: (params) => freeze(params),
          deployPreparedCodeSourcedWorkflow: async () => {
            throw new Error("pending allocation is not ready");
          },
        },
        credentialCipher: CIPHER,
        probeCapabilityRules: [
          { capability: "isolation:workload", effect: "require" },
        ],
        allocationRouter: {
          fenceAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          disconnectAllocation: () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        createAllocationId: () => {
          const id = ids.shift();
          if (id === undefined) throw new Error("unexpected allocation id");
          return id;
        },
        createSidecarId: () => "sc-isolated-probe",
        createToken: () => "probe-token",
      });

      const prepared = await service.prepareProvisionedDeployment(
        prepareArgs("run-probe-policy"),
      );

      expect(prepared.allocationId).toBe("sal-general-workflow");
      expect(sandboxEnsure).toHaveLength(1);
      expect(sandboxDestroy).toHaveLength(1);
      expect(generalEnsure).toHaveLength(0);
      expect(generalDestroy).toHaveLength(0);
      expect(
        await createSidecarAllocationStore(h.db).findByAnchorRunId(
          "run-probe-policy",
        ),
      ).toMatchObject({
        provisionerId: "general",
        status: "pending",
      });
    });

    test("rejects invalid Hub-configured probe capability rules", () => {
      const provisioner = makeProvisioner({
        id: "invalid-probe-policy",
        ensureCalls: [],
        destroyCalls: [],
      });

      expect(() =>
        createWorkflowAllocationService({
          db: h.db,
          plugins: createSidecarPluginRegistry({
            provisioners: [provisioner],
          }),
          preparedDeployer: {
            installAndApproveWorkflowSource: (params) => freeze(params),
            deployPreparedCodeSourcedWorkflow: async () => {
              throw new Error("not reached");
            },
          },
          credentialCipher: CIPHER,
          probeCapabilityRules: [
            { capability: "isolation:*:invalid", effect: "require" },
          ],
          allocationRouter: {
            fenceAllocation: () => undefined,
            waitForAllocatedSidecar: async () => undefined,
            sendProbeToAllocation: async () => probeResult(),
            isAllocatedWorkflowActive: async () => false,
            disconnectAllocation: () => undefined,
          },
          hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        }),
      ).toThrow(/Invalid workflow probe capability rules/);
    });
  },
);
