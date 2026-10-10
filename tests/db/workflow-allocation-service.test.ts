import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  spyOn,
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
  SessionLaunchError,
  type InstallAndApproveWorkflowSourceParams,
  type SidecarProvisioner,
} from "@intx/hub-sessions";
import {
  credentialAad,
  type ResolvedWorkflowLifecyclePolicy,
  type SidecarCapabilityRule,
} from "@intx/types";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import { DeploymentRejectedError } from "@intx/types/sidecar";
import { createApprovalSet } from "@intx/workflow-deploy";
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

const TEST_DEFAULT_LIFECYCLE_POLICY: ResolvedWorkflowLifecyclePolicy = {
  maxLifetime: "7d",
  maxDisconnected: "15m",
  capacityRetention: { completed: "30m", failed: "24h", cancelled: "1h" },
};

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
          return { kind: "destroyed", cleanup: "confirmed" };
        },
      };
    }

    function sharedPluginPools(provisioners: readonly SidecarProvisioner[]) {
      const plugins = createSidecarPluginRegistry({ provisioners });
      return { deploymentPlugins: plugins, probePlugins: plugins };
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
          approvedSurface: createApprovalSet([]),
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

    test.each(["synchronization", "readiness"])(
      "releases a probe when its connection deadline expires during %s",
      async (phase) => {
        const probeId = `sal-connection-${phase}`;
        const anchorRunId = `run-connection-${phase}`;
        const entered = Promise.withResolvers<undefined>();
        const held = Promise.withResolvers<undefined>();
        const signals: AbortSignal[] = [];
        const destroyCalls: unknown[] = [];
        const retireCalls: unknown[] = [];
        let readinessCalls = 0;
        let installCalls = 0;
        const provisioner = makeProvisioner({
          id: "connection-deadline",
          ensureCalls: [],
          destroyCalls,
        });
        const connectTimeoutMs = 23_456;
        const deadlines: (() => void)[] = [];
        const scheduleTimeout = globalThis.setTimeout;
        const timerSpy = spyOn(globalThis, "setTimeout").mockImplementation(
          Object.assign(
            <TArgs extends unknown[]>(
              callback: (...args: TArgs) => void,
              delay?: number,
              ...args: TArgs
            ) => {
              const timer = scheduleTimeout(callback, delay, ...args);
              if (delay === connectTimeoutMs) {
                clearTimeout(timer);
                deadlines.push(() => {
                  callback(...args);
                });
              }
              return timer;
            },
            { __promisify__: scheduleTimeout.__promisify__ },
          ),
        );
        const service = createWorkflowAllocationService({
          db: h.db,
          ...sharedPluginPools([provisioner]),
          preparedDeployer: {
            installAndApproveWorkflowSource: async () => {
              installCalls += 1;
              throw new Error("A timed-out connection must not start probing");
            },
            deployPreparedCodeSourcedWorkflow: async (params) => ({
              anchorRunId: params.anchorRunId,
              deploymentAddress: params.agentAddress,
              publicKey: "public-key",
            }),
          },
          credentialCipher: CIPHER,
          allocationRouter: {
            ...createSidecarRouter({
              authenticateSidecar: async () => null,
              validateSidecarIdentity: async () => false,
              resolveSidecarBindings: async () => [],
              withExecutableWorkflowRun: async (_target, send) => send(),
            }),
            fenceAllocation: () => undefined,
            retireAllocation: (target) => retireCalls.push(target),
            syncSidecar(_sidecarId, signal) {
              if (signal !== undefined) signals.push(signal);
              if (phase !== "synchronization") return Promise.resolve();
              entered.resolve(undefined);
              return held.promise;
            },
            waitForAllocatedSidecar(_target, _timeout, _onValidation, signal) {
              readinessCalls += 1;
              if (signal !== undefined) signals.push(signal);
              entered.resolve(undefined);
              return held.promise;
            },
            sendProbeToAllocation: async () => probeResult(),
            isAllocatedWorkflowActive: async () => false,
            detachAllocation: () => undefined,
          },
          hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
          defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
          createAllocationId: () => probeId,
          createSidecarId: () => `sc-connection-${phase}`,
          createToken: () => "probe-token",
          connectTimeoutMs,
        });
        const preparing = service
          .prepareProvisionedDeployment(prepareArgs(anchorRunId))
          .catch((error: unknown) => error);
        try {
          await Promise.race([entered.promise, preparing]);
          // The same timer covers both phases; readiness gets no fresh budget.
          expect(deadlines).toHaveLength(1);
          expect(signals).toHaveLength(phase === "synchronization" ? 1 : 2);
          expect(new Set(signals).size).toBe(1);
          const expire = deadlines[0];
          if (expire === undefined) throw new Error("No connection deadline");
          expire();
          expect(await preparing).toMatchObject({
            name: "SidecarOperationTimeoutError",
            message: `Probe connection timed out after ${String(connectTimeoutMs)}ms`,
          });
          expect(signals.every((signal) => signal.aborted)).toBe(true);
          expect(destroyCalls).toHaveLength(1);
          expect(retireCalls).toEqual([
            { allocationId: probeId, generation: 0 },
          ]);
          expect(
            await createWorkflowProbeStore(h.db).get(probeId),
          ).toMatchObject({
            status: "failed",
            failureCode: "probe_failed",
          });
          expect(
            await createSidecarAllocationStore(h.db).findByAnchorRunId(
              anchorRunId,
            ),
          ).toBeNull();

          held.resolve(undefined);
          await held.promise;
          expect(readinessCalls).toBe(phase === "synchronization" ? 0 : 1);
          expect(installCalls).toBe(0);
        } finally {
          held.resolve(undefined);
          await preparing;
          timerSpy.mockRestore();
        }
      },
    );

    test.each(["reconnect", "timeout", "release"])(
      "adoption rolls back before waiting for inventory, outcome = %s",
      async (outcome) => {
        const anchorRunId = "run-inventory-adoption";
        const probeId = "sal-inventory-adoption";
        const ensureCalls: unknown[] = [];
        const destroyCalls: unknown[] = [];
        const entered = Promise.withResolvers<undefined>();
        const inventory = Promise.withResolvers<undefined>();
        let known = false;
        let current = new Date("2026-10-08T12:00:00Z");
        let installs = 0;
        const deadlines: (() => void)[] = [];
        const schedule = globalThis.setTimeout;
        const timerSpy = spyOn(globalThis, "setTimeout").mockImplementation(
          Object.assign(
            <TArgs extends unknown[]>(
              callback: (...args: TArgs) => void,
              delay?: number,
              ...args: TArgs
            ) => {
              const timer = schedule(callback, delay, ...args);
              if (delay !== undefined && delay > 20_000 && delay <= 23_456) {
                clearTimeout(timer);
                deadlines.push(() => callback(...args));
              }
              return timer;
            },
            { __promisify__: schedule.__promisify__ },
          ),
        );
        const provisioner = makeProvisioner({
          id: "adoption-inventory",
          ensureCalls,
          destroyCalls,
        });
        const service = createWorkflowAllocationService({
          db: h.db,
          ...sharedPluginPools([provisioner]),
          preparedDeployer: {
            installAndApproveWorkflowSource: (params) => {
              installs++;
              return freeze(params);
            },
            deployPreparedCodeSourcedWorkflow: async () => {
              throw new Error("adoption must not deploy yet");
            },
          },
          credentialCipher: CIPHER,
          allocationRouter: {
            getRetainedIncarnations: () => (known ? [] : undefined),
            async waitForSidecarInventory(sidecarId, signal) {
              expect(sidecarId).toBe("sc-inventory-adoption");
              entered.resolve(undefined);
              const cancel = () => inventory.reject(signal.reason);
              signal.addEventListener("abort", cancel, { once: true });
              try {
                await inventory.promise;
              } finally {
                signal.removeEventListener("abort", cancel);
              }
            },
            fenceAllocation: () => undefined,
            retireAllocation: () => undefined,
            waitForAllocatedSidecar: async () => undefined,
            sendProbeToAllocation: async () => probeResult(),
            isAllocatedWorkflowActive: async () => false,
            reportedDeploymentFailure: () => undefined,
            stoppedDeploymentHistory: async () => undefined,
            detachAllocation: () => undefined,
            syncSidecar: async () => undefined,
          },
          hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
          defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
          createAllocationId: () => probeId,
          createSidecarId: () => "sc-inventory-adoption",
          createToken: () => "probe-token",
          connectTimeoutMs: 23_456,
          now: () => current,
        });
        const preparing = service
          .prepareProvisionedDeployment(prepareArgs(anchorRunId))
          .catch((error: unknown) => error);
        try {
          await Promise.race([entered.promise, preparing]);
          expect(installs).toBe(1);
          expect(ensureCalls).toHaveLength(1);
          expect(destroyCalls).toHaveLength(0);
          expect(
            await createWorkflowRunLaunchSpecStore(h.db).get(anchorRunId),
          ).toBeNull();
          expect(
            await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, anchorRunId),
            }),
          ).toBeUndefined();
          expect(
            await createSidecarAllocationStore(h.db).findByAnchorRunId(
              anchorRunId,
            ),
          ).toBeNull();
          const probes = createWorkflowProbeStore(h.db);
          expect((await probes.get(probeId))?.status).toBe("probing");
          if (outcome === "timeout") {
            const expire = deadlines.at(-1);
            if (expire === undefined) throw new Error("no inventory deadline");
            expire();
          } else {
            if (outcome === "release")
              await probes.transition(probeId, ["probing"], "releasing", {
                failureCode: "probe_cancelled",
              });
            current = new Date(current.getTime() + 20_000);
            known = true;
            inventory.resolve(undefined);
          }
          const result = await preparing;
          if (outcome === "reconnect") {
            expect(result).toMatchObject({
              allocationId: probeId,
              anchorRunId,
            });
            expect((await probes.get(probeId))?.status).toBe("succeeded");
            expect(
              (
                await createSidecarAllocationStore(h.db).findByAnchorRunId(
                  anchorRunId,
                )
              )?.status,
            ).toBe("allocated");
            const saved = await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, anchorRunId),
            });
            expect(saved?.createdAt).toEqual(current);
            expect(saved?.expiresAt).toEqual(
              new Date(current.getTime() + 7 * 24 * 60 * 60_000),
            );
            const adopted = await createSidecarAllocationStore(
              h.db,
            ).findByAnchorRunId(anchorRunId);
            expect(adopted?.createdAt).toEqual(current);
            expect(adopted?.connectDeadline).toEqual(
              new Date(current.getTime() + 23_456),
            );
            expect(destroyCalls).toHaveLength(0);
          } else {
            expect(result).toBeInstanceOf(Error);
            if (outcome === "timeout")
              expect(result).toMatchObject({
                name: "SidecarOperationTimeoutError",
              });
            if (outcome === "release") {
              expect((await probes.get(probeId))?.status).toBe("releasing");
              await service.reconcileReleasingProbes?.();
            }
            expect((await probes.get(probeId))?.status).toBe("failed");
            expect(
              await createSidecarAllocationStore(h.db).findByAnchorRunId(
                anchorRunId,
              ),
            ).toBeNull();
            expect(
              await createWorkflowRunLaunchSpecStore(h.db).get(anchorRunId),
            ).toBeNull();
            expect(destroyCalls).toHaveLength(1);
          }
          expect(installs).toBe(1);
          expect(ensureCalls).toHaveLength(1);
        } finally {
          known = true;
          inventory.resolve(undefined);
          await preparing;
          timerSpy.mockRestore();
        }
      },
    );

    test("persists a probe result and adopts matching provisioned capacity", async () => {
      // A disconnect limit other than the default, so the allocation is seen
      // to carry the deployment's own.
      await h.db.insert(workflowDefinition).values({
        id: DEFINITION_ID,
        tenantId: TENANT_ID,
        name: "workflow-probe-definition",
        lifecyclePolicy: { maxDisconnected: "40m" },
      });
      const ensureCalls: unknown[] = [];
      const destroyCalls: unknown[] = [];
      const detachCalls: unknown[] = [];
      const provisioner = makeProvisioner({
        id: "sandbox",
        ensureCalls,
        destroyCalls,
      });
      const service = createWorkflowAllocationService({
        db: h.db,
        probePlugins: createSidecarPluginRegistry({
          provisioners: [provisioner],
        }),
        deploymentPlugins: createSidecarPluginRegistry({
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
          getRetainedIncarnations: () => [],
          waitForSidecarInventory: async () => undefined,
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          reportedDeploymentFailure: () => undefined,
          stoppedDeploymentHistory: async () => undefined,
          detachAllocation: (target) => detachCalls.push(target),
          syncSidecar: async () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
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
      expect(detachCalls).toEqual([]);
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
      // The allocation carries the deployment's disconnect limit.
      expect(allocation).toMatchObject({
        id: "sal-probe-adopted",
        status: "allocated",
        ensureAcceptedGeneration: 0,
        sidecarId: "sc-probe-adopted",
        maxDisconnectedMs: 2_400_000,
      });
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, "run-probe-adopted"),
        }),
      ).toMatchObject({
        status: "deployed",
        definitionId: DEFINITION_ID,
        lifecyclePolicy: { maxDisconnected: "40m" },
      });
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
        ...sharedPluginPools([provisioner]),
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
          getRetainedIncarnations: () => [],
          waitForSidecarInventory: async () => undefined,
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          reportedDeploymentFailure: () => undefined,
          stoppedDeploymentHistory: async () => undefined,
          detachAllocation: () => undefined,
          syncSidecar: async () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
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

      await service.deployReadyAllocation(allocation, {
        signal: new AbortController().signal,
        leaseId: "initialization-test",
      });

      expect(anchorVisibleDuringDeploy).toBe(true);
    });

    test("tells a first deploy that failed before its frame was sent from an uncertain one", async () => {
      const provisioner = makeProvisioner({
        id: "first-deploy-failure",
        ensureCalls: [],
        destroyCalls: [],
      });
      let failure: Error = new Error("catalog temporarily unavailable");
      const service = createWorkflowAllocationService({
        db: h.db,
        ...sharedPluginPools([provisioner]),
        preparedDeployer: {
          installAndApproveWorkflowSource: (params) => freeze(params),
          deployPreparedCodeSourcedWorkflow: async () => {
            throw failure;
          },
        },
        credentialCipher: CIPHER,
        allocationRouter: {
          getRetainedIncarnations: () => [],
          waitForSidecarInventory: async () => undefined,
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          reportedDeploymentFailure: () => undefined,
          stoppedDeploymentHistory: async () => undefined,
          detachAllocation: () => undefined,
          syncSidecar: async () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
        createAllocationId: () => "sal-first-deploy-failure",
        createSidecarId: () => "sc-first-deploy-failure",
        createToken: () => "first-deploy-failure-token",
      });
      const prepared = await service.prepareProvisionedDeployment(
        prepareArgs("run-first-deploy-failure"),
      );
      const allocation = await createSidecarAllocationStore(
        h.db,
      ).findByAnchorRunId(prepared.anchorRunId);
      if (allocation === null) throw new Error("expected adopted allocation");
      const reconciliation = {
        signal: new AbortController().signal,
        leaseId: "initialization-test",
      };

      await expect(
        service.deployReadyAllocation(allocation, reconciliation),
      ).rejects.toMatchObject({
        name: "SidecarFirstDeployError",
        message: "catalog temporarily unavailable",
      });
      failure = new SessionLaunchError(
        "start",
        new Error("deploy acknowledgement timed out"),
        true,
      );
      await expect(
        service.deployReadyAllocation(allocation, reconciliation),
      ).rejects.toBe(failure);
      failure = new DeploymentRejectedError("capacity_full", "No active slot");
      await expect(
        service.deployReadyAllocation(allocation, reconciliation),
      ).rejects.toBe(failure);
    });

    test("reports a deployed workflow its connected sidecar no longer holds as missing", async () => {
      const provisioner = makeProvisioner({
        id: "deployment-missing",
        ensureCalls: [],
        destroyCalls: [],
      });
      let deploys = 0;
      let connected = false;
      let clock = new Date();
      const reported: {
        error?: string;
        history?: { unreceived: string | null; reportedAt: Date };
      } = {};
      const service = createWorkflowAllocationService({
        db: h.db,
        ...sharedPluginPools([provisioner]),
        preparedDeployer: {
          installAndApproveWorkflowSource: (params) => freeze(params),
          deployPreparedCodeSourcedWorkflow: async (params) => {
            deploys += 1;
            return {
              anchorRunId: params.anchorRunId,
              deploymentAddress: params.agentAddress,
              publicKey: "public-key",
            };
          },
        },
        credentialCipher: CIPHER,
        allocationRouter: {
          getRetainedIncarnations: () => [],
          waitForSidecarInventory: async () => undefined,
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => {
            if (!connected)
              throw new Error("Allocated sidecar is not connected");
            return false;
          },
          reportedDeploymentFailure: () => reported.error,
          stoppedDeploymentHistory: async () => reported.history,
          detachAllocation: () => undefined,
          syncSidecar: async () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
        now: () => clock,
        createAllocationId: () => "sal-deployment-missing",
        createSidecarId: () => "sc-deployment-missing",
        createToken: () => "deployment-missing-token",
      });
      const prepared = await service.prepareProvisionedDeployment(
        prepareArgs("run-deployment-missing"),
      );
      const allocation = await createSidecarAllocationStore(
        h.db,
      ).findByAnchorRunId(prepared.anchorRunId);
      if (allocation === null) throw new Error("expected adopted allocation");
      await h.db
        .update(workflowRun)
        .set({ publicKey: "committed-key" })
        .where(eq(workflowRun.id, prepared.anchorRunId));
      const reconciliation = {
        signal: new AbortController().signal,
        leaseId: "initialization-test",
      };

      // A sidecar that is only cut off may still hold the deployment.
      await expect(
        service.deployReadyAllocation(allocation, reconciliation),
      ).rejects.toThrow("not connected");
      connected = true;
      await expect(
        service.deployReadyAllocation(allocation, reconciliation),
      ).rejects.toMatchObject({ name: "SidecarDeploymentMissingError" });
      // A sidecar that still holds the deployment, stopped on its own, says why.
      const reportedAt = clock;
      reported.error = "The child ended itself";
      reported.history = { unreceived: null, reportedAt };
      await expect(
        service.deployReadyAllocation(allocation, reconciliation),
      ).rejects.toMatchObject({
        name: "SidecarDeploymentStoppedError",
        message: "The child ended itself",
      });
      // History the stopped copy committed is waited for, for a while.
      reported.history = {
        unreceived: "refs/heads/main is at b on the Hub and c on the worker",
        reportedAt,
      };
      await expect(
        service.deployReadyAllocation(allocation, reconciliation),
      ).rejects.toMatchObject({
        name: "SidecarDeploymentHistoryPendingError",
        retryAt: new Date(reportedAt.getTime() + 1_000),
      });
      clock = new Date(reportedAt.getTime() + 60_000);
      await expect(
        service.deployReadyAllocation(allocation, reconciliation),
      ).rejects.toMatchObject({
        name: "SidecarDeploymentStoppedError",
        message:
          "The child ended itself; history it committed that the Hub never received is lost: refs/heads/main is at b on the Hub and c on the worker",
      });
      expect(deploys).toBe(0);
    });

    test("turns adopted probe capacity into its allocation without reconnecting", async () => {
      const credentialResolver = createSidecarCredentialResolver({ db: h.db });
      const router = createSidecarRouter({
        withExecutableWorkflowRun: async (_target, send) => send(),
        authenticateSidecar: async ({ token }) =>
          credentialResolver.resolve(token),
        validateSidecarIdentity: credentialResolver.isCurrent,
        resolveSidecarBindings: credentialResolver.resolveBindings,
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
              type: "hello",
              sidecarId: request.sidecarId,
              token: request.token,
              incarnations: [],
            }),
          );
          return { kind: "accepted" };
        },
        async destroy() {
          return { kind: "destroyed", cleanup: "confirmed" };
        },
      };
      const service = createWorkflowAllocationService({
        db: h.db,
        ...sharedPluginPools([provisioner]),
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
        defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
        createAllocationId: () => "sal-adoption-auth",
        createSidecarId: () => "sc-adoption-auth",
        createToken: () => "adoption-auth-token",
        connectTimeoutMs: 500,
      });

      const prepared = await service.prepareProvisionedDeployment(
        prepareArgs("run-adoption-auth"),
      );

      if (issuedToken === undefined) throw new Error("expected issued token");
      await router.syncSidecar("sc-adoption-auth");
      await router.waitForAllocatedSidecar(
        { allocationId: prepared.allocationId, generation: 0 },
        500,
      );

      expect(probeWs?.closed).toBe(false);
      expect(await credentialResolver.resolve(issuedToken)).toEqual({
        sidecarId: "sc-adoption-auth",
      });
      expect(
        await credentialResolver.resolveBindings("sc-adoption-auth"),
      ).toEqual([
        {
          kind: "allocated",
          sidecarId: "sc-adoption-auth",
          allocationId: prepared.allocationId,
          tenantId: TENANT_ID,
          anchorRunId: prepared.anchorRunId,
          workflowRunAddress: prepared.deploymentAddress,
          generation: 0,
        },
      ]);
    });

    test("releases adopted probe capacity when deployment persistence fails", async () => {
      const anchorRunId = "run-probe-persistence-failure";
      await seedWorkflowRun(h.db, { id: anchorRunId, tenantId: TENANT_ID });
      const ensureCalls: unknown[] = [];
      const destroyCalls: unknown[] = [];
      const detachCalls: unknown[] = [];
      const provisioner = makeProvisioner({
        id: "sandbox",
        ensureCalls,
        destroyCalls,
      });
      const service = createWorkflowAllocationService({
        db: h.db,
        ...sharedPluginPools([provisioner]),
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
          getRetainedIncarnations: () => [],
          waitForSidecarInventory: async () => undefined,
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          reportedDeploymentFailure: () => undefined,
          stoppedDeploymentHistory: async () => undefined,
          detachAllocation: (target) => detachCalls.push(target),
          syncSidecar: async () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
        createAllocationId: () => "sal-probe-persistence-failure",
        createSidecarId: () => "sc-probe-persistence-failure",
        createToken: () => "probe-token",
      });

      await expect(
        service.prepareProvisionedDeployment(prepareArgs(anchorRunId)),
      ).rejects.toThrow();

      expect(ensureCalls).toHaveLength(1);
      expect(destroyCalls).toHaveLength(1);
      expect(detachCalls).toEqual([
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
      const retireCalls: unknown[] = [];
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
          return { kind: "destroyed", cleanup: "confirmed" };
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
        ...sharedPluginPools([probeProvisioner, deploymentProvisioner]),
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
          getRetainedIncarnations: () => [],
          waitForSidecarInventory: async () => undefined,
          fenceAllocation: () => undefined,
          retireAllocation: (target) => retireCalls.push(target),
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          reportedDeploymentFailure: () => undefined,
          stoppedDeploymentHistory: async () => undefined,
          detachAllocation: () => undefined,
          syncSidecar: async () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
        createAllocationId: () => "sal-probe-cleanup-retry",
        createSidecarId: () => "sc-probe-cleanup-retry",
        createToken: () => "probe-token",
      });

      await expect(
        service.prepareProvisionedDeployment(prepareArgs(anchorRunId)),
      ).rejects.toThrow();

      expect(ensureCalls).toHaveLength(1);
      expect(destroyCalls).toHaveLength(1);
      expect(retireCalls).toHaveLength(0);
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
      expect(retireCalls).toEqual([
        { allocationId: "sal-probe-cleanup-retry", generation: 0 },
      ]);
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
          return { kind: "destroyed", cleanup: "confirmed" };
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
        ...sharedPluginPools([provisioner]),
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
          getRetainedIncarnations: () => [],
          waitForSidecarInventory: async () => undefined,
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          reportedDeploymentFailure: () => undefined,
          stoppedDeploymentHistory: async () => undefined,
          detachAllocation: () => undefined,
          syncSidecar: async () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
      });

      if (service.initialize === undefined) {
        throw new Error("workflow probe startup cleanup is unavailable");
      }
      await service.initialize();

      expect(destroyCalls).toHaveLength(0);
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
      await expect(service.reconcileReleasingProbes()).rejects.toThrow(
        "Failed to clean up releasing workflow probes",
      );
      expect(destroyCalls).toHaveLength(1);
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
        probePlugins: createSidecarPluginRegistry({
          provisioners: [sandbox],
        }),
        deploymentPlugins: createSidecarPluginRegistry({
          provisioners: [worker],
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
          getRetainedIncarnations: () => [],
          waitForSidecarInventory: async () => undefined,
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          reportedDeploymentFailure: () => undefined,
          stoppedDeploymentHistory: async () => undefined,
          detachAllocation: () => undefined,
          syncSidecar: async () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
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
        ...sharedPluginPools([general, sandbox]),
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
          getRetainedIncarnations: () => [],
          waitForSidecarInventory: async () => undefined,
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          sendProbeToAllocation: async () => probeResult(),
          isAllocatedWorkflowActive: async () => false,
          reportedDeploymentFailure: () => undefined,
          stoppedDeploymentHistory: async () => undefined,
          detachAllocation: () => undefined,
          syncSidecar: async () => undefined,
        },
        hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
        defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
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
          ...sharedPluginPools([provisioner]),
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
            getRetainedIncarnations: () => [],
            waitForSidecarInventory: async () => undefined,
            fenceAllocation: () => undefined,
            retireAllocation: () => undefined,
            waitForAllocatedSidecar: async () => undefined,
            sendProbeToAllocation: async () => probeResult(),
            isAllocatedWorkflowActive: async () => false,
            reportedDeploymentFailure: () => undefined,
            stoppedDeploymentHistory: async () => undefined,
            detachAllocation: () => undefined,
            syncSidecar: async () => undefined,
          },
          hubWebSocketUrl: "wss://hub.example.test/api/sidecars/ws",
          defaultLifecyclePolicy: TEST_DEFAULT_LIFECYCLE_POLICY,
        }),
      ).toThrow(/Invalid workflow probe capability rules/);
    });
  },
);
