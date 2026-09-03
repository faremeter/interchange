import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq } from "drizzle-orm";

import { createNoopCredentialCipher } from "@intx/crypto";
import {
  credential as credentialTable,
  workflowDefinition,
} from "@intx/db/schema";
import {
  pushCredentialReconcile,
  pushSourceUpdates,
  type SidecarRouter,
} from "@intx/hub-sessions";
import type { InferenceSource } from "@intx/types/runtime";
import type { CredentialDelivery } from "@intx/types/sidecar";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedCredential,
  seedGrant,
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedPrincipal,
  seedProvider,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

// The source push fans out to the running instances a launch produces, which
// are folded `workflow_run` rows -- NOT `agent_instance` rows. This suite pins
// that the enumeration finds those runs (so a rotation actually reaches a
// launched instance) and that it excludes the deployment-anchored and terminal
// runs that must not be pushed at.

type Pushed = { address: string; sources: InferenceSource[]; default: string };

// A SidecarRouter that records the source pushes it receives. The push path
// exercises sendCredentialsUpdate (material) then sendSourcesUpdate; the rest of
// the surface is unused.
function recordingRouter(pushed: Pushed[]): SidecarRouter {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only the credentials + sources push methods are exercised by the push path
  return {
    // The push delivers the credential material before the source list, so the
    // recorder must accept the credentials frame; this suite only asserts on the
    // source push, so the credentials handler is a no-op.
    sendCredentialsUpdate: () => Promise.resolve(),
    sendSourcesUpdate: (
      address: string,
      sources: InferenceSource[],
      defaultSource: string,
    ) => {
      pushed.push({ address, sources, default: defaultSource });
      return Promise.resolve();
    },
  } as unknown as SidecarRouter;
}

type Reconciled = {
  address: string;
  delivery: CredentialDelivery;
  revoke: string[] | undefined;
};

// A SidecarRouter that records the reconcile pushes (delivery + revoke) it
// receives over sendCredentialsUpdate.
function reconcilingRouter(captured: Reconciled[]): SidecarRouter {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only sendCredentialsUpdate is exercised by the reconcile path
  return {
    sendCredentialsUpdate: (
      address: string,
      delivery: CredentialDelivery,
      revoke?: string[],
    ) => {
      captured.push({ address, delivery, revoke });
      return Promise.resolve();
    },
  } as unknown as SidecarRouter;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "credential source push run fan-out (real DB)",
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
      // A single credential-backed offering for model "opus", plus a folded
      // definition (id wfd_1) whose creator is authorized to use every
      // credential. A run anchored on wfd_1 resolves to one launchable source
      // (mof_a / sk-anthropic).
      await seedTenants(h.db, [{ id: "tnt_root" }]);
      await seedProvider(h.db, {
        id: "prv_x",
        tenantId: "tnt_root",
        name: "prv-x",
        // The reconcile re-resolves a credential's origin from its own provider,
        // so the provider needs an API base URL. Harmless to the source-push
        // tests, which resolve origin via the model offering.
        apiBaseUrl: "https://api.anthropic.com",
      });
      await seedCredential(h.db, {
        id: "cred_a",
        tenantId: "tnt_root",
        providerId: "prv_x",
        name: "cred-a",
        secret: "sk-anthropic",
      });
      await seedModel(h.db, {
        id: "mdl_opus",
        tenantId: "tnt_root",
        canonicalName: "opus",
      });
      await seedModelProvider(h.db, {
        id: "mpv_anthropic",
        tenantId: "tnt_root",
        name: "anthropic",
        credentialId: "cred_a",
      });
      await seedModelOffering(h.db, {
        id: "mof_a",
        tenantId: "tnt_root",
        modelId: "mdl_opus",
        providerId: "mpv_anthropic",
        priority: 0,
        capabilities: [],
      });
      await seedPrincipal(h.db, { id: "prn_creator", tenantId: "tnt_root" });
      await seedGrant(h.db, {
        id: "grt_creator_use",
        tenantId: "tnt_root",
        principalId: "prn_creator",
        resource: "credential:*",
        action: "use",
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_1",
        tenantId: "tnt_root",
        creatorPrincipalId: "prn_creator",
        name: "agent-1",
        modelRequirements: [{ model: "opus" }],
      });
    });

    test("pushes to a running folded run's address", async () => {
      await seedWorkflowRun(h.db, {
        id: "run1",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        address: "run1@tnt.test",
        status: "running",
      });

      const pushed: Pushed[] = [];
      await pushSourceUpdates(
        h.db,
        recordingRouter(pushed),
        "tnt_root",
        createNoopCredentialCipher(),
      );

      expect(pushed).toHaveLength(1);
      expect(pushed[0]?.address).toBe("run1@tnt.test");
      expect(pushed[0]?.default).toBe("mof_a");
      expect(pushed[0]?.sources.map((s) => s.credentialId)).toEqual(["cred_a"]);
    });

    test("skips the terminal, address-less, and deployment-anchored runs", async () => {
      // Terminal folded run: right shape but no longer running.
      await seedWorkflowRun(h.db, {
        id: "run_done",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        address: "run_done@tnt.test",
        status: "completed",
      });
      // Address-less child run: routes via its deployment, not a per-run push.
      await seedWorkflowRun(h.db, {
        id: "run_child",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        address: null,
        status: "running",
      });
      // Deployment-anchor run: owns a deployment id (self) and a
      // workflow-derived address, so it must never be pushed at here.
      await seedWorkflowRun(h.db, {
        id: "run_anchor",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        anchorRunId: "run_anchor",
        address: "run_anchor@tnt.test",
        status: "running",
      });

      const pushed: Pushed[] = [];
      await pushSourceUpdates(
        h.db,
        recordingRouter(pushed),
        "tnt_root",
        createNoopCredentialCipher(),
      );

      expect(pushed).toHaveLength(0);
    });

    test("reconcile re-resolves a live credential and merges it (no revoke)", async () => {
      await seedWorkflowRun(h.db, {
        id: "dep_1",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        anchorRunId: "dep_1",
        address: "dep_1@tnt.test",
        status: "running",
        credentialRefs: { credentialIds: ["cred_a"], bindings: [] },
      });

      const captured: Reconciled[] = [];
      await pushCredentialReconcile(
        h.db,
        reconcilingRouter(captured),
        "dep_1@tnt.test",
        createNoopCredentialCipher(),
      );

      expect(captured).toHaveLength(1);
      expect(captured[0]?.address).toBe("dep_1@tnt.test");
      expect(
        captured[0]?.delivery.materials.map((m) => m.credentialId),
      ).toEqual(["cred_a"]);
      // Live secret re-resolved (noop cipher passes the seed through).
      expect(captured[0]?.delivery.materials[0]?.secret).toBe("sk-anthropic");
      // Nothing dead, so no revoke.
      expect(captured[0]?.revoke).toBeUndefined();
    });

    test("reconcile revokes a deleted or revoked deploy-time credential and drops its binding", async () => {
      await seedWorkflowRun(h.db, {
        id: "dep_2",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        anchorRunId: "dep_2",
        address: "dep_2@tnt.test",
        status: "running",
        credentialRefs: {
          credentialIds: ["cred_a"],
          bindings: [
            { handle: "h", credentialId: "cred_a", consumer: "tool:@intx/x" },
          ],
        },
      });
      await h.db
        .update(credentialTable)
        .set({ status: "revoked" })
        .where(eq(credentialTable.id, "cred_a"));

      const captured: Reconciled[] = [];
      await pushCredentialReconcile(
        h.db,
        reconcilingRouter(captured),
        "dep_2@tnt.test",
        createNoopCredentialCipher(),
      );

      expect(captured).toHaveLength(1);
      // Merge: the survivors delivery is empty, and the dead id is named in
      // revoke so the child drops its material and binding.
      expect(captured[0]?.delivery.materials).toEqual([]);
      expect(captured[0]?.delivery.bindings).toEqual([]);
      expect(captured[0]?.revoke).toEqual(["cred_a"]);
    });

    test("reconcile is a no-op for a run that persisted no credential refs", async () => {
      await seedWorkflowRun(h.db, {
        id: "dep_3",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        anchorRunId: "dep_3",
        address: "dep_3@tnt.test",
        status: "running",
      });

      const captured: Reconciled[] = [];
      await pushCredentialReconcile(
        h.db,
        reconcilingRouter(captured),
        "dep_3@tnt.test",
        createNoopCredentialCipher(),
      );

      expect(captured).toHaveLength(0);
    });
  },
);
