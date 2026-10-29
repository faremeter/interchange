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
  modelOffering as modelOfferingTable,
  modelProvider as modelProviderTable,
  workflowDefinition,
} from "@intx/db/schema";
import { credentialSecretColumns } from "@intx/db";
import {
  pushCredentialReconcile,
  pushCredentialRotation,
  type RunCredentialsLoader,
  pushSourceUpdatesSubtree,
  revokeRemovedCredentials,
  type SidecarRouter,
} from "@intx/hub-sessions";
import type { InferenceSource } from "@intx/types/runtime";
import type { CredentialDelivery } from "@intx/types/credential-delivery";
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

const noRunCredentials: RunCredentialsLoader = async () => null;

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
      // tnt_child inherits tnt_root's catalog and credentials, for the pushes
      // that reach a tenant's subtree.
      await seedTenants(h.db, [
        { id: "tnt_root" },
        { id: "tnt_child", parentId: "tnt_root" },
      ]);
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
      await pushSourceUpdatesSubtree(
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
      await pushSourceUpdatesSubtree(
        h.db,
        recordingRouter(pushed),
        "tnt_root",
        createNoopCredentialCipher(),
      );

      expect(pushed).toHaveLength(0);
    });

    test("a source push after a revoke delivers neither the revoked secret nor its source", async () => {
      await seedWorkflowRun(h.db, {
        id: "run1",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        address: "run1@tnt.test",
        status: "running",
      });
      await h.db
        .update(credentialTable)
        .set({ status: "revoked" })
        .where(eq(credentialTable.id, "cred_a"));

      // A catalog edit or the rotation of another credential re-resolves every
      // running instance's sources. Record both frames the push can send.
      const delivered: CredentialDelivery[] = [];
      const pushed: Pushed[] = [];
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only the credentials + sources push methods are exercised by the push path
      const router = {
        sendCredentialsUpdate: (
          _address: string,
          delivery: CredentialDelivery,
        ) => {
          delivered.push(delivery);
          return Promise.resolve();
        },
        sendSourcesUpdate: (
          address: string,
          sources: InferenceSource[],
          defaultSource: string,
        ) => {
          pushed.push({ address, sources, default: defaultSource });
          return Promise.resolve();
        },
      } as unknown as SidecarRouter;
      await pushSourceUpdatesSubtree(
        h.db,
        router,
        "tnt_root",
        createNoopCredentialCipher(),
      );

      expect(delivered).toHaveLength(0);
      expect(pushed).toHaveLength(0);
    });

    test("revokeRemovedCredentials revokes only the deleted and revoked ids", async () => {
      await seedCredential(h.db, {
        id: "cred_revoked",
        tenantId: "tnt_root",
        providerId: "prv_x",
        name: "cred-revoked",
        status: "revoked",
      });
      await seedCredential(h.db, {
        id: "cred_expired",
        tenantId: "tnt_root",
        providerId: "prv_x",
        name: "cred-expired",
        status: "expired",
      });

      const captured: Reconciled[] = [];
      await revokeRemovedCredentials(
        h.db,
        reconcilingRouter(captured),
        "run1@tnt.test",
        ["cred_a", "cred_revoked", "cred_expired", "cred_deleted"],
      );

      // An expired credential stops new launches but is not evicted.
      expect(captured).toEqual([
        {
          address: "run1@tnt.test",
          delivery: { bindings: [], materials: [] },
          revoke: ["cred_revoked", "cred_deleted"],
        },
      ]);
    });

    test("revokeRemovedCredentials sends nothing when every id is alive", async () => {
      const captured: Reconciled[] = [];
      await revokeRemovedCredentials(
        h.db,
        reconcilingRouter(captured),
        "run1@tnt.test",
        ["cred_a"],
      );
      expect(captured).toEqual([]);
    });

    test("revokeRemovedCredentials is not blocked by a live credential with a misconfigured provider", async () => {
      // The re-resolve throws on a provider with no API origin; the removal
      // check reads status only, so a dead id is still revoked.
      await seedProvider(h.db, {
        id: "prv_no_origin",
        tenantId: "tnt_root",
        name: "prv-no-origin",
      });
      await seedCredential(h.db, {
        id: "cred_no_origin",
        tenantId: "tnt_root",
        providerId: "prv_no_origin",
        name: "cred-no-origin",
      });

      const captured: Reconciled[] = [];
      await revokeRemovedCredentials(
        h.db,
        reconcilingRouter(captured),
        "run1@tnt.test",
        ["cred_no_origin", "cred_deleted"],
      );
      expect(captured.map((c) => c.revoke)).toEqual([["cred_deleted"]]);
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
        noRunCredentials,
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
        noRunCredentials,
      );

      expect(captured).toHaveLength(1);
      // Merge: the survivors delivery is empty, and the dead id is named in
      // revoke so the child drops its material and binding.
      expect(captured[0]?.delivery.materials).toEqual([]);
      expect(captured[0]?.delivery.bindings).toEqual([]);
      expect(captured[0]?.revoke).toEqual(["cred_a"]);
    });

    // A router whose first credentials frame runs `duringFirstSend` before it
    // settles: a removal that commits after the pusher read the credential but
    // while its upsert is in flight. Every frame is recorded in send order.
    function racingRouter(
      captured: Reconciled[],
      duringFirstSend: () => Promise<void>,
      opts: { rejectFirst?: boolean } = {},
    ): SidecarRouter {
      let sends = 0;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only the credentials + sources push methods are exercised by the push paths
      return {
        sendCredentialsUpdate: async (
          address: string,
          delivery: CredentialDelivery,
          revoke?: string[],
        ) => {
          captured.push({ address, delivery, revoke });
          sends += 1;
          if (sends === 1) {
            await duringFirstSend();
            if (opts.rejectFirst === true) throw new Error("ack timed out");
          }
        },
        sendSourcesUpdate: () => Promise.resolve(),
      } as unknown as SidecarRouter;
    }

    async function revokeCredA(): Promise<void> {
      await h.db
        .update(credentialTable)
        .set({ status: "revoked" })
        .where(eq(credentialTable.id, "cred_a"));
    }

    async function deleteCredA(): Promise<void> {
      // The catalog provider restricts the delete, so remove it (and its
      // offering) first, as an operator must before deleting a credential a
      // provider references.
      await h.db
        .delete(modelOfferingTable)
        .where(eq(modelOfferingTable.providerId, "mpv_anthropic"));
      await h.db
        .delete(modelProviderTable)
        .where(eq(modelProviderTable.id, "mpv_anthropic"));
      await h.db
        .delete(credentialTable)
        .where(eq(credentialTable.id, "cred_a"));
    }

    for (const [label, remove] of [
      ["revoked", revokeCredA],
      ["deleted", deleteCredA],
    ] as const) {
      test(`reconcile revokes a credential ${label} while its upsert is in flight`, async () => {
        await seedWorkflowRun(h.db, {
          id: "dep_race",
          tenantId: "tnt_root",
          definitionId: "wfd_1",
          anchorRunId: "dep_race",
          address: "dep_race@tnt.test",
          status: "running",
          credentialRefs: { credentialIds: ["cred_a"], bindings: [] },
        });

        const captured: Reconciled[] = [];
        await pushCredentialReconcile(
          h.db,
          racingRouter(captured, remove),
          "dep_race@tnt.test",
          createNoopCredentialCipher(),
          noRunCredentials,
        );

        // The upsert carried the secret it read; the revoke goes out after it,
        // so the sidecar applies the removal last.
        expect(captured).toHaveLength(2);
        expect(
          captured[0]?.delivery.materials.map((m) => m.credentialId),
        ).toEqual(["cred_a"]);
        expect(captured[1]).toEqual({
          address: "dep_race@tnt.test",
          delivery: { bindings: [], materials: [] },
          revoke: ["cred_a"],
        });
      });
    }

    test("reconcile still revokes after its upsert send fails", async () => {
      // A failed send can still have reached the sidecar (a lost ack), so the
      // re-check runs regardless.
      await seedWorkflowRun(h.db, {
        id: "dep_lost_ack",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        anchorRunId: "dep_lost_ack",
        address: "dep_lost_ack@tnt.test",
        status: "running",
        credentialRefs: { credentialIds: ["cred_a"], bindings: [] },
      });

      const captured: Reconciled[] = [];
      await pushCredentialReconcile(
        h.db,
        racingRouter(captured, revokeCredA, { rejectFirst: true }),
        "dep_lost_ack@tnt.test",
        createNoopCredentialCipher(),
        noRunCredentials,
      );

      expect(captured.map((c) => c.revoke)).toEqual([undefined, ["cred_a"]]);
    });

    test("reconcile does not re-check the hub tokens it delivers", async () => {
      // A hub token has no credential row, so a re-check of it would revoke the
      // token the reconcile just delivered.
      await seedWorkflowRun(h.db, {
        id: "dep_hub",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        anchorRunId: "dep_hub",
        address: "dep_hub@tnt.test",
        status: "running",
        credentialRefs: {
          credentialIds: ["cred_a"],
          bindings: [],
          hubConsumers: ["action:a"],
        },
      });
      const captured: Reconciled[] = [];
      await pushCredentialReconcile(
        h.db,
        reconcilingRouter(captured),
        "dep_hub@tnt.test",
        createNoopCredentialCipher(),
        async () => ({
          bindings: [
            {
              handle: "interchange.hub",
              credentialId: "hub:action:a",
              consumer: "action:a",
            },
          ],
          materials: [
            {
              credentialId: "hub:action:a",
              providerKey: "http",
              origin: "https://hub.test",
              secret: "itx_run_token",
            },
          ],
        }),
      );
      expect(captured).toHaveLength(1);
      expect(captured[0]?.revoke).toBeUndefined();
    });

    test("a source push with nothing removed sends a single material frame", async () => {
      await seedWorkflowRun(h.db, {
        id: "run_ok",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        address: "run_ok@tnt.test",
        status: "running",
      });
      const captured: Reconciled[] = [];
      await pushSourceUpdatesSubtree(
        h.db,
        racingRouter(captured, async () => undefined),
        "tnt_root",
        createNoopCredentialCipher(),
      );
      expect(captured).toHaveLength(1);
    });

    test("a source push re-checks after its material send fails, and sends no sources", async () => {
      await seedWorkflowRun(h.db, {
        id: "run_fail",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        address: "run_fail@tnt.test",
        status: "running",
      });
      const captured: Reconciled[] = [];
      const pushed: Pushed[] = [];
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only the credentials + sources push methods are exercised by the push path
      const router = {
        ...racingRouter(captured, revokeCredA, { rejectFirst: true }),
        sendSourcesUpdate: (
          address: string,
          sources: InferenceSource[],
          defaultSource: string,
        ) => {
          pushed.push({ address, sources, default: defaultSource });
          return Promise.resolve();
        },
      } as unknown as SidecarRouter;
      await pushSourceUpdatesSubtree(
        h.db,
        router,
        "tnt_root",
        createNoopCredentialCipher(),
      );
      expect(captured.map((c) => c.revoke)).toEqual([undefined, ["cred_a"]]);
      expect(pushed).toEqual([]);
    });

    test("a source push revokes a credential revoked while its material is in flight", async () => {
      await seedWorkflowRun(h.db, {
        id: "run1",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        address: "run1@tnt.test",
        status: "running",
      });

      const captured: Reconciled[] = [];
      await pushSourceUpdatesSubtree(
        h.db,
        racingRouter(captured, revokeCredA),
        "tnt_root",
        createNoopCredentialCipher(),
      );

      expect(captured).toHaveLength(2);
      expect(
        captured[0]?.delivery.materials.map((m) => m.credentialId),
      ).toEqual(["cred_a"]);
      expect(captured[1]?.revoke).toEqual(["cred_a"]);
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
        noRunCredentials,
      );

      expect(captured).toHaveLength(0);
    });

    test("reconcile restores the run's hub credentials after a reconnect", async () => {
      await seedWorkflowRun(h.db, {
        id: "dep_4",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        anchorRunId: "dep_4",
        address: "dep_4@tnt.test",
        status: "running",
        credentialRefs: {
          credentialIds: [],
          bindings: [],
          hubConsumers: ["action:a"],
        },
      });
      const loadedFor: string[] = [];
      const captured: Reconciled[] = [];
      await pushCredentialReconcile(
        h.db,
        reconcilingRouter(captured),
        "dep_4@tnt.test",
        createNoopCredentialCipher(),
        async (runId) => {
          loadedFor.push(runId);
          return {
            bindings: [
              {
                handle: "interchange.hub",
                credentialId: "hub:action:a",
                consumer: "action:a",
              },
            ],
            materials: [
              {
                credentialId: "hub:action:a",
                providerKey: "http",
                origin: "https://hub.test",
                secret: "itx_run_token",
              },
            ],
          };
        },
      );

      expect(loadedFor).toEqual(["dep_4"]);
      expect(
        captured[0]?.delivery.materials.map((m) => m.credentialId),
      ).toEqual(["hub:action:a"]);
      expect(captured[0]?.revoke).toBeUndefined();
    });

    test("reconcile does not revoke the hub credentials of a run that has no tokens yet", async () => {
      await seedWorkflowRun(h.db, {
        id: "dep_5",
        tenantId: "tnt_root",
        definitionId: "wfd_1",
        anchorRunId: "dep_5",
        address: "dep_5@tnt.test",
        status: "running",
        credentialRefs: {
          credentialIds: [],
          bindings: [],
          hubConsumers: ["action:a"],
        },
      });
      const captured: Reconciled[] = [];
      await pushCredentialReconcile(
        h.db,
        reconcilingRouter(captured),
        "dep_5@tnt.test",
        createNoopCredentialCipher(),
        noRunCredentials,
      );

      // The first trigger can still mint the tokens and deliver them on its
      // barrier, so a revoke here could remove them after that delivery.
      expect(captured[0]?.delivery.materials).toEqual([]);
      expect(captured[0]?.revoke).toBeUndefined();
    });

    describe("pushCredentialRotation", () => {
      // A router that records every credentials frame and source push.
      function rotationRouter(): {
        router: SidecarRouter;
        frames: Reconciled[];
        pushed: Pushed[];
      } {
        const frames: Reconciled[] = [];
        const pushed: Pushed[] = [];
        const router: SidecarRouter = {
          ...recordingRouter(pushed),
          sendCredentialsUpdate:
            reconcilingRouter(frames).sendCredentialsUpdate,
        };
        return { router, frames, pushed };
      }

      async function rotateCredA(): Promise<void> {
        await h.db
          .update(credentialTable)
          .set(
            await credentialSecretColumns(
              createNoopCredentialCipher(),
              "cred_a",
              "sk-rotated",
            ),
          )
          .where(eq(credentialTable.id, "cred_a"));
      }

      async function seedDeployment(
        id: string,
        tenantId: string,
        status: "deployed" | "running" | "completed",
        credentialIds: string[],
      ): Promise<void> {
        await seedWorkflowRun(h.db, {
          id,
          tenantId,
          definitionId: "wfd_1",
          anchorRunId: id,
          address: `${id}@tnt.test`,
          status,
          credentialRefs: { credentialIds, bindings: [] },
        });
      }

      test("reaches each live holder in the subtree and skips the others", async () => {
        await rotateCredA();
        await seedDeployment("dep_root", "tnt_root", "running", ["cred_a"]);
        await seedDeployment("dep_child", "tnt_child", "deployed", ["cred_a"]);
        await seedDeployment("dep_other", "tnt_root", "running", ["cred_b"]);
        await seedDeployment("dep_done", "tnt_root", "completed", ["cred_a"]);
        // An instance resolves its definition in its own tenant only.
        await h.db.insert(workflowDefinition).values({
          id: "wfd_child",
          tenantId: "tnt_child",
          creatorPrincipalId: null,
          name: "agent-child",
          modelRequirements: [{ model: "opus" }],
        });
        await seedWorkflowRun(h.db, {
          id: "run_child",
          tenantId: "tnt_child",
          definitionId: "wfd_child",
          address: "run_child@tnt.test",
          status: "running",
        });

        const { router, frames, pushed } = rotationRouter();
        await pushCredentialRotation(
          h.db,
          router,
          "tnt_root",
          "cred_a",
          createNoopCredentialCipher(),
        );

        expect(frames.map((f) => f.address).sort()).toEqual([
          "dep_child@tnt.test",
          "dep_root@tnt.test",
          "run_child@tnt.test",
        ]);
        for (const frame of frames) {
          expect(frame.revoke).toBeUndefined();
          expect(
            frame.delivery.materials.map((m) => ({
              id: m.credentialId,
              secret: m.secret,
              version: m.secretVersion,
            })),
          ).toEqual([{ id: "cred_a", secret: "sk-rotated", version: 2 }]);
        }
        // Only the folded instance gets a source list; a deployment's sources
        // reference the credential by id and read the rotated cell.
        expect(pushed.map((p) => p.address)).toEqual(["run_child@tnt.test"]);
      });

      test("sends nothing to a deployment once the credential is revoked", async () => {
        await rotateCredA();
        await revokeCredA();
        await seedDeployment("dep_root", "tnt_root", "running", ["cred_a"]);

        const { router, frames, pushed } = rotationRouter();
        await pushCredentialRotation(
          h.db,
          router,
          "tnt_root",
          "cred_a",
          createNoopCredentialCipher(),
        );
        expect(frames).toEqual([]);
        expect(pushed).toEqual([]);
      });
    });
  },
);
