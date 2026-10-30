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
import { credentialSecretColumns } from "@intx/db";
import {
  pushCredentialReconcile,
  pushCredentialRotation,
  recheckDeliveredCredentials,
  type RunCredentialsLoader,
  revokeRemovedCredentials,
  type SidecarRouter,
} from "@intx/hub-sessions";
import type { CredentialDelivery } from "@intx/types/credential-delivery";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedCredential,
  seedProvider,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

// The credential pushes that reach live deployments over `credentials.update`:
// the reconnect reconcile, the post-send re-check, the rotation push and the
// deploy-start re-check, against a real database.

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
  "credential push run fan-out (real DB)",
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
      // One tenant-owned credential and the definition the seeded runs name.
      // tnt_child inherits tnt_root's credentials, for the pushes that reach a
      // tenant's subtree.
      await seedTenants(h.db, [
        { id: "tnt_root" },
        { id: "tnt_child", parentId: "tnt_root" },
      ]);
      await seedProvider(h.db, {
        id: "prv_x",
        tenantId: "tnt_root",
        name: "prv-x",
        // The pushes re-resolve a credential's origin from its own provider,
        // so the provider needs an API base URL.
        apiBaseUrl: "https://api.anthropic.com",
      });
      await seedCredential(h.db, {
        id: "cred_a",
        tenantId: "tnt_root",
        providerId: "prv_x",
        name: "cred-a",
        secret: "sk-anthropic",
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_1",
        tenantId: "tnt_root",
        creatorPrincipalId: null,
        name: "agent-1",
        modelRequirements: [{ model: "opus" }],
      });
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
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only sendCredentialsUpdate is exercised by the push paths
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
      } as unknown as SidecarRouter;
    }

    async function revokeCredA(): Promise<void> {
      await h.db
        .update(credentialTable)
        .set({ status: "revoked" })
        .where(eq(credentialTable.id, "cred_a"));
    }

    async function deleteCredA(): Promise<void> {
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

        const frames: Reconciled[] = [];
        await pushCredentialRotation(
          h.db,
          reconcilingRouter(frames),
          "tnt_root",
          "cred_a",
          createNoopCredentialCipher(),
        );

        expect(frames.map((f) => f.address).sort()).toEqual([
          "dep_child@tnt.test",
          "dep_root@tnt.test",
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
      });

      test("sends nothing to a deployment once the credential is revoked", async () => {
        await rotateCredA();
        await revokeCredA();
        await seedDeployment("dep_root", "tnt_root", "running", ["cred_a"]);

        const frames: Reconciled[] = [];
        await pushCredentialRotation(
          h.db,
          reconcilingRouter(frames),
          "tnt_root",
          "cred_a",
          createNoopCredentialCipher(),
        );
        expect(frames).toEqual([]);
      });
    });

    describe("recheckDeliveredCredentials", () => {
      async function recheck(
        delivered: { credentialId: string; secretVersion?: number }[],
      ): Promise<Reconciled[]> {
        const captured: Reconciled[] = [];
        await recheckDeliveredCredentials(
          h.db,
          reconcilingRouter(captured),
          "dep_1@tnt.test",
          delivered,
          createNoopCredentialCipher(),
        );
        return captured;
      }

      test("sends nothing when the delivered version is current", async () => {
        expect(
          await recheck([{ credentialId: "cred_a", secretVersion: 1 }]),
        ).toEqual([]);
      });

      test("re-sends a credential rotated since the delivery read it", async () => {
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
        const captured = await recheck([
          { credentialId: "cred_a", secretVersion: 1 },
        ]);
        expect(captured).toHaveLength(1);
        expect(captured[0]?.revoke).toBeUndefined();
        expect(
          captured[0]?.delivery.materials.map((m) => ({
            id: m.credentialId,
            secret: m.secret,
            version: m.secretVersion,
          })),
        ).toEqual([{ id: "cred_a", secret: "sk-rotated", version: 2 }]);
      });

      test("revokes a removed credential and does not re-send it", async () => {
        await revokeCredA();
        expect(
          (await recheck([{ credentialId: "cred_a", secretVersion: 1 }])).map(
            (c) => c.revoke,
          ),
        ).toEqual([["cred_a"]]);
      });

      test("an unresolvable rotated credential does not block the revoke of another", async () => {
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
        // Delivered before versions existed, so the live row reads as rotated
        // and the re-resolve throws on the provider with no API origin.
        const captured = await recheck([
          { credentialId: "cred_no_origin" },
          { credentialId: "cred_deleted", secretVersion: 1 },
        ]);
        expect(captured.map((c) => c.revoke)).toEqual([["cred_deleted"]]);
      });
    });
  },
);
