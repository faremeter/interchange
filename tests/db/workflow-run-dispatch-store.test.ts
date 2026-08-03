import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import { createWorkflowRunDispatchStore } from "@intx/db";
import {
  sidecar,
  sidecarAllocation,
  workflowDefinition,
} from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedTenants, seedWorkflowRun } from "@intx/test-harness/seed";

const TENANT_ID = "tnt-dispatch";
const DEFINITION_ID = "wfd-dispatch";
const ANCHOR_RUN_ID = "dep-dispatch";

describe.skipIf(!harnessDbEnvAvailable())(
  "workflowRunDispatchStore (real DB)",
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
      await h.db.insert(workflowDefinition).values({
        id: DEFINITION_ID,
        tenantId: TENANT_ID,
        name: DEFINITION_ID,
      });
      await seedWorkflowRun(h.db, {
        id: ANCHOR_RUN_ID,
        deploymentId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
      });
    });

    async function seedAllocatedSidecar(generation: number) {
      await h.db.insert(sidecar).values({
        id: "sidecar-ack",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        credentialScope: "allocated",
        status: "online",
      });
      await h.db.insert(sidecarAllocation).values({
        id: "allocation-ack",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
        sidecarId: "sidecar-ack",
        placementSharing: "exclusive",
        sidecarReuse: "never",
        status: "allocated",
        generation,
        ensureAcceptedGeneration: generation,
      });
    }

    test("retains a message through acknowledgement until Git settlement", async () => {
      await seedAllocatedSidecar(3);
      const store = createWorkflowRunDispatchStore(h.db);
      const rawMessage = new TextEncoder().encode(
        "Message-ID: <dispatch-1@example>\r\n\r\nrun",
      );
      const enqueued = await store.enqueue({
        id: "dispatch-1",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "dispatch-message-1",
        rawMessage,
        stepGrants: [],
      });
      expect(enqueued.created).toBe(true);
      expect(enqueued.dispatch.status).toBe("pending");

      const claimed = await store.claimNextPending({
        leaseId: "delivery-lease-1",
        leaseDurationMs: 60_000,
      });
      expect(claimed?.id).toBe("dispatch-1");

      const acknowledged = await store.acknowledge({
        allocationId: "allocation-ack",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "dispatch-message-1",
        generation: 3,
      });
      expect(acknowledged?.status).toBe("acknowledged");
      expect(acknowledged?.acknowledgedGeneration).toBe(3);
      expect(acknowledged?.rawMessage).toEqual(rawMessage);
      expect(await store.listUnsettled(ANCHOR_RUN_ID)).toHaveLength(1);

      const settled = await store.settle(ANCHOR_RUN_ID, "dispatch-message-1");
      expect(settled?.status).toBe("settled");
      expect(settled?.rawMessage).toEqual(rawMessage);
      expect(await store.listUnsettled(ANCHOR_RUN_ID)).toEqual([]);
    });

    test("deduplicates an exact message and rejects a conflicting payload", async () => {
      const store = createWorkflowRunDispatchStore(h.db);
      const args = {
        id: "dispatch-dedup",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "dispatch-message-dedup",
        rawMessage: new Uint8Array([1, 2, 3]),
        stepGrants: [],
      };
      expect((await store.enqueue(args)).created).toBe(true);
      expect(
        (
          await store.enqueue({
            ...args,
            id: "dispatch-redelivery",
          })
        ).created,
      ).toBe(false);
      await expect(
        store.enqueue({
          ...args,
          id: "dispatch-conflict",
          rawMessage: new Uint8Array([9, 9, 9]),
        }),
      ).rejects.toThrow(/conflicts with its durable payload/);
    });

    test("requeues sidecar-acknowledged messages for a replacement", async () => {
      await seedAllocatedSidecar(1);
      const store = createWorkflowRunDispatchStore(h.db);
      await store.enqueue({
        id: "dispatch-requeue",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "dispatch-message-requeue",
        rawMessage: new Uint8Array([1]),
        stepGrants: [],
      });
      await store.acknowledge({
        allocationId: "allocation-ack",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "dispatch-message-requeue",
        generation: 1,
      });

      expect(await store.requeueUnsettled(ANCHOR_RUN_ID)).toBe(1);
      const requeued = await store.findById("dispatch-requeue");
      expect(requeued?.status).toBe("pending");
      expect(requeued?.acknowledgedGeneration).toBeNull();
      expect(requeued?.acknowledgedAt).toBeNull();
      expect(requeued?.nextAttemptAt).toBeInstanceOf(Date);
    });

    test("fences acknowledgement with the current allocation generation", async () => {
      await seedAllocatedSidecar(1);

      const store = createWorkflowRunDispatchStore(h.db);
      await store.enqueue({
        id: "dispatch-fenced-ack",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "message-fenced-ack",
        rawMessage: new Uint8Array([1]),
        stepGrants: [],
      });

      expect(
        await store.acknowledge({
          allocationId: "allocation-ack",
          anchorRunId: ANCHOR_RUN_ID,
          messageId: "message-fenced-ack",
          generation: 0,
        }),
      ).toBeNull();
      const acknowledged = await store.acknowledge({
        allocationId: "allocation-ack",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "message-fenced-ack",
        generation: 1,
      });
      expect(acknowledged?.status).toBe("acknowledged");
      expect(acknowledged?.acknowledgedGeneration).toBe(1);

      await store.requeueUnsettled(ANCHOR_RUN_ID);
      await h.db
        .update(sidecarAllocation)
        .set({
          status: "replacing",
          generation: 2,
          ensureAcceptedGeneration: null,
        })
        .where(eq(sidecarAllocation.id, "allocation-ack"));
      expect(
        await store.acknowledge({
          allocationId: "allocation-ack",
          anchorRunId: ANCHOR_RUN_ID,
          messageId: "message-fenced-ack",
          generation: 1,
        }),
      ).toBeNull();
      expect((await store.findById("dispatch-fenced-ack"))?.status).toBe(
        "pending",
      );
    });
  },
);
