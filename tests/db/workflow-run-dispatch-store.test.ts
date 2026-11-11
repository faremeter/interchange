import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq, sql } from "drizzle-orm";

import {
  createWorkflowRunDispatchStore,
  WorkflowRunDispatchPayloadConflictError,
} from "@intx/db";
import {
  sidecar,
  sidecarAllocation,
  workflowDefinition,
  workflowRunDispatch,
} from "@intx/db/schema";
import { deriveDeliveryKey } from "@intx/types/message-id";
import { RunGrantsFrame, SignalDeliverFrame } from "@intx/types/sidecar";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedTenants, seedWorkflowRun } from "@intx/test-harness/seed";

const TENANT_ID = "tnt-dispatch";
const DEFINITION_ID = "wfd-dispatch";
const ANCHOR_RUN_ID = "dep-dispatch";
const SENDER_ADDRESS = "principal-dispatch@tnt-dispatch.example";
const MANIFEST_DIGEST = "1".repeat(64);

function observe(work: Promise<unknown>) {
  let settled = false;
  const result = work.then(
    (value) => {
      settled = true;
      return { status: "fulfilled", value } as const;
    },
    (reason: unknown) => {
      settled = true;
      return { status: "rejected", reason } as const;
    },
  );
  return { result, isSettled: () => settled };
}

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
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
      });
    });

    async function seedAllocatedSidecar(generation: number) {
      await h.db.insert(sidecar).values({
        id: "sidecar-ack",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        status: "online",
      });
      await h.db.insert(sidecarAllocation).values({
        id: "allocation-ack",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
        sidecarId: "sidecar-ack",
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
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "dispatch-message-1",
        senderAddress: SENDER_ADDRESS,
        rawMessage,
        stepGrants: [],
      });
      expect(enqueued.created).toBe(true);
      expect(enqueued.dispatch.kind).toBe("mail");
      expect(enqueued.dispatch.status).toBe("pending");
      expect(enqueued.dispatch.senderAddress).toBe(SENDER_ADDRESS);

      const claimed = await store.claimNextPending({
        leaseId: "delivery-lease-1",
        leaseDurationMs: 60_000,
      });
      expect(claimed?.id).toBe(enqueued.dispatch.id);

      const acknowledged = await store.acknowledge({
        allocationId: "allocation-ack",
        anchorRunId: ANCHOR_RUN_ID,
        deliveryKey: enqueued.dispatch.deliveryKey,
        generation: 3,
      });
      expect(acknowledged?.status).toBe("acknowledged");
      expect(acknowledged?.acknowledgedGeneration).toBe(3);
      expect(acknowledged?.rawMessage).toEqual(rawMessage);
      expect(await store.listUnsettled(ANCHOR_RUN_ID)).toHaveLength(1);

      const settled = await store.settle(
        ANCHOR_RUN_ID,
        enqueued.dispatch.deliveryKey,
      );
      expect(settled?.status).toBe("settled");
      expect(settled?.rawMessage).toEqual(rawMessage);
      expect(await store.listUnsettled(ANCHOR_RUN_ID)).toEqual([]);
    });

    test("excludes an executing dispatch even after its database lease expires", async () => {
      const store = createWorkflowRunDispatchStore(h.db);
      const enqueueNamed = async (name: string) =>
        (
          await store.enqueue({
            anchorRunId: ANCHOR_RUN_ID,
            messageId: `message-${name}`,
            senderAddress: SENDER_ADDRESS,
            rawMessage: new TextEncoder().encode(name),
            stepGrants: [],
          })
        ).dispatch.id;
      const stuck = await enqueueNamed("stuck");
      const healthy = await enqueueNamed("healthy");
      expect(
        (
          await store.claimNextPending({
            leaseId: "original-lease",
            leaseDurationMs: 60_000,
          })
        )?.id,
      ).toBe(stuck);
      await h.db
        .update(workflowRunDispatch)
        .set({ deliveryLeaseExpiresAt: new Date(0) })
        .where(eq(workflowRunDispatch.id, stuck));

      expect(
        (
          await store.claimNextPending({
            leaseId: "healthy-lease",
            leaseDurationMs: 60_000,
            excludedDispatchIds: [stuck],
          })
        )?.id,
      ).toBe(healthy);
      expect(
        await store.claimNextPending({
          leaseId: "excluded-lease",
          leaseDurationMs: 60_000,
          excludedDispatchIds: [stuck],
        }),
      ).toBeNull();
      expect(
        (
          await store.claimNextPending({
            leaseId: "retry-lease",
            leaseDurationMs: 60_000,
            excludedDispatchIds: [],
          })
        )?.id,
      ).toBe(stuck);
    });

    test("deduplicates an exact message and rejects a conflicting payload", async () => {
      const store = createWorkflowRunDispatchStore(h.db);
      const stepGrants = RunGrantsFrame.assert({
        type: "run.grants",
        agentAddress: "workflow@tenant.example",
        runId: ANCHOR_RUN_ID,
        stepGrants: [
          {
            id: "grant-1",
            resource: "tool:mail",
            action: "send",
            effect: "allow",
            origin: "creator",
            conditions: null,
            expiresAt: null,
            roleId: null,
            principalId: null,
          },
        ],
      }).stepGrants;
      const args = {
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "dispatch-message-dedup",
        senderAddress: SENDER_ADDRESS,
        rawMessage: new Uint8Array([1, 2, 3]),
        stepGrants,
      };
      expect((await store.enqueue(args)).created).toBe(true);
      expect((await store.enqueue({ ...args })).created).toBe(false);
      await expect(
        store.enqueue({
          ...args,
          rawMessage: new Uint8Array([9, 9, 9]),
        }),
      ).rejects.toThrow(/conflicts with its durable payload/);
      await expect(
        store.enqueue({
          ...args,
          stepGrants: [],
        }),
      ).rejects.toThrow(/conflicts with its durable payload/);
    });

    test("requeues a run's unsettled messages and leaves the rest alone", async () => {
      await seedAllocatedSidecar(1);
      await seedWorkflowRun(h.db, {
        id: "dep-dispatch-other",
        anchorRunId: "dep-dispatch-other",
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
      });
      await h.db.insert(sidecar).values({
        id: "sidecar-other",
        tokenHashSha256: new Uint8Array([4, 5, 6]),
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        status: "online",
      });
      await h.db.insert(sidecarAllocation).values({
        id: "allocation-other",
        anchorRunId: "dep-dispatch-other",
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
        sidecarId: "sidecar-other",
        status: "allocated",
        generation: 1,
        ensureAcceptedGeneration: 1,
      });
      const store = createWorkflowRunDispatchStore(h.db);
      const enqueueAcknowledged = async (
        name: string,
        allocationId: string,
        anchorRunId: string,
      ) => {
        const { dispatch } = await store.enqueue({
          anchorRunId,
          messageId: `${name}-message`,
          senderAddress: SENDER_ADDRESS,
          rawMessage: new Uint8Array([1]),
          stepGrants: [],
        });
        await store.acknowledge({
          allocationId,
          anchorRunId,
          deliveryKey: dispatch.deliveryKey,
          generation: 1,
        });
        return dispatch;
      };
      const requeue = await enqueueAcknowledged(
        "dispatch-requeue",
        "allocation-ack",
        ANCHOR_RUN_ID,
      );
      const settledDispatch = await enqueueAcknowledged(
        "dispatch-settled",
        "allocation-ack",
        ANCHOR_RUN_ID,
      );
      await store.settle(ANCHOR_RUN_ID, settledDispatch.deliveryKey);
      const otherRunDispatch = await enqueueAcknowledged(
        "dispatch-other-run",
        "allocation-other",
        "dep-dispatch-other",
      );

      expect(await store.requeueUnsettled(ANCHOR_RUN_ID)).toBe(1);
      const requeued = await store.findById(requeue.id);
      expect(requeued?.status).toBe("pending");
      expect(requeued?.acknowledgedGeneration).toBeNull();
      expect(requeued?.acknowledgedAt).toBeNull();
      expect(requeued?.nextAttemptAt).toBeInstanceOf(Date);
      expect((await store.findById(settledDispatch.id))?.status).toBe(
        "settled",
      );
      const otherRun = await store.findById(otherRunDispatch.id);
      expect(otherRun?.status).toBe("acknowledged");
      expect(otherRun?.acknowledgedGeneration).toBe(1);
    });

    test("fences acknowledgement with the current allocation generation", async () => {
      await seedAllocatedSidecar(1);

      const store = createWorkflowRunDispatchStore(h.db);
      const { dispatch: fenced } = await store.enqueue({
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "message-fenced-ack",
        senderAddress: SENDER_ADDRESS,
        rawMessage: new Uint8Array([1]),
        stepGrants: [],
      });

      expect(
        await store.acknowledge({
          allocationId: "allocation-ack",
          anchorRunId: ANCHOR_RUN_ID,
          deliveryKey: fenced.deliveryKey,
          generation: 0,
        }),
      ).toBeNull();
      const acknowledged = await store.acknowledge({
        allocationId: "allocation-ack",
        anchorRunId: ANCHOR_RUN_ID,
        deliveryKey: fenced.deliveryKey,
        generation: 1,
      });
      expect(acknowledged?.status).toBe("acknowledged");
      expect(acknowledged?.acknowledgedGeneration).toBe(1);

      await store.requeueUnsettled(ANCHOR_RUN_ID);
      await h.db
        .update(sidecarAllocation)
        .set({
          status: "releasing",
          generation: 2,
          ensureAcceptedGeneration: null,
        })
        .where(eq(sidecarAllocation.id, "allocation-ack"));
      expect(
        await store.acknowledge({
          allocationId: "allocation-ack",
          anchorRunId: ANCHOR_RUN_ID,
          deliveryKey: fenced.deliveryKey,
          generation: 1,
        }),
      ).toBeNull();
      expect((await store.findById(fenced.id))?.status).toBe("pending");
    });

    test("persists and deduplicates an idempotent signal payload", async () => {
      const store = createWorkflowRunDispatchStore(h.db);
      const args = {
        anchorRunId: ANCHOR_RUN_ID,
        signal: {
          agentAddress: "workflow@tenant.example",
          runId: "workflow@tenant.example",
          signalName: "continue",
          signalId: "signal-1",
          payload: { approved: true },
        },
      };

      const enqueued = await store.enqueueSignal(args);
      expect(enqueued.created).toBe(true);
      expect(enqueued.dispatch.kind).toBe("signal");
      expect(enqueued.dispatch.senderAddress).toBeNull();
      expect(
        SignalDeliverFrame.assert(
          JSON.parse(new TextDecoder().decode(enqueued.dispatch.rawMessage)),
        ),
      ).toEqual({ type: "signal.deliver", ...args.signal });
      expect((await store.enqueueSignal({ ...args })).created).toBe(false);
      await expect(
        store.enqueueSignal({
          ...args,
          signal: { ...args.signal, payload: { approved: false } },
        }),
      ).rejects.toBeInstanceOf(WorkflowRunDispatchPayloadConflictError);
    });

    test("refuses a signal and a mail that share one id string, in either order", async () => {
      const store = createWorkflowRunDispatchStore(h.db);
      const signal = (signalId: string) =>
        store.enqueueSignal({
          anchorRunId: ANCHOR_RUN_ID,
          signal: {
            agentAddress: "workflow@tenant.example",
            runId: "workflow@tenant.example",
            signalName: "continue",
            signalId,
            payload: { approved: true },
          },
        });
      const mail = (messageId: string) =>
        store.enqueue({
          anchorRunId: ANCHOR_RUN_ID,
          messageId,
          senderAddress: SENDER_ADDRESS,
          rawMessage: new TextEncoder().encode("mail"),
          stepGrants: [],
        });

      const first = await signal("signal-first");
      expect(first.dispatch.deliveryKey).toBe("signal-first");
      await expect(mail("signal-first")).rejects.toThrow(
        /conflicts with its durable payload/,
      );

      const mailed = await mail("mail-first-id");
      expect(mailed.dispatch.deliveryKey).toBe(
        await deriveDeliveryKey(SENDER_ADDRESS, "mail-first-id"),
      );
      await expect(signal("mail-first-id")).rejects.toThrow(
        /conflicts with its durable payload/,
      );
    });

    test("refuses a signal whose id names a mail row kept from before the delivery key", async () => {
      const store = createWorkflowRunDispatchStore(h.db);
      const { dispatch } = await store.enqueue({
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "pre-key",
        senderAddress: SENDER_ADDRESS,
        rawMessage: new TextEncoder().encode("mail"),
        stepGrants: [],
      });
      await h.db
        .update(workflowRunDispatch)
        .set({ id: `dispatch:${ANCHOR_RUN_ID}:pre-key`, status: "settled" })
        .where(eq(workflowRunDispatch.id, dispatch.id));

      await expect(
        store.enqueueSignal({
          anchorRunId: ANCHOR_RUN_ID,
          signal: {
            agentAddress: "workflow@tenant.example",
            runId: "workflow@tenant.example",
            signalName: "continue",
            signalId: "pre-key",
            payload: null,
          },
        }),
      ).rejects.toThrow(/conflicts with its durable payload/);
    });

    describe("anchor lock", () => {
      const sharedSignal = {
        agentAddress: "workflow@tenant.example",
        runId: "workflow@tenant.example",
        signalName: "continue",
        signalId: "shared-id",
        payload: null,
      };

      async function backendPid(executor: Pick<TestDb["db"], "execute">) {
        const [backend] = await executor.execute(
          sql`select pg_backend_pid() as pid`,
        );
        const pid = backend?.["pid"];
        if (typeof pid !== "number") throw new Error("Missing backend PID");
        return pid;
      }

      async function waitForBlocked(
        blockerPid: number,
        operation: ReturnType<typeof observe>,
      ) {
        // Postgres exposes lock waits through its activity view, without a
        // notification.
        for (;;) {
          const waiting = await h.db.execute(
            sql`select pid from pg_stat_activity where ${blockerPid} = any(pg_blocking_pids(pid))`,
          );
          const pid = waiting[0]?.["pid"];
          if (typeof pid === "number") return pid;
          if (operation.isSettled()) {
            throw new Error("The enqueue did not wait for the anchor lock");
          }
          await new Promise<void>((resolve) => setImmediate(resolve));
        }
      }

      test("a cross-kind enqueue waits for the anchor lock of a concurrent enqueue", async () => {
        const store = createWorkflowRunDispatchStore(h.db);
        const holding = Promise.withResolvers<number>();
        const release = Promise.withResolvers<undefined>();
        const holder = h.db.transaction(async (tx) => {
          await store.enqueue(
            {
              anchorRunId: ANCHOR_RUN_ID,
              messageId: "shared-id",
              senderAddress: SENDER_ADDRESS,
              rawMessage: new TextEncoder().encode("mail"),
              stepGrants: [],
            },
            tx,
          );
          holding.resolve(await backendPid(tx));
          await release.promise;
        });
        void holder.catch(holding.reject);
        let signal: ReturnType<typeof observe> | undefined;
        try {
          const holderPid = await holding.promise;
          signal = observe(
            store.enqueueSignal({
              anchorRunId: ANCHOR_RUN_ID,
              signal: sharedSignal,
            }),
          );
          await waitForBlocked(holderPid, signal);
        } finally {
          release.resolve(undefined);
          await holder;
          await signal?.result;
        }
        const outcome = await signal.result;
        expect(outcome.status).toBe("rejected");
        if (outcome.status === "rejected") {
          expect(outcome.reason).toBeInstanceOf(
            WorkflowRunDispatchPayloadConflictError,
          );
        }
      });

      test("an enqueue with no caller transaction holds the anchor lock until its insert commits", async () => {
        const store = createWorkflowRunDispatchStore(h.db);
        // Park every mail insert behind a row lock the test holds, so the
        // mail enqueue stops between its anchor lock and its insert.
        await h.db
          .execute(sql`create function park_mail_insert() returns trigger language plpgsql as $$
          begin
            perform 1 from workflow_definition where id = 'wfd-dispatch' for update;
            return new;
          end
        $$`);
        await h.db
          .execute(sql`create trigger park_mail_insert before insert on workflow_run_dispatch
          for each row when (new.kind = 'mail') execute function park_mail_insert()`);
        const parked = Promise.withResolvers<number>();
        const release = Promise.withResolvers<undefined>();
        const parker = h.db.transaction(async (tx) => {
          await tx
            .select()
            .from(workflowDefinition)
            .where(eq(workflowDefinition.id, DEFINITION_ID))
            .for("update");
          parked.resolve(await backendPid(tx));
          await release.promise;
        });
        void parker.catch(parked.reject);
        let mail: ReturnType<typeof observe> | undefined;
        let signal: ReturnType<typeof observe> | undefined;
        try {
          const parkerPid = await parked.promise;
          mail = observe(
            store.enqueue({
              anchorRunId: ANCHOR_RUN_ID,
              messageId: "shared-id",
              senderAddress: SENDER_ADDRESS,
              rawMessage: new TextEncoder().encode("mail"),
              stepGrants: [],
            }),
          );
          const mailPid = await waitForBlocked(parkerPid, mail);
          signal = observe(
            store.enqueueSignal({
              anchorRunId: ANCHOR_RUN_ID,
              signal: sharedSignal,
            }),
          );
          await waitForBlocked(mailPid, signal);
        } finally {
          release.resolve(undefined);
          await parker;
          await Promise.all([mail?.result, signal?.result]);
          await h.db.execute(
            sql`drop trigger park_mail_insert on workflow_run_dispatch`,
          );
          await h.db.execute(sql`drop function park_mail_insert()`);
        }
        const mailOutcome = await mail.result;
        expect(mailOutcome.status).toBe("fulfilled");
        const signalOutcome = await signal.result;
        expect(signalOutcome.status).toBe("rejected");
        if (signalOutcome.status === "rejected") {
          expect(signalOutcome.reason).toBeInstanceOf(
            WorkflowRunDispatchPayloadConflictError,
          );
        }
      });
    });

    test("keeps two senders' mail with one Message-ID apart and de-duplicates one sender's repeat", async () => {
      const store = createWorkflowRunDispatchStore(h.db);
      const other = "other-principal@tnt-dispatch.example";
      const enqueue = (senderAddress: string, body: string) =>
        store.enqueue({
          anchorRunId: ANCHOR_RUN_ID,
          messageId: "<shared@example>",
          senderAddress,
          rawMessage: new TextEncoder().encode(body),
          stepGrants: [],
        });

      const first = await enqueue(SENDER_ADDRESS, "first");
      const second = await enqueue(other, "second");
      expect(first.created).toBe(true);
      expect(second.created).toBe(true);
      expect(second.dispatch.id).not.toBe(first.dispatch.id);

      const repeat = await enqueue(SENDER_ADDRESS, "first");
      expect(repeat.created).toBe(false);
      expect(repeat.dispatch.id).toBe(first.dispatch.id);
      await expect(enqueue(SENDER_ADDRESS, "changed")).rejects.toThrow(
        /conflicts with its durable payload/,
      );
    });

    test("acknowledges only the row of the sender the key names", async () => {
      await seedAllocatedSidecar(1);
      const store = createWorkflowRunDispatchStore(h.db);
      const enqueue = async (senderAddress: string) =>
        (
          await store.enqueue({
            anchorRunId: ANCHOR_RUN_ID,
            messageId: "<ack@example>",
            senderAddress,
            rawMessage: new TextEncoder().encode(senderAddress),
            stepGrants: [],
          })
        ).dispatch;
      const first = await enqueue(SENDER_ADDRESS);
      const second = await enqueue("second@tnt-dispatch.example");
      const ack = {
        allocationId: "allocation-ack",
        anchorRunId: ANCHOR_RUN_ID,
        generation: 1,
      };

      await store.acknowledge({ ...ack, deliveryKey: first.deliveryKey });
      expect((await store.findById(first.id))?.status).toBe("acknowledged");
      expect((await store.findById(second.id))?.status).toBe("pending");
    });

    test("uses the supplied transaction for both dispatch kinds", async () => {
      const store = createWorkflowRunDispatchStore(h.db);
      const rolledBack: string[] = [];
      await expect(
        h.db.transaction(async (tx) => {
          const mail = await store.enqueue(
            {
              anchorRunId: ANCHOR_RUN_ID,
              messageId: "message-rollback",
              senderAddress: SENDER_ADDRESS,
              rawMessage: new Uint8Array([1, 2, 3]),
              stepGrants: [],
            },
            tx,
          );
          rolledBack.push(mail.dispatch.id);
          const signal = await store.enqueueSignal(
            {
              anchorRunId: ANCHOR_RUN_ID,
              signal: {
                agentAddress: "workflow@tenant.example",
                runId: "workflow@tenant.example",
                signalName: "continue",
                signalId: "signal-rollback",
                payload: null,
              },
            },
            tx,
          );
          rolledBack.push(signal.dispatch.id);
          throw new Error("force rollback");
        }),
      ).rejects.toThrow("force rollback");
      expect(rolledBack).toHaveLength(2);
      for (const id of rolledBack) {
        expect(await store.findById(id)).toBeNull();
      }
    });

    test("abandons outstanding attempts without overwriting settled deliveries", async () => {
      const store = createWorkflowRunDispatchStore(h.db);
      const { dispatch: consumed } = await store.enqueue({
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "message-consumed",
        senderAddress: SENDER_ADDRESS,
        rawMessage: new Uint8Array([1]),
        stepGrants: [],
      });
      const { dispatch: tooLate } = await store.enqueueSignal({
        anchorRunId: ANCHOR_RUN_ID,
        signal: {
          agentAddress: "workflow@tenant.example",
          runId: "workflow@tenant.example",
          signalName: "continue",
          signalId: "signal-too-late",
          payload: null,
        },
      });
      await store.settle(ANCHOR_RUN_ID, consumed.deliveryKey);

      await expect(
        store.abandonUnsettled(
          ANCHOR_RUN_ID,
          "workflow_run_terminal",
          "run is terminal",
        ),
      ).resolves.toBe(1);
      expect((await store.findById(consumed.id))?.status).toBe("settled");
      expect(await store.findById(tooLate.id)).toMatchObject({
        status: "abandoned",
        failureCode: "workflow_run_terminal",
        failureMessage: "run is terminal",
      });
      expect((await store.findById(tooLate.id))?.nextAttemptAt).not.toBeNull();
    });

    test("abandonment cannot be retried, but can be resolved by acceptance or rejection", async () => {
      const store = createWorkflowRunDispatchStore(h.db);
      await seedAllocatedSidecar(1);
      const enqueueNamed = async (name: string) =>
        (
          await store.enqueue({
            anchorRunId: ANCHOR_RUN_ID,
            messageId: name,
            senderAddress: SENDER_ADDRESS,
            rawMessage: new Uint8Array([1]),
            stepGrants: [],
          })
        ).dispatch;
      const accepted = await enqueueNamed("accepted");
      const rejected = await enqueueNamed("rejected");
      const unknown = await enqueueNamed("unknown");
      await store.fail({
        anchorRunId: ANCHOR_RUN_ID,
        deliveryKey: rejected.deliveryKey,
        code: "invalid_mail",
        message: "Recorded rejection",
      });
      expect(
        await store.abandonUnsettled(
          ANCHOR_RUN_ID,
          "workflow_cancelled",
          "Stopped",
        ),
      ).toBe(2);
      expect(
        await store.abandonUnsettled(ANCHOR_RUN_ID, "another_reason", "Retry"),
      ).toBe(0);
      expect(
        await store.acknowledge({
          allocationId: "allocation-ack",
          anchorRunId: ANCHOR_RUN_ID,
          generation: 1,
          deliveryKey: accepted.deliveryKey,
        }),
      ).toBeNull();
      expect(await store.requeueUnsettled(ANCHOR_RUN_ID)).toBe(0);
      expect(
        await store.claimNextPending({
          leaseId: "after-abandonment",
          leaseDurationMs: 60_000,
        }),
      ).toBeNull();
      expect(
        (await store.listUnsettled(ANCHOR_RUN_ID)).map((row) => row.status),
      ).toEqual(["abandoned", "abandoned"]);
      expect(await store.findById(unknown.id)).toMatchObject({
        status: "abandoned",
        failureCode: "workflow_cancelled",
      });
      expect(
        (await store.settle(ANCHOR_RUN_ID, accepted.deliveryKey))?.status,
      ).toBe("settled");
      expect(
        await store.settle(ANCHOR_RUN_ID, rejected.deliveryKey),
      ).toBeNull();
      expect(
        (
          await store.fail({
            anchorRunId: ANCHOR_RUN_ID,
            deliveryKey: unknown.deliveryKey,
            code: "invalid_mail",
            message: "Later recorded rejection",
          })
        )?.status,
      ).toBe("failed");
      expect(
        await store.fail({
          anchorRunId: ANCHOR_RUN_ID,
          deliveryKey: accepted.deliveryKey,
          code: "workflow_cancelled",
          message: "Must not overwrite acceptance",
        }),
      ).toBeNull();
    });

    test("rejects a live mail dispatch with no sender", async () => {
      // A mail dispatch that can still be re-dispatched must carry a
      // hub-verified sender. The typed enqueue() API cannot express a null
      // mail sender, so this writes the row directly to exercise the DB
      // constraint that backstops the signal path and any direct writer.
      const error = await h.db
        .insert(workflowRunDispatch)
        .values({
          id: "dispatch-no-sender",
          anchorRunId: ANCHOR_RUN_ID,
          messageId: "message-no-sender",
          deliveryKey: "message-no-sender",
          kind: "mail",
          rawMessage: new Uint8Array([1]),
          stepGrants: [],
        })
        .then(
          () => null,
          (e: unknown) => e,
        );
      expect(error).not.toBeNull();
      // drizzle wraps the driver error as "Failed query: ..."; the violated
      // constraint name is carried on the cause, not the wrapper message.
      const cause = error instanceof Error ? error.cause : error;
      const causeMessage =
        cause instanceof Error ? cause.message : String(cause);
      expect(causeMessage).toContain("workflow_run_dispatch_mail_sender_check");
    });

    test("exempts a terminal mail dispatch with no sender", async () => {
      // A settled or failed mail dispatch never reconstructs a frame, so it
      // needs no sender. This is what lets the migration fail in-flight
      // legacy mail rows in place rather than delete them.
      await h.db.insert(workflowRunDispatch).values({
        id: "dispatch-terminal-no-sender",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "message-terminal-no-sender",
        deliveryKey: "message-terminal-no-sender",
        kind: "mail",
        status: "failed",
        rawMessage: new Uint8Array([1]),
        stepGrants: [],
      });
      const store = createWorkflowRunDispatchStore(h.db);
      const row = await store.findById("dispatch-terminal-no-sender");
      expect(row?.status).toBe("failed");
      expect(row?.senderAddress).toBeNull();
    });
  },
);
