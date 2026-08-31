import { and, eq, inArray, sql } from "drizzle-orm";

import type { WorkflowProbeResultFrame } from "@intx/types/sidecar";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";

import type { DB, DBExecutor } from "./client";
import { sidecar, workflowProbe, type WorkflowProbeStatus } from "./schema";

type DBHandle = DB["db"];
type WorkflowProbeResult = Omit<WorkflowProbeResultFrame, "type" | "requestId">;
type WorkflowProbeRow = typeof workflowProbe.$inferSelect;

export type WorkflowProbe = WorkflowProbeRow;

export type CreateWorkflowProbeArgs = {
  readonly id: string;
  readonly tenantId: string;
  readonly definitionAssetId: string;
  readonly source: WorkflowDefinitionSource;
  readonly entry: string;
  readonly pin?: string;
  readonly provisionerId: string;
  readonly provisionerApiVersion: 1;
  readonly provisionerBindingFingerprint: string;
  readonly now?: Date;
};

export type BindWorkflowProbeSidecarArgs = {
  readonly probeId: string;
  readonly sidecarId: string;
  readonly tokenHashSha256: Uint8Array;
  readonly now?: Date;
};

const activeStatuses = [
  "pending",
  "provisioning",
  "probing",
  "releasing",
] as const satisfies readonly WorkflowProbeStatus[];

function timestamp(now?: Date) {
  return now ?? sql`now()`;
}

export function createWorkflowProbeStore(db: DBHandle) {
  return {
    async create(
      args: CreateWorkflowProbeArgs,
      tx?: DBExecutor,
    ): Promise<WorkflowProbe> {
      const createdAt = timestamp(args.now);
      const [created] = await (tx ?? db)
        .insert(workflowProbe)
        .values({
          id: args.id,
          tenantId: args.tenantId,
          definitionAssetId: args.definitionAssetId,
          source: args.source,
          entry: args.entry,
          ...(args.pin !== undefined ? { pin: args.pin } : {}),
          provisionerId: args.provisionerId,
          provisionerApiVersion: args.provisionerApiVersion,
          provisionerBindingFingerprint: args.provisionerBindingFingerprint,
          createdAt,
          updatedAt: createdAt,
        })
        .returning();
      if (created === undefined) {
        throw new Error(`Failed to create workflow probe ${args.id}`);
      }
      return created;
    },

    async bindSidecar(
      args: BindWorkflowProbeSidecarArgs,
      tx?: DBExecutor,
    ): Promise<WorkflowProbe | null> {
      const bind = async (executor: DBExecutor) => {
        const updatedAt = timestamp(args.now);
        const [probe] = await executor
          .select({ status: workflowProbe.status })
          .from(workflowProbe)
          .where(eq(workflowProbe.id, args.probeId))
          .limit(1)
          .for("update");
        if (probe?.status !== "pending") return null;
        await executor.insert(sidecar).values({
          id: args.sidecarId,
          url: null,
          tokenHashSha256: args.tokenHashSha256,
          status: "offline",
          createdAt: updatedAt,
          updatedAt,
        });
        const [updated] = await executor
          .update(workflowProbe)
          .set({
            sidecarId: args.sidecarId,
            status: "provisioning",
            updatedAt,
          })
          .where(eq(workflowProbe.id, args.probeId))
          .returning();
        return updated ?? null;
      };
      return tx === undefined ? db.transaction(bind) : bind(tx);
    },

    async markProbing(args: {
      probeId: string;
      externalRef?: string;
      now?: Date;
    }): Promise<WorkflowProbe | null> {
      const [updated] = await db
        .update(workflowProbe)
        .set({
          status: "probing",
          ...(args.externalRef !== undefined
            ? { externalRef: args.externalRef }
            : {}),
          updatedAt: timestamp(args.now),
        })
        .where(
          and(
            eq(workflowProbe.id, args.probeId),
            eq(workflowProbe.status, "provisioning"),
          ),
        )
        .returning();
      return updated ?? null;
    },

    async recordResult(
      probeId: string,
      result: WorkflowProbeResult,
      now?: Date,
    ): Promise<WorkflowProbe | null> {
      const [updated] = await db
        .update(workflowProbe)
        .set({ result, updatedAt: timestamp(now) })
        .where(
          and(
            eq(workflowProbe.id, probeId),
            eq(workflowProbe.status, "probing"),
          ),
        )
        .returning();
      return updated ?? null;
    },

    async transition(
      probeId: string,
      from: readonly WorkflowProbeStatus[],
      to: WorkflowProbeStatus,
      opts?: {
        failureCode?: string;
        failureMessage?: string;
        now?: Date;
        tx?: DBExecutor;
      },
    ): Promise<WorkflowProbe | null> {
      const [updated] = await (opts?.tx ?? db)
        .update(workflowProbe)
        .set({
          status: to,
          ...(opts?.failureCode !== undefined
            ? { failureCode: opts.failureCode }
            : {}),
          ...(opts?.failureMessage !== undefined
            ? { failureMessage: opts.failureMessage }
            : {}),
          updatedAt: timestamp(opts?.now),
        })
        .where(
          and(
            eq(workflowProbe.id, probeId),
            inArray(workflowProbe.status, [...from]),
          ),
        )
        .returning();
      return updated ?? null;
    },

    async listActive(): Promise<WorkflowProbe[]> {
      return db.query.workflowProbe.findMany({
        where: inArray(workflowProbe.status, [...activeStatuses]),
      });
    },

    async listReleasing(): Promise<WorkflowProbe[]> {
      return db.query.workflowProbe.findMany({
        where: eq(workflowProbe.status, "releasing"),
      });
    },
  };
}

export type WorkflowProbeStore = ReturnType<typeof createWorkflowProbeStore>;
