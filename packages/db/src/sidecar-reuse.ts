import { and, eq, inArray, ne, or } from "drizzle-orm";

import { MAX_SIDECAR_ACTIVE_DEPLOYMENTS } from "@intx/types/sidecar";

import type { DBExecutor } from "./client";
import { sidecar, sidecarAllocation, workflowProbe } from "./schema";

/** A provisioner named a sidecar the Hub cannot place its work on. */
export class SidecarReuseRejectedError extends Error {
  readonly sidecarId: string;

  constructor(sidecarId: string, reason: string) {
    super(`Cannot place work on sidecar ${sidecarId}: ${reason}`);
    this.name = "SidecarReuseRejectedError";
    this.sidecarId = sidecarId;
  }
}

/** Placement must wait for a complete inventory from the current connection. */
export class SidecarInventoryUnavailableError extends Error {
  constructor(readonly sidecarId: string) {
    super(`Sidecar ${sidecarId} has no current inventory for placement`);
    this.name = "SidecarInventoryUnavailableError";
  }
}

export type SidecarProvisionerBinding = {
  readonly provisionerId: string;
  readonly provisionerApiVersion: 1;
  readonly provisionerBindingFingerprint: string;
};

/** A sidecar's durable confirmation that this incarnation cannot resume. */
export type RetainedSidecarIncarnation = {
  readonly allocationId: string;
  readonly generation: number;
};

export type SidecarAllocationStoreOptions = {
  /** Missing or undefined inventory blocks placement; an empty array is known. */
  readonly getRetainedIncarnations?: (
    sidecarId: string,
  ) => readonly RetainedSidecarIncarnation[] | undefined;
};

/** Lock sidecars in ID order before locking the probe or allocation that moves. */
export async function lockSidecars(
  tx: DBExecutor,
  sidecarIds: readonly string[],
) {
  if (sidecarIds.length === 0) return [];
  return tx
    .select({ id: sidecar.id })
    .from(sidecar)
    .where(inArray(sidecar.id, [...sidecarIds]))
    .orderBy(sidecar.id)
    .for("update");
}

/**
 * Asserts that a provisioner may place a probe or allocation on `sidecarId`.
 * The sidecar must still host other work of the same provisioner binding, so
 * a provisioner cannot claim another backend's sidecar. Work that is being
 * replaced or released counts: the provisioner keeps its hold on the sidecar
 * until `destroy` returns, so from its side the sidecar is still in use.
 * Whether sharing suits the work is the provisioner's decision.
 * Locks the sidecar row so placements and removal of hosting bindings serialize.
 */
export async function assertSidecarReusable(
  tx: DBExecutor,
  args: {
    readonly sidecarId: string;
    readonly binding: SidecarProvisionerBinding;
    readonly placing: { readonly allocationId: string };
  },
): Promise<void> {
  const [row] = await lockSidecars(tx, [args.sidecarId]);
  if (row === undefined) {
    throw new SidecarReuseRejectedError(args.sidecarId, "it does not exist");
  }
  const [hostedAllocation] = await tx
    .select({ id: sidecarAllocation.id })
    .from(sidecarAllocation)
    .where(
      and(
        eq(sidecarAllocation.sidecarId, args.sidecarId),
        ne(sidecarAllocation.id, args.placing.allocationId),
        inArray(sidecarAllocation.status, [
          "provisioning",
          "allocated",
          "replacing",
          "releasing",
        ]),
        eq(sidecarAllocation.provisionerId, args.binding.provisionerId),
        eq(
          sidecarAllocation.provisionerApiVersion,
          args.binding.provisionerApiVersion,
        ),
        eq(
          sidecarAllocation.provisionerBindingFingerprint,
          args.binding.provisionerBindingFingerprint,
        ),
      ),
    )
    .limit(1);
  if (hostedAllocation !== undefined) return;
  const [hostedProbe] = await tx
    .select({ id: workflowProbe.id })
    .from(workflowProbe)
    .where(
      and(
        eq(workflowProbe.sidecarId, args.sidecarId),
        ne(workflowProbe.id, args.placing.allocationId),
        inArray(workflowProbe.status, ["provisioning", "probing", "releasing"]),
        eq(workflowProbe.provisionerId, args.binding.provisionerId),
        eq(
          workflowProbe.provisionerApiVersion,
          args.binding.provisionerApiVersion,
        ),
        eq(
          workflowProbe.provisionerBindingFingerprint,
          args.binding.provisionerBindingFingerprint,
        ),
      ),
    )
    .limit(1);
  if (hostedProbe !== undefined) return;
  throw new SidecarReuseRejectedError(
    args.sidecarId,
    `it hosts no current probe or allocation of provisioner ${args.binding.provisionerId}`,
  );
}

/**
 * Locks the sidecar so placements serialize, counting every reservation
 * except cleanup confirmed removed or an incarnation durably retained.
 * Unknown inventory blocks placement until the sidecar completes its hello.
 */
export async function assertSidecarHasRoom(
  tx: DBExecutor,
  args: {
    readonly sidecarId: string;
    readonly placing: { readonly allocationId: string };
  } & SidecarAllocationStoreOptions,
): Promise<void> {
  const [locked] = await lockSidecars(tx, [args.sidecarId]);
  if (locked === undefined) {
    throw new SidecarReuseRejectedError(args.sidecarId, "it does not exist");
  }
  const reservations = await tx
    .select({
      id: sidecarAllocation.id,
      ensureAcceptedGeneration: sidecarAllocation.ensureAcceptedGeneration,
    })
    .from(sidecarAllocation)
    .where(
      and(
        eq(sidecarAllocation.sidecarId, args.sidecarId),
        ne(sidecarAllocation.id, args.placing.allocationId),
        or(
          inArray(sidecarAllocation.status, ["provisioning", "allocated"]),
          and(
            inArray(sidecarAllocation.status, [
              "replacing",
              "releasing",
              "destroy_failed",
            ]),
            eq(sidecarAllocation.deploymentCleanupConfirmed, false),
          ),
        ),
      ),
    );
  // Read after the database wait: the connection may have changed meanwhile.
  const retained = args.getRetainedIncarnations?.(args.sidecarId);
  if (retained === undefined)
    throw new SidecarInventoryUnavailableError(args.sidecarId);
  const active = reservations.filter(
    (reservation) =>
      !retained.some(
        (incarnation) =>
          incarnation.allocationId === reservation.id &&
          incarnation.generation === reservation.ensureAcceptedGeneration,
      ),
  ).length;
  if (active >= MAX_SIDECAR_ACTIVE_DEPLOYMENTS) {
    throw new SidecarReuseRejectedError(
      args.sidecarId,
      `it already hosts ${String(active)} deployments occupying all active slots`,
    );
  }
}
