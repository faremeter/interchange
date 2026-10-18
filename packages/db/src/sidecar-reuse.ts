import { and, eq, inArray, isNull, ne, or, type SQL } from "drizzle-orm";

import type { SidecarCapacity } from "@intx/types/sidecar";

import type { DBExecutor } from "./client";
import { sidecar, sidecarAllocation } from "./schema";

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
  readonly provisionerApiVersion: 2;
  readonly provisionerBindingFingerprint: string;
};

/** A sidecar's durable confirmation that this incarnation cannot resume. */
export type RetainedSidecarIncarnation = {
  readonly allocationId: string;
  readonly generation: number;
};

export type SidecarInventory = {
  readonly capacity: SidecarCapacity;
  readonly retainedIncarnations: readonly RetainedSidecarIncarnation[];
};

export type SidecarAllocationStoreOptions = {
  /** Missing or undefined inventory blocks placement. */
  readonly getSidecarInventory?: (
    sidecarId: string,
  ) => SidecarInventory | undefined;
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
 * The sidecar must have been minted for the same provisioner binding, so a
 * provisioner cannot claim another backend's sidecar, and for an ensure that
 * carried the same adapter manifest: a sidecar reads its manifest only at
 * start, so work never joins one that loaded another manifest. The sidecar
 * may host nothing but must not be retired. Locks the sidecar row.
 */
export async function assertSidecarReusable(
  tx: DBExecutor,
  args: {
    readonly sidecarId: string;
    readonly binding: SidecarProvisionerBinding;
    readonly adapterManifestDigest: string;
  },
): Promise<void> {
  const [row] = await tx
    .select({
      provisionerId: sidecar.provisionerId,
      provisionerApiVersion: sidecar.provisionerApiVersion,
      provisionerBindingFingerprint: sidecar.provisionerBindingFingerprint,
      adapterManifestDigest: sidecar.adapterManifestDigest,
      retiredAt: sidecar.retiredAt,
    })
    .from(sidecar)
    .where(eq(sidecar.id, args.sidecarId))
    .for("update");
  if (row === undefined) {
    throw new SidecarReuseRejectedError(args.sidecarId, "it does not exist");
  }
  if (row.retiredAt !== null) {
    throw new SidecarReuseRejectedError(
      args.sidecarId,
      "it was retired: no sidecar runs with this identity",
    );
  }
  if (
    row.provisionerId !== args.binding.provisionerId ||
    row.provisionerApiVersion !== args.binding.provisionerApiVersion ||
    row.provisionerBindingFingerprint !==
      args.binding.provisionerBindingFingerprint
  ) {
    throw new SidecarReuseRejectedError(
      args.sidecarId,
      `it was not started for provisioner ${args.binding.provisionerId} with the same binding`,
    );
  }
  if (row.adapterManifestDigest !== args.adapterManifestDigest) {
    throw new SidecarReuseRejectedError(
      args.sidecarId,
      "it booted with another adapter manifest",
    );
  }
}

/**
 * Marks `sidecarId` as gone: reuse stops at once, its token once neither
 * current work nor cleanup is left. Call it before the write that ends the
 * caller's retries.
 */
export async function retireSidecar(
  tx: DBExecutor,
  args: { readonly sidecarId: string; readonly now: Date | SQL },
): Promise<void> {
  await tx
    .update(sidecar)
    .set({ retiredAt: args.now, updatedAt: args.now })
    .where(and(eq(sidecar.id, args.sidecarId), isNull(sidecar.retiredAt)));
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
  const inventory = args.getSidecarInventory?.(args.sidecarId);
  if (inventory === undefined)
    throw new SidecarInventoryUnavailableError(args.sidecarId);
  const active = reservations.filter(
    (reservation) =>
      !inventory.retainedIncarnations.some(
        (incarnation) =>
          incarnation.allocationId === reservation.id &&
          incarnation.generation === reservation.ensureAcceptedGeneration,
      ),
  ).length;
  if (active >= inventory.capacity.maxActiveDeployments) {
    throw new SidecarReuseRejectedError(
      args.sidecarId,
      `it already hosts ${String(active)} deployments occupying all active slots`,
    );
  }
}
