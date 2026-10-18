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

/** What the Hub saw of a reported sidecar's work when it retired it. */
export type ProvisionerSidecarRetirement = {
  /** Allocated work on the sidecar still connected, which the report contradicts. */
  readonly connectedAllocationIds: readonly string[];
};

/** Whether this Hub holds a live connection for the allocation's generation. */
export type AllocationConnectionCheck = (target: {
  readonly allocationId: string;
  readonly generation: number;
}) => boolean;

/**
 * Retires a sidecar its provisioner reports stopped. Returns null, retiring
 * nothing, when no sidecar with that id was minted for `binding`. With
 * `endReconnectWindow`, it also records that the sidecar's work gets no
 * reconnect window: its allocated work without a live connection reaches its
 * connection deadline now, and work that disconnects later gets none either.
 */
export async function retireProvisionerSidecar(
  tx: DBExecutor,
  args: {
    readonly sidecarId: string;
    readonly binding: SidecarProvisionerBinding;
    readonly endReconnectWindow: boolean;
    readonly isConnected: AllocationConnectionCheck;
    readonly now: Date | SQL;
  },
): Promise<ProvisionerSidecarRetirement | null> {
  const [owned] = await tx
    .select({ id: sidecar.id })
    .from(sidecar)
    .where(
      and(
        eq(sidecar.id, args.sidecarId),
        eq(sidecar.provisionerId, args.binding.provisionerId),
        eq(sidecar.provisionerApiVersion, args.binding.provisionerApiVersion),
        eq(
          sidecar.provisionerBindingFingerprint,
          args.binding.provisionerBindingFingerprint,
        ),
      ),
    )
    .for("update");
  if (owned === undefined) return null;
  await retireSidecar(tx, { sidecarId: args.sidecarId, now: args.now });
  const allocated = await tx
    .select({
      allocationId: sidecarAllocation.id,
      generation: sidecarAllocation.generation,
    })
    .from(sidecarAllocation)
    .where(
      and(
        eq(sidecarAllocation.sidecarId, args.sidecarId),
        eq(sidecarAllocation.status, "allocated"),
        eq(
          sidecarAllocation.ensureAcceptedGeneration,
          sidecarAllocation.generation,
        ),
      ),
    )
    .for("update");
  // The deadline cannot tell: work still initializing keeps one while
  // connected, and a drop the Hub has not recorded yet has none.
  const connectedAllocationIds: string[] = [];
  const disconnectedAllocationIds: string[] = [];
  for (const target of allocated)
    (args.isConnected(target)
      ? connectedAllocationIds
      : disconnectedAllocationIds
    ).push(target.allocationId);
  if (args.endReconnectWindow) {
    await tx
      .update(sidecar)
      .set({ reconnectWindowEndedAt: args.now, updatedAt: args.now })
      .where(
        and(
          eq(sidecar.id, args.sidecarId),
          isNull(sidecar.reconnectWindowEndedAt),
        ),
      );
    // Dropping the lease stops a reconciler still waiting on the old
    // deadline, like a lost connection does.
    if (disconnectedAllocationIds.length > 0)
      await tx
        .update(sidecarAllocation)
        .set({
          connectDeadline: args.now,
          nextAttemptAt: args.now,
          reconciliationLeaseId: null,
          reconciliationLeaseExpiresAt: null,
          updatedAt: args.now,
        })
        .where(inArray(sidecarAllocation.id, disconnectedAllocationIds));
  }
  return { connectedAllocationIds };
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
