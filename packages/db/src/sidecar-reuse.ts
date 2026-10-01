import { and, eq, inArray, ne } from "drizzle-orm";

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

export type SidecarProvisionerBinding = {
  readonly provisionerId: string;
  readonly provisionerApiVersion: 1;
  readonly provisionerBindingFingerprint: string;
};

/**
 * Asserts that a provisioner may place a probe or allocation on `sidecarId`.
 * The sidecar must still host other work of the same provisioner binding, so
 * a provisioner cannot claim another backend's sidecar. Work that is being
 * replaced or released counts: the provisioner keeps its hold on the sidecar
 * until `destroy` returns, so from its side the sidecar is still in use.
 * Whether sharing suits the work is the provisioner's decision.
 * Locks the sidecar row so concurrent placements on it serialize.
 */
export async function assertSidecarReusable(
  tx: DBExecutor,
  args: {
    readonly sidecarId: string;
    readonly binding: SidecarProvisionerBinding;
    readonly placing: { readonly allocationId: string };
  },
): Promise<void> {
  const [row] = await tx
    .select({ id: sidecar.id })
    .from(sidecar)
    .where(eq(sidecar.id, args.sidecarId))
    .for("update");
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
