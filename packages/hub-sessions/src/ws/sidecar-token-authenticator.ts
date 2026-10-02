import { and, eq, inArray } from "drizzle-orm";
import { sha256 } from "@intx/crypto";
import { workflowRunExecutability, type DB } from "@intx/db";
import {
  sidecar,
  sidecarAllocation,
  workflowProbe,
  workflowRun,
} from "@intx/db/schema";

import type {
  SidecarCredentialIdentity,
  SidecarCredentialResolver,
  SidecarIdentityUse,
} from "../sidecar-allocation/contracts";
import type { SidecarAuthenticator } from "./sidecar-handler";

export type CreateSidecarTokenAuthenticatorDeps = {
  db: DB["db"];
};

/**
 * Builds a resolver that verifies a sidecar's presented token against the
 * per-sidecar hash stored on the `sidecar` table. The token is hashed with
 * SHA-256 and looked up by its digest; a matching row yields that row's id as
 * the verified identity, and `resolveBindings` reads what that sidecar
 * currently hosts. An unknown token resolves to `null` so the handshake is
 * rejected. A sidecar that hosts nothing current still resolves: the handshake
 * turns it away too, but first undeploys what it reports, since nothing else
 * would. The claimed `sidecarId` on the frame is ignored: identity is derived
 * from the token alone.
 */
export function createSidecarCredentialResolver({
  db,
}: CreateSidecarTokenAuthenticatorDeps): SidecarCredentialResolver {
  async function resolveBindings(
    sidecarId: string,
  ): Promise<SidecarCredentialIdentity[]> {
    const [allocations, probes] = await Promise.all([
      db.query.sidecarAllocation.findMany({
        columns: {
          id: true,
          tenantId: true,
          anchorRunId: true,
          generation: true,
        },
        where: and(
          eq(sidecarAllocation.sidecarId, sidecarId),
          inArray(sidecarAllocation.status, ["provisioning", "allocated"]),
        ),
      }),
      db.query.workflowProbe.findMany({
        columns: { id: true, tenantId: true, generation: true },
        where: and(
          eq(workflowProbe.sidecarId, sidecarId),
          inArray(workflowProbe.status, ["provisioning", "probing"]),
        ),
      }),
    ]);
    const anchors =
      allocations.length === 0
        ? []
        : await db.query.workflowRun.findMany({
            columns: { id: true, address: true },
            where: inArray(
              workflowRun.id,
              allocations.map((allocation) => allocation.anchorRunId),
            ),
          });
    const addressByAnchor = new Map(
      anchors.map((anchor) => [anchor.id, anchor.address]),
    );
    return [
      ...allocations.flatMap((allocation): SidecarCredentialIdentity[] => {
        const address = addressByAnchor.get(allocation.anchorRunId);
        return address === null || address === undefined
          ? []
          : [
              {
                kind: "allocated",
                sidecarId,
                allocationId: allocation.id,
                tenantId: allocation.tenantId,
                anchorRunId: allocation.anchorRunId,
                workflowRunAddress: address,
                generation: allocation.generation,
              },
            ];
      }),
      ...probes.map(
        (probe): SidecarCredentialIdentity => ({
          kind: "probe",
          sidecarId,
          allocationId: probe.id,
          tenantId: probe.tenantId,
          generation: probe.generation,
        }),
      ),
    ];
  }

  async function resolve(token: string) {
    const tokenHash = await sha256(token);
    const row = await db.query.sidecar.findFirst({
      columns: { id: true },
      where: eq(sidecar.tokenHashSha256, tokenHash),
    });
    return row === undefined ? null : { sidecarId: row.id };
  }

  async function isCurrent(
    identity: SidecarCredentialIdentity,
    use: SidecarIdentityUse,
  ): Promise<boolean> {
    if (identity.kind === "probe") {
      if (use === "reclaim") return false;
      const statuses =
        use === "registration"
          ? (["provisioning", "probing"] as const)
          : (["probing"] as const);
      const probe = await db.query.workflowProbe.findFirst({
        columns: { id: true },
        where: and(
          eq(workflowProbe.id, identity.allocationId),
          eq(workflowProbe.sidecarId, identity.sidecarId),
          eq(workflowProbe.tenantId, identity.tenantId),
          eq(workflowProbe.generation, identity.generation),
          inArray(workflowProbe.status, statuses),
        ),
      });
      return probe !== undefined;
    }

    const statuses =
      use === "registration"
        ? (["provisioning", "allocated"] as const)
        : (["allocated"] as const);
    const allocation = await db.query.sidecarAllocation.findFirst({
      where: and(
        eq(sidecarAllocation.id, identity.allocationId),
        eq(sidecarAllocation.sidecarId, identity.sidecarId),
        eq(sidecarAllocation.tenantId, identity.tenantId),
        eq(sidecarAllocation.anchorRunId, identity.anchorRunId),
        eq(sidecarAllocation.generation, identity.generation),
        inArray(sidecarAllocation.status, statuses),
      ),
    });
    if (allocation === undefined) return false;
    if (use === "registration") return true;
    if (allocation.ensureAcceptedGeneration !== identity.generation) {
      return false;
    }
    if (use === "reclaim" && allocation.initializationLeaseId !== null) {
      return false;
    }
    const anchor = await db.query.workflowRun.findFirst({
      columns: {
        address: true,
        publicKey: true,
        status: true,
        expiresAt: true,
        cancellationRequestedAt: true,
      },
      where: eq(workflowRun.id, identity.anchorRunId),
    });
    if (anchor?.address !== identity.workflowRunAddress) return false;
    if (use !== "reclaim") return true;
    // The Hub may have ended the run without the worker recording it, so the
    // run row, not the copy that reconnects, decides whether it is over.
    return (
      anchor.publicKey !== null &&
      workflowRunExecutability(anchor) !== "terminal"
    );
  }

  return { resolve, resolveBindings, isCurrent };
}

export function createSidecarTokenAuthenticator(
  deps: CreateSidecarTokenAuthenticatorDeps,
): SidecarAuthenticator {
  const resolver = createSidecarCredentialResolver(deps);
  return async ({ token }) => resolver.resolve(token);
}
