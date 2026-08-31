import { and, eq, inArray } from "drizzle-orm";
import { sha256 } from "@intx/crypto";
import type { DB } from "@intx/db";
import {
  sidecar,
  sidecarAllocation,
  workflowProbe,
  workflowRun,
} from "@intx/db/schema";

import type {
  SidecarCredentialIdentity,
  SidecarCredentialResolver,
} from "../sidecar-allocation/contracts";
import type { SidecarAuthenticator } from "./sidecar-handler";

export type CreateSidecarTokenAuthenticatorDeps = {
  db: DB["db"];
};

/**
 * Builds an authenticator that verifies a sidecar's presented token
 * against the per-sidecar hash stored on the `sidecar` table. The token
 * is hashed with SHA-256 and looked up by its digest; a matching row
 * yields that row's id as the verified identity, and an unknown token
 * resolves to `null` so the handshake is rejected. The claimed
 * `sidecarId` on the frame is ignored: identity is derived from the
 * token alone.
 */
export function createSidecarCredentialResolver({
  db,
}: CreateSidecarTokenAuthenticatorDeps): SidecarCredentialResolver {
  async function resolve(
    token: string,
  ): Promise<SidecarCredentialIdentity | null> {
    const tokenHash = await sha256(token);
    const row = await db.query.sidecar.findFirst({
      where: eq(sidecar.tokenHashSha256, tokenHash),
    });
    if (row === undefined) return null;

    const allocation = await db.query.sidecarAllocation.findFirst({
      where: eq(sidecarAllocation.sidecarId, row.id),
    });
    if (allocation !== undefined) {
      if (
        allocation.status !== "provisioning" &&
        allocation.status !== "allocated"
      ) {
        return null;
      }
      const anchor = await db.query.workflowRun.findFirst({
        columns: { address: true },
        where: eq(workflowRun.id, allocation.anchorRunId),
      });
      if (anchor?.address === null || anchor?.address === undefined)
        return null;

      return {
        kind: "allocated",
        sidecarId: row.id,
        allocationId: allocation.id,
        tenantId: allocation.tenantId,
        anchorRunId: allocation.anchorRunId,
        workflowRunAddress: anchor.address,
        generation: allocation.generation,
      };
    }

    const probe = await db.query.workflowProbe.findFirst({
      where: eq(workflowProbe.sidecarId, row.id),
    });
    if (
      probe === undefined ||
      (probe.status !== "provisioning" && probe.status !== "probing")
    ) {
      return null;
    }
    return {
      kind: "probe",
      sidecarId: row.id,
      allocationId: probe.id,
      tenantId: probe.tenantId,
      generation: probe.generation,
    };
  }

  async function isCurrent(
    identity: SidecarCredentialIdentity,
    use: "registration" | "readiness" | "routing",
  ): Promise<boolean> {
    if (identity.kind === "probe") {
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
    const anchor = await db.query.workflowRun.findFirst({
      columns: { address: true },
      where: eq(workflowRun.id, identity.anchorRunId),
    });
    return anchor?.address === identity.workflowRunAddress;
  }

  return { resolve, isCurrent };
}

export function createSidecarTokenAuthenticator(
  deps: CreateSidecarTokenAuthenticatorDeps,
): SidecarAuthenticator {
  const resolver = createSidecarCredentialResolver(deps);
  return async ({ token }) => resolver.resolve(token);
}
