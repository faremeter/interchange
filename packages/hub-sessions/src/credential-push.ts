// Shared logic for re-resolving a running instance's inference sources from
// the catalog and pushing the update to its sidecar.
//
// Used after a credential secret rotation (a model provider's credential
// changes the resolved source's apiKey) and on sidecar reconnect.

import { eq, and, inArray } from "drizzle-orm";
import { getLogger } from "@intx/log";
import { agentInstance } from "@intx/db/schema";
import { resolveInstanceModelSources, getDescendantTenants } from "@intx/db";
import type { DB } from "@intx/db";

import type { SidecarRouter } from "./ws/sidecar-handler";

const log = getLogger(["hub", "credentials"]);

/**
 * Re-resolve a single running instance's inference sources from the catalog
 * (the agent's model requirements plus the invoker preferences persisted on
 * the instance) and push the ordered list to its sidecar. The head of the
 * catalog-priority-ordered list is the active default; the tail is the
 * failover chain.
 *
 * No-op when the instance resolves to no launchable source — the resolver's
 * own logger is the signal for why.
 */
export async function pushInstanceSourceUpdate(
  db: DB["db"],
  sidecarRouter: Pick<SidecarRouter, "sendSourcesUpdate">,
  instance: {
    address: string;
    agentId: string;
    tenantId: string;
    modelPreferences: unknown;
  },
): Promise<void> {
  const resolution = await resolveInstanceModelSources(
    db,
    instance.tenantId,
    instance,
  );
  if (!resolution.ok) return;
  const [head] = resolution.sources;
  if (head === undefined) return;
  await sidecarRouter.sendSourcesUpdate(
    instance.address,
    resolution.sources,
    head.id,
  );
}

/**
 * Re-resolve every running instance in the given tenants against the catalog
 * and push the updates to sidecars. Each instance re-resolves from its own
 * tenant's context (its ancestor chain), so the rotated/edited upstream entry
 * flows through. Errors are logged per-instance but do not propagate.
 */
async function pushSourceUpdatesToTenants(
  db: DB["db"],
  sidecarRouter: SidecarRouter,
  tenantIds: string[],
): Promise<void> {
  if (tenantIds.length === 0) return;

  const instances = await db.query.agentInstance.findMany({
    where: and(
      inArray(agentInstance.tenantId, tenantIds),
      eq(agentInstance.status, "running"),
    ),
  });

  if (instances.length === 0) return;

  const results = await Promise.allSettled(
    instances.map((instance) =>
      pushInstanceSourceUpdate(db, sidecarRouter, instance),
    ),
  );

  for (const result of results) {
    if (result.status === "rejected") {
      log.warn`Failed to push source update: ${String(result.reason)}`;
    }
  }
}

/**
 * After a credential secret is rotated, re-resolve every running instance in
 * the tenant against the catalog and push the updates. A rotated secret flows
 * through because resolution dereferences the provider's credential reference
 * to the current secret.
 */
export async function pushSourceUpdates(
  db: DB["db"],
  sidecarRouter: SidecarRouter,
  tenantId: string,
): Promise<void> {
  await pushSourceUpdatesToTenants(db, sidecarRouter, [tenantId]);
}

/**
 * After a catalog edit in a tenant, re-resolve and push to every running
 * instance in that tenant AND its descendants. Descendants inherit the
 * edited tenant's catalog, so a change there (a disabled provider, a new
 * offering, a price update) alters their resolved sources too.
 */
export async function pushSourceUpdatesSubtree(
  db: DB["db"],
  sidecarRouter: SidecarRouter,
  tenantId: string,
): Promise<void> {
  const tenants = await getDescendantTenants(db, tenantId);
  await pushSourceUpdatesToTenants(db, sidecarRouter, tenants);
}
