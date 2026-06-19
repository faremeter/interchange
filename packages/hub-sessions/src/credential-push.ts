// Shared logic for re-resolving a running instance's inference sources from
// the catalog and pushing the update to its sidecar.
//
// Used after a credential secret rotation (a model provider's credential
// changes the resolved source's apiKey) and on sidecar reconnect.

import { eq, and } from "drizzle-orm";
import { getLogger } from "@intx/log";
import { agentInstance } from "@intx/db/schema";
import { resolveInstanceModelSources } from "@intx/db";
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
  tenantId: string,
  instance: { address: string; agentId: string; modelPreferences: unknown },
): Promise<void> {
  const resolution = await resolveInstanceModelSources(db, tenantId, instance);
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
 * After a credential secret is rotated, re-resolve every running instance in
 * the tenant against the catalog and push the updates to sidecars. A rotated
 * secret flows through because resolution dereferences the provider's
 * credential reference to the current secret.
 *
 * Errors are logged per-instance but do not propagate.
 */
export async function pushSourceUpdates(
  db: DB["db"],
  sidecarRouter: SidecarRouter,
  tenantId: string,
): Promise<void> {
  const instances = await db.query.agentInstance.findMany({
    where: and(
      eq(agentInstance.tenantId, tenantId),
      eq(agentInstance.status, "running"),
    ),
  });

  if (instances.length === 0) return;

  const results = await Promise.allSettled(
    instances.map((instance) =>
      pushInstanceSourceUpdate(db, sidecarRouter, tenantId, instance),
    ),
  );

  for (const result of results) {
    if (result.status === "rejected") {
      log.warn`Failed to push source update: ${String(result.reason)}`;
    }
  }
}
