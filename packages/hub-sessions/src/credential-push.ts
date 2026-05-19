// Shared logic for pushing provider credentials to sidecars.
//
// Used by the credentials PATCH route to broadcast updates to every running
// instance in the tenant after a credential secret is rotated.

import { eq, and } from "drizzle-orm";
import { getLogger } from "@intx/log";
import { agentInstance } from "@intx/db/schema";
import { resolveInstanceProviders } from "@intx/db";
import type { DB } from "@intx/db";

import type { SidecarRouter } from "./ws/sidecar-handler";

const log = getLogger(["hub", "credentials"]);

/**
 * After a credential secret is rotated, find all running instances in the
 * tenant that may use credentials from the affected provider, re-resolve
 * their full providers array, and push updates to sidecars.
 *
 * Errors are logged per-instance but do not propagate.
 */
export async function pushProviderUpdates(
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
    instances.map(async (instance) => {
      const providers = await resolveInstanceProviders(db, tenantId, instance);
      if (providers.length === 0) return;
      await sidecarRouter.sendProvidersUpdate(instance.address, providers);
    }),
  );

  for (const result of results) {
    if (result.status === "rejected") {
      log.warn`Failed to push provider update: ${String(result.reason)}`;
    }
  }
}
