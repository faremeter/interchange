// Shared logic for pushing inference-source updates to sidecars.
//
// Used by the credentials PATCH route to broadcast updates to every running
// instance in the tenant after a credential secret is rotated.

import { eq, and } from "drizzle-orm";
import { getLogger } from "@intx/log";
import { agentInstance } from "@intx/db/schema";
import { resolveInstanceSources } from "@intx/db";
import type { DB } from "@intx/db";

import type { SidecarRouter } from "./ws/sidecar-handler";

const log = getLogger(["hub", "credentials"]);

/**
 * After a credential secret is rotated, find all running instances in the
 * tenant that may use credentials from the affected provider, re-resolve
 * their full sources array, and push updates to sidecars.
 *
 * Errors are logged per-instance but do not propagate.
 *
 * **Silent no-op when sources is empty.** `resolveInstanceSources`
 * returns `[]` when an instance's agent has malformed
 * `credentialRequirements` or `modelConfig`. This function skips those
 * instances without emitting a log line of its own — the resolver's
 * `db.credentials` logger is the only signal. When a credential rotation
 * fails to reach an agent operators expect it to reach, grep the
 * `db.credentials` logger for `Invalid modelConfig` or
 * `Invalid credential requirements` warnings keyed on the agent id.
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
    instances.map(async (instance) => {
      const sources = await resolveInstanceSources(db, tenantId, instance);
      if (sources.length === 0) return;
      const [first] = sources;
      if (first === undefined) return;
      await sidecarRouter.sendSourcesUpdate(
        instance.address,
        sources,
        first.id,
      );
    }),
  );

  for (const result of results) {
    if (result.status === "rejected") {
      log.warn`Failed to push source update: ${String(result.reason)}`;
    }
  }
}
