// Shared logic for re-resolving a running instance's inference sources from
// the catalog and pushing the update to its sidecar.
//
// Used after a credential secret rotation (a model provider's credential
// changes the resolved source's secret) and after a catalog edit in a tenant
// or its subtree. An inference source references its credential by id only, so
// the rotated secret rides a `credentials.update` (the unified credential-
// material cell), pushed before the `sources.update` that references it.

import { eq, and, inArray, isNull, isNotNull } from "drizzle-orm";
import { getLogger } from "@intx/log";
import { workflowRun } from "@intx/db/schema";
import { resolveInstanceModelSources, getDescendantTenants } from "@intx/db";
import type { DB } from "@intx/db";
import type { CredentialCipher } from "@intx/types";
import type { CredentialDelivery } from "@intx/types/sidecar";

import type { SidecarRouter } from "./ws/sidecar-handler";

const log = getLogger(["hub", "credentials"]);

/**
 * Re-resolve a single running instance's inference sources from the catalog
 * (the definition's model requirements plus the invoker preferences persisted
 * on the instance) and push the ordered list to its sidecar. The head of the
 * catalog-priority-ordered list is the active default; the tail is the
 * failover chain.
 *
 * No-op when the instance resolves to no launchable source — the resolver's
 * own logger is the signal for why.
 */
export async function pushInstanceSourceUpdate(
  db: DB["db"],
  sidecarRouter: Pick<
    SidecarRouter,
    "sendSourcesUpdate" | "sendCredentialsUpdate"
  >,
  instance: {
    address: string;
    definitionId: string;
    tenantId: string;
    modelPreferences: unknown;
  },
  credentialCipher: CredentialCipher,
): Promise<void> {
  const resolution = await resolveInstanceModelSources(
    db,
    instance.tenantId,
    instance,
    credentialCipher,
  );
  if (!resolution.ok) return;
  const [head] = resolution.sources;
  if (head === undefined) return;
  // Push the credential material before the source list. A source references
  // its credential by id, so the cell must hold the (possibly rotated) secret
  // before the source list that points at it lands. Inference sources carry no
  // binding descriptor. A failure here propagates and aborts the source push --
  // never a stale secret paired with a fresh source list.
  if (resolution.materials.length > 0) {
    const delivery: CredentialDelivery = {
      bindings: [],
      materials: resolution.materials,
    };
    await sidecarRouter.sendCredentialsUpdate(instance.address, delivery);
  }
  await sidecarRouter.sendSourcesUpdate(
    instance.address,
    resolution.sources,
    head.id,
  );
}

/**
 * Re-resolve every running instance in the given tenants against the catalog
 * and push the updates to sidecars. The running instances are the folded runs
 * a launch produces: born running, with a routing address and no deployment.
 * Deployment-anchor runs (which own a deployment id and a workflow-derived
 * address) and address-less child runs route via the deployment, not this
 * per-instance push, so they are excluded. Each instance re-resolves from its
 * own tenant's context (its ancestor chain), so the rotated/edited upstream
 * entry flows through. Errors are logged per-instance but do not propagate.
 */
async function pushSourceUpdatesToTenants(
  db: DB["db"],
  sidecarRouter: SidecarRouter,
  tenantIds: string[],
  credentialCipher: CredentialCipher,
): Promise<void> {
  if (tenantIds.length === 0) return;

  // Callers fire this without awaiting, so it must never reject: a failure to
  // enumerate or push is logged and dropped, not propagated as an unhandled
  // rejection. The push is best effort — the next mutation or a sidecar
  // reconnect re-resolves sources.
  try {
    // The instance-shaped runs: running, addressable, and not anchored on a
    // deployment. `anchorRunId IS NULL` excludes the deployment-anchor runs
    // (which set it to their own id), so no deployment-anchor run address can
    // reach the address-targeted push below. Mirrors the /me/workflows/runs and
    // tenant run-list predicate.
    const instances = await db.query.workflowRun.findMany({
      where: and(
        inArray(workflowRun.tenantId, tenantIds),
        eq(workflowRun.status, "running"),
        isNull(workflowRun.anchorRunId),
        isNotNull(workflowRun.address),
      ),
    });

    if (instances.length === 0) return;

    const results = await Promise.allSettled(
      instances.map(async (instance) => {
        // The isNotNull(address) filter guarantees a value; a null here is a
        // broken invariant. The callback is async, so this throw becomes a
        // rejected promise captured per-instance by allSettled and logged
        // below -- one bad row is surfaced, not fatal to the whole batch.
        if (instance.address === null) {
          throw new Error(
            `running run ${instance.id} matched the non-null-address filter but has a null address`,
          );
        }
        return pushInstanceSourceUpdate(
          db,
          sidecarRouter,
          {
            address: instance.address,
            definitionId: instance.definitionId,
            tenantId: instance.tenantId,
            modelPreferences: instance.modelPreferences,
          },
          credentialCipher,
        );
      }),
    );

    for (const result of results) {
      if (result.status === "rejected") {
        log.warn`Failed to push source update: ${String(result.reason)}`;
      }
    }
  } catch (err: unknown) {
    log.warn`Failed to push source updates: ${String(err)}`;
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
  credentialCipher: CredentialCipher,
): Promise<void> {
  await pushSourceUpdatesToTenants(
    db,
    sidecarRouter,
    [tenantId],
    credentialCipher,
  );
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
  credentialCipher: CredentialCipher,
): Promise<void> {
  let tenants: string[];
  try {
    tenants = await getDescendantTenants(db, tenantId);
  } catch (err: unknown) {
    log.warn`Failed to enumerate descendants for source push: ${String(err)}`;
    return;
  }
  await pushSourceUpdatesToTenants(
    db,
    sidecarRouter,
    tenants,
    credentialCipher,
  );
}
