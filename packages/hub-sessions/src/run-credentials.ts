import { eq } from "drizzle-orm";

import type { DB, RunTokenStore } from "@intx/db";
import { workflowRun } from "@intx/db/schema";
import { deriveRunPrincipalId } from "@intx/hub-common";
import {
  HUB_CREDENTIAL_HANDLE,
  hubCredentialId,
  type CredentialDelivery,
} from "@intx/types/credential-delivery";

/**
 * Load the hub credentials of a deployment's run: one origin-pinned `http`
 * credential for each consumer that holds a run token, bound to the reserved
 * hub handle. Null when the run has no tokens -- it declares no hub access,
 * its principal does not exist yet, or teardown deleted them.
 */
export type RunCredentialsLoader = (
  runId: string,
) => Promise<CredentialDelivery | null>;

/**
 * Add a run's hub credentials, when it has any, to a delivery. A hub id
 * (`hub:<consumer>`) never names a catalog credential, so appending them is a
 * merge.
 */
export function withRunCredentials(
  delivery: CredentialDelivery,
  hub: CredentialDelivery | null,
): CredentialDelivery {
  if (hub === null) return delivery;
  return {
    bindings: [...delivery.bindings, ...hub.bindings],
    materials: [...delivery.materials, ...hub.materials],
  };
}

export type CreateRunCredentialsLoaderDeps = {
  db: DB["db"];
  runTokenStore: RunTokenStore;
  /** The origin a sidecar reaches the hub REST API at. */
  hubApiOrigin: string;
};

export function createRunCredentialsLoader({
  db,
  runTokenStore,
  hubApiOrigin,
}: CreateRunCredentialsLoaderDeps): RunCredentialsLoader {
  return async (runId) => {
    const run = await db.query.workflowRun.findFirst({
      where: eq(workflowRun.id, runId),
      columns: { tenantId: true },
    });
    if (run === undefined) return null;
    // The same derivation that names the principal the tokens were minted for.
    const principalId = await deriveRunPrincipalId(run.tenantId, runId);
    const tokens = await runTokenStore.loadDelivery(principalId);
    if (tokens.length === 0) return null;
    return {
      bindings: tokens.map((token) => ({
        handle: HUB_CREDENTIAL_HANDLE,
        credentialId: hubCredentialId(token.consumer),
        consumer: token.consumer,
      })),
      materials: tokens.map((token) => ({
        credentialId: hubCredentialId(token.consumer),
        providerKey: "http",
        origin: hubApiOrigin,
        secret: token.token,
      })),
    };
  };
}
