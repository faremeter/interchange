import { getLogger } from "@intx/log";

import type { SidecarRouter } from "./ws/sidecar-handler";

const log = getLogger(["hub", "run-grants"]);

/**
 * Send each deployment address its run's committed grants now, after a
 * transaction that changed those rows has committed.
 *
 * Callers fire this without awaiting, so it never rejects: a failed send is
 * logged and dropped. An address that is not routed is normal (its sidecar
 * is offline), and the reconnect resync sends that deployment its grants.
 */
export async function pushRunGrants(
  sidecarRouter: Pick<SidecarRouter, "sendRunGrants">,
  addresses: readonly string[],
): Promise<void> {
  await Promise.all(
    addresses.map(async (address) => {
      try {
        if (!(await sidecarRouter.sendRunGrants(address))) {
          log.info`Run grants for ${address} not sent now: not routed, no run principal, a failed load, or a route change`;
        }
      } catch (err: unknown) {
        log.warn`Failed to push run grants to ${address}: ${String(err)}`;
      }
    }),
  );
}
