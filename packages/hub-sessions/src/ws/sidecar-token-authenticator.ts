import { eq } from "drizzle-orm";
import { sha256 } from "@intx/crypto";
import type { DB } from "@intx/db";
import { sidecar } from "@intx/db/schema";

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
export function createSidecarTokenAuthenticator({
  db,
}: CreateSidecarTokenAuthenticatorDeps): SidecarAuthenticator {
  return async ({ token }) => {
    const tokenHash = await sha256(token);
    const row = await db.query.sidecar.findFirst({
      where: eq(sidecar.tokenHashSha256, tokenHash),
    });
    if (row === undefined) {
      return null;
    }
    return { kind: "sidecar", sidecarId: row.id };
  };
}
