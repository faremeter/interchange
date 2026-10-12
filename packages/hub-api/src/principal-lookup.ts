import { and, eq } from "drizzle-orm";

import { principal } from "@intx/db/schema";
import type { DB } from "@intx/db";

import type { PrincipalRow } from "./context";

export type FindUserPrincipalArgs = {
  tenantId: string;
  userId: string;
};

// Returns the row whatever its status; each caller owns its decision on a
// principal that is not active.
export async function findUserPrincipal(
  db: DB["db"],
  { tenantId, userId }: FindUserPrincipalArgs,
): Promise<PrincipalRow | undefined> {
  return await db.query.principal.findFirst({
    where: and(
      eq(principal.tenantId, tenantId),
      eq(principal.kind, "user"),
      eq(principal.refId, userId),
    ),
  });
}
