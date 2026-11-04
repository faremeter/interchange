// Backfill per-principal signing keys onto the principals that predate the
// feature.
//
// Minting a key lives only in the principal-creation owner, so every principal
// CREATED after that landed already has an active key. This one-shot pass keys
// the pre-existing `user` population so it matches the invariant "every user
// principal has an active key".
//
// Scope is every `user` principal REGARDLESS of status, not just active ones. A
// status flip (e.g. an invite moving from `invited` to `active`) reuses the same
// principal row and does not re-mint, so an active-only pass would leave a
// pre-existing `invited`/`suspended` principal keyless and turn it into a
// keyless ACTIVE principal the moment it is activated.
//
// `workflow` principals are skipped. The pass counts a principal whose only key
// is retired as keyless, so on a workflow principal it would mint a
// hub-custodied key in place of the one that was retired. `agent` is a
// legacy/inert kind that is never keyed at creation either, so it is skipped
// too.
//
// Idempotent: a principal that already holds an active key is skipped, so the
// pass is safe to re-run or resume after a partial failure. Each principal is
// keyed in its own autocommit, so a partial failure leaves the earlier
// principals keyed. Safe to run against a LIVE hub: the hub only mints for
// newly-created principals (disjoint from this pass's pre-existing set), and
// the `principal_key` active-key unique index blocks a double active key.

import { and, eq, inArray } from "drizzle-orm";

import type { DB } from "./client";
import { principal } from "./schema/principals";
import { principalKey } from "./schema/principal-keys";
import type { PrincipalKeyStore } from "./principal-key-store";

const BACKFILLED_KINDS = ["user"] as const;

export type BackfillPrincipalKeysReport = {
  /** Principals that were keyless and received a fresh active key. */
  keysGenerated: number;
  /** Principals that already held an active key and were left untouched. */
  alreadyKeyed: number;
};

export async function backfillPrincipalKeys(
  db: DB["db"],
  principalKeyStore: PrincipalKeyStore,
): Promise<BackfillPrincipalKeysReport> {
  // The left join is scoped to the active key, so `activeKeyId` is null exactly
  // when the principal has no active key -- a principal holding only a retired
  // key counts as keyless and is re-keyed. The active-key unique index makes at
  // most one active row per principal, so each principal appears once.
  const rows = await db
    .select({ principalId: principal.id, activeKeyId: principalKey.id })
    .from(principal)
    .leftJoin(
      principalKey,
      and(
        eq(principalKey.principalId, principal.id),
        eq(principalKey.status, "active"),
      ),
    )
    .where(inArray(principal.kind, [...BACKFILLED_KINDS]));

  const report: BackfillPrincipalKeysReport = {
    keysGenerated: 0,
    alreadyKeyed: 0,
  };
  for (const row of rows) {
    if (row.activeKeyId !== null) {
      report.alreadyKeyed += 1;
      continue;
    }
    await principalKeyStore.generate(row.principalId);
    report.keysGenerated += 1;
  }
  return report;
}
