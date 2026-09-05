import type { DB, DBExecutor } from "./client";
import { principal } from "./schema/principals";
import { parsePrincipalRow } from "./parse-row";

type DBHandle = DB["db"];

type PrincipalInsert = typeof principal.$inferInsert;
type ParsedPrincipal = ReturnType<typeof parsePrincipalRow>;

/**
 * Store for the `principal` table -- the single owner of principal creation.
 * Every principal insert in the system routes through this one factory so the
 * create path lives in exactly one place. Each method accepts an optional
 * transaction handle so the principal row can be written in the same
 * transaction that co-writes its roles and grants.
 */
export function createPrincipalStore(db: DBHandle) {
  return {
    /**
     * Insert a new principal, failing loudly on a natural-key conflict. Callers
     * that have already established the principal is new (a fresh tenant owner,
     * an invite past its existence pre-check) use this so a concurrent duplicate
     * surfaces as a unique violation rather than a silent success.
     */
    async create(
      row: PrincipalInsert,
      tx?: DBExecutor,
    ): Promise<ParsedPrincipal> {
      const [inserted] = await (tx ?? db)
        .insert(principal)
        .values(row)
        .returning();
      if (inserted === undefined) {
        throw new Error(
          `principalStore.create: insert returned no row for ${row.id}`,
        );
      }
      return parsePrincipalRow(inserted);
    },

    /**
     * Idempotent variant of `create` for the run path, where concurrent first
     * deliveries race to reserve the same principal. Returns the parsed row only
     * when this call performed the insert, and `null` when a concurrent winner
     * already holds it, so the caller can fall back to the winner's state.
     *
     * The conflict target is the natural key `(tenantId, kind, refId)`, NOT the
     * surrogate `id`: user principals carry a random `generateId("principal")`,
     * so their only real conflict key is the natural triple. The run path
     * derives a deterministic id that agrees with the triple, so one target
     * unifies both callers. Do not switch this to `principal.id`.
     */
    async createIfAbsent(
      row: PrincipalInsert,
      tx?: DBExecutor,
    ): Promise<ParsedPrincipal | null> {
      const [inserted] = await (tx ?? db)
        .insert(principal)
        .values(row)
        .onConflictDoNothing({
          target: [principal.tenantId, principal.kind, principal.refId],
        })
        .returning();
      return inserted === undefined ? null : parsePrincipalRow(inserted);
    },
  };
}

export type PrincipalStore = ReturnType<typeof createPrincipalStore>;
