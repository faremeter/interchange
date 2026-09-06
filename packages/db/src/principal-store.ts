import type { DB, DBExecutor } from "./client";
import { principal } from "./schema/principals";
import { parsePrincipalRow } from "./parse-row";
import type { PrincipalKeyStore } from "./principal-key-store";

type DBHandle = DB["db"];

type PrincipalInsert = typeof principal.$inferInsert;
type ParsedPrincipal = ReturnType<typeof parsePrincipalRow>;

/**
 * Store for the `principal` table -- the single owner of principal creation.
 * Every principal insert in the system routes through this one factory, and
 * every principal it creates is minted an active signing key in the same
 * transaction, so a principal never exists without a key.
 *
 * Each method accepts an optional transaction handle so the principal row and
 * its key join the transaction that co-writes the principal's roles and grants.
 * When no handle is passed the store opens its own transaction, because the
 * principal insert and the key mint are two writes that must commit together.
 */
export function createPrincipalStore(
  db: DBHandle,
  principalKeyStore: PrincipalKeyStore,
) {
  return {
    /**
     * Insert a new principal and mint its signing key, failing loudly on a
     * natural-key conflict. Callers that have already established the principal
     * is new (a fresh tenant owner, an invite past its existence pre-check) use
     * this so a concurrent duplicate surfaces as a unique violation rather than
     * a silent success.
     */
    async create(
      row: PrincipalInsert,
      tx?: DBExecutor,
    ): Promise<ParsedPrincipal> {
      const run = async (executor: DBExecutor): Promise<ParsedPrincipal> => {
        const [inserted] = await executor
          .insert(principal)
          .values(row)
          .returning();
        if (inserted === undefined) {
          throw new Error(
            `principalStore.create: insert returned no row for ${row.id}`,
          );
        }
        await principalKeyStore.generate(inserted.id, executor);
        return parsePrincipalRow(inserted);
      };
      return tx === undefined ? db.transaction(run) : run(tx);
    },

    /**
     * Idempotent variant of `create` for the run path, where concurrent first
     * deliveries race to reserve the same principal. On a win it inserts the
     * principal, mints its key, and returns the parsed row; on a loss it returns
     * `null` WITHOUT minting -- the winning transaction already minted the one
     * active key -- so the caller can fall back to the winner's state.
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
      const run = async (
        executor: DBExecutor,
      ): Promise<ParsedPrincipal | null> => {
        const [inserted] = await executor
          .insert(principal)
          .values(row)
          .onConflictDoNothing({
            target: [principal.tenantId, principal.kind, principal.refId],
          })
          .returning();
        if (inserted === undefined) return null;
        await principalKeyStore.generate(inserted.id, executor);
        return parsePrincipalRow(inserted);
      };
      return tx === undefined ? db.transaction(run) : run(tx);
    },
  };
}

export type PrincipalStore = ReturnType<typeof createPrincipalStore>;
