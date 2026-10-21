import { hasCode } from "@intx/types";

// Postgres SQLSTATE codes. https://www.postgresql.org/docs/current/errcodes-appendix.html
export const PG_UNIQUE_VIOLATION = "23505";
export const PG_FOREIGN_KEY_VIOLATION = "23503";

// Extract a Postgres SQLSTATE from an error that a driver or ORM may have
// wrapped. postgres-js sets the code on the error it throws; Drizzle
// re-wraps that as the `cause` of a DrizzleQueryError, so the code can sit
// one or more levels down the cause chain. Walk the chain (depth-bounded so
// a self-referential cause cannot loop) and return the first code found.
export function pgErrorCode(err: unknown): string | undefined {
  let cur: unknown = err;
  for (let depth = 0; cur != null && depth < 8; depth++) {
    if (hasCode(cur)) {
      return cur.code;
    }
    cur = (cur as { cause?: unknown }).cause;
  }
  return undefined;
}

// A referencing row prevents this delete: 23503 foreign_key_violation (the
// default ON DELETE NO ACTION) or 23001 restrict_violation (ON DELETE
// RESTRICT). A restrict foreign key raises 23001, not 23503, so both codes
// count.
const PG_RESTRICT_VIOLATION = "23001";

export function isReferencedRowViolation(err: unknown): boolean {
  const code = pgErrorCode(err);
  return code === PG_FOREIGN_KEY_VIOLATION || code === PG_RESTRICT_VIOLATION;
}
