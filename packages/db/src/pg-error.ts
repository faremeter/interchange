import { hasCode } from "@intx/types";

// Postgres SQLSTATE codes. https://www.postgresql.org/docs/current/errcodes-appendix.html
export const PG_UNIQUE_VIOLATION = "23505";
export const PG_FOREIGN_KEY_VIOLATION = "23503";

// Extract a Postgres SQLSTATE from an error a driver or ORM may have wrapped:
// postgres-js sets it on the thrown error, Drizzle re-wraps that as `cause`,
// so walk the cause chain (depth-bounded against a self-referential loop).
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
