import { PG_FOREIGN_KEY_VIOLATION, pgErrorCode } from "@intx/db";

// A referencing row prevents this delete: 23503 foreign_key_violation (the
// default ON DELETE NO ACTION) or 23001 restrict_violation (ON DELETE
// RESTRICT). A restrict foreign key raises 23001, not 23503, so both codes
// count. pgErrorCode walks the drizzle error cause chain to the driver SQLSTATE.
const PG_RESTRICT_VIOLATION = "23001";

export function isReferencedRowViolation(err: unknown): boolean {
  const code = pgErrorCode(err);
  return code === PG_FOREIGN_KEY_VIOLATION || code === PG_RESTRICT_VIOLATION;
}
