// Shared hub-role grant issuance for the `tests/` harnesses.
//
// The hub app connects under its own postgres role, which is not the
// role that owns the migrated tables. Whether those tables live in a
// per-test schema (the hub-subprocess harness) or the `public` schema
// of a per-run database (the browser end-to-end harness), the hub role
// needs the same DML + USAGE grants before it can read or write, and
// both harnesses issue them through this module.

import postgres from "postgres";

export function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Grant the hub app role the DML + USAGE it needs on a migrated schema
 * whose tables are owned by the migration role.
 *
 * @param sql - A postgres.js client connected to the database that
 *   contains the schema, as a role able to grant on those objects
 *   (the migration role, which owns them).
 * @param schema - The schema the hub reads and writes.
 * @param hubRole - The hub app's postgres role name.
 */
export async function grantHubSchemaAccess(
  sql: ReturnType<typeof postgres>,
  schema: string,
  hubRole: string,
): Promise<void> {
  const schemaIdent = quoteIdent(schema);
  const roleIdent = quoteIdent(hubRole);
  await sql.unsafe(`GRANT USAGE ON SCHEMA ${schemaIdent} TO ${roleIdent}`);
  await sql.unsafe(
    `GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${schemaIdent} TO ${roleIdent}`,
  );
  await sql.unsafe(
    `GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ${schemaIdent} TO ${roleIdent}`,
  );
}
