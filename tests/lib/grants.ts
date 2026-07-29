// Hub-role grant issuance for the fresh-database provisioner
// (`db-provision.ts`).
//
// The hub app connects under its own postgres role, which is not the
// role that owns the migrated tables, so the hub role needs DML +
// USAGE grants before it can read or write. This module issues that
// grant set for the browser end-to-end harness's per-run databases.
// The older hub-subprocess harness (`tests/hub-api/lib/git-harness.ts`)
// still inlines its own copy of the same grants and of the identifier
// quoting; consolidating it onto this module is deliberate follow-up
// work.

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
