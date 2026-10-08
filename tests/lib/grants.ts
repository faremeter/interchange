// Hub-role grant issuance shared by the `tests/` harnesses: the hub app
// runs as a postgres role that does not own the migrated tables, so it
// needs the same DML + USAGE grants whether the tables live in a
// per-test schema or a per-run database's `public` schema.

import postgres from "postgres";

export function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Grant the hub app role DML + USAGE on a migrated schema whose tables
 * the migration role owns. `sql` connects as a role able to grant on
 * those objects (the migration role).
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
