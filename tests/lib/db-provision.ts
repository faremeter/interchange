// Per-run fresh-database provisioner for the browser end-to-end harness.
// Unlike `db-harness.ts` (an isolated schema per test), this module
// creates a fresh uniquely-named *database* per run: create, migrate,
// grant, and drop on teardown. CREATE/DROP DATABASE runs on the
// maintenance connection (ambient superuser identity, no explicit role)
// exactly as `bin/db-reset` relies on; only host/port come from `.env`.

import path from "node:path";

import postgres from "postgres";

import { runMigrations, type DBConfig } from "@intx/db";

import { loadHarnessDbConfig, randomSchemaName } from "./db-harness";
import { REPO_ROOT, loadEnvFile, requireKey } from "./env";
import { grantHubSchemaAccess, quoteIdent } from "./grants";

/**
 * A provisioned, migrated database dedicated to a single harness run.
 *
 * `config` connects as the migration role and is what applies DDL. The
 * spawned hub does NOT use these credentials: it runs as the hub role
 * (loaded separately) and only reuses the `database` name.
 */
export type ProvisionedDatabase = {
  database: string;
  config: DBConfig;
  teardown: () => Promise<void>;
};

/**
 * Drop a provisioned database by name via the maintenance connection:
 * terminate remaining backends, then `DROP DATABASE ... WITH (FORCE)`,
 * retrying a bounded number of times so a winding-down connection cannot
 * orphan the database. Shared by `provisionDatabase`'s `teardown` and
 * the provisioning CLI's `down` command.
 */
export async function dropProvisionedDatabase(name: string): Promise<void> {
  const base = loadHarnessDbConfig();
  const dbIdent = quoteIdent(name);

  const dropClient = postgres({
    host: base.host,
    port: base.port,
    database: "postgres",
    max: 1,
    onnotice: () => undefined,
  });
  try {
    await dropClient`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = ${name} AND pid <> pg_backend_pid()`;

    let lastError: unknown;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        await dropClient
          .unsafe(`DROP DATABASE IF EXISTS ${dbIdent} WITH (FORCE)`)
          .simple();
        return;
      } catch (error) {
        lastError = error;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }
    throw new Error(`failed to drop database ${name}`, {
      cause: lastError,
    });
  } finally {
    await dropClient.end();
  }
}

/**
 * Create a fresh, uniquely-named postgres database, grant the migration
 * and hub roles what each needs, migrate into `public`, and return a
 * handle whose `teardown` drops it again.
 */
export async function provisionDatabase(): Promise<ProvisionedDatabase> {
  const base = loadHarnessDbConfig();
  const migrateRole = base.user;

  const hubEnv = await loadEnvFile(path.join(REPO_ROOT, ".env.hub"));
  const hubRole = requireKey(hubEnv, "DB_USER", ".env.hub");

  const database = randomSchemaName();
  const dbIdent = quoteIdent(database);
  const migrateRoleIdent = quoteIdent(migrateRole);
  const hubRoleIdent = quoteIdent(hubRole);

  // Maintenance client: host/port from `.env`, identity inherited from
  // the ambient libpq environment (no `user`/`password`), mirroring
  // `bin/db-reset`.
  const openMaintenance = (targetDatabase: string) =>
    postgres({
      host: base.host,
      port: base.port,
      database: targetDatabase,
      max: 1,
      onnotice: () => undefined,
    });

  const admin = openMaintenance("postgres");
  try {
    // CREATE DATABASE cannot run inside a transaction, so force simple
    // query mode. The name is a quoted identifier, not a bind value.
    await admin.unsafe(`CREATE DATABASE ${dbIdent}`).simple();
  } finally {
    await admin.end();
  }

  const config: DBConfig = { ...base, database };

  // Everything past CREATE DATABASE either completes or the just-created
  // database is dropped, so a mid-provision failure cannot orphan a
  // randomly-named database in the cluster.
  try {
    // Grants must precede migration. The schema-level grant is
    // per-database, so it must run on a connection to the new database;
    // without it, `runMigrations` fails with "permission denied for
    // schema public" on fresh PG15+.
    const adminNewDb = openMaintenance(database);
    try {
      await adminNewDb.unsafe(
        `GRANT ALL ON DATABASE ${dbIdent} TO ${migrateRoleIdent}`,
      );
      await adminNewDb.unsafe(
        `GRANT ALL ON SCHEMA public TO ${migrateRoleIdent}`,
      );
      await adminNewDb.unsafe(
        `GRANT CONNECT ON DATABASE ${dbIdent} TO ${hubRoleIdent}`,
      );
    } finally {
      await adminNewDb.end();
    }

    await runMigrations(config, { schema: "public" });

    // The migration role owns the freshly-migrated tables, so it is the
    // role that can grant the hub role access to them.
    const migrateClient = postgres({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
      database: config.database,
      max: 1,
      onnotice: () => undefined,
    });
    try {
      await grantHubSchemaAccess(migrateClient, "public", hubRole);
    } finally {
      await migrateClient.end();
    }
  } catch (error) {
    try {
      await dropProvisionedDatabase(database);
    } catch {
      // Best-effort cleanup: the original provisioning failure is the
      // meaningful one, so a failure to drop the half-provisioned
      // database must not mask it.
    }
    throw error;
  }

  return {
    database,
    config,
    teardown: () => dropProvisionedDatabase(database),
  };
}
