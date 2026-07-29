// Per-run fresh-database provisioner for the browser end-to-end harness.
//
// Unlike `db-harness.ts`, which gives each test an isolated *schema*
// inside the shared `interchange` database, this module provisions a
// fresh, uniquely-named *database* per run: create, migrate, grant, and
// drop on teardown. The browser harness spawns a hub against the
// returned database name and needs the whole database to itself.
//
// Database lifecycle (CREATE/DROP DATABASE) is owned by the maintenance
// connection, exactly as `bin/db-reset` relies on: the connection omits
// an explicit role so postgres.js inherits the ambient superuser
// identity, and the migration role is never granted cluster-wide
// create-database rights. Only the host/port come from `.env`; the
// identity comes from the ambient libpq environment.

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
 * Drop a provisioned database by name. Reloads host/port from `.env` via
 * `loadHarnessDbConfig` and connects to the maintenance `postgres`
 * database under the ambient superuser identity, exactly as
 * `provisionDatabase` does when it creates the database. Terminates any
 * remaining backends against the target, then drops it with `FORCE`,
 * retrying a bounded number of times so a connection that is still
 * winding down cannot leave the database orphaned.
 *
 * Both `provisionDatabase`'s returned `teardown` and the standalone
 * provisioning CLI's `down` command route through here, so there is a
 * single drop implementation with two entry points.
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
 * Provision a fresh, uniquely-named postgres database: create it under
 * the maintenance (superuser) connection, grant the migration and hub
 * roles what each needs, apply the migrations into `public`, and return
 * a handle whose `teardown` drops the database again.
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
  // `bin/db-reset`. `database` selects which database the connection
  // targets, which is load-bearing for the schema-level grant below.
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
    // per-database, so it must run on a connection to the NEW database;
    // without `GRANT ALL ON SCHEMA public`, `runMigrations` fails with
    // "permission denied for schema public" on fresh PG15+.
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
