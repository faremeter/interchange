#!/usr/bin/env bun

// Standalone provisioning CLI for the browser end-to-end harness.
//
// The Playwright harness (`tests/admin-ui-e2e`) must not import `@intx/*`
// — it is a leaf test package with only `@playwright/test`, `arktype`,
// and `zx` as dependencies. This CLI is the single boundary that reaches
// into the `@intx/test-harness` primitives on its behalf: `up` provisions
// a fresh migrated database and prints the hub's connection env as one
// line of JSON on stdout, and `down` drops a named database again. The
// harness's global setup shells out to both.
//
// Run under `bun --conditions=intx-src` so the `@intx/*` specifiers
// resolve to TypeScript source, exactly as `bin/dev.ts` does.

import path from "node:path";

import {
  dropProvisionedDatabase,
  provisionDatabase,
} from "@intx/test-harness/db-provision";
import {
  REPO_ROOT,
  loadEnvFile,
  optionalKey,
  requireKey,
} from "@intx/test-harness/env";

async function up(): Promise<void> {
  const provisioned = await provisionDatabase();

  // `provisionDatabase` has already created and migrated the database;
  // if the env resolution or stdout write below throws, drop it so the
  // failure does not orphan a database in the cluster.
  try {
    // DB_HOST/DB_PORT come from `.env` (the shared connection target);
    // DB_USER/DB_PASSWORD/BETTER_AUTH_SECRET come from `.env.hub` (the
    // hub app's own role and auth secret). The spawned hub runs as the
    // hub role against the freshly-provisioned database name.
    const shared = await loadEnvFile(path.join(REPO_ROOT, ".env"));
    const hubEnv = await loadEnvFile(path.join(REPO_ROOT, ".env.hub"));

    const payload = {
      database: provisioned.database,
      hubEnv: {
        DB_HOST: requireKey(shared, "DB_HOST", ".env"),
        DB_PORT: requireKey(shared, "DB_PORT", ".env"),
        DB_USER: requireKey(hubEnv, "DB_USER", ".env.hub"),
        DB_PASSWORD: optionalKey(hubEnv, "DB_PASSWORD"),
        BETTER_AUTH_SECRET: requireKey(
          hubEnv,
          "BETTER_AUTH_SECRET",
          ".env.hub",
        ),
        CREDENTIAL_ENCRYPTION_KEY: requireKey(
          hubEnv,
          "CREDENTIAL_ENCRYPTION_KEY",
          ".env.hub",
        ),
      },
    };

    // Await the write callback so a piped stdout is fully drained
    // before the process exits; exiting early can truncate the JSON
    // line the parent parses.
    const line = `${JSON.stringify(payload)}\n`;
    await new Promise<void>((resolve, reject) => {
      process.stdout.write(line, (err) => (err ? reject(err) : resolve()));
    });
  } catch (err) {
    try {
      await dropProvisionedDatabase(provisioned.database);
    } catch {
      // A secondary drop failure must not mask the original error.
    }
    throw err;
  }
}

async function down(database: string): Promise<void> {
  await dropProvisionedDatabase(database);
}

// Success paths let the process exit naturally so stdio drains fully;
// `process.exit` races exit against pending pipe writes. Bad argv sets
// `process.exitCode` instead, which still yields a non-zero exit
// without cutting off the usage message.
const args = process.argv.slice(2);
const command = args[0];

if (command === "up") {
  if (args.length !== 1) {
    process.stderr.write("e2e-provision: usage: e2e-provision up\n");
    process.exitCode = 1;
  } else {
    await up();
  }
} else if (command === "down") {
  const database = args[1];
  if (args.length !== 2 || database === undefined || database === "") {
    process.stderr.write(
      "e2e-provision: usage: e2e-provision down <database>\n",
    );
    process.exitCode = 1;
  } else {
    await down(database);
  }
} else {
  process.stderr.write(
    `e2e-provision: unknown command ${JSON.stringify(command)}; expected "up" or "down <database>"\n`,
  );
  process.exitCode = 1;
}
