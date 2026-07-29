import path from "node:path";

import { afterAll, describe, expect, test } from "bun:test";
import postgres from "postgres";

import {
  harnessDbEnvAvailable,
  loadHarnessDbConfig,
} from "@intx/test-harness/db-harness";
import { provisionDatabase } from "@intx/test-harness/db-provision";
import {
  REPO_ROOT,
  loadEnvFile,
  optionalKey,
  requireKey,
} from "@intx/test-harness/env";

describe.skipIf(!harnessDbEnvAvailable())("provisionDatabase (real DB)", () => {
  const clients: ReturnType<typeof postgres>[] = [];

  const openMaintenance = (database: string) => {
    const base = loadHarnessDbConfig();
    const client = postgres({
      host: base.host,
      port: base.port,
      database,
      max: 1,
      onnotice: () => undefined,
    });
    clients.push(client);
    return client;
  };

  afterAll(async () => {
    for (const client of clients) {
      await client.end();
    }
  });

  test("creates, migrates, grants hub DML, and drops on teardown", async () => {
    const provisioned = await provisionDatabase();

    const admin = openMaintenance("postgres");
    const existing = await admin`
      SELECT 1 FROM pg_database WHERE datname = ${provisioned.database}
    `;
    expect(existing).toHaveLength(1);

    const inDb = openMaintenance(provisioned.database);
    const migratedTables = await inDb`
      SELECT tablename FROM pg_tables
      WHERE schemaname = 'public' AND tablename = 'tenant'
    `;
    expect(migratedTables).toHaveLength(1);

    const hubEnv = await loadEnvFile(path.join(REPO_ROOT, ".env.hub"));
    const shared = await loadEnvFile(path.join(REPO_ROOT, ".env"));
    const merged = { ...shared, ...hubEnv };
    const hub = postgres({
      host: requireKey(merged, "DB_HOST", ".env"),
      port: Number(requireKey(merged, "DB_PORT", ".env")),
      user: requireKey(merged, "DB_USER", ".env.hub"),
      password: optionalKey(merged, "DB_PASSWORD"),
      database: provisioned.database,
      max: 1,
      onnotice: () => undefined,
    });
    try {
      const id = `prov_${Date.now().toString(36)}`;
      await hub`
        INSERT INTO tenant (id, name, slug, domain)
        VALUES (${id}, ${"Provisioner"}, ${id}, ${id})
      `;
      const selected = await hub`SELECT id FROM tenant WHERE id = ${id}`;
      expect(selected).toHaveLength(1);
      await hub`DELETE FROM tenant WHERE id = ${id}`;
    } finally {
      await hub.end();
    }

    await provisioned.teardown();

    const admin2 = openMaintenance("postgres");
    const remaining = await admin2`
      SELECT 1 FROM pg_database WHERE datname = ${provisioned.database}
    `;
    expect(remaining).toHaveLength(0);
  });
});
