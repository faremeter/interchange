// Drive `bin/audit-sender-keys.ts` as the operator runs it -- a real subprocess
// against a seeded schema -- and assert its process contract: it exits non-zero
// when a sender is unresolvable (so a pre-enforce gate actually fails), exits
// zero when every sender resolves, and refuses to run without its encryption
// key. A green-only test here would be worthless -- an unresolved sender that
// still exits zero reads as "safe to enforce" -- so the exit-code assertion is
// the point.

import path from "node:path";

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  loadHarnessDbConfig,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedWorkflowRun } from "@intx/test-harness/seed";

const REPO_ROOT = path.resolve(import.meta.dir, "..", "..");
// The audit never decrypts (it reads plaintext public keys), so any valid
// 32-byte hex satisfies the env-key cipher the driver constructs.
const ENCRYPTION_KEY = "ab".repeat(32);

describe.skipIf(!harnessDbEnvAvailable())("audit-sender-keys driver", () => {
  let h: TestDb;

  beforeAll(async () => {
    h = await createTestDb();
  });

  afterAll(async () => {
    await h.close();
  });

  beforeEach(async () => {
    await h.reset();
  });

  function driverEnv(
    overrides: Record<string, string | undefined>,
  ): Record<string, string> {
    const cfg = loadHarnessDbConfig();
    const base: Record<string, string | undefined> = {
      ...process.env,
      DB_HOST: cfg.host,
      DB_PORT: String(cfg.port),
      DB_NAME: cfg.database,
      DB_USER: cfg.user,
      DB_PASSWORD: cfg.password,
      // Pin the subprocess to this test's isolated schema.
      PG_SCHEMA: h.schema,
      PRINCIPAL_KEY_ENCRYPTION_KEY: ENCRYPTION_KEY,
      ...overrides,
    };
    const env: Record<string, string> = {};
    for (const [k, v] of Object.entries(base)) {
      if (v !== undefined) env[k] = v;
    }
    return env;
  }

  async function runDriver(
    overrides: Record<string, string | undefined> = {},
  ): Promise<{ exitCode: number; output: string }> {
    const proc = Bun.spawn(
      [process.execPath, "--conditions=intx-src", "bin/audit-sender-keys.ts"],
      {
        cwd: REPO_ROOT,
        env: driverEnv(overrides),
        stdout: "pipe",
        stderr: "pipe",
      },
    );
    // The driver's structured log lines land on either stream depending on
    // level, so read both and match the combined output.
    const [stdout, stderr] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ]);
    const exitCode = await proc.exited;
    return { exitCode, output: stdout + stderr };
  }

  test("exits zero when every sender resolves", async () => {
    const { exitCode, output } = await runDriver();
    expect(exitCode).toBe(0);
    // Guard against a false pass: confirm the audit actually ran rather than
    // dying early (which would also exit zero for a different reason).
    expect(output).toContain("Audit complete");
  });

  test("exits non-zero when a sender is unresolvable", async () => {
    await h.db.insert(tenantTable).values({
      id: "tnt_driver",
      name: "driver",
      slug: "driver",
      domain: "driver.localhost",
      parentId: null,
    });
    // A live run carrying a sending address but no key: the genuine
    // unresolvable-signed-sender hole the audit gates on.
    await seedWorkflowRun(h.db, {
      id: "run_driver_hole",
      tenantId: "tnt_driver",
      address: "run_driver_hole@driver.localhost",
      publicKey: null,
      status: "running",
    });

    const { exitCode, output } = await runDriver();
    expect(exitCode).toBe(1);
    expect(output).toContain("Unresolvable");
  });

  test("refuses to run without PRINCIPAL_KEY_ENCRYPTION_KEY", async () => {
    const { exitCode, output } = await runDriver({
      PRINCIPAL_KEY_ENCRYPTION_KEY: undefined,
    });
    expect(exitCode).not.toBe(0);
    expect(output).toContain("PRINCIPAL_KEY_ENCRYPTION_KEY");
  });
});
