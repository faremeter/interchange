import { describe, expect, test } from "bun:test";
import path from "node:path";

const REPO_ROOT = path.resolve(import.meta.dir, "..");

// The backfill seals private key seeds under the cipher the hub decrypts with,
// so a missing PRINCIPAL_KEY_ENCRYPTION_KEY must hard-fail rather than fall back
// to a noop cipher (which would seal seeds unrecoverably). The key check runs
// before any DB connection, so spawning with the key absent surfaces exactly
// that guard.
describe("bin/backfill-principal-keys env guard", () => {
  test("fails hard when PRINCIPAL_KEY_ENCRYPTION_KEY is absent", async () => {
    const env: Record<string, string> = {};
    for (const [key, value] of Object.entries(process.env)) {
      if (key !== "PRINCIPAL_KEY_ENCRYPTION_KEY" && value !== undefined) {
        env[key] = value;
      }
    }

    const proc = Bun.spawn(
      ["bun", "run", "--conditions=intx-src", "bin/backfill-principal-keys.ts"],
      { cwd: REPO_ROOT, env, stdout: "pipe", stderr: "pipe" },
    );
    const exitCode = await proc.exited;
    const stderr = await new Response(proc.stderr).text();

    expect(exitCode).not.toBe(0);
    expect(stderr).toMatch(
      /PRINCIPAL_KEY_ENCRYPTION_KEY environment variable is required/,
    );
  });
});
