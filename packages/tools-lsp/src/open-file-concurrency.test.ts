import { describe, test, expect } from "bun:test";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { createLSPClient } from "./client";

const FAKE = join(import.meta.dir, "./fake-lsp-server.ts");

describe("notify.open under concurrency", () => {
  test("two overlapping opens of the same path", async () => {
    const dir = await mkdtemp(join(tmpdir(), "lsp-conc-"));
    const filePath = join(dir, "conc.ts");
    await writeFile(filePath, "const a = 1;\n");

    const proc = spawn("bun", ["run", FAKE], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    const client = await createLSPClient({
      serverID: "test",
      server: { process: proc },
      root: dir,
    });

    // The shape the middleware produces: a fire-and-forget touch overlapping
    // an awaited one on the same path.
    const [v1, v2] = await Promise.all([
      client.notify.open({ path: filePath }),
      client.notify.open({ path: filePath }),
    ]);

    const v3 = await client.notify.open({ path: filePath });

    await client.shutdown();

    // Two overlapping opens must yield two distinct versions; a duplicate
    // means the read-modify-write on the file map lost an update, and the
    // server would see the same document version twice. A following serial
    // open must then continue from the higher of them rather than repeat it.
    expect(new Set([v1, v2]).size).toBe(2);
    expect(v3).toBe(Math.max(v1, v2) + 1);
  }, 30_000);
});
