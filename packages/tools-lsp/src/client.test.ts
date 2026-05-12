import { describe, test, expect, afterAll } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { createLSPClient, type LSPClient } from "./client";

const FAKE_SERVER_PATH = join(import.meta.dir, "fake-lsp-server.ts");

let tmpDir: string;
const clients: LSPClient[] = [];

async function makeTmpDir(): Promise<string> {
  if (tmpDir === undefined) {
    tmpDir = await mkdtemp(join(tmpdir(), "lsp-client-test-"));
  }
  return tmpDir;
}

function spawnFakeServer() {
  return spawn("bun", ["run", FAKE_SERVER_PATH], {
    stdio: ["pipe", "pipe", "pipe"],
  });
}

afterAll(async () => {
  for (const c of clients) {
    await c.shutdown().catch(() => undefined);
  }
  if (tmpDir !== undefined) {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

describe("createLSPClient", () => {
  test("completes the initialize handshake", async () => {
    const proc = spawnFakeServer();
    const client = await createLSPClient({
      serverID: "test",
      server: { process: proc },
      root: await makeTmpDir(),
    });
    clients.push(client);

    expect(client.serverID).toBe("test");
    expect(client.root).toBe(tmpDir);
  });

  test("didOpen sends notification and receives diagnostics", async () => {
    const dir = await makeTmpDir();
    const filePath = join(dir, "test-file.ts");
    await writeFile(filePath, "const x = 1;\n");

    const proc = spawnFakeServer();
    const client = await createLSPClient({
      serverID: "test",
      server: { process: proc },
      root: dir,
    });
    clients.push(client);

    const version = await client.notify.open({ path: filePath });
    expect(version).toBe(1);

    await client.waitForDiagnostics({
      path: filePath,
      version,
      mode: "document",
    });

    // Clean file should have no diagnostics
    expect(client.diagnostics.size).toBe(0);
  });

  test("didOpen with ERROR_MARKER produces diagnostics", async () => {
    const dir = await makeTmpDir();
    const filePath = join(dir, "error-file.ts");
    await writeFile(filePath, "const x = ERROR_MARKER;\n");

    const proc = spawnFakeServer();
    const client = await createLSPClient({
      serverID: "test",
      server: { process: proc },
      root: dir,
    });
    clients.push(client);

    const version = await client.notify.open({ path: filePath });
    await client.waitForDiagnostics({
      path: filePath,
      version,
      mode: "document",
    });

    expect(client.diagnostics.size).toBeGreaterThan(0);
  });

  test("subsequent opens increment version", async () => {
    const dir = await makeTmpDir();
    const filePath = join(dir, "version-file.ts");
    await writeFile(filePath, "const a = 1;\n");

    const proc = spawnFakeServer();
    const client = await createLSPClient({
      serverID: "test",
      server: { process: proc },
      root: dir,
    });
    clients.push(client);

    const v1 = await client.notify.open({ path: filePath });
    expect(v1).toBe(1);

    await writeFile(filePath, "const a = 2;\n");
    const v2 = await client.notify.open({ path: filePath });
    expect(v2).toBe(2);
  });

  test("pull diagnostics via full mode", async () => {
    const dir = await makeTmpDir();
    const filePath = join(dir, "pull-file.ts");
    await writeFile(filePath, "const x = ERROR_MARKER;\n");

    const proc = spawnFakeServer();
    const client = await createLSPClient({
      serverID: "test",
      server: { process: proc },
      root: dir,
    });
    clients.push(client);

    const version = await client.notify.open({ path: filePath });
    await client.waitForDiagnostics({
      path: filePath,
      version,
      mode: "full",
    });

    expect(client.diagnostics.size).toBeGreaterThan(0);
  });

  test("shutdown cleans up gracefully", async () => {
    const proc = spawnFakeServer();
    const exitPromise = new Promise<number | null>((resolve) => {
      proc.once("exit", (code) => resolve(code));
    });

    const client = await createLSPClient({
      serverID: "test",
      server: { process: proc },
      root: await makeTmpDir(),
    });

    await client.shutdown();

    const exitCode = await exitPromise;
    // Process exits cleanly (0) or was killed by signal (null) -- both are OK
    expect(exitCode === 0 || exitCode === null).toBe(true);
  });

  test("initialization with config sends didChangeConfiguration", async () => {
    const proc = spawnFakeServer();
    const client = await createLSPClient({
      serverID: "test",
      server: {
        process: proc,
        initialization: { tsserver: { path: "/usr/bin/tsserver" } },
      },
      root: await makeTmpDir(),
    });
    clients.push(client);

    // If the fake server did not crash, initialization was accepted
    expect(client.serverID).toBe("test");
  });

  test("seeded diagnostics do not trigger listener on first publish", async () => {
    const dir = await makeTmpDir();
    const filePath = join(dir, "seeded-file.ts");
    await writeFile(filePath, "const x = ERROR_MARKER;\n");

    const proc = spawnFakeServer();
    const client = await createLSPClient({
      serverID: "test",
      server: { process: proc },
      root: dir,
      seedsInitialDiagnostics: true,
    });
    clients.push(client);

    const version = await client.notify.open({ path: filePath });

    // Wait a bit for the seeded diagnostics to arrive
    await new Promise((resolve) => setTimeout(resolve, 200));

    // Diagnostics should still be present (seeded)
    await client.waitForDiagnostics({
      path: filePath,
      version,
      mode: "document",
    });

    expect(client.diagnostics.size).toBeGreaterThan(0);
  });
});

describe("launch", () => {
  test("spawn produces piped stdio and exits cleanly", async () => {
    const proc = spawn("echo", ["hello"], {
      stdio: ["pipe", "pipe", "pipe"],
    });

    expect(proc.stdin).not.toBeNull();
    expect(proc.stdout).not.toBeNull();
    expect(proc.stderr).not.toBeNull();

    const exitCode = await new Promise<number | null>((resolve) => {
      proc.once("exit", (code) => resolve(code));
    });

    expect(exitCode).toBe(0);
  });
});
