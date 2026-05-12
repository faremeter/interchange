import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { createLSPManager, type LSPManager } from "./lsp";
import type { ServerInfo } from "./server";

const FAKE_SERVER_PATH = join(import.meta.dir, "fake-lsp-server.ts");

function makeFakeServerInfo(id = "fake"): ServerInfo {
  return {
    id,
    extensions: [".ts", ".js"],
    async root(_file, ctx) {
      return ctx.directory;
    },
    async spawn(root, _ctx) {
      const proc = spawn("bun", ["run", FAKE_SERVER_PATH], {
        stdio: ["pipe", "pipe", "pipe"],
        cwd: root,
      });
      return { process: proc };
    },
  };
}

function makeBrokenServerInfo(): ServerInfo {
  return {
    id: "broken",
    extensions: [".ts"],
    async root(_file, ctx) {
      return ctx.directory;
    },
    async spawn(_root, _ctx) {
      throw new Error("spawn failed");
    },
  };
}

function makeUnavailableServerInfo(): ServerInfo {
  return {
    id: "unavailable",
    extensions: [".ts"],
    async root(_file, ctx) {
      return ctx.directory;
    },
    async spawn(_root, _ctx) {
      return undefined;
    },
  };
}

let tmpDir: string;
const managers: LSPManager[] = [];

beforeAll(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), "lsp-manager-test-"));
});

afterAll(async () => {
  for (const m of managers) {
    await m.dispose().catch(() => undefined);
  }
  if (tmpDir !== undefined) {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

describe("createLSPManager", () => {
  test("hasClients returns true for supported files", async () => {
    const filePath = join(tmpDir, "test.ts");
    await writeFile(filePath, "const x = 1;\n");

    const mgr = createLSPManager({
      cwd: tmpDir,
      servers: [makeFakeServerInfo()],
    });
    managers.push(mgr);

    const has = await mgr.hasClients(filePath);
    expect(has).toBe(true);
  });

  test("hasClients returns false for files outside cwd", async () => {
    const mgr = createLSPManager({
      cwd: tmpDir,
      servers: [makeFakeServerInfo()],
    });
    managers.push(mgr);

    const has = await mgr.hasClients("/nonexistent/file.ts");
    expect(has).toBe(false);
  });

  test("hasClients returns false for unsupported extensions", async () => {
    const filePath = join(tmpDir, "test.xyz");
    await writeFile(filePath, "content");

    const mgr = createLSPManager({
      cwd: tmpDir,
      servers: [makeFakeServerInfo()],
    });
    managers.push(mgr);

    const has = await mgr.hasClients(filePath);
    expect(has).toBe(false);
  });

  test("touchFile opens file and collects diagnostics", async () => {
    const filePath = join(tmpDir, "touch-test.ts");
    await writeFile(filePath, "const x = ERROR_MARKER;\n");

    const mgr = createLSPManager({
      cwd: tmpDir,
      servers: [makeFakeServerInfo()],
    });
    managers.push(mgr);

    await mgr.touchFile(filePath, "document");
    const diags = await mgr.diagnostics();

    expect(Object.keys(diags).length).toBeGreaterThan(0);
    const fileKey = Object.keys(diags).find((k) => k.includes("touch-test"));
    expect(fileKey).toBeDefined();
  });

  test("broken server is marked and not retried", async () => {
    const filePath = join(tmpDir, "broken-test.ts");
    await writeFile(filePath, "content");

    const mgr = createLSPManager({
      cwd: tmpDir,
      servers: [makeBrokenServerInfo()],
    });
    managers.push(mgr);

    const has1 = await mgr.hasClients(filePath);
    expect(has1).toBe(false);

    // Second call should not retry
    const has2 = await mgr.hasClients(filePath);
    expect(has2).toBe(false);
  });

  test("unavailable server returns no clients", async () => {
    const filePath = join(tmpDir, "unavail-test.ts");
    await writeFile(filePath, "content");

    const mgr = createLSPManager({
      cwd: tmpDir,
      servers: [makeUnavailableServerInfo()],
    });
    managers.push(mgr);

    const has = await mgr.hasClients(filePath);
    expect(has).toBe(false);
  });

  test("status returns connected clients", async () => {
    const filePath = join(tmpDir, "status-test.ts");
    await writeFile(filePath, "const x = 1;\n");

    const mgr = createLSPManager({
      cwd: tmpDir,
      servers: [makeFakeServerInfo("status-server")],
    });
    managers.push(mgr);

    await mgr.hasClients(filePath);
    const st = mgr.status();
    expect(st.length).toBe(1);
    expect(st[0]?.id).toBe("status-server");
    expect(st[0]?.status).toBe("connected");
  });

  test("hover returns a result", async () => {
    const filePath = join(tmpDir, "hover-test.ts");
    await writeFile(filePath, "const x = 1;\n");

    const mgr = createLSPManager({
      cwd: tmpDir,
      servers: [makeFakeServerInfo()],
    });
    managers.push(mgr);

    await mgr.touchFile(filePath);
    const result = await mgr.hover({ file: filePath, line: 0, character: 0 });
    expect(result).not.toBeNull();
  });

  test("definition returns results", async () => {
    const filePath = join(tmpDir, "def-test.ts");
    await writeFile(filePath, "const x = 1;\n");

    const mgr = createLSPManager({
      cwd: tmpDir,
      servers: [makeFakeServerInfo()],
    });
    managers.push(mgr);

    await mgr.touchFile(filePath);
    const result = await mgr.definition({
      file: filePath,
      line: 0,
      character: 0,
    });
    expect(result.length).toBeGreaterThan(0);
  });

  test("dispose shuts down all clients", async () => {
    const filePath = join(tmpDir, "dispose-test.ts");
    await writeFile(filePath, "const x = 1;\n");

    const mgr = createLSPManager({
      cwd: tmpDir,
      servers: [makeFakeServerInfo()],
    });

    await mgr.hasClients(filePath);
    expect(mgr.status().length).toBe(1);

    await mgr.dispose();
    expect(mgr.status().length).toBe(0);
  });

  test("concurrent getClients calls share a single spawn", async () => {
    let spawnCount = 0;
    const dedupServer: ServerInfo = {
      id: "dedup",
      extensions: [".ts"],
      async root(_file, ctx) {
        return ctx.directory;
      },
      async spawn(root, _ctx) {
        spawnCount++;
        const proc = spawn("bun", ["run", FAKE_SERVER_PATH], {
          stdio: ["pipe", "pipe", "pipe"],
          cwd: root,
        });
        return { process: proc };
      },
    };

    const filePath = join(tmpDir, "dedup-test.ts");
    await writeFile(filePath, "const x = 1;\n");

    const mgr = createLSPManager({
      cwd: tmpDir,
      servers: [dedupServer],
    });
    managers.push(mgr);

    await Promise.all([
      mgr.hasClients(filePath),
      mgr.hasClients(filePath),
      mgr.hasClients(filePath),
    ]);

    expect(spawnCount).toBe(1);
  });
});
