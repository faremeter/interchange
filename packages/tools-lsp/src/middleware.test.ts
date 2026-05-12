import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { createPosixTools, type PosixTools } from "@interchange/tools-posix";
import { createLSPPlugin } from "./index";
import type { ServerInfo } from "./server";
import { createLSPManager, type LSPManager } from "./lsp";
import { createLSPMiddleware } from "./middleware";
import { makeLSPToolHandler } from "./tool";

const FAKE_SERVER_PATH = join(import.meta.dir, "fake-lsp-server.ts");

function makeFakeServerInfo(): ServerInfo {
  return {
    id: "fake",
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

let tmpDir: string;
let mgr: LSPManager;
let tools: PosixTools;

function neverAbort(): AbortSignal {
  return new AbortController().signal;
}

beforeAll(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), "lsp-mw-test-"));
  mgr = createLSPManager({ cwd: tmpDir, servers: [makeFakeServerInfo()] });
});

afterAll(async () => {
  if (tools !== undefined) await tools.dispose();
  if (mgr !== undefined) await mgr.dispose().catch(() => undefined);
  if (tmpDir !== undefined) await rm(tmpDir, { recursive: true, force: true });
});

describe("LSP middleware", () => {
  test("appends diagnostics to edit_file result on error", async () => {
    const filePath = join(tmpDir, "mw-edit.ts");
    await writeFile(filePath, "const x = ERROR_MARKER;\n");

    tools = createPosixTools({
      cwd: tmpDir,
      plugins: [
        {
          middleware: createLSPMiddleware(mgr, { cwd: tmpDir }),
        },
      ],
    });

    const result = await tools.run(
      {
        id: "e1",
        name: "edit_file",
        arguments: {
          path: filePath,
          old_string: "const x",
          new_string: "const y",
        },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    // The file should still have ERROR_MARKER after the edit
    // so diagnostics should be appended
    expect(String(result.content)).toContain("<diagnostics");
  });

  test("does not append diagnostics to read_file result", async () => {
    const filePath = join(tmpDir, "mw-read.ts");
    await writeFile(filePath, "const x = ERROR_MARKER;\n");

    const pt = createPosixTools({
      cwd: tmpDir,
      plugins: [
        {
          middleware: createLSPMiddleware(mgr, { cwd: tmpDir }),
        },
      ],
    });

    const result = await pt.run(
      { id: "r1", name: "read_file", arguments: { path: filePath } },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(String(result.content)).not.toContain("<diagnostics");
    await pt.dispose();
  });

  test("minSeverity filters diagnostics", async () => {
    const filePath = join(tmpDir, "mw-severity.ts");
    await writeFile(filePath, "clean code\n");

    const pt = createPosixTools({
      cwd: tmpDir,
      plugins: [
        {
          middleware: createLSPMiddleware(mgr, {
            cwd: tmpDir,
            minSeverity: 1,
          }),
        },
      ],
    });

    const result = await pt.run(
      {
        id: "s1",
        name: "write_file",
        arguments: { path: filePath, content: "clean code\n" },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    // Clean file should have no diagnostics
    expect(String(result.content)).not.toContain("<diagnostics");
    await pt.dispose();
  });
});

describe("LSP tool handler", () => {
  test("hover returns a result", async () => {
    const filePath = join(tmpDir, "tool-hover.ts");
    await writeFile(filePath, "const x = 1;\n");

    const handler = makeLSPToolHandler(mgr, tmpDir);
    const result = await handler(
      {
        id: "h1",
        name: "lsp",
        arguments: {
          operation: "hover",
          filePath,
          line: 1,
          character: 1,
        },
      },
      neverAbort(),
    );

    expect(result.isError).not.toBe(true);
    expect(result.content).not.toContain("no results");
  });

  test("goToDefinition returns results", async () => {
    const filePath = join(tmpDir, "tool-def.ts");
    await writeFile(filePath, "const x = 1;\n");

    const handler = makeLSPToolHandler(mgr, tmpDir);
    const result = await handler(
      {
        id: "d1",
        name: "lsp",
        arguments: {
          operation: "goToDefinition",
          filePath,
          line: 1,
          character: 1,
        },
      },
      neverAbort(),
    );

    expect(result.isError).not.toBe(true);
  });

  test("returns error for unsupported file type", async () => {
    const filePath = join(tmpDir, "tool-unsupported.xyz");
    await writeFile(filePath, "content");

    const handler = makeLSPToolHandler(mgr, tmpDir);
    const result = await handler(
      {
        id: "u1",
        name: "lsp",
        arguments: {
          operation: "hover",
          filePath,
          line: 1,
          character: 1,
        },
      },
      neverAbort(),
    );

    expect(result.isError).toBe(true);
    expect(result.content).toContain("no LSP server available");
  });

  test("returns error for invalid operation", async () => {
    const handler = makeLSPToolHandler(mgr, tmpDir);
    const result = await handler(
      {
        id: "inv1",
        name: "lsp",
        arguments: {
          operation: "invalid",
          filePath: "test.ts",
          line: 1,
          character: 1,
        },
      },
      neverAbort(),
    );

    expect(result.isError).toBe(true);
    expect(result.content).toContain("must be one of");
  });

  test("returns error for missing arguments", async () => {
    const handler = makeLSPToolHandler(mgr, tmpDir);
    const result = await handler(
      {
        id: "miss1",
        name: "lsp",
        arguments: { operation: "hover" },
      },
      neverAbort(),
    );

    expect(result.isError).toBe(true);
    expect(result.content).toContain("must be a string");
  });
});

describe("createLSPPlugin", () => {
  test("creates a plugin with tool, middleware, and dispose", () => {
    const plugin = createLSPPlugin({ cwd: tmpDir });

    expect(plugin.tools).toBeDefined();
    expect(plugin.tools?.length).toBe(1);
    expect(plugin.tools?.[0]?.definition.name).toBe("lsp");
    expect(plugin.middleware).toBeDefined();
    expect(plugin.dispose).toBeDefined();
  });
});
