import { describe, test, expect, afterAll } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createPosixTools } from "./index";

const tools = createPosixTools();

let tmpDir: string;

async function makeTmpDir(): Promise<string> {
  if (tmpDir === undefined) {
    tmpDir = await mkdtemp(join(tmpdir(), "tools-posix-test-"));
  }
  return tmpDir;
}

afterAll(async () => {
  if (tmpDir !== undefined) {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

function neverAbort(): AbortSignal {
  return new AbortController().signal;
}

describe("read_file", () => {
  test("reads an existing file and returns numbered content", async () => {
    const dir = await makeTmpDir();
    const path = join(dir, "hello.txt");
    await tools.run(
      {
        id: "setup",
        name: "write_file",
        arguments: { path, content: "line1\nline2\nline3" },
      },
      neverAbort(),
    );

    const result = await tools.run(
      { id: "r1", name: "read_file", arguments: { path } },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("line1");
    expect(result.content).toContain("line2");
    expect(result.content).toContain("line3");
    expect(result.content).toContain("1");
  });

  test("read_file with offset and limit returns only requested lines", async () => {
    const dir = await makeTmpDir();
    const path = join(dir, "numbered.txt");
    const lines = Array.from({ length: 10 }, (_, i) => `line${i + 1}`).join(
      "\n",
    );
    await tools.run(
      { id: "setup2", name: "write_file", arguments: { path, content: lines } },
      neverAbort(),
    );

    const result = await tools.run(
      { id: "r2", name: "read_file", arguments: { path, offset: 2, limit: 3 } },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("line3");
    expect(result.content).toContain("line4");
    expect(result.content).toContain("line5");
    expect(result.content).not.toContain("line1");
    expect(result.content).not.toContain("line6");
  });

  test("returns error result for a non-existent file", async () => {
    const result = await tools.run(
      {
        id: "r3",
        name: "read_file",
        arguments: { path: "/nonexistent/path/file.txt" },
      },
      neverAbort(),
    );

    expect(result.isError).toBe(true);
    expect(result.content).toContain("file not found");
  });
});

describe("write_file", () => {
  test("writes to a new file and content matches", async () => {
    const dir = await makeTmpDir();
    const path = join(dir, "output.txt");
    const content = "hello, world";

    const result = await tools.run(
      { id: "w1", name: "write_file", arguments: { path, content } },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("bytes");

    const written = await readFile(path, "utf8");
    expect(written).toBe(content);
  });

  test("creates parent directories if they do not exist", async () => {
    const dir = await makeTmpDir();
    const path = join(dir, "deep", "nested", "dir", "file.txt");
    const content = "nested content";

    const result = await tools.run(
      { id: "w2", name: "write_file", arguments: { path, content } },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();

    const written = await readFile(path, "utf8");
    expect(written).toBe(content);
  });
});

describe("run_shell", () => {
  test("echo hello returns output containing hello", async () => {
    const result = await tools.run(
      { id: "s1", name: "run_shell", arguments: { command: "echo hello" } },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("hello");
  });

  test("stderr is captured in output", async () => {
    const result = await tools.run(
      {
        id: "s2",
        name: "run_shell",
        arguments: { command: "echo errout >&2" },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("errout");
  });

  test("non-zero exit code is reported in content", async () => {
    const result = await tools.run(
      { id: "s3", name: "run_shell", arguments: { command: "exit 42" } },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("42");
  });

  test("command that exceeds timeout returns error result", async () => {
    const result = await tools.run(
      {
        id: "s4",
        name: "run_shell",
        arguments: { command: "sleep 60", timeout: 100 },
      },
      neverAbort(),
    );

    expect(result.isError).toBe(true);
    expect(result.content).toContain("timed out");
  });

  test("aborting via signal kills the command", async () => {
    const controller = new AbortController();

    const promise = tools.run(
      {
        id: "s5",
        name: "run_shell",
        arguments: { command: "sleep 60", timeout: 30000 },
      },
      controller.signal,
    );

    setTimeout(() => controller.abort(), 50);

    const result = await promise;
    expect(result.isError).toBe(true);
    expect(result.content).toContain("aborted");
  });
});
