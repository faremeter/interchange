import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import {
  mkdtemp,
  mkdir,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { realpathSync } from "node:fs";
import { createPosixTools, composeMiddleware } from "./index";
import type { PosixTools, ToolHandler, ToolPlugin } from "./index";
import { matchGlob, shouldSkip } from "./glob-match";

let tmpDir: string;
let tools: PosixTools;

beforeAll(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), "tools-posix-test-"));
  tools = createPosixTools({ cwd: tmpDir });
});

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
    const path = join(tmpDir, "hello.txt");
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
    const path = join(tmpDir, "numbered.txt");
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
    const path = join(tmpDir, "output.txt");
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
    const path = join(tmpDir, "deep", "nested", "dir", "file.txt");
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

describe("edit_file", () => {
  test("replaces a unique string successfully", async () => {
    const path = join(tmpDir, "edit-test.txt");
    await writeFile(path, "hello world\nfoo bar\nbaz qux");

    const result = await tools.run(
      {
        id: "e1",
        name: "edit_file",
        arguments: { path, old_string: "foo bar", new_string: "replaced" },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("replaced 1 occurrence");

    const content = await readFile(path, "utf8");
    expect(content).toBe("hello world\nreplaced\nbaz qux");
  });

  test("fails when old_string is not found", async () => {
    const path = join(tmpDir, "edit-nf.txt");
    await writeFile(path, "hello world");

    const result = await tools.run(
      {
        id: "e2",
        name: "edit_file",
        arguments: { path, old_string: "missing", new_string: "x" },
      },
      neverAbort(),
    );

    expect(result.isError).toBe(true);
    expect(result.content).toContain("not found");
  });

  test("fails when old_string has multiple occurrences without replace_all", async () => {
    const path = join(tmpDir, "edit-dup.txt");
    await writeFile(path, "aaa bbb aaa ccc aaa");

    const result = await tools.run(
      {
        id: "e3",
        name: "edit_file",
        arguments: { path, old_string: "aaa", new_string: "x" },
      },
      neverAbort(),
    );

    expect(result.isError).toBe(true);
    expect(result.content).toContain("not unique");
    expect(result.content).toContain("3 occurrences");
  });

  test("replace_all replaces all occurrences", async () => {
    const path = join(tmpDir, "edit-all.txt");
    await writeFile(path, "aaa bbb aaa ccc aaa");

    const result = await tools.run(
      {
        id: "e4",
        name: "edit_file",
        arguments: {
          path,
          old_string: "aaa",
          new_string: "x",
          replace_all: true,
        },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("replaced 3 occurrence");

    const content = await readFile(path, "utf8");
    expect(content).toBe("x bbb x ccc x");
  });

  test("fails on non-existent file", async () => {
    const result = await tools.run(
      {
        id: "e5",
        name: "edit_file",
        arguments: {
          path: "/nonexistent/edit.txt",
          old_string: "a",
          new_string: "b",
        },
      },
      neverAbort(),
    );

    expect(result.isError).toBe(true);
    expect(result.content).toContain("file not found");
  });

  test("handles empty new_string (deletion)", async () => {
    const path = join(tmpDir, "edit-del.txt");
    await writeFile(path, "keep this remove_me keep this too");

    const result = await tools.run(
      {
        id: "e6",
        name: "edit_file",
        arguments: { path, old_string: " remove_me", new_string: "" },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();

    const content = await readFile(path, "utf8");
    expect(content).toBe("keep this keep this too");
  });

  test("fails with empty old_string", async () => {
    const path = join(tmpDir, "edit-empty.txt");
    await writeFile(path, "anything");

    const result = await tools.run(
      {
        id: "e7",
        name: "edit_file",
        arguments: { path, old_string: "", new_string: "x" },
      },
      neverAbort(),
    );

    expect(result.isError).toBe(true);
    expect(result.content).toContain("must not be empty");
  });

  test("new_string with $-patterns is treated literally", async () => {
    const path = join(tmpDir, "edit-dollar.txt");
    await writeFile(path, "const x = PLACEHOLDER;");

    const result = await tools.run(
      {
        id: "e8",
        name: "edit_file",
        arguments: {
          path,
          old_string: "PLACEHOLDER",
          new_string: "$& is not $' special",
        },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();

    const content = await readFile(path, "utf8");
    expect(content).toBe("const x = $& is not $' special;");
  });
});

describe("search_files", () => {
  test("finds files matching a simple pattern", async () => {
    const searchDir = join(tmpDir, "search-simple");
    await mkdir(searchDir);
    await writeFile(join(searchDir, "a.ts"), "");
    await writeFile(join(searchDir, "b.ts"), "");
    await writeFile(join(searchDir, "c.json"), "");

    const result = await tools.run(
      {
        id: "sf1",
        name: "search_files",
        arguments: { pattern: "*.ts", path: searchDir },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("a.ts");
    expect(result.content).toContain("b.ts");
    expect(result.content).not.toContain("c.json");
  });

  test("recursive search with ** pattern", async () => {
    const searchDir = join(tmpDir, "search-recursive");
    await mkdir(join(searchDir, "sub"), { recursive: true });
    await writeFile(join(searchDir, "root.ts"), "");
    await writeFile(join(searchDir, "sub", "nested.ts"), "");
    await writeFile(join(searchDir, "sub", "other.json"), "");

    const result = await tools.run(
      {
        id: "sf2",
        name: "search_files",
        arguments: { pattern: "**/*.ts", path: searchDir },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("nested.ts");
    expect(result.content).not.toContain("other.json");
  });

  test("returns no-match message when nothing matches", async () => {
    const searchDir = join(tmpDir, "search-empty");
    await mkdir(searchDir);
    await writeFile(join(searchDir, "file.txt"), "");

    const result = await tools.run(
      {
        id: "sf3",
        name: "search_files",
        arguments: { pattern: "*.rs", path: searchDir },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("no files matching");
  });

  test("respects max_results", async () => {
    const searchDir = join(tmpDir, "search-limit");
    await mkdir(searchDir);
    for (let i = 0; i < 5; i++) {
      await writeFile(join(searchDir, `file${i}.ts`), "");
    }

    const result = await tools.run(
      {
        id: "sf4",
        name: "search_files",
        arguments: { pattern: "*.ts", path: searchDir, max_results: 2 },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(typeof result.content).toBe("string");
    const lines = String(result.content).split("\n");
    const fileLines = lines.filter((l) => l.endsWith(".ts"));
    expect(fileLines).toHaveLength(2);
    expect(result.content).toContain("2 of 5 matches shown");
  });

  test("fails on non-existent directory", async () => {
    const result = await tools.run(
      {
        id: "sf5",
        name: "search_files",
        arguments: { pattern: "*.ts", path: "/nonexistent/dir" },
      },
      neverAbort(),
    );

    expect(result.isError).toBe(true);
    expect(result.content).toContain("not found");
  });

  test("** pattern matches files at root and nested levels", async () => {
    const searchDir = join(tmpDir, "search-depth");
    await mkdir(join(searchDir, "sub"), { recursive: true });
    await writeFile(join(searchDir, "root.ts"), "");
    await writeFile(join(searchDir, "sub", "nested.ts"), "");

    const result = await tools.run(
      {
        id: "sf6",
        name: "search_files",
        arguments: { pattern: "**/*.ts", path: searchDir },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("root.ts");
    expect(result.content).toContain("nested.ts");
  });

  test("skips node_modules directories", async () => {
    const searchDir = join(tmpDir, "search-skip");
    await mkdir(join(searchDir, "src"), { recursive: true });
    await mkdir(join(searchDir, "node_modules", "pkg"), { recursive: true });
    await writeFile(join(searchDir, "src", "app.ts"), "");
    await writeFile(join(searchDir, "node_modules", "pkg", "index.ts"), "");

    const result = await tools.run(
      {
        id: "sf7",
        name: "search_files",
        arguments: { pattern: "**/*.ts", path: searchDir },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("app.ts");
    expect(result.content).not.toContain("node_modules");
  });

  test("does not return directory entries", async () => {
    const searchDir = join(tmpDir, "search-nodir");
    await mkdir(join(searchDir, "looks-like.ts"), { recursive: true });
    await writeFile(join(searchDir, "real.ts"), "");

    const result = await tools.run(
      {
        id: "sf8",
        name: "search_files",
        arguments: { pattern: "*.ts", path: searchDir },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("real.ts");
    expect(result.content).not.toContain("looks-like.ts");
  });

  test("includes symlinked files", async () => {
    const searchDir = join(tmpDir, "search-symlink");
    await mkdir(searchDir);
    await writeFile(join(searchDir, "real.ts"), "content");
    await symlink(join(searchDir, "real.ts"), join(searchDir, "link.ts"));

    const result = await tools.run(
      {
        id: "sf9",
        name: "search_files",
        arguments: { pattern: "*.ts", path: searchDir },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("real.ts");
    expect(result.content).toContain("link.ts");
  });
});

describe("grep", () => {
  test("finds matching lines in a file", async () => {
    const path = join(tmpDir, "grep-test.txt");
    await writeFile(path, "alpha\nbeta\ngamma\ndelta\n");

    const result = await tools.run(
      {
        id: "g1",
        name: "grep",
        arguments: { pattern: "beta", path },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain(":2:");
    expect(result.content).toContain("beta");
  });

  test("supports regex patterns", async () => {
    const path = join(tmpDir, "grep-regex.txt");
    await writeFile(path, "no match\nline 42 here\nanother\n100 items\n");

    const result = await tools.run(
      {
        id: "g2",
        name: "grep",
        arguments: { pattern: "\\d+", path },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("42");
    expect(result.content).toContain("100");
    expect(result.content).not.toContain("no match");
  });

  test("reports no matches cleanly", async () => {
    const path = join(tmpDir, "grep-nomatch.txt");
    await writeFile(path, "nothing here\n");

    const result = await tools.run(
      {
        id: "g3",
        name: "grep",
        arguments: { pattern: "zzz", path },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("no matches");
  });

  test("respects glob filter", async () => {
    const grepDir = join(tmpDir, "grep-glob");
    await mkdir(grepDir);
    await writeFile(join(grepDir, "code.ts"), "findme\n");
    await writeFile(join(grepDir, "data.json"), "findme\n");

    const result = await tools.run(
      {
        id: "g4",
        name: "grep",
        arguments: { pattern: "findme", path: grepDir, glob: "*.ts" },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("code.ts");
    expect(result.content).not.toContain("data.json");
  });

  test("context lines work", async () => {
    const path = join(tmpDir, "grep-ctx.txt");
    await writeFile(path, "aaa\nbbb\nccc\nddd\neee\n");

    const result = await tools.run(
      {
        id: "g5",
        name: "grep",
        arguments: { pattern: "ccc", path, context: 1 },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("bbb");
    expect(result.content).toContain("ccc");
    expect(result.content).toContain("ddd");
  });

  test("fails on invalid regex", async () => {
    const path = join(tmpDir, "grep-badregex.txt");
    await writeFile(path, "anything\n");

    const result = await tools.run(
      {
        id: "g6",
        name: "grep",
        arguments: { pattern: "[invalid", path },
      },
      neverAbort(),
    );

    expect(result.isError).toBe(true);
    expect(result.content).toContain("invalid regex");
  });

  test("respects max_results", async () => {
    const path = join(tmpDir, "grep-limit.txt");
    const lines = Array.from({ length: 20 }, (_, i) => `match${i}`).join("\n");
    await writeFile(path, lines);

    const result = await tools.run(
      {
        id: "g7",
        name: "grep",
        arguments: { pattern: "match", path, max_results: 3 },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(typeof result.content).toBe("string");
    const matchLines = String(result.content)
      .split("\n")
      .filter((l) => l.includes(":"));
    expect(matchLines).toHaveLength(3);
    expect(result.content).toContain("3 of 20 matches shown");
  });

  test("searches a single file when path is a file", async () => {
    const path = join(tmpDir, "grep-single.txt");
    await writeFile(path, "target line\nother\n");

    const result = await tools.run(
      {
        id: "g8",
        name: "grep",
        arguments: { pattern: "target", path },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("target line");
    expect(result.content).toContain(":1:");
  });

  test("silently skips binary files in directory search", async () => {
    const grepDir = join(tmpDir, "grep-binary");
    await mkdir(grepDir);
    await writeFile(join(grepDir, "text.txt"), "findme\n");
    await writeFile(join(grepDir, "binary.bin"), Buffer.from([0x00, 0x01]));

    const result = await tools.run(
      {
        id: "g9",
        name: "grep",
        arguments: { pattern: "findme", path: grepDir },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(result.content).toContain("text.txt");
    expect(result.content).not.toContain("binary.bin");
  });

  test("adjacent matches do not duplicate context lines", async () => {
    const path = join(tmpDir, "grep-dedup.txt");
    await writeFile(path, "aaa\nMATCH\nbbb\nMATCH\nccc\n");

    const result = await tools.run(
      {
        id: "g10",
        name: "grep",
        arguments: { pattern: "MATCH", path, context: 1 },
      },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    const output = String(result.content);
    const bbbCount = output.split("bbb").length - 1;
    expect(bbbCount).toBe(1);
  });
});

describe("globToRegex", () => {
  test("* matches within a single segment", () => {
    expect(matchGlob("*.ts", "foo.ts")).toBe(true);
    expect(matchGlob("*.ts", "bar.json")).toBe(false);
    expect(matchGlob("*.ts", "sub/foo.ts")).toBe(false);
  });

  test("** matches zero or more path segments", () => {
    expect(matchGlob("**/*.ts", "root.ts")).toBe(true);
    expect(matchGlob("**/*.ts", "sub/nested.ts")).toBe(true);
    expect(matchGlob("**/*.ts", "a/b/c/deep.ts")).toBe(true);
    expect(matchGlob("**/*.ts", "foo.json")).toBe(false);
  });

  test("** at end matches everything remaining", () => {
    expect(matchGlob("src/**", "src/foo.ts")).toBe(true);
    expect(matchGlob("src/**", "src/a/b/c.ts")).toBe(true);
    expect(matchGlob("src/**", "lib/foo.ts")).toBe(false);
  });

  test("standalone ** matches any path", () => {
    expect(matchGlob("**", "anything")).toBe(true);
    expect(matchGlob("**", "a/b/c")).toBe(true);
  });

  test("** in the middle matches intermediate segments", () => {
    expect(matchGlob("src/**/test/*.ts", "src/test/foo.ts")).toBe(true);
    expect(matchGlob("src/**/test/*.ts", "src/a/b/test/foo.ts")).toBe(true);
    expect(matchGlob("src/**/test/*.ts", "src/a/b/test/foo.json")).toBe(false);
  });

  test("? matches exactly one character", () => {
    expect(matchGlob("?.ts", "a.ts")).toBe(true);
    expect(matchGlob("?.ts", "ab.ts")).toBe(false);
    expect(matchGlob("?.ts", ".ts")).toBe(false);
  });

  test("literal dots are not regex wildcards", () => {
    expect(matchGlob("file.txt", "file.txt")).toBe(true);
    expect(matchGlob("file.txt", "filextxt")).toBe(false);
  });

  test("compound extensions work", () => {
    expect(matchGlob("*.test.ts", "foo.test.ts")).toBe(true);
    expect(matchGlob("*.test.ts", "foo.ts")).toBe(false);
    expect(matchGlob("**/*.test.ts", "src/a/foo.test.ts")).toBe(true);
  });

  test("brace expansion throws a clear error", () => {
    expect(() => matchGlob("*.{ts,tsx}", "foo.ts")).toThrow(
      "brace expansion is not supported",
    );
    expect(() => matchGlob("**/*.{js,jsx}", "a/b.js")).toThrow(
      "brace expansion is not supported",
    );
  });
});

describe("shouldSkip", () => {
  test("skips node_modules at any depth", () => {
    expect(shouldSkip("node_modules/foo")).toBe(true);
    expect(shouldSkip("src/node_modules/bar")).toBe(true);
  });

  test("skips .git at any depth", () => {
    expect(shouldSkip(".git/objects")).toBe(true);
    expect(shouldSkip("sub/.git/config")).toBe(true);
  });

  test("does not skip normal paths", () => {
    expect(shouldSkip("src/foo.ts")).toBe(false);
    expect(shouldSkip("lib/utils/helper.ts")).toBe(false);
  });
});

describe("composeMiddleware", () => {
  const signal = new AbortController().signal;

  test("empty middleware array returns base handler unchanged", async () => {
    const base: ToolHandler = async (call) => ({
      callId: call.id,
      content: "base",
    });
    const composed = composeMiddleware([], base);

    const result = await composed(
      { id: "t1", name: "test", arguments: {} },
      signal,
    );
    expect(result.content).toBe("base");
  });

  test("first plugin is outermost in the chain", async () => {
    const order: string[] = [];

    const outer =
      (next: ToolHandler): ToolHandler =>
      async (call, sig) => {
        order.push("outer-before");
        const result = await next(call, sig);
        order.push("outer-after");
        return {
          ...result,
          content: `outer(${result.content})`,
        };
      };

    const inner =
      (next: ToolHandler): ToolHandler =>
      async (call, sig) => {
        order.push("inner-before");
        const result = await next(call, sig);
        order.push("inner-after");
        return {
          ...result,
          content: `inner(${result.content})`,
        };
      };

    const base: ToolHandler = async (call) => {
      order.push("base");
      return { callId: call.id, content: "base" };
    };

    const composed = composeMiddleware([outer, inner], base);
    const result = await composed(
      { id: "t2", name: "test", arguments: {} },
      signal,
    );

    expect(result.content).toBe("outer(inner(base))");
    expect(order).toEqual([
      "outer-before",
      "inner-before",
      "base",
      "inner-after",
      "outer-after",
    ]);
  });

  test("middleware can short-circuit by not calling next", async () => {
    const blocker =
      (_next: ToolHandler): ToolHandler =>
      async (call) => ({
        callId: call.id,
        content: "blocked",
        isError: true,
      });

    const base: ToolHandler = async () => {
      throw new Error("should not be reached");
    };

    const composed = composeMiddleware([blocker], base);
    const result = await composed(
      { id: "t3", name: "test", arguments: {} },
      signal,
    );

    expect(result.content).toBe("blocked");
    expect(result.isError).toBe(true);
  });
});

describe("plugin wiring", () => {
  test("plugin tools are included in definitions", () => {
    const plugin: ToolPlugin = {
      tools: [
        {
          definition: {
            name: "custom_tool",
            description: "A custom tool",
            inputSchema: { type: "object", properties: {}, required: [] },
          },
          handler: async (call) => ({ callId: call.id, content: "custom" }),
        },
      ],
    };

    const pt = createPosixTools({ cwd: tmpDir, plugins: [plugin] });
    const names = pt.definitions.map((d) => d.name);
    expect(names).toContain("custom_tool");
    expect(names).toContain("read_file");
  });

  test("plugin tool handler is callable", async () => {
    const plugin: ToolPlugin = {
      tools: [
        {
          definition: {
            name: "echo_tool",
            description: "Echoes input",
            inputSchema: { type: "object", properties: {}, required: [] },
          },
          handler: async (call) => ({
            callId: call.id,
            content: `echo: ${String(call.arguments.msg)}`,
          }),
        },
      ],
    };

    const pt = createPosixTools({ cwd: tmpDir, plugins: [plugin] });
    const result = await pt.run(
      { id: "p1", name: "echo_tool", arguments: { msg: "hello" } },
      neverAbort(),
    );

    expect(result.content).toBe("echo: hello");
  });

  test("duplicate tool name throws", () => {
    const plugin: ToolPlugin = {
      tools: [
        {
          definition: {
            name: "read_file",
            description: "Duplicate",
            inputSchema: { type: "object", properties: {}, required: [] },
          },
          handler: async (call) => ({ callId: call.id, content: "" }),
        },
      ],
    };

    expect(() => createPosixTools({ cwd: tmpDir, plugins: [plugin] })).toThrow(
      "already registered",
    );
  });

  test("dispose runs disposers in reverse order", async () => {
    const order: string[] = [];
    const pluginA: ToolPlugin = {
      dispose: async () => {
        order.push("a");
      },
    };
    const pluginB: ToolPlugin = {
      dispose: async () => {
        order.push("b");
      },
    };

    const pt = createPosixTools({
      cwd: tmpDir,
      plugins: [pluginA, pluginB],
    });
    await pt.dispose();

    expect(order).toEqual(["b", "a"]);
  });

  test("dispose throws AggregateError when callbacks fail", async () => {
    const plugin: ToolPlugin = {
      dispose: async () => {
        throw new Error("boom");
      },
    };

    const pt = createPosixTools({ cwd: tmpDir, plugins: [plugin] });

    await expect(pt.dispose()).rejects.toThrow(AggregateError);
  });

  test("middleware exception is caught and returned as error result", async () => {
    const plugin: ToolPlugin = {
      middleware: (_next) => async () => {
        throw new Error("middleware explosion");
      },
    };

    const pt = createPosixTools({ cwd: tmpDir, plugins: [plugin] });
    const result = await pt.run(
      { id: "mx", name: "read_file", arguments: { path: "/dev/null" } },
      neverAbort(),
    );

    expect(result.isError).toBe(true);
    expect(result.content).toContain("middleware explosion");
  });

  test("double dispose is a no-op", async () => {
    let count = 0;
    const plugin: ToolPlugin = {
      dispose: async () => {
        count++;
      },
    };

    const pt = createPosixTools({ cwd: tmpDir, plugins: [plugin] });
    await pt.dispose();
    await pt.dispose();

    expect(count).toBe(1);
  });

  test("run_shell executes in the configured cwd", async () => {
    const pt = createPosixTools({ cwd: tmpDir });
    const result = await pt.run(
      { id: "cwd1", name: "run_shell", arguments: { command: "pwd" } },
      neverAbort(),
    );

    expect(result.isError).toBeFalsy();
    expect(String(result.content).trim()).toBe(realpathSync(tmpDir));
  });

  test("invalid cwd throws at construction", () => {
    expect(() => createPosixTools({ cwd: "/nonexistent/path" })).toThrow(
      "cwd does not exist",
    );
  });
});
