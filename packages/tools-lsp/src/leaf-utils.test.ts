import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  type Diagnostic,
  type Range,
  DiagnosticSeverity,
} from "vscode-languageserver-types";
import { pretty, report } from "./diagnostic";
import { containsPath, type LSPContext } from "./context";
import { languageId } from "./language";
import { nearestRoot, exists, type NearestRootContext } from "./fs";

function makeDiagnostic(
  line: number,
  col: number,
  message: string,
  severity?: DiagnosticSeverity,
): Diagnostic {
  const range: Range = {
    start: { line, character: col },
    end: { line, character: col },
  };
  return severity !== undefined
    ? { range, message, severity }
    : { range, message };
}

describe("diagnostic", () => {
  describe("pretty", () => {
    test("formats an error diagnostic", () => {
      const d = makeDiagnostic(0, 4, "Type error", DiagnosticSeverity.Error);
      expect(pretty(d)).toBe("ERROR [1:5] Type error");
    });

    test("formats a warning diagnostic", () => {
      const d = makeDiagnostic(
        9,
        0,
        "Unused variable",
        DiagnosticSeverity.Warning,
      );
      expect(pretty(d)).toBe("WARN [10:1] Unused variable");
    });

    test("defaults to ERROR when severity is omitted", () => {
      const d = makeDiagnostic(0, 0, "Something wrong");
      expect(pretty(d)).toBe("ERROR [1:1] Something wrong");
    });
  });

  describe("report", () => {
    test("returns empty string when no diagnostics match severity", () => {
      const d = makeDiagnostic(0, 0, "A warning", DiagnosticSeverity.Warning);
      const result = report("file.ts", [d], 1);
      expect(result).toBe("");
    });

    test("includes diagnostics at or below minSeverity", () => {
      const err = makeDiagnostic(0, 0, "An error", DiagnosticSeverity.Error);
      const warn = makeDiagnostic(
        1,
        0,
        "A warning",
        DiagnosticSeverity.Warning,
      );
      const result = report("file.ts", [err, warn], 2);
      expect(result).toContain("An error");
      expect(result).toContain("A warning");
      expect(result).toContain('<diagnostics file="file.ts">');
      expect(result).toContain("</diagnostics>");
    });

    test("caps at 20 diagnostics per file", () => {
      const diagnostics = Array.from({ length: 25 }, (_, i) =>
        makeDiagnostic(i, 0, `Error ${i}`, DiagnosticSeverity.Error),
      );
      const result = report("file.ts", diagnostics, 1);
      expect(result).toContain("Error 19");
      expect(result).not.toContain("Error 20");
      expect(result).toContain("... and 5 more");
    });

    test("returns empty string for empty diagnostics array", () => {
      expect(report("file.ts", [])).toBe("");
    });
  });
});

describe("containsPath", () => {
  const ctx: LSPContext = {
    cwd: "/project/workspace",
    worktree: "/project",
  };

  test("file inside cwd is contained", () => {
    expect(containsPath("/project/workspace/src/file.ts", ctx)).toBe(true);
  });

  test("file at cwd root is contained", () => {
    expect(containsPath("/project/workspace", ctx)).toBe(true);
  });

  test("file inside worktree but outside cwd is contained", () => {
    expect(containsPath("/project/other/file.ts", ctx)).toBe(true);
  });

  test("file outside both is not contained", () => {
    expect(containsPath("/other/file.ts", ctx)).toBe(false);
  });

  test("worktree = / rejects everything outside cwd", () => {
    const rootCtx: LSPContext = { cwd: "/project", worktree: "/" };
    expect(containsPath("/project/file.ts", rootCtx)).toBe(true);
    expect(containsPath("/other/file.ts", rootCtx)).toBe(false);
  });

  test("prefix collision does not match", () => {
    const narrowCtx: LSPContext = {
      cwd: "/project/workspace",
      worktree: "/project/workspace",
    };
    expect(containsPath("/project/workspacefoo/file.ts", narrowCtx)).toBe(
      false,
    );
  });
});

describe("languageId", () => {
  test("maps TypeScript extensions", () => {
    expect(languageId(".ts")).toBe("typescript");
    expect(languageId(".tsx")).toBe("typescriptreact");
    expect(languageId(".mts")).toBe("typescript");
  });

  test("maps JavaScript extensions", () => {
    expect(languageId(".js")).toBe("javascript");
    expect(languageId(".jsx")).toBe("javascriptreact");
    expect(languageId(".mjs")).toBe("javascript");
  });

  test("returns plaintext for unknown extensions", () => {
    expect(languageId(".xyz")).toBe("plaintext");
    expect(languageId(".custom")).toBe("plaintext");
  });
});

describe("nearestRoot", () => {
  let tmpDir: string;

  beforeAll(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "lsp-test-root-"));
    await mkdir(join(tmpDir, "project", "packages", "a"), { recursive: true });
    await writeFile(join(tmpDir, "project", "bun.lock"), "");
    await writeFile(
      join(tmpDir, "project", "packages", "a", "package.json"),
      "{}",
    );
  });

  afterAll(async () => {
    if (tmpDir !== undefined) {
      await rm(tmpDir, { recursive: true, force: true });
    }
  });

  test("finds the nearest lock file", async () => {
    const findRoot = nearestRoot(["bun.lock", "package-lock.json"]);
    const ctx: NearestRootContext = {
      directory: join(tmpDir, "project"),
    };
    const root = await findRoot(
      join(tmpDir, "project", "packages", "a", "index.ts"),
      ctx,
    );
    expect(root).toBe(join(tmpDir, "project"));
  });

  test("returns directory when no lock file is found", async () => {
    const findRoot = nearestRoot(["nonexistent.lock"]);
    const ctx: NearestRootContext = {
      directory: join(tmpDir, "project"),
    };
    const root = await findRoot(
      join(tmpDir, "project", "packages", "a", "index.ts"),
      ctx,
    );
    expect(root).toBe(join(tmpDir, "project"));
  });

  test("excludes directories with exclude marker", async () => {
    await writeFile(
      join(tmpDir, "project", "packages", "a", "deno.json"),
      "{}",
    );
    const findRoot = nearestRoot(["bun.lock"], ["deno.json"]);
    const ctx: NearestRootContext = {
      directory: join(tmpDir, "project"),
    };
    const root = await findRoot(
      join(tmpDir, "project", "packages", "a", "index.ts"),
      ctx,
    );
    expect(root).toBeUndefined();
  });
});

describe("exists", () => {
  test("returns true for an existing file", async () => {
    expect(await exists("/dev/null")).toBe(true);
  });

  test("returns false for a nonexistent path", async () => {
    expect(await exists("/nonexistent/path/file.txt")).toBe(false);
  });
});
