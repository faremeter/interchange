import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import { PackageJSON, isContainedEntryPath } from "./package-json";

function accepts(data: unknown): boolean {
  return !(PackageJSON(data) instanceof type.errors);
}

describe("PackageJSON interchange.credentials", () => {
  test("accepts a well-formed credential declaration", () => {
    expect(
      accepts({
        name: "tools-x",
        version: "1.0.0",
        interchange: {
          tools: "./dist/bundle.js",
          credentials: [
            { handle: "gh", scopes: ["repo"] },
            { handle: "stripe" },
          ],
        },
      }),
    ).toBe(true);
  });

  test("accepts a package with no credential declaration", () => {
    expect(
      accepts({
        name: "tools-x",
        version: "1.0.0",
        interchange: { tools: "./dist/bundle.js" },
      }),
    ).toBe(true);
  });

  test("rejects a malformed handle", () => {
    expect(
      accepts({
        name: "tools-x",
        version: "1.0.0",
        interchange: { credentials: [{ handle: "Bad Handle!" }] },
      }),
    ).toBe(false);
  });

  test("rejects non-array credentials", () => {
    expect(
      accepts({
        name: "tools-x",
        version: "1.0.0",
        interchange: { credentials: { handle: "gh" } },
      }),
    ).toBe(false);
  });

  test("rejects a non-string scope", () => {
    expect(
      accepts({
        name: "tools-x",
        version: "1.0.0",
        interchange: { credentials: [{ handle: "gh", scopes: [123] }] },
      }),
    ).toBe(false);
  });

  test("rejects a duplicate handle within one package", () => {
    expect(
      accepts({
        name: "tools-x",
        version: "1.0.0",
        interchange: { credentials: [{ handle: "gh" }, { handle: "gh" }] },
      }),
    ).toBe(false);
  });
});

describe("PackageJSON", () => {
  test("accepts a package with an interchange.tools entry", () => {
    const result = PackageJSON({
      name: "@intx/tools-posix",
      version: "1.2.3",
      interchange: { tools: "./dist/tools.js" },
    });
    expect(result instanceof type.errors).toBe(false);
    expect(result).toMatchObject({
      interchange: { tools: "./dist/tools.js" },
    });
  });

  test("accepts a package with an interchange.workflow entry", () => {
    const result = PackageJSON({
      name: "@intx/workflow-example",
      version: "1.2.3",
      interchange: { workflow: "./dist/wf.js" },
    });
    expect(result instanceof type.errors).toBe(false);
    expect(result).toMatchObject({
      interchange: { workflow: "./dist/wf.js" },
    });
  });

  test("accepts a package with both tools and workflow", () => {
    const result = PackageJSON({
      name: "@intx/combined",
      version: "1.2.3",
      interchange: { tools: "./dist/tools.js", workflow: "./dist/wf.js" },
    });
    expect(result instanceof type.errors).toBe(false);
    expect(result).toMatchObject({
      interchange: { tools: "./dist/tools.js", workflow: "./dist/wf.js" },
    });
  });

  test("accepts a package with neither tools nor workflow", () => {
    const result = PackageJSON({
      name: "left-pad",
      version: "1.3.0",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a package with an empty interchange object", () => {
    const result = PackageJSON({
      name: "left-pad",
      version: "1.3.0",
      interchange: {},
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a non-string interchange.workflow", () => {
    const result = PackageJSON({
      name: "@intx/workflow-example",
      version: "1.2.3",
      interchange: { workflow: 42 },
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a non-string interchange.tools", () => {
    const result = PackageJSON({
      name: "@intx/tools-posix",
      version: "1.2.3",
      interchange: { tools: 42 },
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("ignores undeclared keys alongside workflow", () => {
    const result = PackageJSON({
      name: "@intx/workflow-example",
      version: "1.2.3",
      description: "an upstream npm field",
      interchange: { workflow: "./dist/wf.js", extra: "passthrough" },
    });
    expect(result instanceof type.errors).toBe(false);
  });
});

describe("isContainedEntryPath", () => {
  test("accepts a package-relative entry", () => {
    expect(isContainedEntryPath("index.js")).toBe(true);
    expect(isContainedEntryPath("./dist/index.js")).toBe(true);
    expect(isContainedEntryPath("src/nested/wf.js")).toBe(true);
  });

  test("accepts an entry that normalizes to the package root", () => {
    expect(isContainedEntryPath(".")).toBe(true);
  });

  test("rejects a parent-traversal entry", () => {
    expect(isContainedEntryPath("../escape.js")).toBe(false);
    expect(isContainedEntryPath("dist/../../escape.js")).toBe(false);
  });

  test("rejects an absolute entry", () => {
    expect(isContainedEntryPath("/etc/passwd")).toBe(false);
  });

  test("rejects an absolute entry regardless of its leading segment", () => {
    expect(isContainedEntryPath("/pkg/index.js")).toBe(false);
    expect(isContainedEntryPath("/pkg")).toBe(false);
  });

  test("rejects a sibling that shares the root as a prefix", () => {
    expect(isContainedEntryPath("../pkg-sibling/index.js")).toBe(false);
  });
});
