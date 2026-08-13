import { describe, expect, test } from "bun:test";
import path from "node:path";

import { browserPath } from "./browser-path";

describe("browserPath", () => {
  test.each([
    [[], "."],
    [["/repo", ".git", "objects"], "/repo/.git/objects"],
    [["repo", "state", "..", "audit"], "repo/audit"],
    [["/", "repo", "//state"], "/repo/state"],
  ] as const)("join(%j)", (parts, expected) => {
    expect(browserPath.join(...parts)).toBe(expected);
    expect(browserPath.join(...parts)).toBe(path.posix.join(...parts));
  });

  test.each([
    ["/repo", "/repo/.git/objects", ".git/objects"],
    ["/repo/state", "/repo/audit", "../audit"],
    ["/repo", "/repo", ""],
    ["repo", "other/file", "../other/file"],
  ])("relative(%s, %s)", (from, to, expected) => {
    expect(browserPath.relative(from, to)).toBe(expected);
  });

  test.each([
    ["/repo/../state", "/state"],
    ["repo/./state", "/repo/state"],
    ["/../../repo", "/repo"],
    ["", "/"],
  ])("resolve(%s)", (filepath, expected) => {
    expect(browserPath.resolve(filepath)).toBe(expected);
  });
});
