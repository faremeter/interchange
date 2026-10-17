// Pure POSIX path helpers, with `join` also checked against
// `path.posix.join`.

import { describe, expect, test } from "bun:test";
import path from "node:path";

import { join, normalize, relative, resolve } from "./posix-path";

describe("posix-path", () => {
  test.each([
    [[], "."],
    [["/repo", ".git", "objects"], "/repo/.git/objects"],
    [["repo", "state", "..", "audit"], "repo/audit"],
    [["/", "repo", "//state"], "/repo/state"],
  ] as const)("join(%j)", (parts, expected) => {
    expect(join(...parts)).toBe(expected);
    expect(join(...parts)).toBe(path.posix.join(...parts));
  });

  test.each([
    ["/repo", "/repo/.git/objects", ".git/objects"],
    ["/repo/state", "/repo/audit", "../audit"],
    ["/repo", "/repo", ""],
    ["repo", "other/file", "../other/file"],
  ])("relative(%s, %s)", (from, to, expected) => {
    expect(relative(from, to)).toBe(expected);
  });

  test.each([
    ["/repo/../state", "/state"],
    ["repo/./state", "/repo/state"],
    ["/../../repo", "/repo"],
    ["", "/"],
  ])("resolve(%s)", (filepath, expected) => {
    expect(resolve(filepath)).toBe(expected);
  });

  test.each([
    ["/x/", "/x"],
    ["/x//y/./z", "/x/y/z"],
    ["/..", "/"],
    ["/a/../..", "/"],
    ["a/../..", ".."],
    ["../a", "../a"],
    ["..foo", "..foo"],
    ["./", "."],
    ["", "."],
  ])("normalize(%s)", (filepath, expected) => {
    expect(normalize(filepath)).toBe(expected);
  });
});
