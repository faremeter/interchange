import { describe, test, expect } from "bun:test";
import { glob } from "./glob";

describe("glob.match literal patterns", () => {
  test("exact match returns true", () => {
    expect(glob.match("refs/heads/main", "refs/heads/main")).toBe(true);
  });

  test("non-match returns false", () => {
    expect(glob.match("refs/heads/main", "refs/heads/dev")).toBe(false);
  });

  test("empty pattern matches empty input", () => {
    expect(glob.match("", "")).toBe(true);
    expect(glob.match("", "anything")).toBe(false);
  });
});

describe("glob.match single-star segment patterns", () => {
  test("* matches a single segment", () => {
    expect(glob.match("refs/heads/*", "refs/heads/main")).toBe(true);
    expect(glob.match("refs/heads/*", "refs/heads/dev")).toBe(true);
  });

  test("* does not cross /", () => {
    expect(glob.match("refs/heads/*", "refs/heads/feature/foo")).toBe(false);
  });

  test("* mid-segment matches the rest of the segment", () => {
    expect(glob.match("refs/heads/feat-*", "refs/heads/feat-x")).toBe(true);
    expect(glob.match("refs/heads/feat-*", "refs/heads/feat-")).toBe(true);
    expect(glob.match("refs/heads/feat-*", "refs/heads/feature-y/foo")).toBe(
      false,
    );
  });

  test("* matches empty segment content", () => {
    expect(glob.match("refs/heads/*", "refs/heads/")).toBe(true);
  });

  test("multiple single-stars in one pattern", () => {
    expect(glob.match("refs/*/main", "refs/heads/main")).toBe(true);
    expect(glob.match("refs/*/main", "refs/tags/main")).toBe(true);
    expect(glob.match("refs/*/main", "refs/main")).toBe(false);
  });
});

describe("glob.match double-star patterns", () => {
  test("** matches across /", () => {
    expect(glob.match("refs/**", "refs/heads/main")).toBe(true);
    expect(glob.match("refs/**", "refs/heads/feature/foo/bar")).toBe(true);
  });

  test("** matches zero segments", () => {
    expect(glob.match("refs/**", "refs/")).toBe(true);
  });

  test("** in the middle", () => {
    expect(glob.match("refs/**/main", "refs/heads/main")).toBe(true);
    expect(glob.match("refs/**/main", "refs/heads/feature/main")).toBe(true);
    expect(glob.match("refs/**/main", "refs/main")).toBe(false);
  });

  test("standalone ** at top level matches anything", () => {
    expect(glob.match("**", "refs/heads/main")).toBe(true);
    expect(glob.match("**", "")).toBe(true);
  });
});

describe("glob.match truth table", () => {
  const cases: [string, string, boolean][] = [
    ["refs/heads/main", "refs/heads/main", true],
    ["refs/heads/main", "refs/heads/dev", false],
    ["refs/heads/*", "refs/heads/main", true],
    ["refs/heads/*", "refs/heads/feature/x", false],
    ["refs/*/main", "refs/heads/main", true],
    ["refs/*/main", "refs/heads/feature/main", false],
    ["refs/**", "refs/heads/main", true],
    ["refs/**", "refs/heads/feature/x", true],
    ["refs/**/main", "refs/heads/main", true],
    ["refs/**/main", "refs/main", false],
    ["**", "anything/at/all", true],
    ["refs/heads/feat-*", "refs/heads/feat-x", true],
    ["refs/heads/feat-*", "refs/heads/release-x", false],
    ["refs/heads/*-prod", "refs/heads/main-prod", true],
    ["refs/heads/*-prod", "refs/heads/feature/main-prod", false],
    ["refs/tags/v*", "refs/tags/v1", true],
    ["refs/tags/v*", "refs/tags/v1.2.3", true],
    ["refs/tags/v*", "refs/tags/release", false],
    ["a/b/c", "a/b", false],
    ["a/b", "a/b/c", false],
  ];

  for (const [pattern, input, expected] of cases) {
    test(`match("${pattern}", "${input}") === ${expected}`, () => {
      expect(glob.match(pattern, input)).toBe(expected);
    });
  }
});
