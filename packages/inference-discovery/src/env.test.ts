import { describe, test, expect } from "bun:test";
import { requireEnv, requireEnvSet } from "./env";

describe("requireEnv", () => {
  test("returns the value when set", () => {
    expect(requireEnv("FOO", { FOO: "bar" })).toBe("bar");
  });

  test("throws when missing", () => {
    expect(() => requireEnv("FOO", {})).toThrow(/FOO/);
  });

  test("throws when empty", () => {
    expect(() => requireEnv("FOO", { FOO: "" })).toThrow(/FOO/);
  });
});

describe("requireEnvSet", () => {
  test("returns all values when all set", () => {
    const result = requireEnvSet(["A", "B"], { A: "1", B: "2", C: "3" });
    expect(result).toEqual({ A: "1", B: "2" });
  });

  test("throws listing all missing names", () => {
    expect(() => requireEnvSet(["A", "B", "C"], { B: "2" })).toThrow(/A, C/);
  });

  test("treats empty values as missing", () => {
    expect(() => requireEnvSet(["A"], { A: "" })).toThrow(/A/);
  });
});
