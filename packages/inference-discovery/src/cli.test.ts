import { describe, test, expect } from "bun:test";
import { parseCLI } from "./cli";

describe("parseCLI", () => {
  test("returns help for --help", () => {
    const result = parseCLI(["--help"]);
    expect(result.kind).toBe("help");
    if (result.kind === "help") {
      expect(result.message).toMatch(/Usage: discover/);
    }
  });

  test("returns help for -h", () => {
    expect(parseCLI(["-h"]).kind).toBe("help");
  });

  test("returns error when --provider missing", () => {
    const result = parseCLI(["--all"]);
    expect(result.kind).toBe("error");
    if (result.kind === "error") {
      expect(result.message).toMatch(/--provider/);
    }
  });

  test("returns run for --provider X --all", () => {
    const result = parseCLI(["--provider", "google-genai", "--all"]);
    expect(result.kind).toBe("run");
    if (result.kind === "run") {
      expect(result.provider).toBe("google-genai");
      expect(result.all).toBe(true);
      expect(result.models).toEqual([]);
      expect(result.capabilities).toEqual([]);
    }
  });

  test("collects repeated --model flags", () => {
    const result = parseCLI([
      "--provider",
      "p",
      "--model",
      "m1",
      "--model",
      "m2",
    ]);
    expect(result.kind).toBe("run");
    if (result.kind === "run") {
      expect(result.models).toEqual(["m1", "m2"]);
    }
  });

  test("collects repeated --only flags", () => {
    const result = parseCLI([
      "--provider",
      "p",
      "--only",
      "plain-text",
      "--only",
      "plain-text-streaming",
    ]);
    expect(result.kind).toBe("run");
    if (result.kind === "run") {
      expect(result.capabilities).toEqual([
        "plain-text",
        "plain-text-streaming",
      ]);
    }
  });

  test("rejects --all combined with --model", () => {
    const result = parseCLI(["--provider", "p", "--all", "--model", "m"]);
    expect(result.kind).toBe("error");
    if (result.kind === "error") {
      expect(result.message).toMatch(/mutually exclusive/);
    }
  });

  test("rejects --all combined with --only", () => {
    const result = parseCLI([
      "--provider",
      "p",
      "--all",
      "--only",
      "plain-text",
    ]);
    expect(result.kind).toBe("error");
  });

  test("rejects no scope flags without --all", () => {
    const result = parseCLI(["--provider", "p"]);
    expect(result.kind).toBe("error");
    if (result.kind === "error") {
      expect(result.message).toMatch(/--all|--model|--only/);
    }
  });

  test("rejects unknown flag", () => {
    const result = parseCLI(["--provider", "p", "--bogus"]);
    expect(result.kind).toBe("error");
    if (result.kind === "error") {
      expect(result.message).toMatch(/Unknown argument: --bogus/);
    }
  });

  test("rejects --provider without value", () => {
    const result = parseCLI(["--provider"]);
    expect(result.kind).toBe("error");
    if (result.kind === "error") {
      expect(result.message).toMatch(/requires a value/);
    }
  });

  test("rejects duplicate --provider", () => {
    const result = parseCLI(["--provider", "a", "--provider", "b", "--all"]);
    expect(result.kind).toBe("error");
    if (result.kind === "error") {
      expect(result.message).toMatch(/only be specified once/);
    }
  });

  test("rejects --model without value", () => {
    const result = parseCLI(["--provider", "p", "--model"]);
    expect(result.kind).toBe("error");
  });

  test("rejects --model followed by another flag as value", () => {
    const result = parseCLI(["--provider", "p", "--model", "--all"]);
    expect(result.kind).toBe("error");
  });
});
