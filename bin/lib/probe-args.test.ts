import { describe, expect, test } from "bun:test";
import { CAPABILITIES } from "@intx/inference-discovery/catalog";
import { parseProbeArgs, type ParsedProbeArgs } from "./probe-args";

function run(argv: string[]): Extract<ParsedProbeArgs, { kind: "run" }> {
  const parsed = parseProbeArgs(argv);
  if (parsed.kind !== "run") {
    throw new Error(
      `expected a run, got ${parsed.kind}: ${parsed.kind === "error" ? parsed.message : ""}`,
    );
  }
  return parsed;
}

function errorMessage(argv: string[]): string {
  const parsed = parseProbeArgs(argv);
  if (parsed.kind !== "error") {
    throw new Error(`expected an error, got ${parsed.kind}`);
  }
  return parsed.message;
}

describe("parseProbeArgs", () => {
  test("provider and model with no --only sweeps every capability", () => {
    const { args } = run(["--provider", "openai", "--model", "gpt-4o"]);
    expect(args.provider).toBe("openai");
    expect(args.model).toBe("gpt-4o");
    expect(args.modelClass).toBeUndefined();
    expect(args.outDir).toBeUndefined();
    expect(args.capabilities.length).toBe(CAPABILITIES.length);
  });

  test("--only restricts and is repeatable", () => {
    const { args } = run([
      "--provider",
      "openai",
      "--model",
      "gpt-4o",
      "--only",
      "plain-text",
      "--only",
      "function-calling",
    ]);
    expect(args.capabilities).toEqual(["plain-text", "function-calling"]);
  });

  test("--out is carried through unresolved", () => {
    const { args } = run([
      "--provider",
      "openai",
      "--model",
      "gpt-4o",
      "--out",
      "tmp/custom",
    ]);
    expect(args.outDir).toBe("tmp/custom");
  });

  test("--model-class is accepted for google-genai", () => {
    const { args } = run([
      "--provider",
      "google-genai",
      "--model",
      "imagen-unknown",
      "--model-class",
      "image",
    ]);
    expect(args.modelClass).toBe("image");
  });

  test("--model-class is rejected for a non-google provider", () => {
    expect(
      errorMessage([
        "--provider",
        "openai",
        "--model",
        "gpt-4o",
        "--model-class",
        "image",
      ]),
    ).toMatch(/only to google-genai/);
  });

  test("an invalid --model-class value errors", () => {
    expect(
      errorMessage([
        "--provider",
        "google-genai",
        "--model",
        "m",
        "--model-class",
        "audio",
      ]),
    ).toMatch(/model-class must be one of/);
  });

  test("an unknown capability errors", () => {
    expect(
      errorMessage([
        "--provider",
        "openai",
        "--model",
        "gpt-4o",
        "--only",
        "not-a-capability",
      ]),
    ).toMatch(/unknown capability/);
  });

  test("a missing provider errors", () => {
    expect(errorMessage(["--model", "gpt-4o"])).toMatch(
      /--provider is required/,
    );
  });

  test("a missing model errors", () => {
    expect(errorMessage(["--provider", "openai"])).toMatch(
      /--model is required/,
    );
  });

  test("an unknown argument errors", () => {
    expect(errorMessage(["--frobnicate"])).toMatch(/unknown argument/);
  });

  test("a flag consumed as a value is treated as missing", () => {
    // --provider's value slot is the next --flag, so it is not consumed.
    expect(errorMessage(["--provider", "--model", "gpt-4o"])).toMatch(
      /--provider needs a value/,
    );
  });

  test("--help short-circuits to help", () => {
    expect(parseProbeArgs(["--provider", "openai", "--help"]).kind).toBe(
      "help",
    );
  });
});
