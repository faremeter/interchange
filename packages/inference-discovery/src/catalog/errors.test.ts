import { describe, test, expect } from "bun:test";
import { CapabilityNotBuildableError } from "./errors";

describe("CapabilityNotBuildableError", () => {
  test("is an Error subclass carrying the capability", () => {
    const err = new CapabilityNotBuildableError(
      "vision-input",
      "provider: cannot build vision-input for model foo",
    );
    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(CapabilityNotBuildableError);
    expect(err.name).toBe("CapabilityNotBuildableError");
    expect(err.capability).toBe("vision-input");
    expect(err.message).toBe(
      "provider: cannot build vision-input for model foo",
    );
  });

  test("is distinguishable from a bare Error by instanceof", () => {
    const bare: unknown = new Error("some other failure");
    const typed: unknown = new CapabilityNotBuildableError(
      "plain-text",
      "nope",
    );
    expect(bare instanceof CapabilityNotBuildableError).toBe(false);
    expect(typed instanceof CapabilityNotBuildableError).toBe(true);
  });
});
