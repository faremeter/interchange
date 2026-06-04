import { describe, test, expect } from "bun:test";

import { validateNamespacedId } from "./namespace";

describe("validateNamespacedId", () => {
  test("accepts scoped ids with three segments", () => {
    expect(() => validateNamespacedId("@intx/agent/default")).not.toThrow();
    expect(() =>
      validateNamespacedId("@my-org/my-workflow/special-director"),
    ).not.toThrow();
    expect(() =>
      validateNamespacedId("@anthropic/sdk/mail-send"),
    ).not.toThrow();
  });

  test("accepts unscoped ids with two segments", () => {
    expect(() =>
      validateNamespacedId("lodash-style/director-name"),
    ).not.toThrow();
    expect(() => validateNamespacedId("pkg/name")).not.toThrow();
  });

  test("rejects bare ids without a package portion", () => {
    expect(() => validateNamespacedId("default")).toThrow(
      /must be package-namespaced/,
    );
  });

  test("rejects scoped ids missing the name segment", () => {
    expect(() => validateNamespacedId("@intx/agent")).toThrow(
      /must be package-namespaced/,
    );
  });

  test("rejects scoped ids with an empty name segment", () => {
    expect(() => validateNamespacedId("@intx/agent/")).toThrow(
      /must be package-namespaced/,
    );
  });

  test("rejects unscoped ids missing the name segment", () => {
    expect(() => validateNamespacedId("pkg")).toThrow(
      /must be package-namespaced/,
    );
  });

  test("rejects the empty string", () => {
    expect(() => validateNamespacedId("")).toThrow(
      /must be package-namespaced/,
    );
  });

  test("rejects ids that lead with a slash", () => {
    expect(() => validateNamespacedId("/pkg/name")).toThrow(
      /must be package-namespaced/,
    );
  });

  test("error message includes the JSON-encoded offending id", () => {
    expect(() => validateNamespacedId("bad id")).toThrow(/"bad id"/);
  });
});
