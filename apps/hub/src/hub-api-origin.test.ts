import { describe, expect, test } from "bun:test";

import { resolveHubApiOrigin } from "./hub-api-origin";

describe("resolveHubApiOrigin", () => {
  test("reduces an http or https URL to its origin", () => {
    expect(
      resolveHubApiOrigin("HUB_API_ORIGIN", "https://hub.example:8443"),
    ).toBe("https://hub.example:8443");
    expect(
      resolveHubApiOrigin("HUB_API_ORIGIN", "http://localhost:3000/"),
    ).toBe("http://localhost:3000");
  });

  test("refuses a value that is not an http or https URL", () => {
    expect(() => resolveHubApiOrigin("HUB_API_ORIGIN", "hub.example")).toThrow(
      "is not a URL",
    );
    expect(() =>
      resolveHubApiOrigin("HUB_API_ORIGIN", "ftp://hub.example"),
    ).toThrow("must be an http or https URL");
  });

  test.each([
    "https://hub.example/api",
    "https://user:secret@hub.example",
    "https://hub.example/?x=1",
    "https://hub.example/#top",
  ])("refuses more than an origin: %s", (value) => {
    expect(() => resolveHubApiOrigin("HUB_API_ORIGIN", value)).toThrow(
      "must be an origin",
    );
  });

  test("with a handler, reports more than an origin and uses the origin", () => {
    // The auth base URL may carry a path for its own use, so the hub must
    // still start when the API origin falls back to it.
    const reported: string[] = [];
    const origin = resolveHubApiOrigin(
      "BETTER_AUTH_BASE_URL",
      "https://hub.example/auth",
      () => reported.push("more than an origin"),
    );
    expect(origin).toBe("https://hub.example");
    expect(reported).toEqual(["more than an origin"]);
  });

  test("with a handler, an origin reports nothing", () => {
    const reported: string[] = [];
    resolveHubApiOrigin("BETTER_AUTH_BASE_URL", "https://hub.example", () =>
      reported.push("more than an origin"),
    );
    expect(reported).toEqual([]);
  });
});
