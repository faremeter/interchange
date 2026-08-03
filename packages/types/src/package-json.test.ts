import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import { PackageJSON } from "./package-json";

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
