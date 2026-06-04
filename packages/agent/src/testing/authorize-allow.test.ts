import { describe, test, expect } from "bun:test";

import { permissiveAuthorize } from "./authorize-allow";

describe("permissiveAuthorize", () => {
  test("returns { effect: 'allow' } regardless of resource and action", async () => {
    const authorize = permissiveAuthorize();
    const result = await authorize("tool:any", "invoke");
    expect(result.effect).toBe("allow");
    expect(result.matchingGrants).toEqual([]);
    expect(result.resolvedBy).toBeNull();
  });

  test("each call returns a fresh function", () => {
    expect(permissiveAuthorize()).not.toBe(permissiveAuthorize());
  });
});
