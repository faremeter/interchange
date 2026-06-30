import { describe, test, expect } from "bun:test";

import { base64urlEncode } from "@intx/types";

import { paginatedResponse, parsePageParams } from "./pagination";

describe("pagination cursors", () => {
  test("round-trips a cursor emitted by paginatedResponse", () => {
    const createdAt = new Date("2024-01-02T03:04:05.678Z");
    const rows = [{ createdAt, id: "tok_abc" }];
    const { nextCursor } = paginatedResponse(["item"], rows, 1);
    if (nextCursor === null) throw new Error("expected a non-null cursor");

    const { cursor } = parsePageParams({
      cursor: nextCursor,
      limit: undefined,
    });
    expect(cursor).toEqual({ t: createdAt.toISOString(), id: "tok_abc" });
  });

  test("rejects a cursor that is not valid base64url as no cursor", () => {
    const { cursor } = parsePageParams({ cursor: "!!!!", limit: undefined });
    expect(cursor).toBeNull();
  });

  test("rejects a base64url cursor whose payload is not JSON", () => {
    const bad = base64urlEncode(new TextEncoder().encode("not json"));
    const { cursor } = parsePageParams({ cursor: bad, limit: undefined });
    expect(cursor).toBeNull();
  });

  test("rejects a base64url cursor whose JSON has the wrong shape", () => {
    const bad = base64urlEncode(
      new TextEncoder().encode(JSON.stringify({ t: "not-a-timestamp", id: 5 })),
    );
    const { cursor } = parsePageParams({ cursor: bad, limit: undefined });
    expect(cursor).toBeNull();
  });
});
