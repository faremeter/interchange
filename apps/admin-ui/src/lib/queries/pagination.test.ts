import { describe, test, expect } from "bun:test";

import { pageRequestPath } from "./pagination";

describe("pageRequestPath", () => {
  test("returns the base path unchanged on the first page", () => {
    expect(pageRequestPath("/api/tenants/tnt_1/approvals", undefined)).toBe(
      "/api/tenants/tnt_1/approvals",
    );
  });

  test("appends the cursor as a query parameter", () => {
    expect(pageRequestPath("/api/tenants/tnt_1/approvals", "abc123")).toBe(
      "/api/tenants/tnt_1/approvals?cursor=abc123",
    );
  });

  test("url-encodes a cursor containing reserved characters", () => {
    expect(pageRequestPath("/api/tenants/tnt_1/approvals", "a b/c+d=")).toBe(
      "/api/tenants/tnt_1/approvals?cursor=a+b%2Fc%2Bd%3D",
    );
  });

  test("merges the cursor into an existing query string", () => {
    expect(
      pageRequestPath("/api/tenants/tnt_1/instances?status=running", "abc123"),
    ).toBe("/api/tenants/tnt_1/instances?status=running&cursor=abc123");
  });

  test("handles a base path ending in a bare question mark", () => {
    expect(pageRequestPath("/api/tenants/tnt_1/wallets?", "abc123")).toBe(
      "/api/tenants/tnt_1/wallets?cursor=abc123",
    );
  });

  test("overwrites an existing cursor rather than duplicating it", () => {
    expect(
      pageRequestPath("/api/tenants/tnt_1/wallets?cursor=old", "new"),
    ).toBe("/api/tenants/tnt_1/wallets?cursor=new");
  });
});
