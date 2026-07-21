import { describe, test, expect } from "bun:test";

import { approvalsRequestPath } from "./tenants";

describe("approvalsRequestPath", () => {
  test("omits the query string on the first page", () => {
    expect(approvalsRequestPath("tnt_1", undefined)).toBe(
      "/api/tenants/tnt_1/approvals",
    );
  });

  test("appends the cursor as a query parameter", () => {
    expect(approvalsRequestPath("tnt_1", "abc123")).toBe(
      "/api/tenants/tnt_1/approvals?cursor=abc123",
    );
  });

  test("url-encodes a cursor containing reserved characters", () => {
    expect(approvalsRequestPath("tnt_1", "a b/c+d=")).toBe(
      "/api/tenants/tnt_1/approvals?cursor=a+b%2Fc%2Bd%3D",
    );
  });
});
