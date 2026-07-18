import { describe, test, expect } from "bun:test";

import { parseApprovalSnapshot } from "./approval-snapshot";

describe("parseApprovalSnapshot", () => {
  test("narrows a well-formed snapshot", () => {
    const result = parseApprovalSnapshot(
      {
        name: "charge_card",
        description: "Charge the customer's card",
        inputSchema: { type: "object" },
      },
      { amount: 100 },
    );
    expect(result).toEqual({
      ok: true,
      name: "charge_card",
      description: "Charge the customer's card",
      inputSchema: { type: "object" },
      arguments: { amount: 100 },
    });
  });

  test("reports malformed when the name is not a string", () => {
    const result = parseApprovalSnapshot({ name: 42, description: "x" }, {});
    expect(result.ok).toBe(false);
  });

  test("reports malformed when the description is missing", () => {
    const result = parseApprovalSnapshot({ name: "charge_card" }, {});
    expect(result.ok).toBe(false);
  });
});
