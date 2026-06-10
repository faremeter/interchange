// parseAgentId is the @intx/hub-sessions wrapper around the @intx/types
// parseAgentAddress parser. The wrapper exists to throw on invalid
// input rather than return null, and to preserve the raw address in
// the error message for diagnostic context. These tests pin the
// wrapper's behavior so a future change to the delegation (e.g.,
// reverting to a local splitter, or shifting to a stricter contract)
// has to update them deliberately. The underlying parser is tested in
// packages/types/src/agent-address.test.ts.

import { describe, test, expect } from "bun:test";

import { parseAgentId } from "./hub-session-lookups";

describe("parseAgentId", () => {
  test("returns the instance id from a canonical address", () => {
    expect(parseAgentId("ins_abc123@tenant.example")).toBe("ins_abc123");
  });

  test("throws when the @ is missing", () => {
    expect(() => parseAgentId("ins_abc123")).toThrow(
      'Invalid agent address: "ins_abc123"',
    );
  });

  test("throws when the local part lacks the ins_ prefix", () => {
    expect(() => parseAgentId("usr_alice@tenant.example")).toThrow(
      'Invalid agent address: "usr_alice@tenant.example"',
    );
  });

  test("throws when the domain part is empty", () => {
    expect(() => parseAgentId("ins_abc123@")).toThrow(
      'Invalid agent address: "ins_abc123@"',
    );
  });

  test("throws when the local part is empty", () => {
    expect(() => parseAgentId("@tenant.example")).toThrow(
      'Invalid agent address: "@tenant.example"',
    );
  });

  test("throws on empty string", () => {
    expect(() => parseAgentId("")).toThrow('Invalid agent address: ""');
  });

  test("preserves the raw address in the error message for diagnostics", () => {
    expect(() => parseAgentId("not-an-address")).toThrow("not-an-address");
  });

  test("splits on the first @ — multi-@ addresses keep the rest as domain", () => {
    expect(parseAgentId("ins_abc@foo@bar")).toBe("ins_abc");
  });
});
