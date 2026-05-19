import { describe, test, expect } from "bun:test";

import {
  formatAgentAddress,
  isAgentAddress,
  parseAgentAddress,
} from "./agent-address";

describe("formatAgentAddress", () => {
  test("joins instanceId and domain with @", () => {
    expect(formatAgentAddress("ins_abc123", "tenant.example")).toBe(
      "ins_abc123@tenant.example",
    );
  });
});

describe("parseAgentAddress", () => {
  test("splits a well-formed address", () => {
    expect(parseAgentAddress("ins_abc123@tenant.example")).toEqual({
      instanceId: "ins_abc123",
      domain: "tenant.example",
    });
  });

  test("returns null when instance prefix is missing", () => {
    expect(parseAgentAddress("usr_alice@tenant.example")).toBeNull();
  });

  test("returns null when the @ is missing", () => {
    expect(parseAgentAddress("ins_abc123")).toBeNull();
  });

  test("returns null when the local part is empty", () => {
    expect(parseAgentAddress("@tenant.example")).toBeNull();
  });

  test("returns null when the domain part is empty", () => {
    expect(parseAgentAddress("ins_abc123@")).toBeNull();
  });

  test("does not validate the shape of the domain", () => {
    expect(parseAgentAddress("ins_abc123@not a real domain")).toEqual({
      instanceId: "ins_abc123",
      domain: "not a real domain",
    });
  });

  test("splits on the first @ and treats the rest as the domain", () => {
    expect(parseAgentAddress("ins_abc123@foo@bar")).toEqual({
      instanceId: "ins_abc123",
      domain: "foo@bar",
    });
  });
});

describe("isAgentAddress", () => {
  test("true for ins_-prefixed addresses with a domain", () => {
    expect(isAgentAddress("ins_abc123@tenant.example")).toBe(true);
  });

  test("false for non-agent local parts", () => {
    expect(isAgentAddress("usr_alice@tenant.example")).toBe(false);
  });

  test("false for bare instance IDs without a domain", () => {
    expect(isAgentAddress("ins_abc123")).toBe(false);
  });
});
