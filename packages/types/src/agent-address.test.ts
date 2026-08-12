import { describe, test, expect } from "bun:test";

import {
  formatRunAddress,
  isRunAddress,
  parseRunAddress,
} from "./agent-address";

describe("formatRunAddress", () => {
  test("joins runId and domain with @", () => {
    expect(formatRunAddress("ins_abc123", "tenant.example")).toBe(
      "ins_abc123@tenant.example",
    );
  });
});

describe("parseRunAddress", () => {
  test("splits a well-formed address", () => {
    expect(parseRunAddress("ins_abc123@tenant.example")).toEqual({
      runId: "ins_abc123",
      domain: "tenant.example",
    });
  });

  test("returns null when the run prefix is missing", () => {
    expect(parseRunAddress("usr_alice@tenant.example")).toBeNull();
  });

  test("returns null when the @ is missing", () => {
    expect(parseRunAddress("ins_abc123")).toBeNull();
  });

  test("returns null when the local part is empty", () => {
    expect(parseRunAddress("@tenant.example")).toBeNull();
  });

  test("returns null when the domain part is empty", () => {
    expect(parseRunAddress("ins_abc123@")).toBeNull();
  });

  test("does not validate the shape of the domain", () => {
    expect(parseRunAddress("ins_abc123@not a real domain")).toEqual({
      runId: "ins_abc123",
      domain: "not a real domain",
    });
  });

  test("splits on the first @ and treats the rest as the domain", () => {
    expect(parseRunAddress("ins_abc123@foo@bar")).toEqual({
      runId: "ins_abc123",
      domain: "foo@bar",
    });
  });
});

describe("isRunAddress", () => {
  test("true for ins_-prefixed addresses with a domain", () => {
    expect(isRunAddress("ins_abc123@tenant.example")).toBe(true);
  });

  test("false for non-run local parts", () => {
    expect(isRunAddress("usr_alice@tenant.example")).toBe(false);
  });

  test("false for bare run IDs without a domain", () => {
    expect(isRunAddress("ins_abc123")).toBe(false);
  });
});
