import { describe, test, expect } from "bun:test";

import {
  formatRunAddress,
  isRunAddress,
  parseAddress,
  parseRunAddress,
} from "./agent-address";

describe("formatRunAddress", () => {
  test("joins runId and domain with @", () => {
    expect(formatRunAddress("run_abc123", "tenant.example")).toBe(
      "run_abc123@tenant.example",
    );
  });
});

describe("parseAddress", () => {
  test("splits any local part from its domain without a prefix check", () => {
    expect(parseAddress("usr_alice@tenant.example")).toEqual({
      localPart: "usr_alice",
      domain: "tenant.example",
    });
    expect(parseAddress("run_abc123@tenant.example")).toEqual({
      localPart: "run_abc123",
      domain: "tenant.example",
    });
  });

  test("returns null when the @ is missing", () => {
    expect(parseAddress("no-at-sign")).toBeNull();
  });

  test("returns null when the local part is empty", () => {
    expect(parseAddress("@tenant.example")).toBeNull();
  });

  test("returns null when the domain part is empty", () => {
    expect(parseAddress("usr_alice@")).toBeNull();
  });

  test("splits on the first @ and treats the rest as the domain", () => {
    expect(parseAddress("usr_alice@foo@bar")).toEqual({
      localPart: "usr_alice",
      domain: "foo@bar",
    });
  });
});

describe("parseRunAddress", () => {
  test("splits a well-formed address", () => {
    expect(parseRunAddress("run_abc123@tenant.example")).toEqual({
      runId: "run_abc123",
      domain: "tenant.example",
    });
  });

  test("returns null when the run prefix is missing", () => {
    expect(parseRunAddress("usr_alice@tenant.example")).toBeNull();
  });

  test("returns null when the @ is missing", () => {
    expect(parseRunAddress("run_abc123")).toBeNull();
  });

  test("returns null when the local part is empty", () => {
    expect(parseRunAddress("@tenant.example")).toBeNull();
  });

  test("returns null when the domain part is empty", () => {
    expect(parseRunAddress("run_abc123@")).toBeNull();
  });

  test("does not validate the shape of the domain", () => {
    expect(parseRunAddress("run_abc123@not a real domain")).toEqual({
      runId: "run_abc123",
      domain: "not a real domain",
    });
  });

  test("splits on the first @ and treats the rest as the domain", () => {
    expect(parseRunAddress("run_abc123@foo@bar")).toEqual({
      runId: "run_abc123",
      domain: "foo@bar",
    });
  });
});

describe("isRunAddress", () => {
  test("true for run_-prefixed addresses with a domain", () => {
    expect(isRunAddress("run_abc123@tenant.example")).toBe(true);
  });

  test("false for non-run local parts", () => {
    expect(isRunAddress("usr_alice@tenant.example")).toBe(false);
  });

  test("false for bare run IDs without a domain", () => {
    expect(isRunAddress("run_abc123")).toBe(false);
  });
});
