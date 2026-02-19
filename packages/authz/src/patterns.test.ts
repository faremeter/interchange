import { describe, test, expect } from "bun:test";

import { matchPattern } from "./patterns";

describe("matchPattern", () => {
  test("bare wildcard matches anything", () => {
    expect(matchPattern("*", "agent:agt_abc")).toBe(true);
    expect(matchPattern("*", "")).toBe(true);
    expect(matchPattern("*", "anything")).toBe(true);
  });

  test("exact match", () => {
    expect(matchPattern("agent:agt_abc", "agent:agt_abc")).toBe(true);
    expect(matchPattern("agent:agt_abc", "agent:agt_xyz")).toBe(false);
    expect(matchPattern("read", "read")).toBe(true);
    expect(matchPattern("read", "write")).toBe(false);
  });

  test("type-level wildcard matches any identifier", () => {
    expect(matchPattern("agent:*", "agent:agt_abc")).toBe(true);
    expect(matchPattern("agent:*", "agent:agt_xyz")).toBe(true);
    expect(matchPattern("agent:*", "wallet:wal_123")).toBe(false);
  });

  test("prefix wildcard", () => {
    expect(matchPattern("wallet:wal_*", "wallet:wal_123")).toBe(true);
    expect(matchPattern("wallet:wal_*", "wallet:xyz")).toBe(false);
  });

  test("no wildcard requires exact match", () => {
    expect(matchPattern("agent:agt_abc", "agent:agt_abc")).toBe(true);
    expect(matchPattern("documents", "documents:doc_1")).toBe(false);
  });

  test("wildcard at start", () => {
    expect(matchPattern("*:read", "agent:read")).toBe(true);
    expect(matchPattern("*:read", "wallet:read")).toBe(true);
    expect(matchPattern("*:read", "agent:write")).toBe(false);
  });

  test("multiple wildcards", () => {
    expect(matchPattern("*:*", "agent:agt_abc")).toBe(true);
    expect(matchPattern("a*b*c", "abc")).toBe(true);
    expect(matchPattern("a*b*c", "aXXbYYc")).toBe(true);
    expect(matchPattern("a*b*c", "aXXbYY")).toBe(false);
  });

  test("pattern with no wildcards and different lengths", () => {
    expect(matchPattern("ab", "abc")).toBe(false);
    expect(matchPattern("abc", "ab")).toBe(false);
  });

  test("empty string pattern against empty string target", () => {
    expect(matchPattern("", "")).toBe(true);
  });

  test("empty string pattern does not match non-empty target", () => {
    expect(matchPattern("", "agent:agt_abc")).toBe(false);
  });

  test("consecutive wildcards behave like a single wildcard", () => {
    expect(matchPattern("a**b", "aXb")).toBe(true);
    expect(matchPattern("a**b", "ab")).toBe(true);
    expect(matchPattern("***", "anything")).toBe(true);
  });

  test("nested colon resource patterns (api:stripe:*)", () => {
    expect(matchPattern("api:stripe:*", "api:stripe:charges")).toBe(true);
    expect(matchPattern("api:stripe:*", "api:stripe:refunds")).toBe(true);
    expect(matchPattern("api:stripe:*", "api:plaid:accounts")).toBe(false);
    expect(matchPattern("api:*", "api:stripe:charges")).toBe(true);
  });

  test("trailing wildcard matches zero characters", () => {
    expect(matchPattern("abc*", "abc")).toBe(true);
    expect(matchPattern("agent:*", "agent:")).toBe(true);
  });

  test("patterns are case-sensitive", () => {
    expect(matchPattern("Agent:*", "agent:agt_abc")).toBe(false);
    expect(matchPattern("agent:*", "Agent:agt_abc")).toBe(false);
    expect(matchPattern("READ", "read")).toBe(false);
  });

  test("wildcard-only variants", () => {
    expect(matchPattern("**", "anything")).toBe(true);
    expect(matchPattern("**", "")).toBe(true);
  });
});
