import { describe, test, expect } from "bun:test";

import { patternSpecificity, grantSpecificity } from "./specificity";

describe("patternSpecificity", () => {
  test("bare wildcard has zero specificity", () => {
    expect(patternSpecificity("*")).toBe(0);
  });

  test("type-level wildcard scores by literal length", () => {
    // "agent:" has 6 literal chars
    expect(patternSpecificity("agent:*")).toBe(6);
  });

  test("prefix wildcard scores by literal length", () => {
    // "wallet:wal_" has 11 literal chars
    expect(patternSpecificity("wallet:wal_*")).toBe(11);
  });

  test("exact match gets bonus of 1000", () => {
    // "agent:agt_abc" has 13 chars + 1000 bonus
    expect(patternSpecificity("agent:agt_abc")).toBe(1013);
  });

  test("more specific patterns score higher", () => {
    const scores = [
      patternSpecificity("*"),
      patternSpecificity("agent:*"),
      patternSpecificity("agent:agt_*"),
      patternSpecificity("agent:agt_abc"),
    ];

    for (let i = 1; i < scores.length; i++) {
      const prev = scores[i - 1] ?? 0;
      expect(scores[i]).toBeGreaterThan(prev);
    }
  });

  test("action patterns follow same rules", () => {
    expect(patternSpecificity("*")).toBe(0);
    expect(patternSpecificity("read")).toBe(1004); // 4 chars + 1000
    expect(patternSpecificity("manage")).toBe(1006); // 6 chars + 1000
  });
});

describe("grantSpecificity", () => {
  test("combines resource and action specificity", () => {
    expect(grantSpecificity("*", "*")).toBe(0);
    expect(grantSpecificity("agent:*", "read")).toBe(6 + 1004);
    expect(grantSpecificity("agent:agt_abc", "manage")).toBe(1013 + 1006);
  });

  test("more specific grant beats less specific", () => {
    // Wildcard everything
    const s1 = grantSpecificity("*", "*");
    // Type-level wildcard with specific action
    const s2 = grantSpecificity("agent:*", "read");
    // Exact resource and action
    const s3 = grantSpecificity("agent:agt_abc", "manage");

    expect(s2).toBeGreaterThan(s1);
    expect(s3).toBeGreaterThan(s2);
  });
});

describe("patternSpecificity edge cases", () => {
  test("empty string gets exact match bonus", () => {
    // Empty string has no wildcard, so it gets the 1000 bonus + 0 literal chars
    expect(patternSpecificity("")).toBe(1000);
  });

  test("multi-wildcard pattern scores only literal characters", () => {
    // "*:*" has 1 literal char (':') and contains wildcards
    expect(patternSpecificity("*:*")).toBe(1);
  });

  test("nested colon pattern scores all literal characters", () => {
    // "api:stripe:*" has 11 literal chars ("api:stripe:") and contains a wildcard
    expect(patternSpecificity("api:stripe:*")).toBe(11);
    // "api:stripe:charges" has 18 chars and no wildcard -> 18 + 1000
    expect(patternSpecificity("api:stripe:charges")).toBe(1018);
  });

  test("specificity is character-count based, not segment-aware", () => {
    // Two patterns with same literal length but different structure
    // score identically, proving specificity is purely character-based
    const a = patternSpecificity("abcdef:*"); // 7 literal chars
    const b = patternSpecificity("ab:cd:e*"); // 7 literal chars
    expect(a).toBe(b);
  });
});
