import { describe, test, expect } from "bun:test";

import { patternSpecificity, grantSpecificity } from "./specificity";

describe("patternSpecificity", () => {
  test("bare wildcard has zero specificity", () => {
    expect(patternSpecificity("*")).toBe(0);
  });

  test("type-level wildcard scores by literal length", () => {
    expect(patternSpecificity("agent:*")).toBe(6);
  });

  test("prefix wildcard scores by literal length", () => {
    expect(patternSpecificity("wallet:wal_*")).toBe(11);
  });

  test("exact match gets bonus of 1000", () => {
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
    expect(patternSpecificity("read")).toBe(1004);
    expect(patternSpecificity("manage")).toBe(1006);
  });
});

describe("grantSpecificity", () => {
  test("combines resource and action specificity", () => {
    expect(grantSpecificity("*", "*")).toBe(0);
    expect(grantSpecificity("agent:*", "read")).toBe(6 + 1004);
    expect(grantSpecificity("agent:agt_abc", "manage")).toBe(1013 + 1006);
  });

  test("more specific grant beats less specific", () => {
    const s1 = grantSpecificity("*", "*");
    const s2 = grantSpecificity("agent:*", "read");
    const s3 = grantSpecificity("agent:agt_abc", "manage");

    expect(s2).toBeGreaterThan(s1);
    expect(s3).toBeGreaterThan(s2);
  });
});

describe("patternSpecificity edge cases", () => {
  test("empty string gets exact match bonus", () => {
    expect(patternSpecificity("")).toBe(1000);
  });

  test("multi-wildcard pattern scores only literal characters", () => {
    expect(patternSpecificity("*:*")).toBe(1);
  });

  test("nested colon pattern scores all literal characters", () => {
    expect(patternSpecificity("api:stripe:*")).toBe(11);
    expect(patternSpecificity("api:stripe:charges")).toBe(1018);
  });

  test("specificity is character-count based, not segment-aware", () => {
    const a = patternSpecificity("abcdef:*"); // 7 literal chars
    const b = patternSpecificity("ab:cd:e*"); // 7 literal chars
    expect(a).toBe(b);
  });
});
