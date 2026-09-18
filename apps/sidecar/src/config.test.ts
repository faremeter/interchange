import { describe, test, expect } from "bun:test";

import { parseReconnectDelayMs } from "./config";

// The first link of the reconnect-delay chain: operator env string to the
// `reconnectDelayMs` option the boot edge forwards to the hub link. The
// remaining links are pinned elsewhere -- that the fixture puts "3000" in the
// spawned sidecar's env (`tests/hub-agent/lib/deploy-flow-env.test.ts`), and
// that the option reaches the reconnect scheduler as the delay
// (`packages/hub-agent/src/ws/hub-link.test.ts`).
describe("parseReconnectDelayMs", () => {
  test("yields undefined when the variable is unset", () => {
    expect(parseReconnectDelayMs(undefined)).toBeUndefined();
  });

  test("yields undefined for an empty or whitespace-only value", () => {
    expect(parseReconnectDelayMs("")).toBeUndefined();
    expect(parseReconnectDelayMs("   ")).toBeUndefined();
  });

  test("parses the production delay the reconnect tests pin", () => {
    expect(parseReconnectDelayMs("3000")).toBe(3000);
  });

  test("parses the short delay the deploy-flow fixture defaults to", () => {
    expect(parseReconnectDelayMs("250")).toBe(250);
  });

  test("ignores whitespace around the delay", () => {
    expect(parseReconnectDelayMs("  250  ")).toBe(250);
  });

  test("reads exponent notation as its full value", () => {
    expect(parseReconnectDelayMs("1e5")).toBe(100000);
  });

  test("throws on a non-positive delay", () => {
    expect(() => parseReconnectDelayMs("0")).toThrow(
      /SIDECAR_RECONNECT_DELAY_MS must be a positive integer \(milliseconds\), got 0/,
    );
    expect(() => parseReconnectDelayMs("-1")).toThrow(
      /SIDECAR_RECONNECT_DELAY_MS must be a positive integer \(milliseconds\), got -1/,
    );
  });

  test("throws on a value that is not a number", () => {
    expect(() => parseReconnectDelayMs("abc")).toThrow(
      /SIDECAR_RECONNECT_DELAY_MS must be a positive integer \(milliseconds\), got abc/,
    );
  });

  // A digit-at-a-time reading accepts these as their leading numeric prefix,
  // which is how a mistyped delay reaches the hub link as a real value: "5abc"
  // would arrive as a 5ms backoff, a hot reconnect loop against the hub. The
  // rule rejects the whole string instead, so the typo fails the boot.
  test("throws on a number with a trailing unit or garbage", () => {
    expect(() => parseReconnectDelayMs("5abc")).toThrow(
      /SIDECAR_RECONNECT_DELAY_MS must be a positive integer \(milliseconds\), got 5abc/,
    );
    expect(() => parseReconnectDelayMs("3000ms")).toThrow(
      /SIDECAR_RECONNECT_DELAY_MS must be a positive integer \(milliseconds\), got 3000ms/,
    );
  });

  test("throws on a fractional delay rather than truncating it", () => {
    expect(() => parseReconnectDelayMs("2500.9")).toThrow(
      /SIDECAR_RECONNECT_DELAY_MS must be a positive integer \(milliseconds\), got 2500\.9/,
    );
  });
});
