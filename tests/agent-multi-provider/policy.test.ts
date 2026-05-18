// Unit tests for the multi-provider example's policy module. The
// failover path is exercised here with a synthetic `invoke` rather
// than through the full agent: the inference layer's own retry
// behaviour makes the live HTTP path a poor signal for "did the
// outer policy swap providers?". Testing the policy module in
// isolation pins the contract `withFailover` actually advertises.

import { describe, expect, test } from "bun:test";

import {
  pickModelTier,
  routeProvider,
  withFailover,
  type ProviderEntry,
} from "@interchange/example-agent-multi-provider";
import type { ProviderConfig } from "@interchange/types/runtime";

const PRIMARY_CFG: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://primary.example",
  apiKey: "sk-primary",
  model: "primary-model",
};
const FALLBACK_CFG: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://fallback.example",
  apiKey: "sk-fallback",
  model: "fallback-model",
};

const PRIMARY: ProviderEntry = { name: "primary", config: PRIMARY_CFG };
const FALLBACK: ProviderEntry = { name: "fallback", config: FALLBACK_CFG };

describe("pickModelTier", () => {
  test("short prompts pick the cheap tier", () => {
    expect(pickModelTier("hi")).toBe("cheap");
  });

  test("long prompts pick the smart tier", () => {
    const long = "x".repeat(120);
    expect(pickModelTier(long)).toBe("smart");
  });
});

describe("routeProvider", () => {
  test("overlays the chosen model onto the primary config", () => {
    const r = routeProvider({
      prompt: "x".repeat(120),
      primary: PRIMARY,
      models: { cheap: "h", smart: "s" },
    });
    expect(r.tier).toBe("smart");
    expect(r.model).toBe("s");
    expect(r.provider.apiKey).toBe(PRIMARY_CFG.apiKey);
    expect(r.provider.model).toBe("s");
  });

  test("does not mutate the source ProviderConfig", () => {
    const original = { ...PRIMARY_CFG };
    routeProvider({
      prompt: "x".repeat(120),
      primary: PRIMARY,
      models: { cheap: "h", smart: "s" },
    });
    expect(PRIMARY_CFG).toEqual(original);
  });
});

describe("withFailover", () => {
  test("returns the primary's result on first-try success", async () => {
    const applied: ProviderConfig[] = [];
    const r = await withFailover({
      primary: PRIMARY,
      fallback: FALLBACK,
      applyProvider: (cfg) => applied.push(cfg),
      invoke: async () => "primary-result",
    });
    expect(r.served).toBe(PRIMARY);
    expect(r.attempts).toEqual([PRIMARY]);
    expect(applied).toEqual([PRIMARY_CFG]);
    expect(r.result).toBe("primary-result");
  });

  test("swaps to fallback and retries when the primary attempt rejects", async () => {
    const applied: ProviderConfig[] = [];
    let calls = 0;
    const r = await withFailover({
      primary: PRIMARY,
      fallback: FALLBACK,
      applyProvider: (cfg) => applied.push(cfg),
      invoke: async () => {
        calls++;
        if (calls === 1) throw new Error("primary down");
        return "fallback-result";
      },
    });
    expect(r.served).toBe(FALLBACK);
    expect(r.attempts).toEqual([PRIMARY, FALLBACK]);
    expect(applied).toEqual([PRIMARY_CFG, FALLBACK_CFG]);
    expect(r.result).toBe("fallback-result");
    expect(calls).toBe(2);
    // The primary's failure must come back attached so the caller
    // can log it instead of silently losing it.
    const primaryError = r.primaryError;
    if (!(primaryError instanceof Error)) {
      throw new Error("expected primaryError to be an Error");
    }
    expect(primaryError.message).toBe("primary down");
  });

  test("throws a wrapper that names both errors when fallback also fails", async () => {
    let calls = 0;
    let thrown: unknown;
    try {
      await withFailover({
        primary: PRIMARY,
        fallback: FALLBACK,
        applyProvider: () => undefined,
        invoke: async () => {
          calls++;
          throw new Error(calls === 1 ? "primary down" : "fallback down");
        },
      });
    } catch (err) {
      thrown = err;
    }
    expect(calls).toBe(2);
    if (!(thrown instanceof Error)) {
      throw new Error("expected withFailover to throw an Error");
    }
    // Both failure messages must appear in the wrapper text so an
    // operator log line carries the full picture without walking
    // the cause chain.
    expect(thrown.message).toContain("withFailover: both providers failed");
    expect(thrown.message).toContain(`Primary (${PRIMARY.name}): primary down`);
    expect(thrown.message).toContain(
      `Fallback (${FALLBACK.name}): fallback down`,
    );
    // `cause` points at the fallback Error so standard Error-chain
    // tooling still works. The caught fallback object itself is not
    // mutated — withFailover constructs a fresh wrapper.
    const fallbackError = thrown.cause;
    if (!(fallbackError instanceof Error)) {
      throw new Error("expected wrapper.cause to be the fallback Error");
    }
    expect(fallbackError.message).toBe("fallback down");
    expect(fallbackError.cause).toBeUndefined();
  });
});
