import { describe, test, expect } from "bun:test";

import { CanonicalizationError, canonicalizeForHash } from "./canonicalize";

const decoder = new TextDecoder();

function decode(bytes: Uint8Array): string {
  return decoder.decode(bytes);
}

describe("canonicalizeForHash", () => {
  describe("primitives", () => {
    test("encodes null", () => {
      expect(decode(canonicalizeForHash(null))).toBe("null");
    });

    test("encodes booleans", () => {
      expect(decode(canonicalizeForHash(true))).toBe("true");
      expect(decode(canonicalizeForHash(false))).toBe("false");
    });

    test("encodes finite numbers", () => {
      expect(decode(canonicalizeForHash(0))).toBe("0");
      expect(decode(canonicalizeForHash(-1.5))).toBe("-1.5");
      expect(decode(canonicalizeForHash(42))).toBe("42");
    });

    test("encodes strings", () => {
      expect(decode(canonicalizeForHash("hello"))).toBe(`"hello"`);
    });

    test("NFC-normalizes strings", () => {
      // Decomposed "café" (U+0301 combining acute) vs precomposed.
      const decomposed = "café";
      const precomposed = "café";
      expect(decomposed).not.toBe(precomposed);
      expect(decode(canonicalizeForHash(decomposed))).toBe(
        decode(canonicalizeForHash(precomposed)),
      );
    });
  });

  describe("objects", () => {
    test("sorts object keys lexicographically", () => {
      const out = canonicalizeForHash({ b: 1, a: 2, c: 3 });
      expect(decode(out)).toBe(`{"a":2,"b":1,"c":3}`);
    });

    test("produces identical bytes for two reorderings of the same object", () => {
      const a = canonicalizeForHash({ x: 1, y: 2, z: 3 });
      const b = canonicalizeForHash({ z: 3, y: 2, x: 1 });
      expect(decode(a)).toBe(decode(b));
    });

    test("sorts nested object keys", () => {
      const out = canonicalizeForHash({ outer: { z: 1, a: 2 } });
      expect(decode(out)).toBe(`{"outer":{"a":2,"z":1}}`);
    });

    test("preserves array order", () => {
      const out = canonicalizeForHash([3, 1, 2]);
      expect(decode(out)).toBe(`[3,1,2]`);
    });

    test("NFC-normalizes object keys", () => {
      const decomposedKey = "café";
      const precomposedKey = "café";
      const a = canonicalizeForHash({ [decomposedKey]: 1 });
      const b = canonicalizeForHash({ [precomposedKey]: 1 });
      expect(decode(a)).toBe(decode(b));
    });
  });

  describe("rejections", () => {
    test("rejects undefined", () => {
      expect(() => canonicalizeForHash(undefined)).toThrow(
        CanonicalizationError,
      );
    });

    test("rejects NaN", () => {
      expect(() => canonicalizeForHash(NaN)).toThrow(CanonicalizationError);
    });

    test("rejects positive infinity", () => {
      expect(() => canonicalizeForHash(Number.POSITIVE_INFINITY)).toThrow(
        CanonicalizationError,
      );
    });

    test("rejects negative infinity", () => {
      expect(() => canonicalizeForHash(Number.NEGATIVE_INFINITY)).toThrow(
        CanonicalizationError,
      );
    });

    test("rejects Date instances", () => {
      expect(() => canonicalizeForHash(new Date())).toThrow(
        CanonicalizationError,
      );
    });

    test("rejects Map instances", () => {
      expect(() => canonicalizeForHash(new Map())).toThrow(
        CanonicalizationError,
      );
    });

    test("rejects Set instances", () => {
      expect(() => canonicalizeForHash(new Set())).toThrow(
        CanonicalizationError,
      );
    });

    test("rejects functions", () => {
      expect(() => canonicalizeForHash(() => 1)).toThrow(CanonicalizationError);
    });

    test("rejects symbols", () => {
      expect(() => canonicalizeForHash(Symbol("x"))).toThrow(
        CanonicalizationError,
      );
    });

    test("rejects bigints", () => {
      expect(() => canonicalizeForHash(BigInt(1))).toThrow(
        CanonicalizationError,
      );
    });

    test("rejects RegExp instances", () => {
      expect(() => canonicalizeForHash(/x/)).toThrow(CanonicalizationError);
    });

    test("rejects Uint8Array", () => {
      expect(() => canonicalizeForHash(new Uint8Array([1]))).toThrow(
        CanonicalizationError,
      );
    });

    test("rejects class instances (non-plain objects)", () => {
      class Custom {
        x = 1;
      }
      expect(() => canonicalizeForHash(new Custom())).toThrow(
        CanonicalizationError,
      );
    });

    test("rejects cycles", () => {
      const obj: Record<string, unknown> = {};
      obj.self = obj;
      expect(() => canonicalizeForHash(obj)).toThrow(/cycle detected/);
    });

    test("reports the path to a nested rejection", () => {
      let caught: unknown;
      try {
        canonicalizeForHash({ outer: { inner: new Date() } });
      } catch (err) {
        caught = err;
      }
      if (!(caught instanceof CanonicalizationError)) {
        throw new Error("expected CanonicalizationError");
      }
      expect(caught.path).toEqual(["outer", "inner"]);
    });

    test("rejects two distinct keys that NFC-normalize to the same value", () => {
      // "café" (precomposed) and "café" (e + combining
      // acute) are different strings in memory; both NFC-normalize to
      // "café". Silently collapsing the two would drop one of
      // the values and the deploy hash would no longer be a faithful
      // function of the input -- explicit failure is the defensive
      // disposition.
      const precomposed = "café";
      const decomposed = "café";
      expect(precomposed).not.toBe(decomposed);
      const obj: Record<string, number> = {};
      obj[precomposed] = 1;
      obj[decomposed] = 2;
      expect(Object.keys(obj).length).toBe(2);
      expect(() => canonicalizeForHash(obj)).toThrow(/NFC-normalize/);
    });

    test("sorts NFC-normalized keys so pre- and post-normalized inputs match", () => {
      // The decomposed form "ô" sorts BEFORE "p" because it
      // begins with U+006F. Its NFC-normalized form "ô" sorts
      // AFTER "p" because U+00F4 > U+0070. canonicalizeForHash must
      // sort *after* normalization so a producer that pre-normalizes
      // and a producer that doesn't hash the same logical value to the
      // same bytes.
      const decomposed = "ô"; // o + combining circumflex
      const precomposed = "ô"; // ô precomposed
      expect(decomposed.normalize("NFC")).toBe(precomposed);
      const a = canonicalizeForHash({ [decomposed]: 1, p: 2 });
      const b = canonicalizeForHash({ [precomposed]: 1, p: 2 });
      expect(decode(a)).toBe(decode(b));
    });
  });

  describe("composite shapes", () => {
    test("encodes a small AgentDefinition-shaped record deterministically", () => {
      const a = canonicalizeForHash({
        id: "planner",
        systemPrompt: "You are the planner.",
        capabilities: ["plan/write"],
        inference: {
          sources: [
            {
              provider: "anthropic",
              model: "claude-opus-4-6",
              parameters: { temperature: 0.3 },
            },
          ],
        },
      });

      const b = canonicalizeForHash({
        inference: {
          sources: [
            {
              parameters: { temperature: 0.3 },
              model: "claude-opus-4-6",
              provider: "anthropic",
            },
          ],
        },
        capabilities: ["plan/write"],
        systemPrompt: "You are the planner.",
        id: "planner",
      });

      expect(decode(a)).toBe(decode(b));
    });
  });
});
