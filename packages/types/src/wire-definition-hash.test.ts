import { describe, test, expect } from "bun:test";

import {
  canonicalJsonStringify,
  computeWireDefinitionHash,
} from "./wire-definition-hash";

describe("canonicalJsonStringify", () => {
  test("is independent of object key order", () => {
    const a = { id: "w-1", stepOrder: ["s1"], steps: { s1: { kind: "a" } } };
    const b = { steps: { s1: { kind: "a" } }, stepOrder: ["s1"], id: "w-1" };
    expect(canonicalJsonStringify(a)).toBe(canonicalJsonStringify(b));
  });

  test("sorts keys at every nesting level", () => {
    const value = { b: { d: 1, c: 2 }, a: [3, 2, 1] };
    expect(canonicalJsonStringify(value)).toBe(
      '{"a":[3,2,1],"b":{"c":2,"d":1}}',
    );
  });

  test("preserves array order", () => {
    expect(canonicalJsonStringify(["z", "a"])).toBe('["z","a"]');
    expect(canonicalJsonStringify(["a", "z"])).not.toBe(
      canonicalJsonStringify(["z", "a"]),
    );
  });

  test("serializes primitives via JSON.stringify", () => {
    expect(canonicalJsonStringify(null)).toBe("null");
    expect(canonicalJsonStringify(42)).toBe("42");
    expect(canonicalJsonStringify("hi")).toBe('"hi"');
    expect(canonicalJsonStringify(true)).toBe("true");
  });
});

describe("computeWireDefinitionHash", () => {
  test("is stable across key-ordering differences", async () => {
    const a = { id: "w-1", stepOrder: ["s1"], steps: { s1: { kind: "step" } } };
    const b = { steps: { s1: { kind: "step" } }, stepOrder: ["s1"], id: "w-1" };
    expect(await computeWireDefinitionHash(a)).toBe(
      await computeWireDefinitionHash(b),
    );
  });

  test("differs across different definitions", async () => {
    const a = { id: "w-1", stepOrder: ["s1"], steps: { s1: {} } };
    const b = { id: "w-2", stepOrder: ["s1"], steps: { s1: {} } };
    expect(await computeWireDefinitionHash(a)).not.toBe(
      await computeWireDefinitionHash(b),
    );
  });

  test("matches the pinned known-vector hash", async () => {
    // Snapshot of the pre-move output; proves the moved implementation is
    // byte-identical to the sidecar's original. Reordering the keys of the
    // same definition must yield the same digest.
    const definition = {
      id: "w-1",
      stepOrder: ["s1", "s2"],
      steps: { s2: { kind: "b" }, s1: { kind: "a" } },
    };
    const reordered = {
      steps: { s1: { kind: "a" }, s2: { kind: "b" } },
      stepOrder: ["s1", "s2"],
      id: "w-1",
    };
    const expected =
      "50b2f0c5a74e704dbc1dc82af4c782b3eed636a89865ce84976dcc77c3ed2dfc";
    expect(await computeWireDefinitionHash(definition)).toBe(expected);
    expect(await computeWireDefinitionHash(reordered)).toBe(expected);
  });
});
