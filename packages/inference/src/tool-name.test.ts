import { describe, expect, test } from "bun:test";

import {
  decodeToolName,
  encodeToolName,
  type ToolNameLimit,
} from "./tool-name";

const OPENAI: ToolNameLimit = { provider: "openai", maxLength: 64 };
const TIGHT: ToolNameLimit = { provider: "tight", maxLength: 8 };

// Package-qualified ids: the namespace segment charset `[A-Za-z0-9._-]` plus
// the `@`, `/`, and `:` separators. These carry out-of-charset characters and
// must be rewritten.
const REWRITTEN_NAMES = [
  "@intx/tools-posix/sidecar-bundle:run_shell",
  "@intx/tools-mail/sidecar-bundle:send_message",
  "@intx/tools-lsp/sidecar-bundle:find_references",
  "tools-posix/sidecar-bundle:run_shell",
  "a.b.c",
];

// Names already valid on the wire. The codec must leave these untouched, in
// both directions — a provider echoes back exactly what it was sent.
const WIRE_SAFE_NAMES = [
  "toolA",
  "run_shell",
  "get_weather",
  "noisy-d",
  "agentATool",
  "_internal",
];

const WIRE_CHARSET = /^[A-Za-z0-9_-]+$/;

describe("encodeToolName", () => {
  test("leaves already-valid names untouched", () => {
    for (const name of WIRE_SAFE_NAMES) {
      expect(encodeToolName(name, OPENAI)).toBe(name);
    }
  });

  test("rewrites out-of-charset names to a letter-leading wire-safe form", () => {
    for (const name of REWRITTEN_NAMES) {
      const wire = encodeToolName(name, OPENAI);
      expect(wire).toMatch(WIRE_CHARSET);
      expect(wire.charAt(0)).toMatch(/[A-Za-z]/);
      expect(wire).not.toBe(name);
    }
  });

  test("escapes each out-of-charset character as its uppercase hex byte", () => {
    expect(
      encodeToolName("@intx/tools-posix/sidecar-bundle:run_shell", OPENAI),
    ).toBe("IX_-40intx-2Ftools-2Dposix-2Fsidecar-2Dbundle-3Arun_shell");
  });

  test("rewrites a leading-digit name so it is valid where a letter lead is required", () => {
    expect(encodeToolName("2fast", OPENAI)).toBe("IX_2fast");
  });

  test("force-encodes a valid name that collides with the marker prefix", () => {
    // Otherwise it would pass through, and decode would strip the marker.
    expect(encodeToolName("IX_foo", OPENAI)).toBe("IX_IX_foo");
  });

  test("throws with an actionable message when the encoding exceeds the limit", () => {
    expect(() =>
      encodeToolName("@intx/tools-posix/sidecar-bundle:run_shell", TIGHT),
    ).toThrow(/exceeds the 8-char limit for provider "tight"/);
  });

  test("throws naming the source tool name so the diagnostic is fixable", () => {
    expect(() => encodeToolName("a/b/c/d/e/f", TIGHT)).toThrow(
      /a\/b\/c\/d\/e\/f/,
    );
  });

  test("throws when an already-valid name exceeds the limit on the passthrough path", () => {
    // A wire-safe name is never rewritten, but it can still be too long; the
    // length check must cover the passthrough path, not just rewrites.
    expect(() => encodeToolName("aaaaaaaaaaaa", TIGHT)).toThrow(
      /exceeds the 8-char limit for provider "tight"/,
    );
  });
});

describe("decodeToolName", () => {
  test("round-trips every rewritten name back to the exact original", () => {
    for (const name of [...REWRITTEN_NAMES, "2fast", "IX_foo"]) {
      expect(decodeToolName(encodeToolName(name, OPENAI))).toBe(name);
    }
  });

  test("returns a wire-safe name unchanged in both directions", () => {
    for (const name of WIRE_SAFE_NAMES) {
      // A provider echoes the name it was sent; a name that never needed
      // encoding must survive decode verbatim. `toolA` starts with the letter
      // the marker escape once collided on — pin that it is not mangled.
      expect(decodeToolName(name)).toBe(name);
    }
  });

  test("passes through a marker-prefixed name that is not a valid encoding", () => {
    // Malformed hex escape.
    expect(decodeToolName("IX_-zz")).toBe("IX_-zz");
    // Lowercase hex is never emitted, so this is not one of our encodings; it
    // still decodes structurally, but the passthrough guard is on the marker.
    expect(decodeToolName("IX_-2f")).toBe("/");
  });

  test("passes through an out-of-charset name unchanged", () => {
    expect(decodeToolName("@intx/tools-posix/sidecar-bundle:run_shell")).toBe(
      "@intx/tools-posix/sidecar-bundle:run_shell",
    );
  });
});
