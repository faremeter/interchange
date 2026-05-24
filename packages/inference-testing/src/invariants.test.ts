import { describe, test, expect } from "bun:test";

import type {
  ContentBlock,
  InferenceEvent,
  PartialMessage,
  TokenUsage,
} from "@intx/types/runtime";

import { INVARIANTS, formatEventBrief, type Invariant } from "./invariants";

const EMPTY_PARTIAL: PartialMessage = { text: "" };
const ZERO_USAGE: TokenUsage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

function findInvariant(name: string): Invariant {
  const found = INVARIANTS.find((inv) => inv.name === name);
  if (found === undefined) throw new Error(`unknown invariant: ${name}`);
  return found;
}

function startEvent(seq = 0): InferenceEvent {
  return { type: "inference.start", seq, data: { model: "test" } };
}

function doneEvent(
  seq: number,
  content: ContentBlock[] = [],
  usage: TokenUsage = ZERO_USAGE,
): InferenceEvent {
  return {
    type: "inference.done",
    seq,
    data: {
      turn: { role: "assistant", content, model: "test", timestamp: 0 },
      usage,
    },
  };
}

function errorEvent(seq: number): InferenceEvent {
  return {
    type: "inference.error",
    seq,
    data: {
      error: { category: "retryable", message: "x" },
      partial: EMPTY_PARTIAL,
    },
  };
}

function usageEvent(seq: number, usage: TokenUsage): InferenceEvent {
  return { type: "inference.usage", seq, data: { usage } };
}

function textDelta(seq: number, token: string, index?: number): InferenceEvent {
  return {
    type: "inference.text.delta",
    seq,
    data: {
      token,
      partial: EMPTY_PARTIAL,
      ...(index !== undefined && { index }),
    },
  };
}

function thinkingDelta(
  seq: number,
  token: string,
  index?: number,
): InferenceEvent {
  return {
    type: "inference.thinking.delta",
    seq,
    data: {
      token,
      partial: EMPTY_PARTIAL,
      ...(index !== undefined && { index }),
    },
  };
}

function thinkingSig(
  seq: number,
  signature: string,
  index?: number,
): InferenceEvent {
  return {
    type: "inference.thinking.signature",
    seq,
    data: { signature, ...(index !== undefined && { index }) },
  };
}

function toolStart(
  seq: number,
  callId: string,
  name: string,
  index?: number,
): InferenceEvent {
  return {
    type: "inference.tool_call.start",
    seq,
    data: {
      callId,
      name,
      partial: EMPTY_PARTIAL,
      ...(index !== undefined && { index }),
    },
  };
}

function toolEnd(
  seq: number,
  callId: string,
  name: string,
  args: Record<string, unknown>,
  index?: number,
): InferenceEvent {
  return {
    type: "inference.tool_call.end",
    seq,
    data: {
      callId,
      name,
      arguments: args,
      partial: EMPTY_PARTIAL,
      ...(index !== undefined && { index }),
    },
  };
}

describe("INVARIANTS list — basic shape", () => {
  test("exports a non-empty readonly array", () => {
    expect(INVARIANTS.length).toBeGreaterThan(0);
  });

  test("each invariant has a unique name", () => {
    const names = INVARIANTS.map((inv) => inv.name);
    expect(new Set(names).size).toBe(names.length);
  });

  test("schema_validity is first (foundational ordering)", () => {
    expect(INVARIANTS[0]?.name).toBe("schema_validity");
  });
});

describe("formatEventBrief — payload elision", () => {
  test("elides long text-delta tokens", () => {
    const long = "x".repeat(500);
    const out = formatEventBrief(textDelta(0, long));
    expect(out).not.toContain(long);
    expect(out).toContain("<500 chars>");
  });

  test("never includes the raw redacted-thinking data blob", () => {
    const blob = "EncryptedOpaqueBlob".repeat(100);
    const event: InferenceEvent = {
      type: "inference.thinking.redacted",
      seq: 1,
      data: { redactedThinking: { type: "redacted_thinking", data: blob } },
    };
    const out = formatEventBrief(event);
    expect(out).not.toContain(blob);
  });

  test("never includes raw image base64 data in image_output", () => {
    const blob = "A".repeat(2000);
    const event: InferenceEvent = {
      type: "inference.image_output",
      seq: 1,
      data: {
        image: {
          type: "image",
          source: { kind: "base64", mimeType: "image/png", data: blob },
        },
      },
    };
    const out = formatEventBrief(event);
    expect(out).not.toContain(blob);
    expect(out).toContain("mime=image/png");
  });

  test("elides a long file-reference URI on image_output", () => {
    // Signed URLs from provider file APIs can run long; the reference
    // field must elide just like inline base64 does.
    const longReference = `https://example.com/signed/${"a".repeat(500)}`;
    const event: InferenceEvent = {
      type: "inference.image_output",
      seq: 1,
      data: {
        image: {
          type: "image",
          source: {
            kind: "file-reference",
            mimeType: "image/png",
            reference: longReference,
          },
        },
      },
    };
    const out = formatEventBrief(event);
    expect(out).not.toContain(longReference);
    expect(out).toContain("file-reference");
  });

  test("elides citedText on inference.citation", () => {
    const long = "lorem ipsum ".repeat(50);
    const event: InferenceEvent = {
      type: "inference.citation",
      seq: 1,
      data: {
        citation: {
          type: "citation",
          citedText: long,
          source: { uri: "https://example.com/article" },
        },
      },
    };
    const out = formatEventBrief(event);
    expect(out).not.toContain(long);
    expect(out).toContain("citedText");
  });

  test("elides code on inference.code_execution.start", () => {
    const longCode = "x = 1\n".repeat(100);
    const event: InferenceEvent = {
      type: "inference.code_execution.start",
      seq: 1,
      data: {
        request: {
          type: "code_execution_request",
          id: "srvtoolu_01",
          code: longCode,
        },
      },
    };
    const out = formatEventBrief(event);
    expect(out).not.toContain(longCode);
    expect(out).toContain("code:");
  });

  test("summarizes inference.code_execution.result without dumping stdout", () => {
    const event: InferenceEvent = {
      type: "inference.code_execution.result",
      seq: 1,
      data: {
        result: {
          type: "code_execution_result",
          requestId: "srvtoolu_01",
          status: "ok",
          stdout: "x".repeat(1000),
        },
      },
    };
    const out = formatEventBrief(event);
    expect(out).not.toContain("x".repeat(1000));
    expect(out).toContain("srvtoolu_01");
    expect(out).toContain("ok");
  });
});

describe("schema_validity invariant", () => {
  const inv = findInvariant("schema_validity");

  test("passes on an empty array", () => {
    expect(inv.check([])).toEqual([]);
  });

  test("passes on a well-formed sequence", () => {
    expect(inv.check([startEvent(0), textDelta(1, "x"), doneEvent(2)])).toEqual(
      [],
    );
  });

  test("flags an event that fails arktype validation", () => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- modeling an adapter bug that produces a structurally-typed but runtime-invalid event (missing required `partial` field)
    const bad = {
      type: "inference.text.delta",
      seq: 0,
      data: { token: "x" },
    } as unknown as InferenceEvent;
    const violations = inv.check([bad]);
    expect(violations.length).toBeGreaterThan(0);
    expect(violations[0]?.invariant).toBe("schema_validity");
  });
});

describe("tool_call_pairing invariant", () => {
  const inv = findInvariant("tool_call_pairing");

  test("passes on an empty array", () => {
    expect(inv.check([])).toEqual([]);
  });

  test("passes when every start has a matching end", () => {
    expect(
      inv.check([
        startEvent(0),
        toolStart(1, "call_a", "search"),
        toolEnd(2, "call_a", "search", { q: "x" }),
        doneEvent(3),
      ]),
    ).toEqual([]);
  });

  test("flags a start without an end", () => {
    const violations = inv.check([
      toolStart(0, "call_orphan", "search"),
      doneEvent(1),
    ]);
    expect(violations.some((v) => v.message.includes("call_orphan"))).toBe(
      true,
    );
  });

  test("flags an end without a start (orphan end)", () => {
    const violations = inv.check([
      toolEnd(0, "call_phantom", "search", {}),
      doneEvent(1),
    ]);
    expect(
      violations.some((v) =>
        v.message.includes("no preceding tool_call.start"),
      ),
    ).toBe(true);
  });

  test("flags a duplicate tool_call.start for the same callId", () => {
    // Two starts with the same callId is structurally invalid; the
    // wire emits one start per call.
    const violations = inv.check([
      toolStart(0, "call_dup", "search"),
      toolStart(1, "call_dup", "search"),
      toolEnd(2, "call_dup", "search", {}),
      doneEvent(3),
    ]);
    expect(
      violations.some(
        (v) =>
          v.message.includes("call_dup") && v.message.includes("appears 2"),
      ),
    ).toBe(true);
  });

  test("flags a duplicate tool_call.end for the same callId", () => {
    const violations = inv.check([
      toolStart(0, "call_a", "search"),
      toolEnd(1, "call_a", "search", {}),
      toolEnd(2, "call_a", "search", {}),
      doneEvent(3),
    ]);
    expect(
      violations.some(
        (v) =>
          v.message.includes("tool_call.end") &&
          v.message.includes("appears 2"),
      ),
    ).toBe(true);
  });
});

describe("terminal_exclusivity invariant", () => {
  const inv = findInvariant("terminal_exclusivity");

  test("flags an empty array as missing terminal", () => {
    const violations = inv.check([]);
    expect(
      violations.some((v) => v.message.includes("no terminal event")),
    ).toBe(true);
  });

  test("passes with exactly one inference.done", () => {
    expect(inv.check([startEvent(0), doneEvent(1)])).toEqual([]);
  });

  test("passes with exactly one inference.error", () => {
    expect(inv.check([startEvent(0), errorEvent(1)])).toEqual([]);
  });

  test("flags both done and error appearing", () => {
    const violations = inv.check([startEvent(0), doneEvent(1), errorEvent(2)]);
    expect(
      violations.some((v) =>
        v.message.includes("both inference.done and inference.error"),
      ),
    ).toBe(true);
  });

  test("flags multiple done events", () => {
    const violations = inv.check([startEvent(0), doneEvent(1), doneEvent(2)]);
    expect(
      violations.some((v) => v.message.includes("multiple inference.done")),
    ).toBe(true);
  });
});

describe("usage_coherence_monotonic_non_decreasing invariant", () => {
  const inv = findInvariant("usage_coherence_monotonic_non_decreasing");

  test("passes on an empty array", () => {
    expect(inv.check([])).toEqual([]);
  });

  test("passes on monotonically growing usage", () => {
    expect(
      inv.check([
        usageEvent(0, { ...ZERO_USAGE, input: 10 }),
        usageEvent(1, { ...ZERO_USAGE, input: 10, output: 5 }),
        doneEvent(2, [], { ...ZERO_USAGE, input: 10, output: 12 }),
      ]),
    ).toEqual([]);
  });

  test("flags negative usage fields", () => {
    const violations = inv.check([
      usageEvent(0, { ...ZERO_USAGE, output: -1 }),
    ]);
    expect(violations.some((v) => v.message.includes("negative"))).toBe(true);
  });

  test("flags NaN usage", () => {
    const violations = inv.check([
      usageEvent(0, { ...ZERO_USAGE, input: NaN }),
    ]);
    expect(violations.some((v) => v.message.includes("not a finite"))).toBe(
      true,
    );
  });

  test("flags decreasing input across events", () => {
    const violations = inv.check([
      usageEvent(0, { ...ZERO_USAGE, input: 100 }),
      usageEvent(1, { ...ZERO_USAGE, input: 50 }),
    ]);
    expect(violations.some((v) => v.message.includes("decreased"))).toBe(true);
  });
});

describe("recognized_content_blocks invariant", () => {
  const inv = findInvariant("recognized_content_blocks");

  test("passes on an empty array", () => {
    expect(inv.check([])).toEqual([]);
  });

  test("passes when all blocks have known types", () => {
    expect(inv.check([doneEvent(0, [{ type: "text", text: "hi" }])])).toEqual(
      [],
    );
  });

  test("flags an unknown block type in inference.done content", () => {
    const sneaky = { type: "fictional_block_type", text: "x" };
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- modeling an adapter bug that emits a content block whose type isn't part of the ContentBlock union
    const event = {
      type: "inference.done" as const,
      seq: 0,
      data: {
        turn: {
          role: "assistant" as const,
          content: [sneaky],
          model: "test",
          timestamp: 0,
        },
        usage: ZERO_USAGE,
      },
    } as unknown as InferenceEvent;
    const violations = inv.check([event]);
    expect(
      violations.some((v) => v.message.includes("fictional_block_type")),
    ).toBe(true);
  });
});

describe("tool_args_parse_as_json invariant", () => {
  const inv = findInvariant("tool_args_parse_as_json");

  test("passes on an empty array", () => {
    expect(inv.check([])).toEqual([]);
  });

  test("passes on parsed arguments", () => {
    expect(
      inv.check([toolEnd(0, "call_a", "search", { q: "x" }), doneEvent(1)]),
    ).toEqual([]);
  });

  test("flags arguments wrapped in _raw (unparseable)", () => {
    const violations = inv.check([
      toolEnd(0, "call_a", "search", { _raw: '{"unterminated' }),
    ]);
    expect(violations.some((v) => v.message.includes("_raw"))).toBe(true);
  });
});

describe("redacted_thinking_data_non_empty invariant", () => {
  const inv = findInvariant("redacted_thinking_data_non_empty");

  test("passes on an empty array", () => {
    expect(inv.check([])).toEqual([]);
  });

  test("passes when a thinking block has no signature (non-Anthropic providers)", () => {
    // OpenCode-Zen / kimi emits reasoning_content thinking without a
    // signature; the invariant deliberately does NOT flag this.
    expect(
      inv.check([doneEvent(0, [{ type: "thinking", thinking: "hmm" }])]),
    ).toEqual([]);
  });

  test("passes when a redacted_thinking block has a non-empty data blob", () => {
    expect(
      inv.check([
        doneEvent(0, [{ type: "redacted_thinking", data: "encrypted-blob" }]),
      ]),
    ).toEqual([]);
  });

  test("flags a redacted_thinking block with empty data", () => {
    const violations = inv.check([
      doneEvent(0, [{ type: "redacted_thinking", data: "" }]),
    ]);
    expect(violations.some((v) => v.message.includes("empty data"))).toBe(true);
  });

  test("flags an inference.thinking.redacted streaming event with empty data", () => {
    // Streaming consumers subscribing to the event before the finalized
    // turn arrives need the data populated even more than done consumers
    // — an empty blob silently corrupts downstream state.
    const event: InferenceEvent = {
      type: "inference.thinking.redacted",
      seq: 0,
      data: { redactedThinking: { type: "redacted_thinking", data: "" } },
    };
    const violations = inv.check([event]);
    expect(violations.some((v) => v.message.includes("empty data"))).toBe(true);
  });
});

describe("index_density invariant", () => {
  const inv = findInvariant("index_density");

  test("passes on an empty array", () => {
    expect(inv.check([])).toEqual([]);
  });

  test("passes when no events use index (single-block scenario)", () => {
    expect(inv.check([textDelta(0, "Hello"), textDelta(1, " world")])).toEqual(
      [],
    );
  });

  test("passes when indices are dense from 0", () => {
    expect(
      inv.check([
        textDelta(0, "first", 0),
        textDelta(1, "second", 1),
        textDelta(2, "third", 2),
      ]),
    ).toEqual([]);
  });

  test("flags mixed-mode (some events with index, some without)", () => {
    const violations = inv.check([
      textDelta(0, "first", 0),
      textDelta(1, "second"), // no index
    ]);
    expect(violations.some((v) => v.message.includes("mixes"))).toBe(true);
  });

  test("flags non-dense indices (gap)", () => {
    const violations = inv.check([
      textDelta(0, "first", 0),
      textDelta(1, "third", 2), // skips 1
    ]);
    expect(violations.some((v) => v.message.includes("not dense"))).toBe(true);
  });

  test("separately clusters by tool callId", () => {
    // Each tool call is its own cluster, so each carrying index 0 is fine.
    expect(
      inv.check([
        toolStart(0, "call_a", "f", 0),
        toolEnd(1, "call_a", "f", {}, 0),
        toolStart(2, "call_b", "g", 0),
        toolEnd(3, "call_b", "g", {}, 0),
      ]),
    ).toEqual([]);
  });
});

describe("signature_precedence invariant", () => {
  const inv = findInvariant("signature_precedence");

  test("passes on an empty array", () => {
    expect(inv.check([])).toEqual([]);
  });

  test("passes when a signature follows a delta with the same index", () => {
    expect(
      inv.check([thinkingDelta(0, "thought", 0), thinkingSig(1, "sig_xyz", 0)]),
    ).toEqual([]);
  });

  test("passes when single-block (no index) ordering holds", () => {
    expect(
      inv.check([thinkingDelta(0, "thought"), thinkingSig(1, "sig_xyz")]),
    ).toEqual([]);
  });

  test("flags a signature with no preceding delta at the same index", () => {
    const violations = inv.check([
      thinkingDelta(0, "thought-for-block-0", 0),
      // signature is for block 1, but no delta at index 1 precedes it
      thinkingSig(1, "sig_for_block_1", 1),
    ]);
    expect(
      violations.some((v) =>
        v.message.includes("no preceding inference.thinking.delta"),
      ),
    ).toBe(true);
  });
});
