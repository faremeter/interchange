import { describe, test, expect } from "bun:test";
import { reconstructResponseFromSSE } from "./sse";

function sse(chunks: unknown[]): Uint8Array {
  const body = chunks.map((c) => `data: ${JSON.stringify(c)}\n\n`).join("");
  return new TextEncoder().encode(body);
}

describe("reconstructResponseFromSSE", () => {
  test("reconstructs a single-chunk functionCall with its thoughtSignature", () => {
    const bytes = sse([
      {
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  functionCall: {
                    name: "get_weather",
                    args: { location: "Boston, MA" },
                  },
                  thoughtSignature: "sig-1",
                },
              ],
            },
          },
        ],
      },
    ]);
    expect(reconstructResponseFromSSE(bytes)).toEqual({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              {
                functionCall: {
                  name: "get_weather",
                  args: { location: "Boston, MA" },
                },
                thoughtSignature: "sig-1",
              },
            ],
          },
        },
      ],
    });
  });

  test("coalesces consecutive plain-text deltas", () => {
    const bytes = sse([
      {
        candidates: [{ content: { role: "model", parts: [{ text: "The " }] } }],
      },
      {
        candidates: [
          { content: { role: "model", parts: [{ text: "capital." }] } },
        ],
      },
    ]);
    expect(reconstructResponseFromSSE(bytes)).toEqual({
      candidates: [
        { content: { role: "model", parts: [{ text: "The capital." }] } },
      ],
    });
  });

  test("keeps a thought-text part and a functionCall part separate", () => {
    const bytes = sse([
      {
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "Let me think.", thought: true }],
            },
          },
        ],
      },
      {
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  functionCall: { name: "get_weather", args: {} },
                  thoughtSignature: "sig-2",
                },
              ],
            },
          },
        ],
      },
    ]);
    expect(reconstructResponseFromSSE(bytes)).toEqual({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              { text: "Let me think.", thought: true },
              {
                functionCall: { name: "get_weather", args: {} },
                thoughtSignature: "sig-2",
              },
            ],
          },
        },
      ],
    });
  });

  test("coalesces split thought text without bleeding into the functionCall", () => {
    const bytes = sse([
      {
        candidates: [
          { content: { role: "model", parts: [{ text: "A", thought: true }] } },
        ],
      },
      {
        candidates: [
          { content: { role: "model", parts: [{ text: "B", thought: true }] } },
        ],
      },
      {
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ functionCall: { name: "f", args: {} } }],
            },
          },
        ],
      },
    ]);
    expect(reconstructResponseFromSSE(bytes)).toEqual({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              { text: "AB", thought: true },
              { functionCall: { name: "f", args: {} } },
            ],
          },
        },
      ],
    });
  });

  test("does not merge plain text with thought text across shapes", () => {
    const bytes = sse([
      {
        candidates: [
          { content: { role: "model", parts: [{ text: "visible" }] } },
        ],
      },
      {
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "thinky", thought: true }],
            },
          },
        ],
      },
    ]);
    expect(reconstructResponseFromSSE(bytes)).toEqual({
      candidates: [
        {
          content: {
            role: "model",
            parts: [{ text: "visible" }, { text: "thinky", thought: true }],
          },
        },
      ],
    });
  });

  test("a thoughtSignature-bearing text part is emitted as-is and breaks a text run", () => {
    const bytes = sse([
      { candidates: [{ content: { role: "model", parts: [{ text: "a" }] } }] },
      {
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "b", thoughtSignature: "sig" }],
            },
          },
        ],
      },
      { candidates: [{ content: { role: "model", parts: [{ text: "c" }] } }] },
    ]);
    expect(reconstructResponseFromSSE(bytes)).toEqual({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              { text: "a" },
              { text: "b", thoughtSignature: "sig" },
              { text: "c" },
            ],
          },
        },
      ],
    });
  });

  test("skips trailing usage-only chunks", () => {
    const bytes = sse([
      { candidates: [{ content: { role: "model", parts: [{ text: "hi" }] } }] },
      { usageMetadata: { totalTokenCount: 5 } },
    ]);
    expect(reconstructResponseFromSSE(bytes)).toEqual({
      candidates: [{ content: { role: "model", parts: [{ text: "hi" }] } }],
    });
  });

  test("throws on a malformed chunk", () => {
    const bytes = sse([{ candidates: "nope" }]);
    expect(() => reconstructResponseFromSSE(bytes)).toThrow(/candidates/);
  });

  test("throws when the stream carries no data payloads", () => {
    const bytes = new TextEncoder().encode(": comment line\n\n");
    expect(() => reconstructResponseFromSSE(bytes)).toThrow(/no data payloads/);
  });

  test("throws when no content chunk carries a role", () => {
    const bytes = sse([
      { candidates: [{ content: { parts: [{ text: "hi" }] } }] },
    ]);
    expect(() => reconstructResponseFromSSE(bytes)).toThrow(/carried a role/);
  });
});
