// Tests for the friendlier `ReplyOnceToolCall` shape accepted by
// `scenario.replyOnce`. The original shape — `{ callId, name, argsJSON }` —
// requires the caller to stringify args and invent a callId. The new
// shape — `{ name, args, callId? }` — auto-stringifies and auto-IDs.
// Both shapes must continue to work, and they must be mixable in the
// same array (a test can pin a single callId for assertion while
// letting the others auto-generate).

import { describe, test, expect } from "bun:test";

import { setupHarness } from "./harness";

async function drainResponse(response: Response): Promise<string> {
  const body = response.body;
  if (body === null) throw new Error("response body is null");
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let out = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value !== undefined) out += decoder.decode(value, { stream: true });
  }
  out += decoder.decode();
  return out;
}

type ExtractedToolCall = {
  id: string | undefined;
  name: string | undefined;
  argumentsConcat: string;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// Pull every `tool_calls` delta out of an OpenAI SSE response stream
// and concatenate the per-tool fields the test cares about. The shape
// matches the openai wire DSL emitted by `wire.completeResponse`. The
// function is purely defensive: every level of the nested JSON is
// shape-checked before drilling down, so a wire-format change surfaces
// here as missing data rather than a crashing cast.
async function extractOpenAIToolCalls(
  response: Response,
): Promise<ExtractedToolCall[]> {
  const text = await drainResponse(response);
  const byIndex = new Map<number, ExtractedToolCall>();
  for (const line of text.split("\n")) {
    if (!line.startsWith("data: ")) continue;
    const payload = line.slice("data: ".length);
    if (payload === "[DONE]") continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(payload);
    } catch {
      continue;
    }
    if (!isRecord(parsed)) continue;
    const choices = parsed["choices"];
    if (!Array.isArray(choices)) continue;
    const choice0: unknown = choices[0];
    if (!isRecord(choice0)) continue;
    const delta = choice0["delta"];
    if (!isRecord(delta)) continue;
    const tools = delta["tool_calls"];
    if (!Array.isArray(tools)) continue;
    for (const tc of tools) {
      if (!isRecord(tc)) continue;
      const idx = tc["index"];
      if (typeof idx !== "number") continue;
      const entry: ExtractedToolCall = byIndex.get(idx) ?? {
        id: undefined,
        name: undefined,
        argumentsConcat: "",
      };
      const idVal = tc["id"];
      if (typeof idVal === "string") entry.id = idVal;
      const fn = tc["function"];
      if (isRecord(fn)) {
        const nameVal = fn["name"];
        if (typeof nameVal === "string") entry.name = nameVal;
        const argsVal = fn["arguments"];
        if (typeof argsVal === "string") entry.argumentsConcat += argsVal;
      }
      byIndex.set(idx, entry);
    }
  }
  const out: ExtractedToolCall[] = [];
  const indices = [...byIndex.keys()].sort((a, b) => a - b);
  for (const i of indices) {
    const entry = byIndex.get(i);
    if (entry !== undefined) out.push(entry);
  }
  return out;
}

describe("Scenario.replyOnce — ReplyOnceToolCall shape", () => {
  test("explicit { callId, name, argsJSON } shape (unchanged)", async () => {
    const harness = setupHarness();
    try {
      harness.scenario.replyOnce("openai", {
        toolCalls: [
          {
            callId: "call_explicit",
            name: "searchTool",
            argsJSON: JSON.stringify({ q: "hello" }),
          },
        ],
      });
      const f = harness.deps.fetch("https://example/x");
      await harness.run();
      const r = await f;
      const tools = await extractOpenAIToolCalls(r);
      expect(tools).toHaveLength(1);
      const t = tools[0];
      if (t === undefined) throw new Error("expected one tool");
      expect(t.id).toBe("call_explicit");
      expect(t.name).toBe("searchTool");
      expect(JSON.parse(t.argumentsConcat)).toEqual({ q: "hello" });
    } finally {
      harness.dispose();
    }
  });

  test("friendlier { name, args } shape auto-stringifies args and auto-generates callId", async () => {
    const harness = setupHarness();
    try {
      harness.scenario.replyOnce("openai", {
        toolCalls: [
          {
            name: "proposeTask",
            args: { idHint: "1a-greet", level: 1, dependsOn: [] },
          },
        ],
      });
      const f = harness.deps.fetch("https://example/x");
      await harness.run();
      const r = await f;
      const tools = await extractOpenAIToolCalls(r);
      expect(tools).toHaveLength(1);
      const t = tools[0];
      if (t === undefined) throw new Error("expected one tool");
      expect(t.id).not.toBeUndefined();
      // Auto-generated callId matches the harness's `call_auto_<n>`
      // shape so tests can pattern-match if they need to.
      expect(t.id).toMatch(/^call_auto_\d+$/);
      expect(t.name).toBe("proposeTask");
      expect(JSON.parse(t.argumentsConcat)).toEqual({
        idHint: "1a-greet",
        level: 1,
        dependsOn: [],
      });
    } finally {
      harness.dispose();
    }
  });

  test("friendlier shape honours an explicit callId when supplied", async () => {
    const harness = setupHarness();
    try {
      harness.scenario.replyOnce("openai", {
        toolCalls: [
          {
            name: "finalizePlan",
            args: { verificationMode: "baseline-equality" },
            callId: "call_finalize_pinned",
          },
        ],
      });
      const f = harness.deps.fetch("https://example/x");
      await harness.run();
      const r = await f;
      const tools = await extractOpenAIToolCalls(r);
      expect(tools).toHaveLength(1);
      const t = tools[0];
      if (t === undefined) throw new Error("expected one tool");
      expect(t.id).toBe("call_finalize_pinned");
      expect(t.name).toBe("finalizePlan");
    } finally {
      harness.dispose();
    }
  });

  test("explicit and friendlier shapes can be mixed in the same array", async () => {
    const harness = setupHarness();
    try {
      harness.scenario.replyOnce("openai", {
        toolCalls: [
          {
            callId: "call_pin_a",
            name: "proposeTask",
            argsJSON: JSON.stringify({ idHint: "a" }),
          },
          {
            name: "proposeTask",
            args: { idHint: "b" },
          },
          {
            callId: "call_pin_c",
            name: "finalizePlan",
            argsJSON: JSON.stringify({}),
          },
        ],
      });
      const f = harness.deps.fetch("https://example/x");
      await harness.run();
      const r = await f;
      const tools = await extractOpenAIToolCalls(r);
      expect(tools).toHaveLength(3);
      const [t0, t1, t2] = tools;
      if (t0 === undefined || t1 === undefined || t2 === undefined) {
        throw new Error("expected three tools");
      }
      expect(t0.id).toBe("call_pin_a");
      expect(JSON.parse(t0.argumentsConcat)).toEqual({ idHint: "a" });
      expect(t1.id).toMatch(/^call_auto_\d+$/);
      expect(JSON.parse(t1.argumentsConcat)).toEqual({ idHint: "b" });
      expect(t2.id).toBe("call_pin_c");
      expect(JSON.parse(t2.argumentsConcat)).toEqual({});
    } finally {
      harness.dispose();
    }
  });

  test("anthropic provider accepts the friendlier shape too", async () => {
    // Body shape diverges across providers but the input contract is
    // the same. Sanity-check that anthropic dispatch doesn't trip on
    // the auto-generated callId or the JSON.stringify'd args.
    const harness = setupHarness();
    try {
      harness.scenario.replyOnce("anthropic", {
        toolCalls: [
          {
            name: "search",
            args: { q: "anthropic" },
          },
        ],
        headUsage: {
          input: 10,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          thinking: 0,
        },
        tailUsage: {
          input: 0,
          output: 5,
          cacheRead: 0,
          cacheWrite: 0,
          thinking: 0,
        },
      });
      const f = harness.deps.fetch("https://example/x");
      await harness.run();
      const r = await f;
      const text = await drainResponse(r);
      // Anthropic's tool_use block carries the auto-generated id and
      // name; the input_json_delta carries the stringified args.
      expect(text).toContain('"name":"search"');
      expect(text).toMatch(/"id":"call_auto_\d+"/);
      expect(text).toContain('"partial_json":"{\\"q\\":\\"anthropic\\"}"');
    } finally {
      harness.dispose();
    }
  });

  test("pinned callId starting with the reserved `call_auto_` prefix is rejected", () => {
    // The harness mints `call_auto_<n>` ids for the friendlier
    // `{ name, args }` shape. A pinned callId in either shape that
    // shadows that namespace would collide with later auto-generated
    // ids on the same run; reject at registration rather than wait
    // for the mid-stream confusion.
    const harness = setupHarness();
    try {
      expect(() =>
        harness.scenario.replyOnce("openai", {
          toolCalls: [
            {
              callId: "call_auto_999",
              name: "search",
              argsJSON: JSON.stringify({ q: "x" }),
            },
          ],
        }),
      ).toThrow(/reserved.*prefix/);

      expect(() =>
        harness.scenario.replyOnce("openai", {
          toolCalls: [
            {
              callId: "call_auto_pinned",
              name: "search",
              args: { q: "x" },
            },
          ],
        }),
      ).toThrow(/reserved.*prefix/);
    } finally {
      harness.dispose();
    }
  });
});
