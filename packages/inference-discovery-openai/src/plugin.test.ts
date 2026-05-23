import { describe, test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import path from "node:path";
import {
  INTENTS,
  SUPPORT_MATRIX,
  getFixtureDir,
  type Capability,
  type SupportEntry,
} from "@intx/inference-discovery/catalog";
import type { CaptureStep, CapturedResponse } from "@intx/inference-discovery";
import { createOpencodeZenPlugin } from "./index";
import { extractReasoningTrace } from "./deployments/opencode-zen";
import { createOpenaiIterator } from "./protocol/iterator";
import { buildRequestBody } from "./protocol/body";

const REPO_ROOT = path.resolve(import.meta.dirname, "..", "..", "..");

const TEST_API_KEY = "test-key";
const TEST_BASE_URL = "https://opencode.ai/zen/go/v1";
const TEST_CHAT_URL = `${TEST_BASE_URL}/chat/completions`;

const MULTI_TURN_CAPABILITIES: ReadonlySet<Capability> = new Set<Capability>([
  "function-calling-multi-turn",
]);

function makePlugin() {
  return createOpencodeZenPlugin({
    apiKey: TEST_API_KEY,
    baseUrl: TEST_BASE_URL,
  });
}

function loadFixtureJSON(entry: SupportEntry, ...parts: string[]): unknown {
  const relDir = getFixtureDir(entry);
  if (relDir === null) {
    throw new Error(
      `entry has no fixture dir: ${entry.provider}/${entry.model}/${entry.capability}`,
    );
  }
  const filePath = path.join(REPO_ROOT, relDir, ...parts);
  return JSON.parse(readFileSync(filePath, "utf8"));
}

type Schema =
  | { kind: "scalar" }
  | { kind: "array"; element: Schema }
  | { kind: "object"; fields: Record<string, Schema> }
  | { kind: "empty-array" };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isScalar(value: unknown): boolean {
  return (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  );
}

function mergeSchemas(a: Schema, b: Schema): Schema {
  if (a.kind === "empty-array") return b;
  if (b.kind === "empty-array") return a;
  if (a.kind !== b.kind) {
    throw new Error(
      `incompatible array element schemas: ${a.kind} vs ${b.kind}`,
    );
  }
  if (a.kind === "object" && b.kind === "object") {
    const merged: Record<string, Schema> = { ...a.fields };
    for (const [key, schemaB] of Object.entries(b.fields)) {
      const existing = merged[key];
      merged[key] = existing ? mergeSchemas(existing, schemaB) : schemaB;
    }
    return { kind: "object", fields: merged };
  }
  if (a.kind === "array" && b.kind === "array") {
    return { kind: "array", element: mergeSchemas(a.element, b.element) };
  }
  return a;
}

function extractSchema(value: unknown): Schema {
  if (isScalar(value)) return { kind: "scalar" };
  if (Array.isArray(value)) {
    if (value.length === 0) return { kind: "empty-array" };
    let element: Schema = extractSchema(value[0]);
    for (let i = 1; i < value.length; i++) {
      element = mergeSchemas(element, extractSchema(value[i]));
    }
    return { kind: "array", element };
  }
  if (isRecord(value)) {
    const fields: Record<string, Schema> = {};
    for (const [key, child] of Object.entries(value)) {
      fields[key] = extractSchema(child);
    }
    return { kind: "object", fields };
  }
  throw new Error(`unsupported value type: ${typeof value}`);
}

const EPHEMERAL_KEYS = new Set(["reasoning_content", "name", "index"]);

function pruneEphemeral(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((v) => pruneEphemeral(v));
  }
  if (isRecord(value)) {
    const out: Record<string, unknown> = {};
    for (const [key, child] of Object.entries(value)) {
      if (EPHEMERAL_KEYS.has(key)) continue;
      out[key] = pruneEphemeral(child);
    }
    return out;
  }
  return value;
}

function schemaContains(actual: Schema, expected: Schema): boolean {
  if (expected.kind === "empty-array") {
    return actual.kind === "array" || actual.kind === "empty-array";
  }
  if (actual.kind === "empty-array") {
    return false;
  }
  if (actual.kind !== expected.kind) return false;
  if (actual.kind === "object" && expected.kind === "object") {
    for (const [key, expectedChild] of Object.entries(expected.fields)) {
      const actualChild = actual.fields[key];
      if (!actualChild) return false;
      if (!schemaContains(actualChild, expectedChild)) return false;
    }
    return true;
  }
  if (actual.kind === "array" && expected.kind === "array") {
    return schemaContains(actual.element, expected.element);
  }
  return true;
}

function describeSchema(schema: Schema, indent = 0): string {
  const pad = "  ".repeat(indent);
  if (schema.kind === "object") {
    const entries = Object.entries(schema.fields)
      .map(([k, v]) => `${pad}  ${k}: ${describeSchema(v, indent + 1)}`)
      .join("\n");
    return `{\n${entries}\n${pad}}`;
  }
  if (schema.kind === "array") {
    return `${describeSchema(schema.element, indent)}[]`;
  }
  return schema.kind;
}

const OPENCODE_CAPTURED: SupportEntry[] = SUPPORT_MATRIX.filter(
  (e) => e.provider === "opencode-zen" && e.outcome === "captured",
);

function collectSteps(opts: {
  model: string;
  capability: Capability;
  responses: readonly CapturedResponse[];
}): CaptureStep[] {
  const intent = INTENTS[opts.capability];
  const iter = createOpenaiIterator(TEST_BASE_URL)({
    model: opts.model,
    capability: opts.capability,
    intent,
  });
  const steps: CaptureStep[] = [];
  let i = 0;
  let next = iter.next();
  while (!next.done) {
    steps.push(next.value);
    const response = opts.responses[i];
    i += 1;
    if (response === undefined) break;
    next = iter.next(response);
  }
  return steps;
}

describe("createOpencodeZenPlugin", () => {
  test("exposes the five OpenCode-Zen models", () => {
    const plugin = makePlugin();
    expect(plugin.name).toBe("opencode-zen");
    expect([...plugin.models].sort()).toEqual(
      [
        "deepseek-v4-pro",
        "glm-5.1",
        "kimi-k2.6",
        "mimo-v2-omni",
        "qwen3.6-plus",
      ].sort(),
    );
  });

  test("redacts the Authorization header", () => {
    const plugin = makePlugin();
    expect(plugin.redactRequestHeaders).toContain("authorization");
  });

  test("buildAuthHeaders attaches Bearer token", () => {
    const plugin = makePlugin();
    const headers = plugin.buildAuthHeaders();
    expect(headers.Authorization).toBe("Bearer test-key");
  });
});

describe("buildRequestBody capability dispatch", () => {
  test("plain-text returns user message with no stream flag", () => {
    const body = buildRequestBody({
      model: "kimi-k2.6",
      capability: "plain-text",
      intent: INTENTS["plain-text"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    expect(body.model).toBe("kimi-k2.6");
    expect(body.stream).toBeUndefined();
    const messages = body.messages;
    if (!Array.isArray(messages)) throw new Error("expected messages array");
    expect(messages.length).toBe(1);
  });

  test("plain-text-streaming sets stream: true", () => {
    const body = buildRequestBody({
      model: "kimi-k2.6",
      capability: "plain-text-streaming",
      intent: INTENTS["plain-text-streaming"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    expect(body.stream).toBe(true);
  });

  test("function-calling produces messages + tools", () => {
    const body = buildRequestBody({
      model: "kimi-k2.6",
      capability: "function-calling",
      intent: INTENTS["function-calling"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    expect(Array.isArray(body.tools)).toBe(true);
  });

  test("function-calling-multi-turn throws (multi-step capability)", () => {
    expect(() =>
      buildRequestBody({
        model: "kimi-k2.6",
        capability: "function-calling-multi-turn",
        intent: INTENTS["function-calling-multi-turn"],
      }),
    ).toThrow(/multi-step capability/);
  });

  test("vision-input embeds image as data URI", () => {
    const body = buildRequestBody({
      model: "kimi-k2.6",
      capability: "vision-input",
      intent: INTENTS["vision-input"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    const messages = body.messages;
    if (!Array.isArray(messages) || !isRecord(messages[0])) {
      throw new Error("expected messages[0] to be a record");
    }
    const content = messages[0].content;
    if (!Array.isArray(content)) throw new Error("expected content array");
    const imagePart = content.find(
      (p) => isRecord(p) && p.type === "image_url",
    );
    if (!isRecord(imagePart)) throw new Error("expected image_url part");
    const imageUrl = imagePart.image_url;
    if (!isRecord(imageUrl)) throw new Error("expected image_url record");
    expect(typeof imageUrl.url).toBe("string");
    expect(String(imageUrl.url).startsWith("data:image/jpeg;base64,")).toBe(
      true,
    );
  });

  test("reasoning-content uses user message only", () => {
    const body = buildRequestBody({
      model: "kimi-k2.6",
      capability: "reasoning-content",
      intent: INTENTS["reasoning-content"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    expect(body.stream).toBeUndefined();
  });

  test("reasoning-content-streaming sets stream: true", () => {
    const body = buildRequestBody({
      model: "kimi-k2.6",
      capability: "reasoning-content-streaming",
      intent: INTENTS["reasoning-content-streaming"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    expect(body.stream).toBe(true);
  });

  test("throws on capability not in opencode-zen support set", () => {
    expect(() =>
      buildRequestBody({
        model: "kimi-k2.6",
        capability: "audio-input",
        intent: INTENTS["audio-input"],
      }),
    ).toThrow();
  });
});

describe("fixture oracle: every captured (model, capability) matches structure", () => {
  test("there is at least one captured opencode-zen entry", () => {
    expect(OPENCODE_CAPTURED.length).toBeGreaterThan(0);
  });

  for (const entry of OPENCODE_CAPTURED) {
    test(`structural match: ${entry.model} / ${entry.capability}`, () => {
      if (MULTI_TURN_CAPABILITIES.has(entry.capability)) {
        const turn1Response: CapturedResponse = {
          status: 200,
          headers: {},
          parsed: loadFixtureJSON(entry, "turn-1", "response.json"),
          bytes: null,
        };
        const steps = collectSteps({
          model: entry.model,
          capability: entry.capability,
          responses: [turn1Response],
        });
        expect(steps.length).toBe(2);
        const [step1, step2] = steps;
        if (step1 === undefined || step2 === undefined) {
          throw new Error("expected two steps for multi-turn");
        }
        expect(step1.subdir).toBe("turn-1");
        expect(step2.subdir).toBe("turn-2");
        expect(step1.url).toBe(TEST_CHAT_URL);
        expect(step2.url).toBe(TEST_CHAT_URL);

        const captured1 = loadFixtureJSON(entry, "turn-1", "request.json");
        const captured2 = loadFixtureJSON(entry, "turn-2", "request.json");

        const cap1Schema = extractSchema(pruneEphemeral(captured1));
        const built1Schema = extractSchema(pruneEphemeral(step1.body));
        if (!schemaContains(built1Schema, cap1Schema)) {
          throw new Error(
            [
              "turn-1 schema mismatch",
              `captured: ${describeSchema(cap1Schema)}`,
              `built:    ${describeSchema(built1Schema)}`,
            ].join("\n"),
          );
        }

        const cap2Schema = extractSchema(pruneEphemeral(captured2));
        const built2Schema = extractSchema(pruneEphemeral(step2.body));
        if (!schemaContains(built2Schema, cap2Schema)) {
          throw new Error(
            [
              "turn-2 schema mismatch",
              `captured: ${describeSchema(cap2Schema)}`,
              `built:    ${describeSchema(built2Schema)}`,
            ].join("\n"),
          );
        }
        return;
      }

      const captured = loadFixtureJSON(entry, "request.json");
      const steps = collectSteps({
        model: entry.model,
        capability: entry.capability,
        responses: [],
      });
      expect(steps.length).toBe(1);
      const [only] = steps;
      if (only === undefined) throw new Error("expected one step");
      expect(only.subdir).toBeNull();
      expect(only.url).toBe(TEST_CHAT_URL);

      const capturedSchema = extractSchema(pruneEphemeral(captured));
      const builtSchema = extractSchema(pruneEphemeral(only.body));

      const ok = schemaContains(builtSchema, capturedSchema);
      if (!ok) {
        const msg = [
          "schema mismatch",
          `captured: ${describeSchema(capturedSchema)}`,
          `built:    ${describeSchema(builtSchema)}`,
        ].join("\n");
        throw new Error(msg);
      }

      if (!isRecord(only.body)) {
        throw new Error("expected built body to be record");
      }
      expect(only.body.model).toBe(entry.model);

      const wantsStream = entry.capability.endsWith("-streaming");
      if (wantsStream) {
        expect(only.body.stream).toBe(true);
      }
    });
  }
});

describe("extractReasoningTrace", () => {
  test("returns trace for Moonshot-style reasoning_details", () => {
    const parsed = {
      choices: [
        {
          message: {
            reasoning_details: [{ text: "step one" }],
          },
        },
      ],
    };
    const trace = extractReasoningTrace(parsed);
    expect(trace).not.toBeNull();
    if (trace === null) throw new Error("expected trace");
    expect(trace.fieldPath).toBe("choices.0.message.reasoning_details");
  });

  test("returns trace for Moonshot-style reasoning field", () => {
    const parsed = {
      choices: [
        {
          message: {
            reasoning: "thought process",
          },
        },
      ],
    };
    const trace = extractReasoningTrace(parsed);
    expect(trace).not.toBeNull();
    if (trace === null) throw new Error("expected trace");
    expect(trace.fieldPath).toBe("choices.0.message.reasoning");
  });

  test("returns trace for Fireworks-style reasoning_content", () => {
    const parsed = {
      choices: [
        {
          message: {
            reasoning_content: "chain of thought",
          },
        },
      ],
    };
    const trace = extractReasoningTrace(parsed);
    expect(trace).not.toBeNull();
    if (trace === null) throw new Error("expected trace");
    expect(trace.fieldPath).toBe("choices.0.message.reasoning_content");
  });

  test("returns null when no reasoning fields present", () => {
    const parsed = {
      choices: [{ message: { content: "regular text" } }],
    };
    expect(extractReasoningTrace(parsed)).toBeNull();
  });

  test("returns null for empty reasoning fields", () => {
    const parsed = {
      choices: [{ message: { reasoning: "" } }],
    };
    expect(extractReasoningTrace(parsed)).toBeNull();
  });

  test("returns null for malformed input", () => {
    expect(extractReasoningTrace(null)).toBeNull();
    expect(extractReasoningTrace("string")).toBeNull();
    expect(extractReasoningTrace({})).toBeNull();
  });
});

describe("plugin via stub fetch", () => {
  test("plug-in returned by factory is callable and well-formed", () => {
    const plugin = makePlugin();
    expect(typeof plugin.buildAuthHeaders).toBe("function");
    expect(typeof plugin.iterateCaptureSteps).toBe("function");
    expect(typeof plugin.extractReasoningTrace).toBe("function");
  });
});
