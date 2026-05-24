import { describe, test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  INTENTS,
  SUPPORT_MATRIX,
  getFixtureDir,
  type Capability,
  type ToolDecl,
} from "@intx/inference-discovery/catalog";
import type { CaptureStep, CapturedResponse } from "@intx/inference-discovery";
import { createGoogleGenaiPlugin, iterateCaptureSteps } from "./index";
import { buildRequestBody } from "./request-body";
import { buildEndpointURL, isStreamingCapability } from "./endpoint";

const TEST_API_KEY = "test-key";
const REPO_ROOT = fileURLToPath(new URL("../../../", import.meta.url));

const MULTI_TURN_CAPABILITIES: ReadonlySet<Capability> = new Set<Capability>([
  "function-calling-multi-turn",
  "function-calling-multi-turn-streaming",
  "function-calling-with-thinking",
  "function-calling-with-thinking-streaming",
]);

const FILES_API_CAPABILITIES: ReadonlySet<Capability> = new Set<Capability>([
  "files-api-reference",
  "files-api-reference-streaming",
]);

function readFixtureJSON(fixtureDir: string, ...parts: string[]): unknown {
  const raw = readFileSync(join(REPO_ROOT, fixtureDir, ...parts), "utf8");
  return JSON.parse(raw);
}

function readFixtureBytes(fixtureDir: string, ...parts: string[]): Uint8Array {
  const buf = readFileSync(join(REPO_ROOT, fixtureDir, ...parts));
  return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
}

function fixtureFileExists(fixtureDir: string, ...parts: string[]): boolean {
  try {
    readFileSync(join(REPO_ROOT, fixtureDir, ...parts));
    return true;
  } catch {
    return false;
  }
}

function loadTurn1Response(fixtureDir: string): unknown {
  if (fixtureFileExists(fixtureDir, "turn-1", "response.json")) {
    return readFixtureJSON(fixtureDir, "turn-1", "response.json");
  }
  const turn2Body = readFixtureJSON(fixtureDir, "turn-2", "request.json");
  if (!isPlainObject(turn2Body)) {
    throw new Error(`turn-2 request body is not an object in ${fixtureDir}`);
  }
  const contents = turn2Body.contents;
  if (!Array.isArray(contents) || contents.length < 2) {
    throw new Error(
      `turn-2 request body has no model-role assistant content in ${fixtureDir}`,
    );
  }
  const assistantContent = contents[1];
  return { candidates: [{ content: assistantContent }] };
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype
  );
}

function normalizeForStructuralComparison(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(normalizeForStructuralComparison);
  }
  if (!isPlainObject(value)) {
    return value;
  }
  const out: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(value)) {
    if (key === "text" && typeof child === "string") {
      out[key] = "<text>";
      continue;
    }
    if (key === "data" && typeof child === "string") {
      out[key] = "<base64>";
      continue;
    }
    if (key === "fileUri" && typeof child === "string") {
      out[key] = "<file-uri>";
      continue;
    }
    if (key === "name" && typeof child === "string") {
      out[key] = "<name>";
      continue;
    }
    if (key === "description" && typeof child === "string") {
      out[key] = "<description>";
      continue;
    }
    if (key === "allowedFunctionNames" && Array.isArray(child)) {
      out[key] = child.map(() => "<name>");
      continue;
    }
    if (key === "thoughtSignature" && typeof child === "string") {
      out[key] = "<thought-signature>";
      continue;
    }
    if (key === "response" && isPlainObject(child)) {
      out[key] = "<response>";
      continue;
    }
    if (key === "assetPath" && typeof child === "string") {
      out[key] = "<asset-path>";
      continue;
    }
    if (key === "contentLength" && typeof child === "number") {
      out[key] = "<content-length>";
      continue;
    }
    if (key === "displayName" && typeof child === "string") {
      out[key] = "<display-name>";
      continue;
    }
    out[key] = normalizeForStructuralComparison(child);
  }
  return out;
}

function googleGenaiCapturedEntries() {
  return SUPPORT_MATRIX.filter(
    (entry) =>
      entry.provider === "google-genai" && entry.outcome === "captured",
  );
}

function firstToolFor(capability: Capability): ToolDecl {
  const tools = INTENTS[capability].tools;
  if (tools === undefined || tools.length === 0) {
    throw new Error(`INTENTS[${capability}] has no tools`);
  }
  const [tool] = tools;
  if (tool === undefined) {
    throw new Error(`INTENTS[${capability}] tools[0] is undefined`);
  }
  return tool;
}

function collectSteps(opts: {
  model: string;
  capability: Capability;
  responses: readonly CapturedResponse[];
}): CaptureStep[] {
  const intent = INTENTS[opts.capability];
  const iter = iterateCaptureSteps({
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
    if (response === undefined) {
      // No more responses available; stop pumping the iterator. This is
      // expected for the final step.
      break;
    }
    next = iter.next(response);
  }
  return steps;
}

describe("createGoogleGenaiPlugin", () => {
  test("declares provider name, models, and redaction lists", () => {
    const plugin = createGoogleGenaiPlugin({ apiKey: TEST_API_KEY });
    expect(plugin.name).toBe("google-genai");
    expect(plugin.models).toEqual([
      "gemini-2.5-flash",
      "gemini-2.5-flash-image",
    ]);
    expect(plugin.redactRequestHeaders).toEqual(["x-goog-api-key"]);
    expect(plugin.redactResponseHeaders).toEqual([]);
  });

  test("buildAuthHeaders returns x-goog-api-key", () => {
    const plugin = createGoogleGenaiPlugin({ apiKey: TEST_API_KEY });
    expect(plugin.buildAuthHeaders()).toEqual({
      "x-goog-api-key": TEST_API_KEY,
    });
  });

  test("buildAuthHeaders rejects an empty apiKey", () => {
    const plugin = createGoogleGenaiPlugin({ apiKey: "" });
    expect(() => plugin.buildAuthHeaders()).toThrow(/apiKey/);
  });
});

describe("buildEndpointURL", () => {
  test("returns generateContent for non-streaming capabilities", () => {
    expect(
      buildEndpointURL({ model: "gemini-2.5-flash", capability: "plain-text" }),
    ).toBe(
      "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
    );
  });

  test("returns streamGenerateContent?alt=sse for streaming capabilities", () => {
    expect(
      buildEndpointURL({
        model: "gemini-2.5-flash",
        capability: "plain-text-streaming",
      }),
    ).toBe(
      "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
    );
  });

  test("uses the image model for image-output", () => {
    expect(
      buildEndpointURL({
        model: "gemini-2.5-flash-image",
        capability: "image-output",
      }),
    ).toBe(
      "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-image:generateContent",
    );
  });

  test("isStreamingCapability matches every -streaming suffix", () => {
    expect(isStreamingCapability("plain-text-streaming")).toBe(true);
    expect(isStreamingCapability("plain-text")).toBe(false);
    expect(isStreamingCapability("image-output-streaming")).toBe(true);
  });
});

describe("buildRequestBody — unsupported pairs", () => {
  test("throws when capability is not in the model's matrix (text model + image-output)", () => {
    expect(() =>
      buildRequestBody({
        model: "gemini-2.5-flash",
        capability: "image-output",
        intent: INTENTS["image-output"],
      }),
    ).toThrow(/does not support capability/);
  });

  test("throws when capability is not in the model's matrix (image model + plain-text)", () => {
    expect(() =>
      buildRequestBody({
        model: "gemini-2.5-flash-image",
        capability: "plain-text",
        intent: INTENTS["plain-text"],
      }),
    ).toThrow(/does not support capability/);
  });

  test("throws when capability is not in the model's matrix (image model + safety-classification)", () => {
    expect(() =>
      buildRequestBody({
        model: "gemini-2.5-flash-image",
        capability: "safety-classification",
        intent: INTENTS["safety-classification"],
      }),
    ).toThrow(/does not support capability/);
  });

  test("throws for an unknown model", () => {
    expect(() =>
      buildRequestBody({
        model: "gemini-99-unknown",
        capability: "plain-text",
        intent: INTENTS["plain-text"],
      }),
    ).toThrow(/unknown model/);
  });

  test("throws for capabilities no google-genai model exposes (e.g. function-calling)", () => {
    expect(() =>
      buildRequestBody({
        model: "gemini-2.5-flash",
        capability: "function-calling",
        intent: INTENTS["function-calling"],
      }),
    ).toThrow();
  });
});

describe("buildRequestBody — wire-shape spot checks", () => {
  test("plain-text produces a single user turn with the intent prompt", () => {
    const body = buildRequestBody({
      model: "gemini-2.5-flash",
      capability: "plain-text",
      intent: INTENTS["plain-text"],
    });
    expect(body).toEqual({
      contents: [
        {
          role: "user",
          parts: [{ text: INTENTS["plain-text"].prompt }],
        },
      ],
    });
  });

  test("plain-text-streaming adds maxOutputTokens=400 and thinkingBudget=0", () => {
    const body = buildRequestBody({
      model: "gemini-2.5-flash",
      capability: "plain-text-streaming",
      intent: INTENTS["plain-text-streaming"],
    });
    expect(body).toEqual({
      contents: [
        {
          role: "user",
          parts: [{ text: INTENTS["plain-text-streaming"].prompt }],
        },
      ],
      generationConfig: {
        maxOutputTokens: 400,
        thinkingConfig: { thinkingBudget: 0 },
      },
    });
  });

  test("function-calling-with-thinking sets thinkingBudget=1024, includeThoughts=true, mode=ANY", () => {
    const capability: Capability = "function-calling-with-thinking";
    const body = buildRequestBody({
      model: "gemini-2.5-flash",
      capability,
      intent: INTENTS[capability],
    });
    const tool = firstToolFor(capability);
    expect(body).toEqual({
      contents: [
        {
          role: "user",
          parts: [{ text: INTENTS[capability].prompt }],
        },
      ],
      tools: [
        {
          functionDeclarations: [
            {
              name: tool.name,
              description: tool.description,
              parameters: tool.parameters,
            },
          ],
        },
      ],
      toolConfig: {
        functionCallingConfig: {
          mode: "ANY",
          allowedFunctionNames: [tool.name],
        },
      },
      generationConfig: {
        thinkingConfig: { thinkingBudget: 1024, includeThoughts: true },
      },
    });
  });

  test("function-calling-multi-turn sets thinkingBudget=0 with no includeThoughts", () => {
    const capability: Capability = "function-calling-multi-turn";
    const body = buildRequestBody({
      model: "gemini-2.5-flash",
      capability,
      intent: INTENTS[capability],
    });
    const tool = firstToolFor(capability);
    expect(body).toEqual({
      contents: [
        {
          role: "user",
          parts: [{ text: INTENTS[capability].prompt }],
        },
      ],
      tools: [
        {
          functionDeclarations: [
            {
              name: tool.name,
              description: tool.description,
              parameters: tool.parameters,
            },
          ],
        },
      ],
      toolConfig: {
        functionCallingConfig: {
          mode: "ANY",
          allowedFunctionNames: [tool.name],
        },
      },
      generationConfig: {
        thinkingConfig: { thinkingBudget: 0 },
      },
    });
  });

  test("code-execution declares the codeExecution tool", () => {
    const body = buildRequestBody({
      model: "gemini-2.5-flash",
      capability: "code-execution",
      intent: INTENTS["code-execution"],
    });
    expect(body).toEqual({
      contents: [
        {
          role: "user",
          parts: [{ text: INTENTS["code-execution"].prompt }],
        },
      ],
      tools: [{ codeExecution: {} }],
    });
  });

  test("safety-classification produces an unconstrained single user turn (no generationConfig)", () => {
    const body = buildRequestBody({
      model: "gemini-2.5-flash",
      capability: "safety-classification",
      intent: INTENTS["safety-classification"],
    });
    expect(body).toEqual({
      contents: [
        {
          role: "user",
          parts: [{ text: INTENTS["safety-classification"].prompt }],
        },
      ],
    });
  });

  test("safety-classification-streaming produces the same body as non-streaming (endpoint differs, not body)", () => {
    const nonStreaming = buildRequestBody({
      model: "gemini-2.5-flash",
      capability: "safety-classification",
      intent: INTENTS["safety-classification"],
    });
    const streaming = buildRequestBody({
      model: "gemini-2.5-flash",
      capability: "safety-classification-streaming",
      intent: INTENTS["safety-classification-streaming"],
    });
    expect(streaming).toEqual(nonStreaming);
  });

  test("grounding declares the googleSearch tool", () => {
    const body = buildRequestBody({
      model: "gemini-2.5-flash",
      capability: "grounding",
      intent: INTENTS["grounding"],
    });
    expect(body).toEqual({
      contents: [
        {
          role: "user",
          parts: [{ text: INTENTS["grounding"].prompt }],
        },
      ],
      tools: [{ googleSearch: {} }],
    });
  });

  test("image-output sets responseModalities to [TEXT, IMAGE]", () => {
    const body = buildRequestBody({
      model: "gemini-2.5-flash-image",
      capability: "image-output",
      intent: INTENTS["image-output"],
    });
    expect(body).toEqual({
      contents: [
        {
          role: "user",
          parts: [{ text: INTENTS["image-output"].prompt }],
        },
      ],
      generationConfig: {
        responseModalities: ["TEXT", "IMAGE"],
      },
    });
  });

  test("vision-input embeds a base64 inlineData part with image/jpeg", () => {
    const body = buildRequestBody({
      model: "gemini-2.5-flash",
      capability: "vision-input",
      intent: INTENTS["vision-input"],
    });
    const normalized = normalizeForStructuralComparison(body);
    expect(normalized).toEqual({
      contents: [
        {
          role: "user",
          parts: [
            { text: "<text>" },
            { inlineData: { mimeType: "image/jpeg", data: "<base64>" } },
          ],
        },
      ],
    });
  });

  test("buildRequestBody throws for files-api-reference (multi-step only via iterateCaptureSteps)", () => {
    expect(() =>
      buildRequestBody({
        model: "gemini-2.5-flash",
        capability: "files-api-reference",
        intent: INTENTS["files-api-reference"],
      }),
    ).toThrow(/multi-step/);
    expect(() =>
      buildRequestBody({
        model: "gemini-2.5-flash",
        capability: "files-api-reference-streaming",
        intent: INTENTS["files-api-reference-streaming"],
      }),
    ).toThrow(/multi-step/);
  });
});

describe("fixture-oracle: iterateCaptureSteps structurally matches every captured step's wire", () => {
  for (const entry of googleGenaiCapturedEntries()) {
    test(`${entry.model} / ${entry.capability}`, () => {
      const fixtureDir = getFixtureDir(entry);
      if (fixtureDir === null) {
        throw new Error(
          `entry ${entry.model}/${entry.capability} has no captured fixture directory`,
        );
      }

      const isMultiTurn = MULTI_TURN_CAPABILITIES.has(entry.capability);
      const isFilesApi = FILES_API_CAPABILITIES.has(entry.capability);

      if (isMultiTurn) {
        const turn1ParsedResponse = loadTurn1Response(fixtureDir);
        const turn1Response: CapturedResponse = {
          status: 200,
          headers: {},
          parsed: turn1ParsedResponse,
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
          throw new Error("expected exactly two steps for multi-turn");
        }
        expect(step1.subdir).toBe("turn-1");
        expect(step2.subdir).toBe("turn-2");

        const expectedURL = buildEndpointURL({
          model: entry.model,
          capability: entry.capability,
        });
        expect(step1.url).toBe(expectedURL);
        expect(step2.url).toBe(expectedURL);

        const captured1 = readFixtureJSON(fixtureDir, "turn-1", "request.json");
        const captured2 = readFixtureJSON(fixtureDir, "turn-2", "request.json");
        expect(normalizeForStructuralComparison(step1.body)).toEqual(
          normalizeForStructuralComparison(captured1),
        );
        expect(normalizeForStructuralComparison(step2.body)).toEqual(
          normalizeForStructuralComparison(captured2),
        );
        return;
      }

      if (isFilesApi) {
        const uploadResponse: CapturedResponse = {
          status: 200,
          headers: {},
          parsed: readFixtureJSON(fixtureDir, "upload", "response.json"),
          bytes: null,
        };
        const steps = collectSteps({
          model: entry.model,
          capability: entry.capability,
          responses: [uploadResponse],
        });
        expect(steps.length).toBe(2);
        const [uploadStep, generateStep] = steps;
        if (uploadStep === undefined || generateStep === undefined) {
          throw new Error("expected exactly two steps for files-api");
        }
        expect(uploadStep.subdir).toBe("upload");
        expect(generateStep.subdir).toBe("generate");
        expect(uploadStep.url).toBe(
          "https://generativelanguage.googleapis.com/upload/v1beta/files",
        );
        expect(generateStep.url).toBe(
          buildEndpointURL({
            model: entry.model,
            capability: entry.capability,
          }),
        );

        if (uploadStep.kind !== "raw") {
          throw new Error("expected files-api upload step to be raw-bytes");
        }
        const capturedUploadBytes = readFixtureBytes(
          fixtureDir,
          "upload",
          "request.bin",
        );
        expect(uploadStep.contentType).toBe("application/pdf");
        expect(Array.from(uploadStep.body)).toEqual(
          Array.from(capturedUploadBytes),
        );
        if (generateStep.kind !== "json") {
          throw new Error("expected files-api generate step to be JSON");
        }
        const capturedGenerate = readFixtureJSON(
          fixtureDir,
          "generate",
          "request.json",
        );
        expect(normalizeForStructuralComparison(generateStep.body)).toEqual(
          normalizeForStructuralComparison(capturedGenerate),
        );
        return;
      }

      const steps = collectSteps({
        model: entry.model,
        capability: entry.capability,
        responses: [],
      });
      expect(steps.length).toBe(1);
      const [only] = steps;
      if (only === undefined) {
        throw new Error("expected exactly one step for single-step capability");
      }
      expect(only.subdir).toBeNull();
      expect(only.url).toBe(
        buildEndpointURL({
          model: entry.model,
          capability: entry.capability,
        }),
      );
      const captured = readFixtureJSON(fixtureDir, "request.json");
      expect(normalizeForStructuralComparison(only.body)).toEqual(
        normalizeForStructuralComparison(captured),
      );
    });
  }
});

describe("createGoogleGenaiPlugin — end-to-end through stub fetch", () => {
  test("a stubbed POST round-trips through the plugin", async () => {
    const plugin = createGoogleGenaiPlugin({ apiKey: TEST_API_KEY });
    const iter = plugin.iterateCaptureSteps({
      model: "gemini-2.5-flash",
      capability: "plain-text",
      intent: INTENTS["plain-text"],
    });
    const first = iter.next();
    if (first.done) throw new Error("expected one step");
    const step = first.value;
    const headers = {
      "Content-Type": "application/json",
      ...plugin.buildAuthHeaders(),
    };

    let observedURL = "";
    let observedHeaders: Record<string, string> = {};
    let observedBody = "";
    const stubFetch = (
      input: string,
      init: {
        method: string;
        headers: Record<string, string>;
        body: string;
      },
    ): Promise<Response> => {
      observedURL = input;
      observedHeaders = init.headers;
      observedBody = init.body;
      return Promise.resolve(
        new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    };

    const response = await stubFetch(step.url, {
      method: "POST",
      headers,
      body: JSON.stringify(step.body),
    });

    expect(observedURL).toBe(
      "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
    );
    expect(observedHeaders["x-goog-api-key"]).toBe(TEST_API_KEY);
    expect(observedHeaders["Content-Type"]).toBe("application/json");
    expect(JSON.parse(observedBody)).toEqual({
      contents: [
        {
          role: "user",
          parts: [{ text: INTENTS["plain-text"].prompt }],
        },
      ],
    });
    expect(response.status).toBe(200);
  });
});
