// A live capture written by the probe rig must be byte-identical to what the
// wire-to-session converter produces from the equivalent legacy wire capture.
// Both write exchange files, reconstruct tool dispatches, and stamp a
// `session.json`; they must agree on every byte so the migrated corpus and any
// freshly-recorded session are the same format.
//
// The two producers derive the exchange index differently: the rig counts
// executed steps, the converter ranks wire leaves (turn-1 < turn-2,
// upload < generate). This test drives both across the three capture shapes —
// single-turn, multi-turn with tools, and files-api — so the running counter
// is proven to reproduce the converter's leaf ordering. The files-api case is
// the load-bearing one: the real plug-ins yield `upload` before `generate`, so
// the counter must place the raw upload at exchange 0 exactly as the
// converter's rank sort does.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import {
  runCapture,
  writeCapture,
  type FetchLike,
  type CaptureStep,
  type CapturedResponse,
  type ProviderPlugin,
  type WriteCaptureInput,
} from "@intx/inference-discovery";
import {
  loadCaptureManifest,
  type Capability,
} from "@intx/inference-discovery/catalog";

import { convertWireToSession } from "./convert-wire-to-session";

const BRAND = "anthropic";
const BASE = "https://api.anthropic.com";
const MODEL = "claude-test";
const CAPTURED_AT = "2026-05-22T00:00:00.000Z";

type ExchangeRequest =
  | { kind: "json"; body: unknown }
  | { kind: "raw"; bytes: Uint8Array; contentType: string };

// One request/response pair in a capture. `wireSubPath` is where the legacy
// wire format placed the leaf ("" for a single-turn root leaf); the converter
// ranks these to derive the exchange index, and the rig must match by
// execution order.
type Exchange = {
  wireSubPath: string;
  url: string;
  request: ExchangeRequest;
  responseText: string;
  responseKind: "json" | "sse";
};

type Scenario = {
  name: string;
  capability: Capability;
  exchanges: Exchange[];
};

function responseContentType(kind: "json" | "sse"): string {
  return kind === "sse" ? "text/event-stream" : "application/json";
}

function makeResponse(exchange: Exchange): Response {
  return new Response(exchange.responseText, {
    status: 200,
    headers: { "Content-Type": responseContentType(exchange.responseKind) },
  });
}

// A plug-in that replays a fixed scenario. Auth headers and redaction are empty
// so the recorded headers are fully determined by the step, which keeps the
// wire twin below constructible without re-deriving redaction.
function scenarioPlugin(scenario: Scenario): ProviderPlugin {
  return {
    name: BRAND,
    models: [MODEL],
    redactRequestHeaders: [],
    redactResponseHeaders: [],
    buildAuthHeaders: () => ({}),
    *iterateCaptureSteps(): Generator<CaptureStep, void, CapturedResponse> {
      for (const exchange of scenario.exchanges) {
        const subdir =
          exchange.wireSubPath === "" ? null : exchange.wireSubPath;
        if (exchange.request.kind === "json") {
          yield {
            kind: "json",
            subdir,
            url: exchange.url,
            body: exchange.request.body,
          };
        } else {
          yield {
            kind: "raw",
            subdir,
            url: exchange.url,
            contentType: exchange.request.contentType,
            body: exchange.request.bytes,
          };
        }
      }
    },
  };
}

function scenarioFetch(scenario: Scenario): FetchLike {
  let index = 0;
  return async () => {
    const exchange = scenario.exchanges[index];
    index += 1;
    if (exchange === undefined) {
      throw new Error("scenarioFetch: more requests than scenario exchanges");
    }
    return makeResponse(exchange);
  };
}

// Build the legacy wire tree the converter reads. Each leaf is written through
// the same `writeCapture` the rig uses, with inputs mirroring what the rig
// derives from the step and the response — so the converter's verbatim copy of
// these leaves is exactly what the rig writes into its exchanges.
async function buildWireTree(
  wireDir: string,
  scenario: Scenario,
): Promise<void> {
  for (const exchange of scenario.exchanges) {
    const leafDir =
      exchange.wireSubPath === ""
        ? wireDir
        : path.join(wireDir, exchange.wireSubPath);

    const responseBytes = new TextEncoder().encode(exchange.responseText);
    const responseHeaders: Record<string, string> = {};
    makeResponse(exchange).headers.forEach((value, key) => {
      responseHeaders[key] = value;
    });

    const requestContentType =
      exchange.request.kind === "json"
        ? "application/json"
        : exchange.request.contentType;

    const input: WriteCaptureInput = {
      request:
        exchange.request.kind === "json"
          ? { kind: "json", body: exchange.request.body }
          : {
              kind: "raw",
              bytes: exchange.request.bytes,
              contentType: exchange.request.contentType,
            },
      requestHeaders: { "Content-Type": requestContentType },
      redactRequestHeaders: [],
      response:
        exchange.responseKind === "json"
          ? { kind: "json", bytes: responseBytes }
          : { kind: "sse", bytes: responseBytes },
      responseHeaders,
      redactResponseHeaders: [],
    };
    await writeCapture(leafDir, input);
  }
}

// Every file under `root`, as sorted repo-relative paths, so a missing file on
// either side fails on the path-set comparison before any byte comparison.
async function walkFiles(root: string): Promise<string[]> {
  const files: string[] = [];
  async function recurse(rel: string): Promise<void> {
    const entries = await fs.readdir(path.join(root, rel), {
      withFileTypes: true,
    });
    for (const entry of entries) {
      const childRel = rel === "" ? entry.name : `${rel}/${entry.name}`;
      if (entry.isDirectory()) {
        await recurse(childRel);
      } else {
        files.push(childRel);
      }
    }
  }
  await recurse("");
  return files.sort();
}

const WEATHER_TOOL = {
  name: "get_weather",
  description: "Look up the weather for a city.",
  input_schema: {
    type: "object",
    properties: { city: { type: "string" } },
    required: ["city"],
  },
};

const SCENARIOS: Scenario[] = [
  {
    name: "single-turn streaming",
    capability: "plain-text-streaming",
    exchanges: [
      {
        wireSubPath: "",
        url: `${BASE}/v1/messages`,
        request: {
          kind: "json",
          body: {
            model: MODEL,
            max_tokens: 1024,
            stream: true,
            messages: [{ role: "user", content: "Say hi." }],
          },
        },
        responseText:
          'event: message_start\ndata: {"type":"message_start"}\n\n' +
          'event: content_block_delta\ndata: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hi"}}\n\n' +
          'event: message_stop\ndata: {"type":"message_stop"}\n\n',
        responseKind: "sse",
      },
    ],
  },
  {
    name: "multi-turn with tools",
    capability: "function-calling-multi-turn",
    exchanges: [
      {
        wireSubPath: "turn-1",
        url: `${BASE}/v1/messages`,
        request: {
          kind: "json",
          body: {
            model: MODEL,
            max_tokens: 1024,
            tools: [WEATHER_TOOL],
            messages: [{ role: "user", content: "What is the weather in SF?" }],
          },
        },
        responseText: JSON.stringify({
          id: "msg_1",
          type: "message",
          role: "assistant",
          content: [
            {
              type: "tool_use",
              id: "toolu_1",
              name: "get_weather",
              input: { city: "SF" },
            },
          ],
          stop_reason: "tool_use",
        }),
        responseKind: "json",
      },
      {
        wireSubPath: "turn-2",
        url: `${BASE}/v1/messages`,
        request: {
          kind: "json",
          body: {
            model: MODEL,
            max_tokens: 1024,
            tools: [WEATHER_TOOL],
            messages: [
              { role: "user", content: "What is the weather in SF?" },
              {
                role: "assistant",
                content: [
                  {
                    type: "tool_use",
                    id: "toolu_1",
                    name: "get_weather",
                    input: { city: "SF" },
                  },
                ],
              },
              {
                role: "user",
                content: [
                  {
                    type: "tool_result",
                    tool_use_id: "toolu_1",
                    content: "72F and clear",
                  },
                ],
              },
            ],
          },
        },
        responseText: JSON.stringify({
          id: "msg_2",
          type: "message",
          role: "assistant",
          content: [{ type: "text", text: "It is 72F and clear in SF." }],
          stop_reason: "end_turn",
        }),
        responseKind: "json",
      },
    ],
  },
  {
    name: "files-api",
    capability: "files-api-reference",
    exchanges: [
      {
        wireSubPath: "upload",
        url: `${BASE}/v1/files`,
        request: {
          kind: "raw",
          bytes: new Uint8Array([0x25, 0x50, 0x44, 0x46, 0x2d, 0x31]),
          contentType: "application/pdf",
        },
        responseText: JSON.stringify({ id: "file_1", type: "file" }),
        responseKind: "json",
      },
      {
        wireSubPath: "generate",
        url: `${BASE}/v1/messages`,
        request: {
          kind: "json",
          body: {
            model: MODEL,
            max_tokens: 1024,
            messages: [
              {
                role: "user",
                content: [
                  {
                    type: "document",
                    source: { type: "file", file_id: "file_1" },
                  },
                  { type: "text", text: "Summarize this document." },
                ],
              },
            ],
          },
        },
        responseText: JSON.stringify({
          id: "msg_3",
          type: "message",
          role: "assistant",
          content: [{ type: "text", text: "The document is a stub." }],
          stop_reason: "end_turn",
        }),
        responseKind: "json",
      },
    ],
  },
];

describe("rig capture is byte-identical to the wire-to-session converter", () => {
  let root: string;

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "rig-equiv-"));
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  for (const scenario of SCENARIOS) {
    test(scenario.name, async () => {
      const rigDir = path.join(root, "rig");
      const wireDir = path.join(root, "wire");
      const convDir = path.join(root, "converted");

      await runCapture({
        plugin: scenarioPlugin(scenario),
        model: MODEL,
        capability: scenario.capability,
        intent: { prompt: "unused" },
        outDir: rigDir,
        now: () => new Date(CAPTURED_AT),
        fetch: scenarioFetch(scenario),
      });

      await buildWireTree(wireDir, scenario);
      await convertWireToSession({
        wireDir,
        sessionDir: convDir,
        source: { provider: BRAND, model: MODEL, baseURL: BASE },
        origin: "live",
        capturedAt: CAPTURED_AT,
        capability: scenario.capability,
      });

      const rigFiles = await walkFiles(rigDir);
      const convFiles = await walkFiles(convDir);
      expect(rigFiles).toEqual(convFiles);

      for (const rel of rigFiles) {
        const rigBytes = await fs.readFile(path.join(rigDir, rel));
        const convBytes = await fs.readFile(path.join(convDir, rel));
        // Label the comparison by path so a mismatch names the file.
        expect(`${rel}:${rigBytes.toString("base64")}`).toBe(
          `${rel}:${convBytes.toString("base64")}`,
        );
      }

      // The rig's output must be consumable by the real session loader.
      const manifest = await loadCaptureManifest(rigDir);
      expect(manifest.schemaVersion).toBe("2");
      expect(manifest.origin).toBe("live");
      expect(manifest.capability).toBe(scenario.capability);
      expect(manifest.source).toEqual({
        provider: BRAND,
        model: MODEL,
        baseURL: BASE,
      });
    });
  }
});
