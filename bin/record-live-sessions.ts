#!/usr/bin/env bun
/* eslint-disable no-console */

// Record one committed LIVE multi-turn tool session per adapter against real
// provider endpoints. Credentials come from the environment at recording time
// and each session lands under its discovery package's `live-sessions/<brand>/`
// tree — deliberately outside the `sessions/<provider>/` tree the discovery
// probe rig overwrites.
//
// Unlike record-example-sessions.ts (synthetic, deterministic, credential-free)
// this drives the REAL model: it declares a tool, prompts a question that needs
// it, and asserts the model actually emitted a tool call — failing loudly rather
// than committing a session that skipped the tool path. The provider has no
// forced-tool-choice control here, so a model that answers without calling the
// tool fails the run; re-run or adjust the prompt/model. It makes real, paid
// network calls and must never run in CI.

import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import type {
  ConversationTurn,
  InferenceEvent,
  InferenceOptions,
  InferenceSource,
} from "@intx/types/runtime";
import {
  adapterForCatalogProvider,
  baseURLForCatalogProvider,
} from "@intx/inference-discovery/catalog";
import {
  createRecordingHarness,
  userTurn,
  LIVE_TOOL_DEFINITIONS,
  LIVE_TOOL_PROMPT,
  type RecordingFetchLike,
} from "@intx/inference-testing";

import { OPENAI_FIRSTPARTY_QUIRKS } from "./lib/openai-quirks";

const INFERENCE_OPTIONS: InferenceOptions = { tools: LIVE_TOOL_DEFINITIONS };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// A location-agnostic canned result: the live model chooses the argument
// string, so the handler must answer for whatever city it asked about rather
// than a fixed lookup. The value is recorded to `dispatches/` and fed back to
// the model verbatim, so both sides see the same result.
function weatherResult(args: unknown): unknown {
  const location =
    isRecord(args) && typeof args.location === "string"
      ? args.location
      : "the requested city";
  return { location, temperatureF: 68, conditions: "fog" };
}

type ToolCall = {
  id: string;
  name: string;
  arguments: Record<string, unknown>;
};

function extractToolCalls(turn: ConversationTurn): ToolCall[] {
  const calls: ToolCall[] = [];
  for (const block of turn.content) {
    if (block.type === "tool_call") {
      calls.push({
        id: block.id,
        name: block.name,
        arguments: block.arguments,
      });
    }
  }
  return calls;
}

function expectDone(
  events: InferenceEvent[],
  label: string,
): InferenceEvent & { type: "inference.done" } {
  const done = events.find((e) => e.type === "inference.done");
  if (done === undefined || done.type !== "inference.done") {
    const error = events.find((e) => e.type === "inference.error");
    const detail =
      error !== undefined && error.type === "inference.error"
        ? `: ${JSON.stringify(error.data)}`
        : "";
    throw new Error(`${label}: expected inference.done event${detail}`);
  }
  return done;
}

export interface RecordLiveSessionOpts {
  /** Catalog brand: anthropic, openai, opencode-zen, google-genai. */
  brand: string;
  model: string;
  apiKey: string;
  baseURL: string;
  outputDir: string;
  /** Adapter quirks bag for the source (e.g. first-party OpenAI token field). */
  quirks?: Record<string, unknown>;
  /** Test seam: a stub fetch (paired with the synthetic origin) for unit tests. */
  fetch?: RecordingFetchLike;
  /** Test seam: freeze the capturedAt timestamp. */
  now?: () => Date;
}

// Drive a two-turn tool conversation and write the session. Turn 1 asks the
// question with the tool declared; the model must call the tool. Turn 2 feeds
// the tool result back and records the model's final answer. Injecting `fetch`
// exercises the exact orchestration a live run uses without the network.
export async function recordLiveSession(
  opts: RecordLiveSessionOpts,
): Promise<void> {
  const adapterProvider = adapterForCatalogProvider(opts.brand);
  if (adapterProvider === undefined) {
    throw new Error(
      `record-live-sessions: no adapter mapping for brand ${JSON.stringify(opts.brand)}`,
    );
  }

  await fs.rm(opts.outputDir, { recursive: true, force: true });

  const harness = createRecordingHarness({
    outputDir: opts.outputDir,
    source: {
      provider: adapterProvider,
      model: opts.model,
      baseURL: opts.baseURL,
      ...(opts.quirks !== undefined && Object.keys(opts.quirks).length > 0
        ? { quirks: opts.quirks }
        : {}),
    },
    // Two turns, plus headroom for a provider-side retry, still bounded so a
    // runaway loop can't silently rack up charges.
    maxExchanges: 4,
    // Every provider's auth header, so a live capture never commits a key.
    redactRequestHeaders: ["x-api-key", "authorization", "x-goog-api-key"],
    // set-cookie plus the provider account/org/project identifiers, so a
    // committed capture cannot be traced back to a specific account.
    redactResponseHeaders: [
      "set-cookie",
      "anthropic-organization-id",
      "openai-organization",
      "openai-project",
    ],
    ...(opts.fetch !== undefined
      ? { fetch: opts.fetch, bypassCIGuardForTests: true }
      : {}),
    ...(opts.now !== undefined ? { now: opts.now } : {}),
  });
  harness.onTool("weather", (args) => weatherResult(args));

  const source: InferenceSource = {
    id: `${opts.brand}:${opts.model}`,
    provider: adapterProvider,
    baseURL: opts.baseURL,
    apiKey: opts.apiKey,
    model: opts.model,
    ...(opts.quirks !== undefined ? { quirks: opts.quirks } : {}),
  };

  let seq = 0;
  const turn1Events: InferenceEvent[] = [];
  for await (const ev of harness.runInference({
    turns: [userTurn(LIVE_TOOL_PROMPT)],
    source,
    inferenceOptions: INFERENCE_OPTIONS,
    nextSeq: () => ++seq,
  })) {
    turn1Events.push(ev);
  }
  const turn1Done = expectDone(turn1Events, `${opts.brand} turn 1`);
  const toolCalls = extractToolCalls(turn1Done.data.turn);
  if (toolCalls.length === 0) {
    throw new Error(
      `${opts.brand}/${opts.model}: the model produced no tool call in turn 1, ` +
        `so there is no multi-turn tool session to record. Re-run, or adjust ` +
        `the prompt or model.`,
    );
  }

  const toolResultTurn: ConversationTurn = {
    role: "user",
    content: toolCalls.map((call) => ({
      type: "tool_result",
      callId: call.id,
      content: [
        { type: "text", text: JSON.stringify(weatherResult(call.arguments)) },
      ],
    })),
    timestamp: 0,
  };

  for await (const _ev of harness.runInference({
    turns: [userTurn(LIVE_TOOL_PROMPT), turn1Done.data.turn, toolResultTurn],
    source,
    inferenceOptions: INFERENCE_OPTIONS,
    nextSeq: () => ++seq,
  })) {
    // drain
  }

  await harness.finalize();
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const REPO_ROOT = path.resolve(fileURLToPath(import.meta.url), "..", "..");

// Brand → the discovery package that owns its captures. openai and opencode-zen
// share the openai-compatible adapter package.
type AdapterSpec = {
  brand: string;
  pkg: string;
  apiKeyEnv: string;
  defaultModel: string;
  // Adapter quirks the source carries. Only first-party OpenAI needs one
  // (gpt-5.x rejects max_tokens); the rest run on adapter defaults.
  quirks: Record<string, unknown>;
  /** When set, the base URL is read from this env var (opencode-zen relay). */
  baseURLEnv?: string;
  /** When set, the model defaults to this env var if present. */
  modelEnv?: string;
};

const ADAPTERS: readonly AdapterSpec[] = [
  {
    brand: "anthropic",
    pkg: "inference-discovery-anthropic",
    apiKeyEnv: "ANTHROPIC_API_KEY",
    defaultModel: "claude-haiku-4-5-20251001",
    quirks: {},
  },
  {
    brand: "openai",
    pkg: "inference-discovery-openai",
    apiKeyEnv: "OPENAI_API_KEY",
    defaultModel: "gpt-5.5",
    quirks: OPENAI_FIRSTPARTY_QUIRKS,
  },
  {
    brand: "google-genai",
    pkg: "inference-discovery-google-genai",
    apiKeyEnv: "GOOGLE_API_KEY",
    defaultModel: "gemini-2.5-flash",
    quirks: {},
  },
  {
    brand: "opencode-zen",
    pkg: "inference-discovery-openai",
    apiKeyEnv: "OPENCODE_API_KEY",
    defaultModel: "",
    quirks: {},
    baseURLEnv: "OPENCODE_BASE_URL",
    modelEnv: "OPENCODE_MODEL",
  },
];

function resolveBaseURL(spec: AdapterSpec, env: NodeJS.ProcessEnv): string {
  if (spec.baseURLEnv !== undefined) {
    const value = env[spec.baseURLEnv];
    if (value === undefined || value.length === 0) {
      throw new Error(
        `record-live-sessions: ${spec.baseURLEnv} must be set for ${spec.brand}`,
      );
    }
    return value;
  }
  const base = baseURLForCatalogProvider(spec.brand);
  if (base === undefined) {
    throw new Error(
      `record-live-sessions: no base URL configured for ${spec.brand}`,
    );
  }
  return base;
}

function resolveModel(
  spec: AdapterSpec,
  env: NodeJS.ProcessEnv,
  override: string | undefined,
): string {
  if (override !== undefined) return override;
  if (spec.modelEnv !== undefined) {
    const value = env[spec.modelEnv];
    if (value !== undefined && value.length > 0) return value;
  }
  if (spec.defaultModel.length === 0) {
    throw new Error(
      `record-live-sessions: no model for ${spec.brand}; set ${String(
        spec.modelEnv,
      )} or pass --model`,
    );
  }
  return spec.defaultModel;
}

function requireApiKey(spec: AdapterSpec, env: NodeJS.ProcessEnv): string {
  const key = env[spec.apiKeyEnv];
  if (key === undefined || key.length === 0) {
    throw new Error(
      `record-live-sessions: ${spec.apiKeyEnv} must be set for ${spec.brand}`,
    );
  }
  return key;
}

function parseCLI(argv: string[]): { only?: string; model?: string } {
  const out: { only?: string; model?: string } = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--only") {
      const value = argv[++i];
      if (value === undefined) {
        throw new Error("record-live-sessions: --only requires a value");
      }
      out.only = value;
    } else if (arg === "--model") {
      const value = argv[++i];
      if (value === undefined) {
        throw new Error("record-live-sessions: --model requires a value");
      }
      out.model = value;
    } else if (arg !== undefined && arg.startsWith("--")) {
      throw new Error(`record-live-sessions: unknown flag ${arg}`);
    }
  }
  return out;
}

async function main(argv: string[]): Promise<number> {
  const args = parseCLI(argv);
  const env = process.env;
  const selected = ADAPTERS.filter(
    (spec) => args.only === undefined || spec.brand === args.only,
  );
  if (selected.length === 0) {
    console.error(
      `record-live-sessions: no adapter matches --only ${args.only}`,
    );
    return 1;
  }

  const failures: string[] = [];
  for (const spec of selected) {
    const outputDir = path.join(
      REPO_ROOT,
      "packages",
      spec.pkg,
      "live-sessions",
      spec.brand,
      "tool-multi-turn",
    );
    try {
      const model = resolveModel(spec, env, args.model);
      console.error(`[record-live] start ${spec.brand} model=${model}`);
      await recordLiveSession({
        brand: spec.brand,
        model,
        apiKey: requireApiKey(spec, env),
        baseURL: resolveBaseURL(spec, env),
        outputDir,
        quirks: spec.quirks,
      });
      console.error(`[record-live] done  ${spec.brand} -> ${outputDir}`);
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      console.error(`[record-live] FAIL  ${spec.brand}: ${message}`);
      failures.push(spec.brand);
    }
  }

  if (failures.length > 0) {
    console.error(
      `record-live-sessions: ${failures.length} adapter(s) failed: ${failures.join(", ")}`,
    );
    return 1;
  }
  return 0;
}

if (import.meta.main) {
  const exitCode = await main(process.argv.slice(2));
  process.exit(exitCode);
}
