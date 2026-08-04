#!/usr/bin/env bun
/* eslint-disable no-console */

// Convert a legacy wire-leaf capture directory into a session directory.
//
// The wire-leaf format keeps a capability's request/response pairs either
// directly in the capability dir (single-turn), under `turn-1/`, `turn-2/`, …
// (multi-turn), or under `upload/` + `generate/` (files-api, where `upload/`
// carries raw `request.bin`). The session format keeps one `exchanges/<i>/`
// per request/response pair, a `session.json` capture manifest, and — for
// multi-turn tool conversations — a `dispatches/<i>-<toolName>.json` record of
// each tool the harness ran between turns.
//
// The exchange payloads convert by verbatim copy: a wire leaf's files are
// byte-identical to what an `exchanges/<i>/` dir holds, so no re-encoding is
// needed and the request bytes the canonical matcher compares against survive
// untouched.
//
// Dispatches are not stored in the wire format; they are reconstructed. A
// multi-turn wire capture's final request embeds the whole transcript — every
// assistant tool call and every tool result fed back to it — so walking that
// one request recovers each (call, result) pair exactly once. The result is
// stored in the form that reproduces the wire when the replay harness feeds it
// back through `createToolResultTurn` and the provider adapter: the verbatim
// content string for Anthropic and OpenAI (which pass strings through
// untouched), and the response object for Google (which the canonical matcher
// re-sorts, making key order immaterial).

import { promises as fs } from "node:fs";
import * as path from "node:path";

import { type } from "arktype";

import {
  FixtureManifest,
  writeCaptureManifest,
  type CaptureManifest,
  type Capability,
} from "@intx/inference-discovery/catalog";

export type WireSource = {
  provider: string;
  model: string;
  baseURL: string;
};

export type ConvertWireToSessionOpts = {
  /** Capability root of a wire-leaf capture (holds the leaves + manifest.json). */
  wireDir: string;
  /** Output session directory (created if absent). */
  sessionDir: string;
  source: WireSource;
  origin: "live" | "synthetic";
  capturedAt: string;
  capability?: Capability;
  observedModelVersion?: string | null;
};

type ReconstructedDispatch = {
  toolName: string;
  args: unknown;
  result: unknown;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// A wire leaf: a directory that directly holds a request file. `rawUpload` marks
// the files-api `upload/` leaf, whose `request.bin` is not a runInference call
// and carries no tools to reconstruct.
type WireLeaf = { subPath: string; rawUpload: boolean };

async function readdirOrEmpty(dir: string): Promise<string[]> {
  try {
    return await fs.readdir(dir);
  } catch (cause) {
    const code =
      cause !== null && typeof cause === "object" && "code" in cause
        ? cause.code
        : undefined;
    if (code === "ENOENT") return [];
    throw cause;
  }
}

async function discoverLeaves(wireDir: string): Promise<WireLeaf[]> {
  const leaves: WireLeaf[] = [];
  async function walk(dir: string, subPath: string): Promise<void> {
    const entries = await readdirOrEmpty(dir);
    const hasRequestJson = entries.includes("request.json");
    const hasRequestBin = entries.includes("request.bin");
    // A directory holding a request file is a leaf; leaves do not nest.
    if (hasRequestJson || hasRequestBin) {
      leaves.push({ subPath, rawUpload: hasRequestBin && !hasRequestJson });
      return;
    }
    const subdirs: string[] = [];
    for (const name of entries.sort()) {
      const stat = await fs.stat(path.join(dir, name));
      if (stat.isDirectory()) subdirs.push(name);
    }
    for (const name of subdirs) {
      await walk(
        path.join(dir, name),
        subPath === "" ? name : `${subPath}/${name}`,
      );
    }
  }
  await walk(wireDir, "");
  return leaves;
}

// The chronological rank of a leaf within its capability. A capability is
// exactly one of single-turn, multi-turn, or files-api, so these ranks never
// collide within one capability. An unrecognized subPath is a shape the
// converter has not validated and must not guess at.
function leafRank(subPath: string): number {
  if (subPath === "") return 0;
  const turn = /^turn-(\d+)$/.exec(subPath);
  if (turn !== null) return Number(turn[1]);
  if (subPath === "upload") return 0;
  if (subPath === "generate") return 1;
  throw new Error(
    `convert-wire-to-session: unrecognized wire leaf subPath ${JSON.stringify(subPath)}`,
  );
}

async function copyExchange(
  leafDir: string,
  exchangeDir: string,
): Promise<void> {
  const present = new Set(await fs.readdir(leafDir));
  const hasRequestJson = present.has("request.json");
  const hasRequestBin = present.has("request.bin");
  if (hasRequestJson === hasRequestBin) {
    throw new Error(
      `convert-wire-to-session: ${leafDir} must have exactly one of request.json / request.bin`,
    );
  }
  const hasResponseJson = present.has("response.json");
  const hasResponseSse = present.has("response.sse");
  if (hasResponseJson === hasResponseSse) {
    throw new Error(
      `convert-wire-to-session: ${leafDir} must have exactly one of response.json / response.sse`,
    );
  }
  if (!present.has("request-headers.json")) {
    throw new Error(
      `convert-wire-to-session: ${leafDir} is missing request-headers.json`,
    );
  }
  if (!present.has("response-headers.json")) {
    throw new Error(
      `convert-wire-to-session: ${leafDir} is missing response-headers.json`,
    );
  }
  const files = [
    hasRequestJson ? "request.json" : "request.bin",
    "request-headers.json",
    hasResponseJson ? "response.json" : "response.sse",
    "response-headers.json",
  ];
  await fs.mkdir(exchangeDir, { recursive: true });
  for (const file of files) {
    await fs.copyFile(path.join(leafDir, file), path.join(exchangeDir, file));
  }
}

// ---------------------------------------------------------------------------
// Dispatch reconstruction
// ---------------------------------------------------------------------------

function anthropicResultString(content: unknown): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    if (content.length !== 1) {
      throw new Error(
        `convert-wire-to-session: Anthropic tool_result content must be a string or a single text block, got ${String(content.length)} blocks`,
      );
    }
    const only = content[0];
    if (
      !isRecord(only) ||
      only.type !== "text" ||
      typeof only.text !== "string"
    ) {
      throw new Error(
        "convert-wire-to-session: Anthropic tool_result content block must be a text block",
      );
    }
    return only.text;
  }
  throw new Error(
    "convert-wire-to-session: Anthropic tool_result content must be a string or text-block array",
  );
}

function pairById(
  calls: { id: string; name: string; args: unknown }[],
  results: Map<string, unknown>,
  provider: string,
): ReconstructedDispatch[] {
  const dispatches: ReconstructedDispatch[] = [];
  const paired = new Set<string>();
  for (const call of calls) {
    if (!results.has(call.id)) {
      throw new Error(
        `convert-wire-to-session: ${provider} tool_call ${JSON.stringify(call.id)} has no matching tool_result in the request transcript`,
      );
    }
    dispatches.push({
      toolName: call.name,
      args: call.args,
      result: results.get(call.id),
    });
    paired.add(call.id);
  }
  for (const id of results.keys()) {
    if (!paired.has(id)) {
      throw new Error(
        `convert-wire-to-session: ${provider} tool_result ${JSON.stringify(id)} has no matching tool_call in the request transcript`,
      );
    }
  }
  return dispatches;
}

function requireMessages(body: Record<string, unknown>): unknown[] {
  const messages = body.messages;
  if (!Array.isArray(messages)) {
    throw new Error(
      "convert-wire-to-session: request body has no messages array",
    );
  }
  return messages;
}

function extractAnthropicDispatches(
  body: Record<string, unknown>,
): ReconstructedDispatch[] {
  const calls: { id: string; name: string; args: unknown }[] = [];
  const results = new Map<string, unknown>();
  for (const message of requireMessages(body)) {
    if (!isRecord(message) || !Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (!isRecord(block)) continue;
      if (block.type === "tool_use") {
        calls.push({
          id: String(block.id),
          name: String(block.name),
          args: block.input,
        });
      } else if (block.type === "tool_result") {
        const id = String(block.tool_use_id);
        if (results.has(id)) {
          throw new Error(
            `convert-wire-to-session: Anthropic duplicate tool_result for ${JSON.stringify(id)}`,
          );
        }
        results.set(id, anthropicResultString(block.content));
      }
    }
  }
  return pairById(calls, results, "anthropic");
}

function extractOpenAIDispatches(
  body: Record<string, unknown>,
): ReconstructedDispatch[] {
  const calls: { id: string; name: string; args: unknown }[] = [];
  const results = new Map<string, unknown>();
  for (const message of requireMessages(body)) {
    if (!isRecord(message)) continue;
    if (message.role === "assistant" && Array.isArray(message.tool_calls)) {
      for (const toolCall of message.tool_calls) {
        if (!isRecord(toolCall)) continue;
        const fn = toolCall.function;
        if (!isRecord(fn)) {
          throw new Error(
            "convert-wire-to-session: OpenAI tool_call is missing a function object",
          );
        }
        calls.push({
          id: String(toolCall.id),
          name: String(fn.name),
          args: JSON.parse(String(fn.arguments)),
        });
      }
    } else if (message.role === "tool") {
      const id = String(message.tool_call_id);
      if (results.has(id)) {
        throw new Error(
          `convert-wire-to-session: OpenAI duplicate tool result for ${JSON.stringify(id)}`,
        );
      }
      // The tool message content is the verbatim string the client fed back;
      // storing it unparsed reproduces it exactly when the adapter re-emits it.
      results.set(id, message.content);
    }
  }
  return pairById(calls, results, "openai");
}

function extractGoogleDispatches(
  body: Record<string, unknown>,
): ReconstructedDispatch[] {
  const contents = body.contents;
  if (!Array.isArray(contents)) {
    throw new Error(
      "convert-wire-to-session: Google request body has no contents array",
    );
  }
  const calls: { name: string; args: unknown }[] = [];
  const responses: { name: string; response: unknown }[] = [];
  for (const content of contents) {
    if (!isRecord(content) || !Array.isArray(content.parts)) continue;
    for (const part of content.parts) {
      if (!isRecord(part)) continue;
      if (isRecord(part.functionCall)) {
        calls.push({
          name: String(part.functionCall.name),
          args: part.functionCall.args,
        });
      } else if (isRecord(part.functionResponse)) {
        responses.push({
          name: String(part.functionResponse.name),
          response: part.functionResponse.response,
        });
      }
    }
  }
  // Gemini carries no call ids; a functionResponse pairs with the functionCall
  // at the same ordinal position. Assert the names line up so a divergence
  // fails loudly instead of binding a result to the wrong call.
  if (calls.length !== responses.length) {
    throw new Error(
      `convert-wire-to-session: Google functionCall/functionResponse count mismatch (${String(calls.length)} vs ${String(responses.length)})`,
    );
  }
  const dispatches: ReconstructedDispatch[] = [];
  for (let i = 0; i < calls.length; i++) {
    const call = calls[i];
    const response = responses[i];
    if (call === undefined || response === undefined) continue;
    if (call.name !== response.name) {
      throw new Error(
        `convert-wire-to-session: Google functionResponse at position ${String(i)} names ${JSON.stringify(response.name)} but the functionCall names ${JSON.stringify(call.name)}`,
      );
    }
    dispatches.push({
      toolName: call.name,
      args: call.args,
      result: response.response,
    });
  }
  return dispatches;
}

// `source.provider` is the adapter-registry key the replay harness drives
// runInference against, so the extractor keys off the same name. The catalog
// providers `openai` and `opencode-zen` both speak the OpenAI protocol and
// resolve to the one `openai-compatible` adapter; the CLI maps them before the
// core ever sees them.
function extractDispatches(
  adapterProvider: string,
  body: Record<string, unknown>,
): ReconstructedDispatch[] {
  switch (adapterProvider) {
    case "anthropic":
      return extractAnthropicDispatches(body);
    case "openai-compatible":
      return extractOpenAIDispatches(body);
    case "google-genai":
      return extractGoogleDispatches(body);
    default:
      throw new Error(
        `convert-wire-to-session: no dispatch extractor for adapter provider ${JSON.stringify(adapterProvider)}`,
      );
  }
}

async function reconstructDispatches(
  wireDir: string,
  leaves: WireLeaf[],
  provider: string,
): Promise<ReconstructedDispatch[]> {
  // The final request in the transcript embeds every prior tool call and
  // result, so one JSON request holds the whole dispatch history. A raw upload
  // leaf is not a runInference call, so the last JSON leaf is the transcript.
  let lastJson: WireLeaf | undefined;
  for (const leaf of leaves) {
    if (!leaf.rawUpload) lastJson = leaf;
  }
  if (lastJson === undefined) return [];
  const requestPath = path.join(wireDir, lastJson.subPath, "request.json");
  const parsed: unknown = JSON.parse(await fs.readFile(requestPath, "utf-8"));
  if (!isRecord(parsed)) {
    throw new Error(
      `convert-wire-to-session: ${requestPath} did not parse into a request object`,
    );
  }
  return extractDispatches(provider, parsed);
}

async function writeDispatches(
  sessionDir: string,
  dispatches: ReconstructedDispatch[],
): Promise<void> {
  if (dispatches.length === 0) return;
  const dispatchesDir = path.join(sessionDir, "dispatches");
  await fs.mkdir(dispatchesDir, { recursive: true });
  for (let i = 0; i < dispatches.length; i++) {
    const dispatch = dispatches[i];
    if (dispatch === undefined) continue;
    // The tool name comes from the wire transcript and becomes part of a
    // filename the session loader parses back out; reject anything that is not
    // a bare identifier so a stray path separator cannot escape the directory.
    if (!/^[A-Za-z0-9_-]+$/.test(dispatch.toolName)) {
      throw new Error(
        `convert-wire-to-session: tool name ${JSON.stringify(dispatch.toolName)} is not a bare identifier and cannot form a dispatch filename`,
      );
    }
    const file = path.join(
      dispatchesDir,
      `${String(i)}-${dispatch.toolName}.json`,
    );
    await fs.writeFile(
      file,
      `${JSON.stringify({ args: dispatch.args, result: dispatch.result }, null, 2)}\n`,
    );
  }
}

export async function convertWireToSession(
  opts: ConvertWireToSessionOpts,
): Promise<void> {
  const leaves = await discoverLeaves(opts.wireDir);
  if (leaves.length === 0) {
    throw new Error(
      `convert-wire-to-session: ${opts.wireDir} holds no wire leaves`,
    );
  }
  const ordered = [...leaves].sort(
    (a, b) => leafRank(a.subPath) - leafRank(b.subPath),
  );

  for (let i = 0; i < ordered.length; i++) {
    const leaf = ordered[i];
    if (leaf === undefined) continue;
    await copyExchange(
      path.join(opts.wireDir, leaf.subPath),
      path.join(opts.sessionDir, "exchanges", String(i)),
    );
  }

  const dispatches = await reconstructDispatches(
    opts.wireDir,
    ordered,
    opts.source.provider,
  );
  await writeDispatches(opts.sessionDir, dispatches);

  const manifest: CaptureManifest = {
    schemaVersion: "2",
    source: {
      provider: opts.source.provider,
      model: opts.source.model,
      baseURL: opts.source.baseURL,
    },
    origin: opts.origin,
    capturedAt: opts.capturedAt,
    ...(opts.capability !== undefined ? { capability: opts.capability } : {}),
    ...(opts.observedModelVersion !== undefined
      ? { observedModelVersion: opts.observedModelVersion }
      : {}),
  };
  await writeCaptureManifest(opts.sessionDir, manifest);
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

// Catalog provider name (as it appears in a wire manifest) → the adapter
// registry key the session format records. The `openai` and `opencode-zen`
// catalog providers both speak the OpenAI protocol through one adapter.
const CATALOG_TO_ADAPTER: Record<string, string> = {
  anthropic: "anthropic",
  openai: "openai-compatible",
  "opencode-zen": "openai-compatible",
  "google-genai": "google-genai",
};

type CLIArgs = {
  wireDir: string;
  sessionDir: string;
  baseURL: string;
  origin: "live" | "synthetic";
};

function parseCLI(argv: string[]): CLIArgs {
  const positionals: string[] = [];
  let baseURL: string | undefined;
  let origin: "live" | "synthetic" = "live";
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--base-url") {
      baseURL = argv[++i];
    } else if (arg === "--origin") {
      const value = argv[++i];
      if (value !== "live" && value !== "synthetic") {
        throw new Error(
          `--origin must be "live" or "synthetic", got ${String(value)}`,
        );
      }
      origin = value;
    } else if (arg !== undefined && arg.startsWith("--")) {
      throw new Error(`convert-wire-to-session: unknown flag ${arg}`);
    } else if (arg !== undefined) {
      positionals.push(arg);
    }
  }
  const [wireDir, sessionDir] = positionals;
  if (wireDir === undefined || sessionDir === undefined) {
    throw new Error(
      "usage: convert-wire-to-session <wireDir> <sessionDir> --base-url <url> [--origin live|synthetic]",
    );
  }
  if (baseURL === undefined) {
    throw new Error("convert-wire-to-session: --base-url is required");
  }
  return { wireDir, sessionDir, baseURL, origin };
}

// Convert one wire capability directory, reading provider/model/capability and
// timestamps from its own `manifest.json`. The caller supplies only the fields
// the wire manifest does not carry — the output location, the base URL, and the
// origin — so a caller can convert a capability programmatically without
// reassembling the manifest-read and catalog-to-adapter mapping itself.
export async function convertWireCapability(opts: {
  wireDir: string;
  sessionDir: string;
  baseURL: string;
  origin: "live" | "synthetic";
}): Promise<void> {
  const manifestPath = path.join(opts.wireDir, "manifest.json");
  const parsed: unknown = JSON.parse(await fs.readFile(manifestPath, "utf-8"));
  const manifest = FixtureManifest(parsed);
  if (manifest instanceof type.errors) {
    throw new Error(
      `convert-wire-to-session: ${manifestPath} is not a valid wire manifest: ${manifest.summary}`,
    );
  }
  const adapterProvider = CATALOG_TO_ADAPTER[manifest.provider];
  if (adapterProvider === undefined) {
    throw new Error(
      `convert-wire-to-session: no adapter mapping for catalog provider ${JSON.stringify(manifest.provider)}`,
    );
  }
  await convertWireToSession({
    wireDir: opts.wireDir,
    sessionDir: opts.sessionDir,
    source: {
      provider: adapterProvider,
      model: manifest.model,
      baseURL: opts.baseURL,
    },
    origin: opts.origin,
    capturedAt: manifest.capturedAt,
    capability: manifest.capability,
    ...(manifest.observedModelVersion !== undefined
      ? { observedModelVersion: manifest.observedModelVersion }
      : {}),
  });
}

async function main(argv: string[]): Promise<number> {
  const args = parseCLI(argv);
  await convertWireCapability({
    wireDir: args.wireDir,
    sessionDir: args.sessionDir,
    baseURL: args.baseURL,
    origin: args.origin,
  });
  console.log(`convert-wire-to-session: wrote ${args.sessionDir}`);
  return 0;
}

if (import.meta.main) {
  const exitCode = await main(process.argv.slice(2));
  process.exit(exitCode);
}
