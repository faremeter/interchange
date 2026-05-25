// Session replay: take a captured session directory, register matchers
// for every captured exchange, register tool handlers that serve
// captured dispatch results verbatim, and drive production
// `runInference` through it. Replay surfaces orchestration-layer
// regressions that the single-exchange compat-replay layer (INTR-79)
// cannot see — multi-turn body construction, conversation history
// threading, dispatch wiring, terminal sequencing across turns.
//
// Captured tool results are baked in: real tool handlers are NOT
// invoked at replay time. Re-running a real handler risks producing a
// result that diverges from capture, which causes the next turn's
// request body to diverge, which makes the captured response no longer
// a valid reply — the replay would fail out before any orchestration
// had been exercised. Handler correctness is a separate test concern
// with its own tests.

import fs from "node:fs/promises";
import path from "node:path";

import type {
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

import { UnmatchedFetchError } from "./errors";
import { setupHarness } from "./harness";
import { loadSessionManifest, type SessionManifest } from "./session-manifest";

export class SessionReplayMismatchError extends Error {
  readonly exchangeIndex: number;
  readonly captured: unknown;
  readonly actual: unknown;
  readonly diff: string;
  readonly sessionDir: string;

  constructor(opts: {
    exchangeIndex: number;
    captured: unknown;
    actual: unknown;
    diff: string;
    sessionDir: string;
  }) {
    super(
      `Session replay: exchange ${String(opts.exchangeIndex)} request body ` +
        `did not match the captured request in ${opts.sessionDir}.\n${opts.diff}`,
    );
    this.name = "SessionReplayMismatchError";
    this.exchangeIndex = opts.exchangeIndex;
    this.captured = opts.captured;
    this.actual = opts.actual;
    this.diff = opts.diff;
    this.sessionDir = opts.sessionDir;
  }
}

export interface CreateReplayHarnessOpts {
  /** Absolute path to a session capture directory. */
  sessionDir: string;
  /**
   * Override for the `apiKey` field of the `InferenceSource` constructed
   * from `session.json`. Defaults to `"session-replay-stub"` — the
   * captured headers are served verbatim, so the adapter never sends
   * this value to the (stubbed) network.
   */
  apiKey?: string;
  /**
   * Override for the `id` field of the `InferenceSource` constructed
   * from `session.json`. Defaults to `"<provider>:<model>"`.
   */
  sourceId?: string;
}

export interface RunTurnOpts {
  /**
   * Conversation turns to feed into this `runInference` call. Each
   * call drives one turn; the caller assembles the multi-turn
   * conversation across calls (carrying the assistant turn from the
   * previous `inference.done` plus a user turn with `tool_result`
   * blocks built from the matching captured dispatches).
   *
   * The actual request body the adapter constructs from these turns
   * must match (after canonicalisation) the captured request body for
   * the corresponding exchange; otherwise
   * `SessionReplayMismatchError` surfaces.
   */
  turns: ConversationTurn[];
  /**
   * Optional `nextSeq` generator passed through to `runInference`.
   * Defaults to an internal counter that survives across `runTurn`
   * calls on this harness, so event `seq` values are monotonic across
   * the full replay.
   */
  nextSeq?: () => number;
}

export interface ReplayHarness {
  /** The captured session's loaded manifest. */
  readonly manifest: SessionManifest;
  /** The `InferenceSource` reconstructed from the manifest. */
  readonly source: InferenceSource;
  /**
   * Drive one `runInference` call against the next un-consumed
   * captured exchange. Returns every event the production
   * `runInference` emitted for that turn. The body-aware matcher
   * registered for this exchange routes the adapter's fetch to the
   * captured response; if the actual request body fails canonical
   * comparison against the capture,
   * `SessionReplayMismatchError` is thrown with a diff.
   */
  runTurn(opts: RunTurnOpts): Promise<InferenceEvent[]>;
  /**
   * After driving every turn, call this to verify every captured
   * exchange and dispatch was consumed. Throws
   * `SessionReplayMismatchError` if the actual conversation was
   * shorter than the capture.
   */
  assertFullyConsumed(): void;
  /** Captured exchanges in capture order, exposed for assertions. */
  readonly capturedExchanges: readonly CapturedExchange[];
  /** Captured dispatches in capture order, exposed for assertions. */
  readonly capturedDispatches: readonly CapturedDispatch[];
  /** Dispose the underlying harness. */
  dispose(): void;
}

export interface CapturedExchange {
  index: number;
  capturedRequest: unknown;
  responseHeaders: Record<string, string>;
  responseKind: "sse" | "json";
}

export interface CapturedDispatch {
  index: number;
  toolName: string;
  args: unknown;
  result: unknown;
}

interface InternalExchange extends CapturedExchange {
  canonicalRequestText: string;
  responseBytes: Uint8Array;
}

interface ToolDispatchQueue {
  results: unknown[];
  consumed: number;
}

function parseLeafIndex(name: string): number {
  const dashIdx = name.indexOf("-");
  const prefix = dashIdx === -1 ? name : name.slice(0, dashIdx);
  const n = Number(prefix);
  if (!Number.isInteger(n) || n < 0) {
    throw new Error(
      `Session replay: directory/file name "${name}" does not start with a non-negative integer prefix`,
    );
  }
  return n;
}

function parseToolName(filename: string): string {
  const trimmed = filename.endsWith(".json")
    ? filename.slice(0, -".json".length)
    : filename;
  const dashIdx = trimmed.indexOf("-");
  if (dashIdx === -1) {
    throw new Error(
      `Session replay: dispatch filename "${filename}" does not match the "<index>-<toolName>.json" shape`,
    );
  }
  return trimmed.slice(dashIdx + 1);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function canonicalise(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalise);
  }
  if (isRecord(value)) {
    const sortedKeys = Object.keys(value).sort();
    const out: Record<string, unknown> = {};
    for (const k of sortedKeys) {
      out[k] = canonicalise(value[k]);
    }
    return out;
  }
  return value;
}

function canonicaliseJSONText(text: string): string {
  const parsed: unknown = JSON.parse(text);
  return JSON.stringify(canonicalise(parsed));
}

function shortDiff(captured: unknown, actual: unknown): string {
  const capturedText = JSON.stringify(canonicalise(captured), null, 2);
  const actualText = JSON.stringify(canonicalise(actual), null, 2);
  const capLines = capturedText.split("\n");
  const actLines = actualText.split("\n");
  const lines: string[] = ["Captured request body did not match actual:"];
  const max = Math.max(capLines.length, actLines.length);
  for (let i = 0; i < max; i++) {
    const c = capLines[i];
    const a = actLines[i];
    if (c === a) continue;
    lines.push(`  -- captured[${String(i)}]: ${c ?? "<missing>"}`);
    lines.push(`  ++ actual  [${String(i)}]: ${a ?? "<missing>"}`);
    if (lines.length >= 24) {
      lines.push("  (diff truncated)");
      break;
    }
  }
  return lines.join("\n");
}

async function readJSONObject(
  filePath: string,
): Promise<Record<string, unknown>> {
  const text = await fs.readFile(filePath, "utf-8");
  const parsed: unknown = JSON.parse(text);
  if (!isRecord(parsed)) {
    throw new Error(
      `Session replay: ${filePath} did not parse as a JSON object`,
    );
  }
  return parsed;
}

async function loadExchanges(sessionDir: string): Promise<InternalExchange[]> {
  const exchangesRoot = path.join(sessionDir, "exchanges");
  let names: string[];
  try {
    names = await fs.readdir(exchangesRoot);
  } catch {
    return [];
  }
  const parsed: { name: string; index: number }[] = [];
  for (const name of names) {
    const stat = await fs.stat(path.join(exchangesRoot, name));
    if (!stat.isDirectory()) continue;
    parsed.push({ name, index: parseLeafIndex(name) });
  }
  parsed.sort((a, b) => a.index - b.index);

  const out: InternalExchange[] = [];
  for (const { name, index } of parsed) {
    const dir = path.join(exchangesRoot, name);
    const requestText = await fs.readFile(
      path.join(dir, "request.json"),
      "utf-8",
    );
    const capturedRequest: unknown = JSON.parse(requestText);

    const parsedHeaders = await readJSONObject(
      path.join(dir, "response-headers.json"),
    );
    const headers: Record<string, string> = {};
    for (const [k, v] of Object.entries(parsedHeaders)) {
      if (typeof v === "string") headers[k] = v;
    }

    const ssePath = path.join(dir, "response.sse");
    const jsonPath = path.join(dir, "response.json");
    let responseBytes: Uint8Array;
    let responseKind: "sse" | "json";
    let sseExists = false;
    try {
      const stat = await fs.stat(ssePath);
      sseExists = stat.isFile();
    } catch {
      sseExists = false;
    }
    if (sseExists) {
      responseBytes = new Uint8Array(await fs.readFile(ssePath));
      responseKind = "sse";
    } else {
      try {
        const text = await fs.readFile(jsonPath, "utf-8");
        responseBytes = new TextEncoder().encode(text);
        responseKind = "json";
      } catch (cause) {
        throw new Error(
          `Session replay: exchange ${String(index)} in ${dir} has neither response.sse nor response.json`,
          { cause },
        );
      }
    }

    out.push({
      index,
      capturedRequest,
      canonicalRequestText: canonicaliseJSONText(requestText),
      responseBytes,
      responseHeaders: headers,
      responseKind,
    });
  }

  for (let i = 0; i < out.length; i++) {
    if (out[i]?.index !== i) {
      throw new Error(
        `Session replay: exchange indices are not contiguous starting from 0; ` +
          `expected ${String(i)} at position ${String(i)} but found ${String(out[i]?.index)}`,
      );
    }
  }

  return out;
}

async function loadDispatches(sessionDir: string): Promise<CapturedDispatch[]> {
  const dispatchesRoot = path.join(sessionDir, "dispatches");
  let names: string[];
  try {
    names = await fs.readdir(dispatchesRoot);
  } catch {
    return [];
  }
  const parsed: { name: string; index: number }[] = [];
  for (const name of names) {
    if (!name.endsWith(".json")) continue;
    parsed.push({ name, index: parseLeafIndex(name) });
  }
  parsed.sort((a, b) => a.index - b.index);

  const out: CapturedDispatch[] = [];
  for (const { name, index } of parsed) {
    const toolName = parseToolName(name);
    const fileRecord = await readJSONObject(path.join(dispatchesRoot, name));
    if (!("args" in fileRecord) || !("result" in fileRecord)) {
      throw new Error(
        `Session replay: dispatch ${name} is missing "args" or "result"`,
      );
    }
    out.push({
      index,
      toolName,
      args: fileRecord["args"],
      result: fileRecord["result"],
    });
  }

  return out;
}

export async function createReplayHarness(
  opts: CreateReplayHarnessOpts,
): Promise<ReplayHarness> {
  const { sessionDir } = opts;
  const manifest = await loadSessionManifest(sessionDir);
  const exchanges = await loadExchanges(sessionDir);
  const dispatches = await loadDispatches(sessionDir);

  if (exchanges.length === 0) {
    throw new Error(
      `Session replay: ${sessionDir} contains no exchanges; nothing to replay`,
    );
  }

  const source: InferenceSource = {
    id: opts.sourceId ?? `${manifest.source.provider}:${manifest.source.model}`,
    provider: manifest.source.provider,
    baseURL: manifest.source.baseURL,
    apiKey: opts.apiKey ?? "session-replay-stub",
    model: manifest.source.model,
  };

  const inner = setupHarness();
  let turnCount = 0;
  let internalSeq = 0;

  // The exchanges have already been pre-canonicalised; their
  // `canonicalRequestText` is what each matcher compares against.
  // Matchers are registered lazily — one per `runTurn` call — so
  // that each turn's scheduled chunks fire at the right virtual
  // time relative to that turn's run.

  // Per-tool dispatch queues. Each invocation pops the next result
  // from the queue for that tool. Args from the recording are exposed
  // via `capturedDispatches` for the caller to assert against; the
  // replay handler itself does not validate them, because args
  // divergence would already show up as a body divergence on the
  // exchange that carries the tool_result block.
  const dispatchQueues = new Map<string, ToolDispatchQueue>();
  for (const dispatch of dispatches) {
    let queue = dispatchQueues.get(dispatch.toolName);
    if (queue === undefined) {
      queue = { results: [], consumed: 0 };
      dispatchQueues.set(dispatch.toolName, queue);
    }
    queue.results.push(dispatch.result);
  }
  for (const [toolName, queue] of dispatchQueues) {
    inner.scenario.onTool(toolName, () => {
      if (queue.consumed >= queue.results.length) {
        throw new Error(
          `Session replay: tool "${toolName}" dispatched more times than the ` +
            `capture recorded (${String(queue.results.length)} captured dispatches)`,
        );
      }
      const next = queue.results[queue.consumed++];
      return next;
    });
  }

  const runTurn = async (runOpts: RunTurnOpts): Promise<InferenceEvent[]> => {
    const exchangeIndex = turnCount;
    if (exchangeIndex >= exchanges.length) {
      throw new Error(
        `Session replay: runTurn called ${String(turnCount + 1)} times but the ` +
          `capture has only ${String(exchanges.length)} exchanges`,
      );
    }
    const exchange = exchanges[exchangeIndex];
    if (exchange === undefined) {
      throw new Error(
        `Session replay: exchange ${String(exchangeIndex)} missing from loaded set (internal bug)`,
      );
    }
    turnCount++;

    // Register this turn's matcher and enqueue its response stream
    // RIGHT NOW so chunks fire at the current virtual time. Lazy
    // registration is what makes the multi-turn shape work: we want
    // turn N's chunks scheduled relative to clock.now() at the moment
    // turn N starts, not relative to harness construction.
    const stream = inner.scenario.createStream();
    stream.enqueueAt(inner.clock.now() + 1, exchange.responseBytes);
    stream.closeAt(inner.clock.now() + 2);
    inner.scenario.whenRequestBodyMatches(
      (bodyText) => {
        try {
          return (
            canonicaliseJSONText(bodyText) === exchange.canonicalRequestText
          );
        } catch {
          return false;
        }
      },
      stream,
      { headers: exchange.responseHeaders },
    );

    const nextSeq = runOpts.nextSeq ?? (() => ++internalSeq);
    const events: InferenceEvent[] = [];
    const collector = (async () => {
      for await (const ev of inner.runInference({
        turns: runOpts.turns,
        source,
        nextSeq,
      })) {
        events.push(ev);
      }
    })();

    try {
      await Promise.all([collector, inner.run()]);
    } catch (cause) {
      if (cause instanceof UnmatchedFetchError) {
        const fetches = cause.waiting;
        const headerLines = fetches
          .slice(0, 3)
          .map(
            (f) =>
              `  ${f.method} ${f.url}` +
              (Object.keys(f.headers).length > 0
                ? ` headers=${JSON.stringify(f.headers)}`
                : ""),
          )
          .join("\n");
        throw new SessionReplayMismatchError({
          exchangeIndex,
          captured: exchange.capturedRequest,
          actual: null,
          diff:
            `Expected a request matching captured exchange ${String(exchangeIndex)}, ` +
            `but no fetch matched.\nUnmatched fetches:\n${headerLines}\n` +
            `(The harness does not capture unmatched request bodies; ` +
            `inspect the failing call's body via a logger if the diff is needed.)`,
          sessionDir,
        });
      }
      throw cause;
    }

    // Verify the actual body matches even when canonicalisation
    // would have produced a false positive (rare — would require two
    // distinct shapes to canonicalise identically). The matched
    // request is the most recent one routed; `matchedRequests` is
    // ordered, so the matched index corresponds to `turnCount - 1`.
    const allMatched = inner.scenario.matchedRequests();
    const actualRequest = allMatched[turnCount - 1];
    if (actualRequest !== undefined) {
      const actualBodyText = await actualRequest.text();
      const actualCanonical = canonicaliseJSONText(actualBodyText);
      if (actualCanonical !== exchange.canonicalRequestText) {
        const actualJSON: unknown = JSON.parse(actualBodyText);
        throw new SessionReplayMismatchError({
          exchangeIndex,
          captured: exchange.capturedRequest,
          actual: actualJSON,
          diff: shortDiff(exchange.capturedRequest, actualJSON),
          sessionDir,
        });
      }
    }

    return events;
  };

  const assertFullyConsumed = (): void => {
    if (turnCount < exchanges.length) {
      const expectedIndex = turnCount;
      const expected = exchanges[expectedIndex];
      if (expected !== undefined) {
        throw new SessionReplayMismatchError({
          exchangeIndex: expectedIndex,
          captured: expected.capturedRequest,
          actual: null,
          diff:
            `Replay ended after ${String(turnCount)} exchange(s), but the capture has ${String(exchanges.length)}. ` +
            `The caller stopped driving runTurn before all captured exchanges were consumed.`,
          sessionDir,
        });
      }
    }
    for (const [toolName, queue] of dispatchQueues) {
      if (queue.consumed < queue.results.length) {
        throw new Error(
          `Session replay: tool "${toolName}" was dispatched ` +
            `${String(queue.consumed)} time(s), but the capture has ` +
            `${String(queue.results.length)} dispatch(es) recorded.`,
        );
      }
    }
  };

  const capturedExchanges: CapturedExchange[] = exchanges.map((e) => ({
    index: e.index,
    capturedRequest: e.capturedRequest,
    responseHeaders: e.responseHeaders,
    responseKind: e.responseKind,
  }));

  return {
    manifest,
    source,
    runTurn,
    assertFullyConsumed,
    capturedExchanges,
    capturedDispatches: dispatches,
    dispose: () => inner.dispose(),
  };
}
