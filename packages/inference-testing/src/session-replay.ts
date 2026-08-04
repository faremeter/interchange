// Session replay: take a captured session directory, register matchers
// for every captured exchange, register tool handlers that serve
// captured dispatch results verbatim, and drive production
// `runInference` through it. Replay surfaces orchestration-layer
// regressions that the single-exchange compat-replay layer cannot
// see — multi-turn body construction, conversation history
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

import { runInference, type Dependencies } from "@intx/inference";
import { createDefaultDependencies } from "@intx/inference/providers";

import { UnmatchedFetchError } from "./errors";
import { setupHarness } from "./harness";
import {
  loadCaptureManifest,
  type CaptureManifest,
} from "@intx/inference-discovery/catalog";
import {
  INVARIANTS,
  type Invariant,
  type InvariantViolation,
} from "./invariants";
import { isDelayedEnvelope } from "./tool-handler";

/**
 * Discriminates the specific shape of the contract violation. Used by
 * `SessionReplayMismatchError.kind` so callers can branch (e.g., "I
 * expected the conversation to terminate early; ignore
 * exchanges_under_consumed but re-raise body_diverged").
 */
export type SessionReplayMismatchKind =
  | "body_diverged"
  | "no_matcher_fired"
  | "exchanges_under_consumed"
  | "exchanges_over_consumed"
  | "dispatches_under_consumed"
  | "dispatches_over_consumed";

export class SessionReplayMismatchError extends Error {
  readonly kind: SessionReplayMismatchKind;
  readonly exchangeIndex: number | null;
  readonly toolName: string | null;
  readonly captured: unknown;
  readonly actual: unknown;
  readonly diff: string;
  readonly sessionDir: string;

  constructor(opts: {
    kind: SessionReplayMismatchKind;
    exchangeIndex?: number | null;
    toolName?: string | null;
    captured: unknown;
    actual: unknown;
    diff: string;
    sessionDir: string;
  }) {
    const exchangeIndex = opts.exchangeIndex ?? null;
    const toolName = opts.toolName ?? null;
    const subject =
      exchangeIndex !== null
        ? `exchange ${String(exchangeIndex)}`
        : toolName !== null
          ? `tool "${toolName}"`
          : "(session)";
    super(
      `Session replay [${opts.kind}]: ${subject} in ${opts.sessionDir}.\n${opts.diff}`,
    );
    this.name = "SessionReplayMismatchError";
    this.kind = opts.kind;
    this.exchangeIndex = exchangeIndex;
    this.toolName = toolName;
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
  readonly manifest: CaptureManifest;
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

// The request side is a discriminated union: a JSON request carries its
// parsed body (matched by canonical-JSON equality), a raw request carries only
// its byte length on the public shape (the bytes match by byte equality — the
// Files-API upload step, not driven by the runInference replay). The response
// side is orthogonal and identical across both.
export type CapturedExchange = {
  index: number;
  responseHeaders: Record<string, string>;
  responseKind: "sse" | "json";
} & (
  | { requestKind: "json"; capturedRequest: unknown }
  | { requestKind: "raw"; requestByteLength: number }
);

export interface CapturedDispatch {
  index: number;
  toolName: string;
  args: unknown;
  result: unknown;
}

type InternalExchange = {
  index: number;
  responseHeaders: Record<string, string>;
  responseKind: "sse" | "json";
  responseBytes: Uint8Array;
} & (
  | {
      requestKind: "json";
      capturedRequest: unknown;
      canonicalRequestText: string;
    }
  | { requestKind: "raw"; requestBytes: Uint8Array }
);

// Byte-equality is the match primitive for a raw request body (request.bin);
// canonical-JSON equality does not apply to opaque bytes. The runInference
// replay driver never invokes this — it rejects raw exchanges up front — so it
// exists for a future Files-API-aware driver that replays the upload step.
export function requestBytesMatch(a: Uint8Array, b: Uint8Array): boolean {
  return Buffer.from(a).equals(Buffer.from(b));
}

// The captured request to surface in a mismatch diagnostic: the parsed JSON
// body for a JSON exchange, or null for a raw one. Raw exchanges never reach
// the runInference replay driver (rejected at construction), so the raw arm is
// defensive.
function reportedRequest(e: InternalExchange): unknown {
  return e.requestKind === "json" ? e.capturedRequest : null;
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

async function fileExists(p: string): Promise<boolean> {
  try {
    const stat = await fs.stat(p);
    return stat.isFile();
  } catch (cause) {
    const code =
      cause !== null && typeof cause === "object" && "code" in cause
        ? cause.code
        : undefined;
    if (code === "ENOENT") return false;
    throw cause;
  }
}

// A captured dispatch result shaped `{ result, virtualDelayMs: <finite
// non-negative number> }` collides with the test-harness "delayed
// envelope" return shape that `ToolHandlerRegistry` unwraps. If a
// session ever carried such a result and the replay harness served
// it verbatim to `scenario.onTool`, the registry would schedule a
// virtual delay and pass only the inner `.result` to the reactor.
// That's wrong-but-not-loud behavior; recording rejects it at write
// time too via the same predicate. Reject at load time so the
// failure is contained to the load boundary.
function rejectEnvelopeShape(
  index: number,
  toolName: string,
  result: unknown,
): void {
  if (!isDelayedEnvelope(result)) return;
  throw new Error(
    `Session replay: dispatch ${String(index)} for tool "${toolName}" has a ` +
      `captured result shaped { result, virtualDelayMs: ${String(result.virtualDelayMs)} }, which ` +
      `would be unwrapped as a test-harness delayed envelope by the tool ` +
      `dispatch registry. Captured results must not collide with that shape.`,
  );
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

    // A well-formed exchange carries exactly one request body: `request.json`
    // (a JSON body, matched by canonical-JSON equality) or `request.bin` (raw
    // bytes — e.g. a Files-API upload — matched by byte equality). Both
    // present or neither present is a malformed capture. A raw request loads
    // fine here; whether a given replay driver can *drive* it is a separate
    // constraint the driver enforces (createReplayHarness rejects raw).
    const requestJsonPath = path.join(dir, "request.json");
    const requestBinPath = path.join(dir, "request.bin");
    const jsonRequestExists = await fileExists(requestJsonPath);
    const binRequestExists = await fileExists(requestBinPath);
    if (binRequestExists && jsonRequestExists) {
      throw new Error(
        `Session replay: exchange ${String(index)} in ${dir} has both ` +
          `request.json and request.bin; the capture is malformed.`,
      );
    }
    if (!binRequestExists && !jsonRequestExists) {
      throw new Error(
        `Session replay: exchange ${String(index)} in ${dir} has no ` +
          `request.json and no request.bin; the capture is malformed.`,
      );
    }
    let request:
      | {
          requestKind: "json";
          capturedRequest: unknown;
          canonicalRequestText: string;
        }
      | { requestKind: "raw"; requestBytes: Uint8Array };
    if (jsonRequestExists) {
      const requestText = await fs.readFile(requestJsonPath, "utf-8");
      request = {
        requestKind: "json",
        capturedRequest: JSON.parse(requestText),
        canonicalRequestText: canonicaliseJSONText(requestText),
      };
    } else {
      request = {
        requestKind: "raw",
        requestBytes: new Uint8Array(await fs.readFile(requestBinPath)),
      };
    }

    const parsedHeaders = await readJSONObject(
      path.join(dir, "response-headers.json"),
    );
    const headers: Record<string, string> = {};
    for (const [k, v] of Object.entries(parsedHeaders)) {
      if (typeof v === "string") headers[k] = v;
    }

    const ssePath = path.join(dir, "response.sse");
    const jsonPath = path.join(dir, "response.json");
    const sseExists = await fileExists(ssePath);
    const jsonExists = await fileExists(jsonPath);
    // Both present is a sign of a bad merge or partial regeneration;
    // refuse to guess which one is canonical.
    if (sseExists && jsonExists) {
      throw new Error(
        `Session replay: exchange ${String(index)} in ${dir} has both ` +
          `response.sse and response.json; the capture is malformed.`,
      );
    }
    let responseBytes: Uint8Array;
    let responseKind: "sse" | "json";
    if (sseExists) {
      responseBytes = new Uint8Array(await fs.readFile(ssePath));
      responseKind = "sse";
    } else if (jsonExists) {
      const text = await fs.readFile(jsonPath, "utf-8");
      responseBytes = new TextEncoder().encode(text);
      responseKind = "json";
    } else {
      throw new Error(
        `Session replay: exchange ${String(index)} in ${dir} has neither response.sse nor response.json`,
      );
    }

    out.push({
      index,
      ...request,
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
    rejectEnvelopeShape(index, toolName, fileRecord["result"]);
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
  const manifest = await loadCaptureManifest(sessionDir);
  const exchanges = await loadExchanges(sessionDir);
  const dispatches = await loadDispatches(sessionDir);

  if (exchanges.length === 0) {
    throw new Error(
      `Session replay: ${sessionDir} contains no exchanges; nothing to replay`,
    );
  }

  // This harness drives replay through `runInference` (see runTurn). A raw
  // request body is a Files-API upload step, which is not a `runInference`
  // call, so this driver cannot faithfully replay it. Reject loudly here — at
  // the layer that owns the constraint — rather than let runTurn register a
  // JSON matcher that silently never binds. A Files-API-aware driver is the
  // layer that would drive such a session.
  const rawExchange = exchanges.find((e) => e.requestKind === "raw");
  if (rawExchange !== undefined) {
    throw new Error(
      `Session replay: ${sessionDir} contains a raw-byte request (Files-API ` +
        `upload) at exchange ${String(rawExchange.index)}, which the ` +
        `runInference replay driver cannot drive; a Files-API-aware driver is ` +
        `required.`,
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
        throw new SessionReplayMismatchError({
          kind: "dispatches_over_consumed",
          toolName,
          captured: queue.results,
          actual: null,
          diff:
            `Tool "${toolName}" dispatched more times than the capture recorded ` +
            `(${String(queue.results.length)} captured dispatches).`,
          sessionDir,
        });
      }
      const next = queue.results[queue.consumed++];
      return next;
    });
  }

  // After a runTurn throws a `SessionReplayMismatchError`, the inner
  // harness retains the stream and matcher we registered for the
  // failed turn — the harness exposes no API to retract a matcher.
  // Subsequent runTurn calls would register the next exchange's
  // matcher on top of the stale one, and the predicates only happen
  // to converge because both exchanges' canonical bodies differ.
  // Rather than rely on the accident, the harness is poisoned after
  // any runTurn failure: a fresh replay harness is required to
  // re-drive the session.
  let poisoned = false;

  const runTurn = async (runOpts: RunTurnOpts): Promise<InferenceEvent[]> => {
    if (poisoned) {
      throw new Error(
        `Session replay: this harness was poisoned by an earlier runTurn ` +
          `failure. Construct a fresh replay harness to re-drive the session.`,
      );
    }
    const exchangeIndex = turnCount;
    if (exchangeIndex >= exchanges.length) {
      const lastExchange = exchanges[exchanges.length - 1];
      if (lastExchange === undefined) {
        throw new Error(
          `Session replay: exchanges array is empty (internal bug; loader ` +
            `should have rejected this session)`,
        );
      }
      throw new SessionReplayMismatchError({
        kind: "exchanges_over_consumed",
        exchangeIndex,
        captured: reportedRequest(lastExchange),
        actual: null,
        diff:
          `runTurn called ${String(turnCount + 1)} times but the capture has only ` +
          `${String(exchanges.length)} exchanges.`,
        sessionDir,
      });
    }
    const exchange = exchanges[exchangeIndex];
    if (exchange === undefined) {
      throw new Error(
        `Session replay: exchange ${String(exchangeIndex)} missing from loaded set (internal bug)`,
      );
    }
    if (exchange.requestKind !== "json") {
      // Unreachable: createReplayHarness rejects sessions carrying a raw
      // exchange. Kept so a future change that relaxes that guard fails loudly
      // here rather than silently registering a JSON matcher that never binds.
      throw new Error(
        `Session replay: exchange ${String(exchangeIndex)} is a raw-byte ` +
          `request; the runInference replay driver only drives JSON exchanges.`,
      );
    }
    // Track which matched request count we had BEFORE the turn so we
    // can identify this turn's matched request by index after it
    // fires. We rely on the invariant that runTurn is the only code
    // path in this harness that registers matchers — see the index
    // safety check below.
    const matchedBefore = inner.scenario.matchedRequests().length;

    // Register this turn's matcher and enqueue its response stream
    // RIGHT NOW so chunks fire at the current virtual time. Lazy
    // registration is what makes the multi-turn shape work: we want
    // turn N's chunks scheduled relative to clock.now() at the moment
    // turn N starts, not relative to harness construction.
    //
    // The body-aware predicate is the only place the actual body is
    // compared against the captured body. If the canonical forms
    // disagree the predicate returns false, no matcher binds, and
    // the harness eventually raises `UnmatchedFetchError` — which
    // we translate to `SessionReplayMismatchError` below.
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

    // Only bump `turnCount` once this turn settles successfully.
    // Otherwise a throw leaves `turnCount` at a value that no
    // longer matches `exchanges[turnCount]`, and the next runTurn
    // call would register the wrong exchange's matcher on top of
    // the stale one.
    const nextSeq = runOpts.nextSeq ?? (() => ++internalSeq);
    const inferenceController = new AbortController();
    const events: InferenceEvent[] = [];
    const collector = (async (): Promise<void> => {
      for await (const ev of inner.runInference({
        turns: runOpts.turns,
        source,
        nextSeq,
        signal: inferenceController.signal,
        // Replay is deterministic: every captured exchange has a
        // one-to-one matcher, and a fetch that fails to bind has no
        // semantically-meaningful "retry the same prompt" outcome —
        // the next attempt would just hit the same bound-or-unbound
        // result. Pin an abort-only policy so the inert scheduler
        // does not deadlock the wrapper on the retry-delay setTimeout
        // when an unmatched-fetch error surfaces through the harness.
        inferenceOptions: { retryPolicy: () => ({ kind: "abort" }) },
      })) {
        events.push(ev);
      }
    })();

    // `Promise.allSettled` lets us inspect both promises even when
    // the run path rejects first. If the harness raised
    // `UnmatchedFetchError`, the collector promise is still awaiting
    // an iterator that will never resolve — we abort it so the
    // surrounding test does not inherit a phantom in-flight
    // iteration.
    const [runResult, collectResult] = await Promise.allSettled([
      inner.run(),
      collector,
    ]);

    if (runResult.status === "rejected") {
      // Abort the iterator so its parked `await fetch(...)` rejects
      // promptly rather than hanging until `dispose()`.
      inferenceController.abort();
      // Drain the collector's rejection so the unhandled-rejection
      // tracker doesn't fire. We re-throw the run-side error which
      // is the primary failure; the collector's rejection is a
      // downstream symptom of the same cause.
      await collector.catch(() => undefined);
      const cause = runResult.reason;
      poisoned = true;
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
          kind: "no_matcher_fired",
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
    if (collectResult.status === "rejected") {
      poisoned = true;
      // Detect the "no onTool handler registered for tool X" error
      // the inner harness throws when production runInference emits
      // a tool_call.end for a tool name the capture has no dispatches
      // for. The error originates inside @intx/inference-testing's
      // own harness.ts and the message is stable within this package;
      // translate it to a SessionReplayMismatchError so the caller
      // sees a discriminated mismatch instead of a generic
      // "Harness.runInference" failure.
      const reason = collectResult.reason;
      if (reason instanceof Error) {
        const match = /tool "([^"]+)" but no handler was registered/.exec(
          reason.message,
        );
        if (match !== null) {
          const toolName = match[1] ?? "<unknown>";
          throw new SessionReplayMismatchError({
            kind: "dispatches_over_consumed",
            toolName,
            captured: [],
            actual: null,
            diff:
              `Production runInference emitted a tool_call.end for tool ` +
              `"${toolName}" during exchange ${String(exchangeIndex)}, but the ` +
              `captured session has no dispatches recorded for that tool. ` +
              `Either the production reactor's wire output diverged from ` +
              `the recording (more likely), or the capture is incomplete.`,
            sessionDir,
          });
        }
      }
      throw reason;
    }

    // Sanity: exactly one new matched request should have landed on
    // the inner harness during this turn. Anything else means a
    // future helper has started routing fetches outside `runTurn` —
    // and the `allMatched[turnCount]` indexing pattern this harness
    // uses would silently desync, so fail loudly.
    const matchedAfter = inner.scenario.matchedRequests().length;
    const newlyMatched = matchedAfter - matchedBefore;
    if (newlyMatched !== 1) {
      poisoned = true;
      throw new Error(
        `Session replay: runTurn expected exactly one matched request for ` +
          `exchange ${String(exchangeIndex)} but observed ${String(newlyMatched)}. ` +
          `runTurn must be the only path that routes fetches through this harness.`,
      );
    }

    turnCount++;
    return events;
  };

  const assertFullyConsumed = (): void => {
    if (turnCount < exchanges.length) {
      const expectedIndex = turnCount;
      const expected = exchanges[expectedIndex];
      // `turnCount < exchanges.length` proves `expectedIndex` is in
      // range and `loadExchanges` has already verified the array
      // density, so `expected` must be defined here. Invariant-throw
      // rather than silently skip so a future refactor that breaks
      // the invariant fails loudly.
      if (expected === undefined) {
        throw new Error(
          `Session replay: exchanges[${String(expectedIndex)}] missing despite ` +
            `turnCount=${String(turnCount)} < length=${String(exchanges.length)} (internal bug)`,
        );
      }
      throw new SessionReplayMismatchError({
        kind: "exchanges_under_consumed",
        exchangeIndex: expectedIndex,
        captured: reportedRequest(expected),
        actual: null,
        diff:
          `Replay ended after ${String(turnCount)} exchange(s), but the capture has ${String(exchanges.length)}. ` +
          `The caller stopped driving runTurn before all captured exchanges were consumed.`,
        sessionDir,
      });
    }
    for (const [toolName, queue] of dispatchQueues) {
      if (queue.consumed < queue.results.length) {
        throw new SessionReplayMismatchError({
          kind: "dispatches_under_consumed",
          toolName,
          captured: queue.results,
          actual: queue.consumed,
          diff:
            `Tool "${toolName}" was dispatched ${String(queue.consumed)} time(s), ` +
            `but the capture has ${String(queue.results.length)} dispatch(es) recorded.`,
          sessionDir,
        });
      }
    }
  };

  const capturedExchanges: CapturedExchange[] = exchanges.map((e) =>
    e.requestKind === "json"
      ? {
          index: e.index,
          requestKind: "json",
          capturedRequest: e.capturedRequest,
          responseHeaders: e.responseHeaders,
          responseKind: e.responseKind,
        }
      : {
          index: e.index,
          requestKind: "raw",
          requestByteLength: e.requestBytes.length,
          responseHeaders: e.responseHeaders,
          responseKind: e.responseKind,
        },
  );

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

// Anything-goes parser replay: drive each captured JSON exchange's response
// through the real adapter with a stub fetch, ignoring the request body and
// the conversation entirely. This is the loose counterpart to
// createReplayHarness's strict, body-matched, chained replay — it lets the
// parser regression exercise every captured response's decoding without
// threading a multi-turn conversation or dispatches.
//
// It shares the session loader (loadExchanges) with strict replay but drives
// the raw @intx/inference `runInference` directly, NOT the inner harness, so a
// captured response that terminates on a tool_call collects its events instead
// of tripping the inner harness's auto-dispatch (which throws for want of a
// registered handler). Raw-byte (Files-API upload) exchanges are skipped
// per-exchange rather than rejecting the whole session, so a two-step
// Files-API session's generate exchange is still exercised.
//
// Each replayed exchange also carries the invariant violations found over its
// decoded event stream, so a caller checking parser output against the wire
// contract reads events and violations from one uniform result rather than
// re-running the invariants itself.
export type ParserReplayResult =
  | {
      index: number;
      kind: "replayed";
      events: InferenceEvent[];
      violations: InvariantViolation[];
    }
  | { index: number; kind: "skipped"; reason: "raw_request" };

export type ParserReplayOpts = {
  sessionDir: string;
  /** Invariants to apply to each replayed exchange. Defaults to INVARIANTS. */
  invariants?: readonly Invariant[];
};

export async function replayResponsesForParsing(
  opts: ParserReplayOpts,
): Promise<ParserReplayResult[]> {
  const { sessionDir } = opts;
  const checks = opts.invariants ?? INVARIANTS;
  const manifest = await loadCaptureManifest(sessionDir);
  const exchanges = await loadExchanges(sessionDir);
  if (exchanges.length === 0) {
    throw new Error(
      `Parser replay: ${sessionDir} contains no exchanges; nothing to replay`,
    );
  }

  const source: InferenceSource = {
    id: `${manifest.source.provider}:${manifest.source.model}`,
    provider: manifest.source.provider,
    baseURL: manifest.source.baseURL,
    apiKey: "session-replay-stub",
    model: manifest.source.model,
  };

  // Build the adapter registry + scheduler once; only `fetch` (which closes
  // over the per-exchange call counter and captured bytes) varies per turn.
  const baseDeps = createDefaultDependencies();

  const results: ParserReplayResult[] = [];
  for (const exchange of exchanges) {
    if (exchange.requestKind === "raw") {
      // A Files-API upload step is not a runInference call; there is no
      // adapter response to parse, so skip it. The sibling generate exchange
      // is a normal JSON exchange and is still replayed.
      results.push({
        index: exchange.index,
        kind: "skipped",
        reason: "raw_request",
      });
      continue;
    }

    // Serve the captured response verbatim, including its content-type header,
    // which the harness branches on to pick the SSE or JSON decode path. An
    // abort-only retry policy keeps a parse failure surfacing as one
    // inference.error event rather than a retry that re-opens the fetch; a
    // single fetch call is therefore expected.
    let fetchCallCount = 0;
    const headers = new Headers(exchange.responseHeaders);
    const deps: Dependencies = {
      ...baseDeps,
      fetch: () => {
        fetchCallCount++;
        return Promise.resolve(
          new Response(exchange.responseBytes, { status: 200, headers }),
        );
      },
    };

    let seq = 0;
    const events: InferenceEvent[] = [];
    for await (const event of runInference({
      turns: [
        { role: "user", content: [{ type: "text", text: "x" }], timestamp: 0 },
      ],
      source,
      nextSeq: () => ++seq,
      deps,
      inferenceOptions: { retryPolicy: () => ({ kind: "abort" }) },
    })) {
      events.push(event);
    }

    if (fetchCallCount !== 1) {
      throw new Error(
        `Parser replay: exchange ${String(exchange.index)} in ${sessionDir} ` +
          `opened ${String(fetchCallCount)} fetch call(s); expected exactly one.`,
      );
    }

    const violations: InvariantViolation[] = [];
    for (const invariant of checks) {
      violations.push(...invariant.check(events));
    }

    results.push({
      index: exchange.index,
      kind: "replayed",
      events,
      violations,
    });
  }

  return results;
}
