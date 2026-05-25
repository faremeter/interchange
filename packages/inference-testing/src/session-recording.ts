// Session recording: drive real `runInference` calls against real (or
// test-supplied) providers, observing every request/response exchange and
// every tool dispatch, and writing the conversation to disk in the session
// capture format.
//
// Recorded sessions later feed `createReplayHarness` so the orchestration
// regressions only surface when turns chain (cross-turn body construction,
// dispatch wiring, conversation length growth) can be re-run against
// frozen wire and frozen tool I/O.

import fs from "node:fs/promises";
import path from "node:path";

import {
  assertNotCI,
  detectResponseKind,
  writeCapture,
  type ResponseBody,
} from "@intx/inference-discovery";
import {
  HarnessId,
  runInference,
  type Dependencies,
  type InferenceHarnessOptions,
} from "@intx/inference";
import type { InferenceEvent } from "@intx/types/runtime";

import { writeSessionManifest, type SessionManifest } from "./session-manifest";
import { isDelayedEnvelope, type ToolHandler } from "./tool-handler";

/**
 * The recording harness's fetch override has the same signature as the
 * production `Dependencies["fetch"]`. Tests pass a stub that returns
 * synthetic provider wire bytes; production recording scripts omit this
 * field and let the harness call real `globalThis.fetch`.
 */
export type RecordingFetchLike = Dependencies["fetch"];

export interface CreateRecordingHarnessOpts {
  /** Absolute path to the session directory. Created if it does not exist. */
  outputDir: string;
  /**
   * Provider/model/baseURL the recording targets. Written verbatim into
   * the top-level `session.json`. The replay harness consumes this to
   * construct the `InferenceSource` passed to `runInference`.
   */
  source: SessionManifest["source"];
  /**
   * Hard ceiling on how many fetch calls the harness will wrap before
   * throwing `SessionRecordingBudgetExceededError`. Guards against
   * runaway reactor loops silently racking up provider charges.
   */
  maxExchanges: number;
  /** Header names redacted from each captured request. Case-insensitive. */
  redactRequestHeaders: readonly string[];
  /** Header names redacted from each captured response. Case-insensitive. */
  redactResponseHeaders: readonly string[];
  /**
   * Test seam: when supplied, used in place of `globalThis.fetch`. Must
   * be paired with `bypassCIGuardForTests: true`; supplying one without
   * the other throws at construction.
   */
  fetch?: RecordingFetchLike;
  /**
   * Test seam: skip the inference-discovery CI guard at construction
   * time. Must be paired with a `fetch` override; supplying one without
   * the other throws.
   */
  bypassCIGuardForTests?: boolean;
  /** Override for the `capturedAt` timestamp written to `session.json`. */
  now?: () => Date;
}

export interface RecordingHarness {
  /** Dependencies object to pass into production `runInference` calls. */
  readonly deps: Dependencies;
  /**
   * Register a real tool handler. The recording harness calls it
   * whenever the reactor emits `inference.tool_call.end`, observes the
   * args and the returned value, and writes both to
   * `dispatches/<index>-<toolName>.json`.
   */
  onTool(name: string, handler: ToolHandler): void;
  /**
   * Drive a `runInference` call. Wraps production `runInference` with
   * `deps` already injected and intercepts tool dispatch.
   */
  runInference(
    opts: Omit<InferenceHarnessOptions, "deps">,
  ): AsyncIterable<InferenceEvent>;
  /**
   * Write `session.json`. Required for the directory to be a complete
   * session capture. Callers wrap in try/finally so an aborted
   * recording still produces a (truncated but readable) session.
   */
  finalize(): Promise<void>;
}

export class SessionRecordingBudgetExceededError extends Error {
  readonly maxExchanges: number;
  /** Number of fetches the harness wrapped before the budget tripped. */
  readonly wrapped: number;
  /** Number of fetches attempted (always `wrapped + 1`). */
  readonly attempted: number;

  constructor(maxExchanges: number, wrapped: number) {
    super(
      `Session recording exceeded maxExchanges=${String(maxExchanges)} ` +
        `on the ${String(wrapped + 1)}th attempted fetch (${String(wrapped)} ` +
        `fetches wrapped before the budget tripped). A runaway reactor loop ` +
        `or a mis-sized budget is likely; raise maxExchanges only after ` +
        `confirming the conversation is what you expected.`,
    );
    this.name = "SessionRecordingBudgetExceededError";
    this.maxExchanges = maxExchanges;
    this.wrapped = wrapped;
    this.attempted = wrapped + 1;
  }
}

interface ExtractedRequest {
  url: string;
  method: string;
  headers: Record<string, string>;
  // `null` distinguishes "no body" from "empty body". Node's undici
  // rejects bodies on GET/HEAD methods with a TypeError; passing
  // `null` here lets `recordingFetch` leave `init.body` unset on
  // those methods.
  bodyForSend: string | Uint8Array | null;
  bodyForCapture:
    | { kind: "json"; body: unknown }
    | { kind: "raw"; bytes: Uint8Array; contentType: string };
}

function headersToRecord(
  headers: NonNullable<RequestInit["headers"]> | Headers | undefined,
): Record<string, string> {
  const out: Record<string, string> = {};
  if (headers === undefined) return out;
  if (headers instanceof Headers) {
    headers.forEach((value, key) => {
      out[key] = value;
    });
    return out;
  }
  if (Array.isArray(headers)) {
    for (const entry of headers) {
      const [k, v] = entry;
      // TypeScript enforces string values in HeadersInit array
      // tuples, but the recording wrapper is a public entry point —
      // a JS caller could violate the contract at runtime. Reject
      // non-strings explicitly so the captured headers file never
      // carries non-string values.
      if (typeof k !== "string" || typeof v !== "string") {
        throw new Error(
          `Session recording: array-tuple header entry has non-string ` +
            `key or value (key type=${typeof k}, value type=${typeof v}); ` +
            `headers must be strings.`,
        );
      }
      out[k] = v;
    }
    return out;
  }
  for (const [k, v] of Object.entries(headers)) {
    // Record-style headers reach this branch. Same non-string
    // rejection applies — the captured file is strictly string-
    // valued and we will not silently coerce.
    if (typeof v !== "string") {
      throw new Error(
        `Session recording: header "${k}" has non-string value ` +
          `(type=${typeof v}); headers must be strings.`,
      );
    }
    out[k] = v;
  }
  return out;
}

async function extractRequest(
  input: string | URL | Request,
  init: RequestInit | undefined,
): Promise<ExtractedRequest> {
  let url: string;
  let method: string;
  let headers: Record<string, string>;
  let body: RequestInit["body"];

  if (input instanceof Request) {
    url = input.url;
    method = init?.method ?? input.method;
    headers = headersToRecord(init?.headers ?? input.headers);
    body =
      init?.body !== undefined
        ? init.body
        : new Uint8Array(await input.clone().arrayBuffer());
  } else {
    url = input instanceof URL ? input.toString() : input;
    method = init?.method ?? "GET";
    headers = headersToRecord(init?.headers);
    body = init?.body ?? null;
  }

  const contentType =
    Object.entries(headers).find(
      ([k]) => k.toLowerCase() === "content-type",
    )?.[1] ?? "application/octet-stream";

  let bodyForSend: string | Uint8Array | null;
  let bodyForCapture: ExtractedRequest["bodyForCapture"];

  if (body === null || body === undefined || body === "") {
    // Empty string is treated as "no body" too — undici rejects an
    // empty-string body on GET/HEAD, and there's no behavior the
    // empty string forwards that `null` does not.
    bodyForSend = null;
    bodyForCapture = { kind: "raw", bytes: new Uint8Array(), contentType };
  } else if (typeof body === "string") {
    bodyForSend = body;
    if (contentType.startsWith("application/json")) {
      // Recording is meant to be observation, not validation. A
      // malformed JSON body is something the production code would
      // happily forward to the network — surfacing it as a recording
      // failure would change the program's behavior under recording.
      // Fall back to raw capture when JSON.parse rejects.
      try {
        const parsed: unknown = JSON.parse(body);
        bodyForCapture = { kind: "json", body: parsed };
      } catch {
        bodyForCapture = {
          kind: "raw",
          bytes: new TextEncoder().encode(body),
          contentType,
        };
      }
    } else {
      bodyForCapture = {
        kind: "raw",
        bytes: new TextEncoder().encode(body),
        contentType,
      };
    }
  } else if (body instanceof Uint8Array) {
    bodyForSend = body;
    bodyForCapture = { kind: "raw", bytes: body, contentType };
  } else if (body instanceof ArrayBuffer) {
    const bytes = new Uint8Array(body);
    bodyForSend = bytes;
    bodyForCapture = { kind: "raw", bytes, contentType };
  } else {
    throw new Error(
      `Session recording: unsupported request body type ${String(
        Object.prototype.toString.call(body),
      )}; session captures expect string or byte bodies from the inference layer`,
    );
  }

  return { url, method, headers, bodyForSend, bodyForCapture };
}

function responseHeadersToRecord(response: Response): Record<string, string> {
  const out: Record<string, string> = {};
  response.headers.forEach((value, key) => {
    out[key] = value;
  });
  return out;
}

async function bufferResponseBody(
  response: Response,
): Promise<{ captured: ResponseBody; reconstructed: Uint8Array }> {
  const kind = detectResponseKind(response.headers);
  const buf = await response.arrayBuffer();
  const bytes = new Uint8Array(buf);
  if (kind === "sse") {
    return { captured: { kind: "sse", bytes }, reconstructed: bytes };
  }
  // A malformed JSON response is something the production adapter
  // would surface from its own parser. The recording wrapper should
  // not crash differently than production would; fall back to SSE-
  // shaped raw capture when JSON.parse rejects so the response bytes
  // still make it to disk verbatim.
  const text = new TextDecoder().decode(bytes);
  try {
    const parsed: unknown = JSON.parse(text);
    return {
      captured: { kind: "json", bytes, parsed },
      reconstructed: bytes,
    };
  } catch {
    return {
      captured: { kind: "sse", bytes },
      reconstructed: bytes,
    };
  }
}

function dispatchFilename(index: number, toolName: string): string {
  return `${String(index)}-${toolName}.json`;
}

async function writeDispatch(
  outputDir: string,
  index: number,
  toolName: string,
  args: unknown,
  result: unknown,
): Promise<void> {
  const dispatchesDir = path.join(outputDir, "dispatches");
  await fs.mkdir(dispatchesDir, { recursive: true });
  await fs.writeFile(
    path.join(dispatchesDir, dispatchFilename(index, toolName)),
    `${JSON.stringify({ args, result }, null, 2)}\n`,
  );
}

export function createRecordingHarness(
  opts: CreateRecordingHarnessOpts,
): RecordingHarness {
  const {
    outputDir,
    source,
    maxExchanges,
    redactRequestHeaders,
    redactResponseHeaders,
    fetch: fetchOverride,
    bypassCIGuardForTests,
    now,
  } = opts;

  if ((fetchOverride !== undefined) !== (bypassCIGuardForTests === true)) {
    throw new Error(
      "createRecordingHarness: `fetch` and `bypassCIGuardForTests: true` " +
        "must be supplied together. They form the unit test seam; " +
        "supplying one without the other risks either an accidentally " +
        "live recording in CI or a production script that silently " +
        "skipped the CI guard.",
    );
  }

  if (bypassCIGuardForTests !== true) {
    assertNotCI();
  }

  if (!Number.isInteger(maxExchanges) || maxExchanges <= 0) {
    throw new Error(
      `createRecordingHarness: maxExchanges must be a positive integer (got ${String(maxExchanges)})`,
    );
  }

  const underlyingFetch: RecordingFetchLike =
    fetchOverride ?? globalThis.fetch.bind(globalThis);

  let exchangeCount = 0;
  // When the budget trips inside `recordingFetch`, production
  // `runInference` catches the throw and converts it to an
  // `inference.error` event — the iterator completes normally and the
  // caller never sees the real cause. Stash the error here so
  // `harnessRunInference` can re-throw it after iteration drains, and
  // so `finalize` can refuse to write a manifest over a truncated
  // capture.
  let budgetError: SessionRecordingBudgetExceededError | null = null;

  const recordingFetch: Dependencies["fetch"] = async (input, init) => {
    if (exchangeCount >= maxExchanges) {
      const err = new SessionRecordingBudgetExceededError(
        maxExchanges,
        exchangeCount,
      );
      budgetError = err;
      throw err;
    }
    const exchangeIndex = exchangeCount++;

    const extracted = await extractRequest(input, init);
    const realInit: RequestInit = {
      method: extracted.method,
      headers: extracted.headers,
    };
    // GET/HEAD methods reject any body (including the empty string).
    // Leave `init.body` unset entirely when no body is present so
    // hypothetical future adapters that issue GETs work.
    if (extracted.bodyForSend !== null) {
      realInit.body = extracted.bodyForSend;
    }
    // Forward an abort signal from either `init.signal` or — when
    // the caller passed a `Request` as the first argument — from the
    // Request's own signal. The body/headers/method extraction path
    // already handles the Request input form symmetrically; doing
    // the same for `signal` keeps the wrapper transparent.
    const callerSignal = init?.signal ?? null;
    const requestSignal =
      input instanceof Request && callerSignal === null ? input.signal : null;
    const effectiveSignal = callerSignal ?? requestSignal;
    if (effectiveSignal !== null) {
      realInit.signal = effectiveSignal;
    }

    const response = await underlyingFetch(extracted.url, realInit);
    const responseHeaders = responseHeadersToRecord(response);
    const { captured, reconstructed } = await bufferResponseBody(response);

    const exchangeDir = path.join(
      outputDir,
      "exchanges",
      String(exchangeIndex),
    );
    await writeCapture(exchangeDir, {
      request: extracted.bodyForCapture,
      requestHeaders: extracted.headers,
      redactRequestHeaders,
      response: captured,
      responseHeaders,
      redactResponseHeaders,
    });

    return new Response(reconstructed, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  };

  // Production scheduler — recording drives real fetch on the real
  // wall clock, so production `runInference` timers should fire
  // normally. (The default scheduler created by `runInference` when
  // `deps.scheduler` is omitted would work too, but providing it
  // explicitly here keeps the dependency surface visible.)
  const harnessSymbol = Symbol("RecordingHarnessInstance");
  const deps: Dependencies = {
    fetch: recordingFetch,
    scheduler: {
      setTimeout(callback, delayMs) {
        const handle = setTimeout(callback, delayMs);
        return () => {
          clearTimeout(handle);
        };
      },
    },
    [HarnessId]: harnessSymbol,
  };

  // Tool dispatch capture. Handlers run for real; their args and return
  // values are written to disk. Sync and promise-returning handlers are
  // supported; the `{ result, virtualDelayMs }` delayed-envelope shape
  // accepted by `setupHarness` is rejected here, because virtual delays
  // are a test-harness construct that has no meaning during a real
  // recording.
  const handlers = new Map<string, ToolHandler>();
  let dispatchCount = 0;
  // Each `captureDispatch` returns a promise we cannot block the
  // iterator on (the production runInference would deadlock waiting
  // for the iterator to advance). We park each promise in
  // `inFlightDispatches` for `finalize` to await. The promises also
  // attach a `.catch` that stashes the first failure into
  // `firstDispatchError` so the iterator's yield path and
  // `finalize` can surface the rejection to the caller — without
  // the catch, Bun/Node would raise the rejection as unhandled
  // before either await point could observe it.
  const inFlightDispatches: Promise<void>[] = [];
  let firstDispatchError: unknown = null;

  const onTool = (name: string, handler: ToolHandler): void => {
    if (handlers.has(name)) {
      throw new Error(
        `createRecordingHarness.onTool: a handler is already registered for tool "${name}"`,
      );
    }
    handlers.set(name, handler);
  };

  const captureDispatch = async (
    index: number,
    name: string,
    args: unknown,
    handler: ToolHandler,
  ): Promise<void> => {
    const ret: unknown = handler(args);
    // `Promise.resolve` unwraps both real promises and PromiseLike
    // values, and is a no-op for plain values — covers all three
    // handler return shapes (sync, async, native promise) in one path.
    const resolved: unknown = await Promise.resolve(ret);
    if (isDelayedEnvelope(resolved)) {
      throw new Error(
        `Session recording: handler for tool "${name}" returned a ` +
          `{ result, virtualDelayMs } envelope. Virtual delays are a test-harness ` +
          `construct and are not supported during recording. Return the result ` +
          `directly (or a promise that resolves to the result).`,
      );
    }
    if (resolved === undefined) {
      throw new Error(
        `Session recording: handler for tool "${name}" resolved to undefined; ` +
          `return a concrete result so the dispatch can be captured.`,
      );
    }
    await writeDispatch(outputDir, index, name, args, resolved);
  };

  const harnessRunInference = (
    opts: Omit<InferenceHarnessOptions, "deps">,
  ): AsyncIterable<InferenceEvent> => {
    async function* iterate(): AsyncGenerator<InferenceEvent> {
      const inner = runInference({ ...opts, deps });
      for await (const event of inner) {
        if (event.type === "inference.tool_call.end") {
          const { name, arguments: args } = event.data;
          const handler = handlers.get(name);
          if (handler === undefined) {
            throw new Error(
              `Session recording: inference.tool_call.end observed for tool ` +
                `"${name}" but no handler was registered via onTool. Register ` +
                `a handler so the dispatch can be captured.`,
            );
          }
          const index = dispatchCount++;
          // Attach `.catch` immediately so the rejection lands in
          // `firstDispatchError` rather than escaping as an
          // unhandled rejection. The original promise still goes
          // into `inFlightDispatches` so `finalize` awaits its
          // settlement; the catch produces a settled-void promise
          // that follows the same lifecycle.
          const tracked = captureDispatch(index, name, args, handler).catch(
            (err: unknown) => {
              if (firstDispatchError === null) firstDispatchError = err;
            },
          );
          inFlightDispatches.push(tracked);
        }
        if (firstDispatchError !== null) {
          throw firstDispatchError;
        }
        yield event;
      }
      if (firstDispatchError !== null) {
        throw firstDispatchError;
      }
      if (budgetError !== null) {
        throw budgetError;
      }
    }
    return iterate();
  };

  const finalize = async (): Promise<void> => {
    // Write the session manifest FIRST so that even if a downstream
    // step throws — a dispatch write failing, the budget guard
    // tripping — the directory on disk has a loadable session.json.
    // The replay loader treats a missing session.json as a hard
    // error, and we'd rather hand an interrupted recording back to
    // the user as a partially-loadable artifact than a black hole.
    const capturedAt = (now ?? (() => new Date()))().toISOString();
    await writeSessionManifest(outputDir, {
      sessionSchemaVersion: "1",
      source,
      capturedAt,
    });
    if (inFlightDispatches.length > 0) {
      // The tracked promises have a catch attached that stashes any
      // rejection into `firstDispatchError`; `Promise.all` here
      // therefore never rejects, it just resolves once every
      // captureDispatch has settled.
      await Promise.all(inFlightDispatches);
    }
    if (firstDispatchError !== null) {
      throw firstDispatchError;
    }
    if (budgetError !== null) {
      throw budgetError;
    }
  };

  return { deps, onTool, runInference: harnessRunInference, finalize };
}
