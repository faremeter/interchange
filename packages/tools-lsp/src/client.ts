import { pathToFileURL, fileURLToPath } from "node:url";
import { readFile } from "node:fs/promises";
import type { ChildProcessWithoutNullStreams } from "node:child_process";
import {
  createMessageConnection,
  StreamMessageReader,
  StreamMessageWriter,
  type MessageConnection,
} from "vscode-jsonrpc/node";
import type { Diagnostic } from "vscode-languageserver-types";
import { languageId } from "./language";

const INITIALIZE_TIMEOUT_MS = 45_000;
const DIAGNOSTICS_DOCUMENT_WAIT_MS = 5_000;
const DIAGNOSTICS_FULL_WAIT_MS = 10_000;
const SHUTDOWN_TIMEOUT_MS = 5_000;
// Bound for a notification write. These resolve when the bytes reach the
// child's stdin, not when the server acts on them, so a server that stays
// alive but stops draining parks the write once the pipe buffer fills -- and
// the JSON-RPC writer serializes behind one semaphore, so every later
// notification queues behind the parked one. Unbounded, that hangs the tool
// call awaiting it; bounded, the caller gets an error it can report.
//
// Two seconds is three orders of magnitude above the sub-millisecond flush a
// draining server gives, and it is deliberately far tighter than the request
// budgets above: a wedged server should surface quickly, and `openFile` can
// issue two of these per call.
const NOTIFY_TIMEOUT_MS = 2_000;
// A server that honors `exit` leaves on its own; this is how long it gets to
// do so before the hard kill. Without it the protocol's graceful path is
// never taken, because the kill follows the notification immediately.
//
// Kept generous deliberately. A server that honors `exit` resolves on its
// exit event in a couple of milliseconds and never reaches this bound, so the
// cost falls only on one that ignores `exit` -- and the manager disposes
// clients serially, so that cost is per client. Trading a slower teardown for
// a misbehaving server against a wide margin for a well-behaved one is the
// right way round: the margin is what keeps the shutdown assertion from
// becoming the kind of load-sensitive test this package is being cleaned of.
const EXIT_GRACE_MS = 1_000;

/**
 * Raised by `withTimeout` when its bound elapses. Distinct from whatever the
 * bounded operation itself rejects with, because the two mean different
 * things to a caller: a rejection came back from the operation, while this
 * one means the operation was abandoned and may still be running.
 */
class LSPTimeoutError extends Error {
  constructor(label: string, ms: number) {
    super(`${label}: timed out after ${ms}ms`);
    this.name = "LSPTimeoutError";
  }
}

/**
 * Raised when a path's document state on the server is no longer knowable,
 * so the client refuses to send for it again; see `poisonedFiles`.
 */
export class LSPDocumentOutOfSyncError extends Error {
  constructor(
    public readonly path: string,
    public readonly serverID: string,
    options?: { cause?: unknown },
  ) {
    super(
      `document "${path}" is out of sync with lsp server "${serverID}": an earlier notification write for it was abandoned`,
      options,
    );
    this.name = "LSPDocumentOutOfSyncError";
  }
}

export class LSPInitializeError extends Error {
  constructor(
    public readonly serverID: string,
    options?: { cause?: unknown },
  ) {
    super(`failed to initialize lsp server "${serverID}"`, options);
    this.name = "LSPInitializeError";
  }
}

export interface LSPClient {
  readonly root: string;
  readonly serverID: string;
  readonly connection: MessageConnection;
  readonly diagnostics: Map<string, Diagnostic[]>;
  notify: {
    open(input: { path: string }): Promise<number>;
  };
  waitForDiagnostics(input: {
    path: string;
    version: number;
    mode?: "document" | "full";
    after?: number;
  }): Promise<void>;
  shutdown(): Promise<void>;
}

export interface CreateClientInput {
  serverID: string;
  server: {
    process: ChildProcessWithoutNullStreams;
    initialization?: Record<string, unknown>;
  };
  root: string;
  seedsInitialDiagnostics?: boolean;
}

interface FileState {
  version: number;
  text: string;
}

function withTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new LSPTimeoutError(label, ms)), ms);
  });
  return Promise.race([p, timeout]).finally(() => {
    if (timer !== undefined) clearTimeout(timer);
  });
}

async function stopProcess(
  proc: ChildProcessWithoutNullStreams,
): Promise<void> {
  if (proc.exitCode !== null || proc.signalCode !== null) return;
  proc.kill();
  await new Promise<void>((resolve) => proc.once("exit", () => resolve()));
}

/**
 * Resolves once the child has exited, or after `ms` if it has not. Used to
 * let a server act on `exit` before the hard kill; the caller still calls
 * `stopProcess`, which no-ops on an already-exited child.
 */
async function exitedWithin(
  proc: ChildProcessWithoutNullStreams,
  ms: number,
): Promise<void> {
  if (proc.exitCode !== null || proc.signalCode !== null) return;
  await new Promise<void>((resolve) => {
    const onExit = () => {
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      proc.removeListener("exit", onExit);
      resolve();
    }, ms);
    proc.once("exit", onExit);
  });
}

export async function createLSPClient(
  input: CreateClientInput,
): Promise<LSPClient> {
  const { serverID, server, root } = input;
  const proc = server.process;

  const connection = createMessageConnection(
    new StreamMessageReader(proc.stdout),
    new StreamMessageWriter(proc.stdin),
  );

  const files = new Map<string, FileState>();
  // Paths whose document state on the server is no longer knowable, mapped
  // to the abandoned write that made it so. A notification the bound gave up
  // on is still queued in the JSON-RPC writer, so the server may receive it
  // whenever it resumes draining; the client cannot tell which versions it
  // holds, and must stop sending for that path rather than guess one.
  const poisonedFiles = new Map<string, unknown>();
  // `openFile` reads a path's recorded version, awaits its notifications, and
  // only then records the new one, so two overlapping calls for one path would
  // both read the same version and send it twice. LSP requires document
  // versions to strictly increase, and the middleware's fire-and-forget touch
  // overlaps its awaited one on the same path, so this is reachable rather
  // than theoretical. One chain per path serializes them.
  const openChains = new Map<string, Promise<void>>();
  const pushDiagnostics = new Map<string, Diagnostic[]>();
  const pullDiagnostics = new Map<string, Diagnostic[]>();
  const diagnostics = new Map<string, Diagnostic[]>();

  let hasPullCapability = false;
  let needsPullRefresh = false;
  let seeded = false;

  type DiagnosticListener = (uri: string, version: number) => void;
  const diagnosticListeners = new Set<DiagnosticListener>();

  function recomputeDiagnostics(uri: string): void {
    const push = pushDiagnostics.get(uri) ?? [];
    const pull = pullDiagnostics.get(uri) ?? [];
    const merged = dedupeDiagnostics([...push, ...pull]);
    if (merged.length > 0) {
      diagnostics.set(uri, merged);
    } else {
      diagnostics.delete(uri);
    }
  }

  // --- Notification handlers ---

  connection.onNotification(
    "textDocument/publishDiagnostics",
    (params: { uri: string; diagnostics: Diagnostic[]; version?: number }) => {
      const { uri } = params;

      // TypeScript language server aggressively publishes diagnostics on
      // initial load. Seed the map on the first publish so that a
      // subsequent waitForDiagnostics does not double-wait.
      if (input.seedsInitialDiagnostics && !seeded) {
        seeded = true;
        pushDiagnostics.set(uri, params.diagnostics);
        recomputeDiagnostics(uri);
        return;
      }

      pushDiagnostics.set(uri, params.diagnostics);
      recomputeDiagnostics(uri);

      const fileVersion =
        params.version ?? files.get(fileURLToPath(uri))?.version ?? 0;
      for (const listener of diagnosticListeners) {
        listener(uri, fileVersion);
      }
    },
  );

  connection.onRequest(
    "window/workDoneProgress/create",
    (_params: { token: string | number }) => {
      // Acknowledge progress token creation without tracking it.
    },
  );

  connection.onRequest(
    "workspace/configuration",
    (_params: { items: { section?: string }[] }) => {
      return _params.items.map(() => ({}));
    },
  );

  connection.onRequest(
    "client/registerCapability",
    (params: { registrations: { method: string }[] }) => {
      for (const reg of params.registrations) {
        if (reg.method === "textDocument/diagnostic") {
          hasPullCapability = true;
        }
      }
    },
  );

  connection.onRequest(
    "client/unregisterCapability",
    (params: { unregisterations: { method: string }[] }) => {
      for (const unreg of params.unregisterations) {
        if (unreg.method === "textDocument/diagnostic") {
          hasPullCapability = false;
        }
      }
    },
  );

  connection.onRequest("workspace/workspaceFolders", () => {
    return [{ uri: pathToFileURL(root).href, name: root }];
  });

  connection.onNotification("workspace/diagnostic/refresh", () => {
    needsPullRefresh = true;
  });

  connection.listen();

  // --- Initialize handshake ---

  let initializeResult: Record<string, unknown>;
  try {
    initializeResult = await withTimeout(
      connection.sendRequest("initialize", {
        processId: process.pid,
        rootUri: pathToFileURL(root).href,
        workspaceFolders: [{ uri: pathToFileURL(root).href, name: root }],
        capabilities: {
          textDocument: {
            synchronization: {
              dynamicRegistration: false,
              willSave: false,
              willSaveWaitUntil: false,
              didSave: true,
            },
            publishDiagnostics: {
              relatedInformation: true,
              versionSupport: true,
            },
            diagnostic: {
              dynamicRegistration: true,
            },
          },
          workspace: {
            configuration: true,
            workspaceFolders: true,
            didChangeWatchedFiles: {
              dynamicRegistration: false,
            },
            diagnostics: {
              refreshSupport: true,
            },
          },
        },
      }),
      INITIALIZE_TIMEOUT_MS,
      `initialize ${serverID}`,
    );
    // Part of the handshake, so a failed write is an initialize failure and
    // takes the same cleanup below. Awaiting also means the client cannot be
    // handed out before `initialized` has reached the pipe.
    await withTimeout(
      connection.sendNotification("initialized", {}),
      NOTIFY_TIMEOUT_MS,
      `initialized ${serverID}`,
    );
    if (server.initialization !== undefined) {
      await withTimeout(
        connection.sendNotification("workspace/didChangeConfiguration", {
          settings: server.initialization,
        }),
        NOTIFY_TIMEOUT_MS,
        `didChangeConfiguration ${serverID}`,
      );
    }
  } catch (err) {
    connection.dispose();
    await stopProcess(proc);
    throw new LSPInitializeError(serverID, { cause: err });
  }

  // Check if server supports pull diagnostics natively
  const rawCaps = initializeResult["capabilities"];
  if (typeof rawCaps === "object" && rawCaps !== null) {
    if ("diagnosticProvider" in rawCaps) {
      hasPullCapability = true;
    }
  }

  // --- Document sync ---

  async function openFile(input: { path: string }): Promise<number> {
    const filePath = input.path;
    const previous = openChains.get(filePath);
    let release: () => void = () => undefined;
    const mine = new Promise<void>((resolve) => {
      release = resolve;
    });
    openChains.set(filePath, mine);
    if (previous !== undefined) await previous;
    try {
      return await sendOpenOrChange(filePath);
    } finally {
      release();
      if (openChains.get(filePath) === mine) openChains.delete(filePath);
    }
  }

  /**
   * Sends one version-carrying document notification for `filePath`.
   *
   * A write ends one of three ways and each leaves `files` differently. A
   * completed write put the version on the wire, so the caller records it. A
   * failed write never reached the wire, so the caller leaves the map alone
   * and a later call may reuse the version. A write `NOTIFY_TIMEOUT_MS`
   * abandons is neither, and is not cancellable: the JSON-RPC writer still
   * holds it queued behind the stalled pipe, so the server may receive that
   * version whenever it resumes draining. Reusing the version after that
   * would send it twice -- LSP requires them to strictly increase, and
   * `waitForDiagnostics` would be satisfied by the first send's publish and
   * report diagnostics for stale text as current. So an abandoned write
   * poisons the path instead, and every later call for it raises
   * `LSPDocumentOutOfSyncError` rather than guessing a version.
   */
  async function sendDocumentNotification(
    filePath: string,
    method: string,
    params: Record<string, unknown>,
    label: string,
  ): Promise<void> {
    try {
      await withTimeout(
        connection.sendNotification(method, params),
        NOTIFY_TIMEOUT_MS,
        label,
      );
    } catch (err) {
      if (err instanceof LSPTimeoutError) poisonedFiles.set(filePath, err);
      throw err;
    }
  }

  async function sendOpenOrChange(filePath: string): Promise<number> {
    if (poisonedFiles.has(filePath)) {
      throw new LSPDocumentOutOfSyncError(filePath, serverID, {
        cause: poisonedFiles.get(filePath),
      });
    }

    const uri = pathToFileURL(filePath).href;
    const text = await readFile(filePath, "utf8");
    const ext = filePath.slice(filePath.lastIndexOf("."));
    const existing = files.get(filePath);

    // `files` models what the server holds, so a version is recorded only
    // once its notification is on the wire. Recording first would leave the
    // map a version ahead after a failed write, and `waitForDiagnostics`
    // would then wait for a version the server will never publish and report
    // no diagnostics for a file that has them. A completed write is the
    // strongest available signal: a server that takes the bytes and never
    // acts on them still diverges, and nothing here can detect that. The
    // third ending, an abandoned write, is handled in
    // `sendDocumentNotification`.
    //
    // Reading the version here and recording it after the awaits is only safe
    // because `openFile` serializes per path; see `openChains`.
    if (existing === undefined) {
      const version = 1;
      await sendDocumentNotification(
        filePath,
        "textDocument/didOpen",
        {
          textDocument: {
            uri,
            languageId: languageId(ext),
            version,
            text,
          },
        },
        `didOpen ${filePath}`,
      );
      files.set(filePath, { version, text });
      return version;
    }

    const version = existing.version + 1;

    // Carries no version, and the didChange below is never issued when this
    // one is abandoned, so an abandoned write here leaves `files` describing
    // the server accurately and a later call may reuse `version`. Only the
    // version-carrying writes poison the path.
    await withTimeout(
      connection.sendNotification("workspace/didChangeWatchedFiles", {
        changes: [{ uri, type: 2 }],
      }),
      NOTIFY_TIMEOUT_MS,
      `didChangeWatchedFiles ${filePath}`,
    );

    await sendDocumentNotification(
      filePath,
      "textDocument/didChange",
      {
        textDocument: { uri, version },
        contentChanges: [{ text }],
      },
      `didChange ${filePath}`,
    );

    files.set(filePath, { version, text });

    return version;
  }

  // --- Diagnostics waiting ---

  async function waitForDiagnostics(waitInput: {
    path: string;
    version: number;
    mode?: "document" | "full";
    after?: number;
  }): Promise<void> {
    const uri = pathToFileURL(waitInput.path).href;
    const mode = waitInput.mode ?? "document";
    const timeoutMs =
      mode === "full" ? DIAGNOSTICS_FULL_WAIT_MS : DIAGNOSTICS_DOCUMENT_WAIT_MS;

    if (!pushDiagnostics.has(uri)) {
      await waitForPushNotification(
        uri,
        waitInput.version,
        timeoutMs,
        waitInput.path,
      );
    }

    if (mode === "full" && (hasPullCapability || needsPullRefresh)) {
      needsPullRefresh = false;
      await doPullDiagnostics(uri, timeoutMs, waitInput.path);
    }
  }

  async function waitForPushNotification(
    uri: string,
    version: number,
    timeoutMs: number,
    label: string,
  ): Promise<void> {
    const listener: DiagnosticListener = (notifiedURI, notifiedVersion) => {
      if (notifiedURI === uri && notifiedVersion >= version) {
        diagnosticListeners.delete(listener);
        resolve();
      }
    };

    let resolve: () => void;
    const promise = new Promise<void>((r) => {
      resolve = r;
    });
    diagnosticListeners.add(listener);

    try {
      await withTimeout(promise, timeoutMs, `diagnostics for ${label}`);
    } catch {
      // Timeout is acceptable -- the server may not have diagnostics to send.
    } finally {
      diagnosticListeners.delete(listener);
    }
  }

  async function doPullDiagnostics(
    uri: string,
    timeoutMs: number,
    label: string,
  ): Promise<void> {
    try {
      const pullResult = await withTimeout(
        connection.sendRequest("textDocument/diagnostic", {
          textDocument: { uri },
        }),
        timeoutMs,
        `pull diagnostics for ${label}`,
      );
      const items = extractItems(pullResult);
      pullDiagnostics.set(uri, items);
      recomputeDiagnostics(uri);
    } catch {
      // Pull diagnostic failure is non-fatal.
    }

    try {
      const wsResult = await withTimeout(
        connection.sendRequest("workspace/diagnostic", {}),
        timeoutMs,
        `workspace diagnostics`,
      );
      if (
        wsResult !== null &&
        typeof wsResult === "object" &&
        "items" in wsResult
      ) {
        const wsRecord = wsResult as Record<string, unknown>;
        const wsItems: unknown[] = Array.isArray(wsRecord["items"])
          ? wsRecord["items"]
          : [];
        for (const entry of wsItems) {
          if (
            typeof entry === "object" &&
            entry !== null &&
            "uri" in entry &&
            "items" in entry
          ) {
            const e: Record<string, unknown> = entry;
            const entryURI = String(e["uri"]);
            const entryItems = extractItems(e);
            pullDiagnostics.set(entryURI, entryItems);
            recomputeDiagnostics(entryURI);
          }
        }
      }
    } catch {
      // Workspace diagnostic failure is non-fatal.
    }
  }

  // --- Shutdown ---

  async function shutdown(): Promise<void> {
    try {
      await withTimeout(
        connection.sendRequest("shutdown"),
        SHUTDOWN_TIMEOUT_MS,
        `shutdown ${serverID}`,
      );
      await withTimeout(
        connection.sendNotification("exit"),
        SHUTDOWN_TIMEOUT_MS,
        `exit ${serverID}`,
      );
      // Delivering `exit` is not the same as the server acting on it, so give
      // it a window to leave on its own; see EXIT_GRACE_MS.
      await exitedWithin(proc, EXIT_GRACE_MS);
    } catch {
      // Best-effort shutdown.
    }
    connection.dispose();
    await stopProcess(proc);
  }

  return {
    root,
    serverID,
    connection,
    diagnostics,
    notify: { open: openFile },
    waitForDiagnostics,
    shutdown,
  };
}

function extractItems(result: unknown): Diagnostic[] {
  if (result !== null && typeof result === "object" && "items" in result) {
    const r = result as Record<string, unknown>;
    if (Array.isArray(r["items"])) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- LSP protocol response validated structurally above
      return r["items"] as Diagnostic[];
    }
  }
  return [];
}

function dedupeDiagnostics(items: Diagnostic[]): Diagnostic[] {
  const seen = new Set<string>();
  const result: Diagnostic[] = [];
  for (const d of items) {
    const key = `${d.range.start.line}:${d.range.start.character}:${d.range.end.line}:${d.range.end.character}:${d.message}`;
    if (!seen.has(key)) {
      seen.add(key);
      result.push(d);
    }
  }
  return result;
}
