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
    timer = setTimeout(
      () => reject(new Error(`${label}: timed out after ${ms}ms`)),
      ms,
    );
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
  } catch (err) {
    connection.dispose();
    await stopProcess(proc);
    throw new LSPInitializeError(serverID, { cause: err });
  }

  connection.sendNotification("initialized", {});

  if (server.initialization !== undefined) {
    connection.sendNotification("workspace/didChangeConfiguration", {
      settings: server.initialization,
    });
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
    const uri = pathToFileURL(filePath).href;
    const text = await readFile(filePath, "utf8");
    const ext = filePath.slice(filePath.lastIndexOf("."));
    const existing = files.get(filePath);

    if (existing === undefined) {
      const version = 1;
      files.set(filePath, { version, text });
      connection.sendNotification("textDocument/didOpen", {
        textDocument: {
          uri,
          languageId: languageId(ext),
          version,
          text,
        },
      });
      return version;
    }

    const version = existing.version + 1;
    files.set(filePath, { version, text });

    connection.sendNotification("workspace/didChangeWatchedFiles", {
      changes: [{ uri, type: 2 }],
    });

    connection.sendNotification("textDocument/didChange", {
      textDocument: { uri, version },
      contentChanges: [{ text }],
    });

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
        5_000,
        `shutdown ${serverID}`,
      );
      connection.sendNotification("exit");
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
