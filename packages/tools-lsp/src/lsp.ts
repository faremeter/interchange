import path from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import type { Diagnostic, Hover } from "vscode-languageserver-types";
import { getLogger } from "@interchange/log";
import { createLSPClient, type LSPClient } from "./client";
import * as ServerRegistry from "./server";
import type { ServerInfo, ServerContext } from "./server";
import { containsPath, type LSPContext } from "./context";

const logger = getLogger(["interchange", "tools-lsp"]);

const INTERESTING_SYMBOL_KINDS = new Set<number>([
  5, // Class
  6, // Method
  8, // Constructor
  9, // Enum
  11, // Interface
  12, // Function
  14, // Constant
  2, // Module
  23, // Struct
]);

const MAX_WORKSPACE_SYMBOLS = 50;

export type LocInput = { file: string; line: number; character: number };

export interface LSPManagerOptions {
  cwd: string;
  worktree?: string;
  servers?: ServerInfo[];
}

export interface LSPStatus {
  id: string;
  root: string;
  status: "connected";
}

export interface LSPManager {
  hasClients(file: string): Promise<boolean>;
  touchFile(file: string, mode?: "document" | "full"): Promise<void>;
  diagnostics(): Promise<Record<string, Diagnostic[]>>;

  hover(input: LocInput): Promise<Hover | null>;
  definition(input: LocInput): Promise<unknown[]>;
  references(input: LocInput): Promise<unknown[]>;
  implementation(input: LocInput): Promise<unknown[]>;
  documentSymbol(uri: string): Promise<unknown[]>;
  workspaceSymbol(query: string): Promise<unknown[]>;
  prepareCallHierarchy(input: LocInput): Promise<unknown[]>;
  incomingCalls(input: LocInput): Promise<unknown[]>;
  outgoingCalls(input: LocInput): Promise<unknown[]>;

  status(): LSPStatus[];
  dispose(): Promise<void>;
}

interface State {
  clients: LSPClient[];
  servers: Record<string, ServerInfo>;
  broken: Set<string>;
  spawning: Map<string, Promise<LSPClient | undefined>>;
}

export function createLSPManager(opts: LSPManagerOptions): LSPManager {
  const ctx: LSPContext = {
    cwd: opts.cwd,
    worktree: opts.worktree ?? opts.cwd,
  };
  const serverCtx: ServerContext = {
    directory: ctx.cwd,
    worktree: ctx.worktree,
  };

  const servers: Record<string, ServerInfo> = {};
  for (const s of opts.servers ?? Object.values(ServerRegistry)) {
    servers[s.id] = s;
  }
  const state: State = {
    clients: [],
    servers,
    broken: new Set(),
    spawning: new Map(),
  };

  async function getClients(file: string): Promise<LSPClient[]> {
    if (!containsPath(file, ctx)) return [];
    const ext = path.parse(file).ext || file;
    const result: LSPClient[] = [];

    for (const server of Object.values(state.servers)) {
      if (server.extensions.length && !server.extensions.includes(ext))
        continue;
      const root = await server.root(file, serverCtx);
      if (root === undefined) continue;
      const key = `${root}:${server.id}`;
      if (state.broken.has(key)) continue;

      const existing = state.clients.find(
        (c) => c.root === root && c.serverID === server.id,
      );
      if (existing !== undefined) {
        result.push(existing);
        continue;
      }

      const inflight = state.spawning.get(key);
      if (inflight !== undefined) {
        const client = await inflight;
        if (client !== undefined) result.push(client);
        continue;
      }

      const task = spawnClient(server, root, key);
      state.spawning.set(key, task);
      try {
        const client = await task;
        if (client !== undefined) result.push(client);
      } finally {
        if (state.spawning.get(key) === task) state.spawning.delete(key);
      }
    }
    return result;
  }

  async function spawnClient(
    server: ServerInfo,
    root: string,
    key: string,
  ): Promise<LSPClient | undefined> {
    let handle;
    try {
      handle = await server.spawn(root, serverCtx);
    } catch (err) {
      state.broken.add(key);
      logger.error`failed to spawn lsp server ${server.id}: ${err}`;
      return undefined;
    }
    if (handle === undefined) {
      state.broken.add(key);
      return undefined;
    }
    logger.info`spawned lsp server ${server.id} at ${root}`;

    let client: LSPClient | undefined;
    try {
      client = await createLSPClient({
        serverID: server.id,
        server: handle,
        root,
        ...(server.seedsInitialDiagnostics !== undefined
          ? { seedsInitialDiagnostics: server.seedsInitialDiagnostics }
          : {}),
      });
    } catch (err) {
      state.broken.add(key);
      handle.process.kill();
      logger.error`failed to initialize lsp client ${server.id}: ${err}`;
      return undefined;
    }

    const race = state.clients.find(
      (c) => c.root === root && c.serverID === server.id,
    );
    if (race !== undefined) {
      handle.process.kill();
      return race;
    }
    state.clients.push(client);
    return client;
  }

  async function run<T>(
    file: string,
    fn: (c: LSPClient) => Promise<T>,
  ): Promise<T[]> {
    const clients = await getClients(file);
    const settled = await Promise.allSettled(clients.map(fn));
    const results: T[] = [];
    for (const s of settled) {
      if (s.status === "fulfilled") {
        results.push(s.value);
      } else {
        logger.error`lsp operation failed: ${s.reason}`;
      }
    }
    return results;
  }

  async function runAll<T>(fn: (c: LSPClient) => Promise<T>): Promise<T[]> {
    const settled = await Promise.allSettled(state.clients.map(fn));
    const results: T[] = [];
    for (const s of settled) {
      if (s.status === "fulfilled") {
        results.push(s.value);
      } else {
        logger.error`lsp operation failed: ${s.reason}`;
      }
    }
    return results;
  }

  async function hasClients(file: string): Promise<boolean> {
    const clients = await getClients(file);
    return clients.length > 0;
  }

  async function touchFile(
    file: string,
    mode?: "document" | "full",
  ): Promise<void> {
    const clients = await getClients(file);
    for (const client of clients) {
      const version = await client.notify.open({ path: file });
      await client.waitForDiagnostics({
        path: file,
        version,
        mode: mode ?? "document",
      });
    }
  }

  async function diagnostics(): Promise<Record<string, Diagnostic[]>> {
    const result: Record<string, Diagnostic[]> = {};
    for (const client of state.clients) {
      for (const [uri, diags] of client.diagnostics) {
        try {
          const filePath = fileURLToPath(uri);
          const existing = result[filePath];
          if (existing !== undefined) {
            existing.push(...diags);
          } else {
            result[filePath] = [...diags];
          }
        } catch {
          // Non-file URI -- skip
        }
      }
    }
    return result;
  }

  async function hover(input: LocInput): Promise<Hover | null> {
    const results = await run(input.file, (c) =>
      c.connection
        .sendRequest<Hover | null>("textDocument/hover", {
          textDocument: { uri: pathToFileURL(input.file).href },
          position: { line: input.line, character: input.character },
        })
        .catch(() => null),
    );
    return results.find((r) => r !== null) ?? null;
  }

  async function definition(input: LocInput): Promise<unknown[]> {
    const results = await run(input.file, (c) =>
      c.connection.sendRequest("textDocument/definition", {
        textDocument: { uri: pathToFileURL(input.file).href },
        position: { line: input.line, character: input.character },
      }),
    );
    return flattenResults(results);
  }

  async function references(input: LocInput): Promise<unknown[]> {
    const results = await run(input.file, (c) =>
      c.connection.sendRequest("textDocument/references", {
        textDocument: { uri: pathToFileURL(input.file).href },
        position: { line: input.line, character: input.character },
        context: { includeDeclaration: true },
      }),
    );
    return flattenResults(results);
  }

  async function implementation(input: LocInput): Promise<unknown[]> {
    const results = await run(input.file, (c) =>
      c.connection.sendRequest("textDocument/implementation", {
        textDocument: { uri: pathToFileURL(input.file).href },
        position: { line: input.line, character: input.character },
      }),
    );
    return flattenResults(results);
  }

  async function documentSymbol(uri: string): Promise<unknown[]> {
    const filePath = fileURLToPath(uri);
    const results = await run(filePath, (c) =>
      c.connection.sendRequest("textDocument/documentSymbol", {
        textDocument: { uri },
      }),
    );
    return flattenResults(results);
  }

  async function workspaceSymbol(query: string): Promise<unknown[]> {
    const results = await runAll((c) =>
      c.connection.sendRequest("workspace/symbol", { query }),
    );
    const flat = flattenResults(results);
    return flat
      .filter((s) => {
        if (typeof s !== "object" || s === null || !("kind" in s)) return false;
        const rec: Record<string, unknown> = s;
        const kind = rec["kind"];
        return typeof kind === "number" && INTERESTING_SYMBOL_KINDS.has(kind);
      })
      .slice(0, MAX_WORKSPACE_SYMBOLS);
  }

  async function prepareCallHierarchy(input: LocInput): Promise<unknown[]> {
    const results = await run(input.file, (c) =>
      c.connection.sendRequest("textDocument/prepareCallHierarchy", {
        textDocument: { uri: pathToFileURL(input.file).href },
        position: { line: input.line, character: input.character },
      }),
    );
    return flattenResults(results);
  }

  async function incomingCalls(input: LocInput): Promise<unknown[]> {
    const items = await prepareCallHierarchy(input);
    if (items.length === 0) return [];
    const results = await run(input.file, (c) =>
      c.connection.sendRequest("callHierarchy/incomingCalls", {
        item: items[0],
      }),
    );
    return flattenResults(results);
  }

  async function outgoingCalls(input: LocInput): Promise<unknown[]> {
    const items = await prepareCallHierarchy(input);
    if (items.length === 0) return [];
    const results = await run(input.file, (c) =>
      c.connection.sendRequest("callHierarchy/outgoingCalls", {
        item: items[0],
      }),
    );
    return flattenResults(results);
  }

  function statusList(): LSPStatus[] {
    return state.clients.map((c) => ({
      id: c.serverID,
      root: c.root,
      status: "connected" as const,
    }));
  }

  async function dispose(): Promise<void> {
    const errors: unknown[] = [];
    for (const client of state.clients) {
      try {
        await client.shutdown();
      } catch (err) {
        errors.push(err);
        logger.error`failed to shut down lsp client ${client.serverID}: ${err}`;
      }
    }
    state.clients.length = 0;
    state.broken.clear();
    state.spawning.clear();
    if (errors.length > 0) {
      throw new AggregateError(
        errors,
        "one or more lsp clients failed to shut down",
      );
    }
  }

  return {
    hasClients,
    touchFile,
    diagnostics,
    hover,
    definition,
    references,
    implementation,
    documentSymbol,
    workspaceSymbol,
    prepareCallHierarchy,
    incomingCalls,
    outgoingCalls,
    status: statusList,
    dispose,
  };
}

function flattenResults(results: unknown[]): unknown[] {
  const flat: unknown[] = [];
  for (const r of results) {
    if (Array.isArray(r)) {
      for (const item of r) {
        if (item !== null && item !== undefined) flat.push(item);
      }
    } else if (r !== null && r !== undefined) {
      flat.push(r);
    }
  }
  return flat;
}
