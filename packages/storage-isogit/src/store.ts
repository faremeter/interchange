import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import type {
  ContextStore,
  ContextCommit,
  ConversationMessage,
  PendingOperation,
  TokenUsage,
} from "@interchange/types/runtime";
import { AUTHOR } from "./init";

const CONTEXT_FILE = "context.json";

type ContextData = {
  messages: ConversationMessage[];
  pendingOperations: PendingOperation[];
  tokenUsage: TokenUsage;
};

function parseContextData(raw: unknown): ContextData {
  if (
    typeof raw !== "object" ||
    raw === null ||
    !Array.isArray((raw as Record<string, unknown>)["messages"]) ||
    !Array.isArray((raw as Record<string, unknown>)["pendingOperations"]) ||
    typeof (raw as Record<string, unknown>)["tokenUsage"] !== "object"
  ) {
    throw new Error("context.json has unexpected structure");
  }
  return raw as ContextData;
}

async function readCommitLog(
  dir: string,
  limit: number,
): Promise<ContextCommit[]> {
  const entries = await git.log({ fs, dir, depth: limit });
  return entries.map((e) => {
    const base = {
      hash: e.oid,
      message: e.commit.message.trimEnd(),
      timestamp: e.commit.author.timestamp * 1000,
    };
    const parent = e.commit.parent[0];
    return parent !== undefined ? { ...base, parentHash: parent } : base;
  });
}

/**
 * isomorphic-git-backed implementation of ContextStore.
 *
 * All state is serialized into a single `context.json` file tracked by the
 * git repository at `dir`. The caller is responsible for calling
 * `initAgentRepo(dir)` before constructing this store.
 */
export class IsogitStore implements ContextStore {
  constructor(private readonly dir: string) {}

  async load(_signal?: AbortSignal): Promise<{
    messages: ConversationMessage[];
    pendingOperations: PendingOperation[];
    tokenUsage: TokenUsage;
  }> {
    const contextPath = path.join(this.dir, CONTEXT_FILE);
    const raw = await fs.promises.readFile(contextPath, "utf-8");
    const parsed = JSON.parse(raw) as unknown;
    return parseContextData(parsed);
  }

  async commit(
    messages: ConversationMessage[],
    pendingOperations: PendingOperation[],
    tokenUsage: TokenUsage,
    message: string,
    _signal?: AbortSignal,
  ): Promise<ContextCommit> {
    const data: ContextData = { messages, pendingOperations, tokenUsage };
    const contextPath = path.join(this.dir, CONTEXT_FILE);
    await fs.promises.writeFile(contextPath, JSON.stringify(data, null, 2));

    await git.add({ fs, dir: this.dir, filepath: CONTEXT_FILE });
    const oid = await git.commit({
      fs,
      dir: this.dir,
      message,
      author: AUTHOR,
    });

    const entries = await git.log({ fs, dir: this.dir, depth: 2 });
    const entry = entries[0];
    if (entry === undefined || entry.oid !== oid) {
      throw new Error(
        `Unexpected log state after commit: expected ${oid} as HEAD`,
      );
    }
    const parentOid = entries[1]?.oid;
    const base = {
      hash: oid,
      message: message.trimEnd(),
      timestamp: entry.commit.author.timestamp * 1000,
    };
    return parentOid !== undefined ? { ...base, parentHash: parentOid } : base;
  }

  async branch(name: string, _signal?: AbortSignal): Promise<void> {
    await git.branch({ fs, dir: this.dir, ref: name });
  }

  async log(limit?: number, _signal?: AbortSignal): Promise<ContextCommit[]> {
    return readCommitLog(this.dir, limit ?? 10);
  }

  async readAt(
    hash: string,
    _signal?: AbortSignal,
  ): Promise<ConversationMessage[]> {
    const { blob } = await git.readBlob({
      fs,
      dir: this.dir,
      oid: hash,
      filepath: CONTEXT_FILE,
    });
    const text = new TextDecoder().decode(blob);
    const parsed = JSON.parse(text) as unknown;
    const data = parseContextData(parsed);
    return data.messages;
  }
}
