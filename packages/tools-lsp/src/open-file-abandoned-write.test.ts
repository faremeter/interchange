import { describe, test, expect, afterAll } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import {
  createLSPClient,
  LSPDocumentOutOfSyncError,
  type LSPClient,
} from "./client";

const RESUMING_SERVER = join(import.meta.dir, "resuming-lsp-server.ts");

// Far larger than the pipe and stream buffers can absorb, so the didChange
// carrying it parks mid-write against a server that stopped reading. The
// didChangeWatchedFiles sent just before it is a couple hundred bytes and
// still fits, which makes the parked write deterministically the
// version-carrying one.
const OVERSIZED_TEXT = `const x = 1;\n${"// pad\n".repeat(300_000)}`;

let dir: string;
const clients: LSPClient[] = [];
const procs: ChildProcessWithoutNullStreams[] = [];

afterAll(async () => {
  for (const c of clients) c.connection.dispose();
  for (const p of procs) p.kill("SIGKILL");
  if (dir !== undefined) await rm(dir, { recursive: true, force: true });
});

/**
 * The document versions the server has parsed, as it reports them. The
 * writer serializes behind one semaphore, so this request rides after every
 * notification issued before it and its answer is a barrier: it cannot come
 * back until the server has worked through the backlog.
 */
async function versionsReceived(client: LSPClient): Promise<string[]> {
  const result: unknown = await client.connection.sendRequest(
    "intx/versionsReceived",
  );
  if (
    typeof result !== "object" ||
    result === null ||
    !("received" in result)
  ) {
    throw new Error("fixture answered without a received list");
  }
  const list: unknown = result.received;
  if (!Array.isArray(list)) throw new Error("received is not an array");
  return list.map((entry: unknown) => {
    if (typeof entry !== "string")
      throw new Error("received holds a non-string");
    return entry;
  });
}

describe("notify.open after a notification write is abandoned", () => {
  // `NOTIFY_TIMEOUT_MS` bounds the wait, not the write: the writer cannot
  // cancel it, so an abandoned write stays queued and lands whenever the
  // server resumes draining. The version it carries is then one the server
  // holds, and reusing it would send that version twice -- which LSP forbids
  // and which would settle `waitForDiagnostics` on stale text.
  test("refuses the path rather than re-sending a version the server holds", async () => {
    dir = await mkdtemp(join(tmpdir(), "lsp-abandoned-write-"));
    const filePath = join(dir, "doc.ts");
    await writeFile(filePath, "const x = 1;\n");

    const proc = spawn("bun", ["run", RESUMING_SERVER], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    procs.push(proc);
    const client = await createLSPClient({
      serverID: "resuming",
      server: { process: proc },
      root: dir,
    });
    clients.push(client);

    // The server stopped reading, but this document fits the pipe, so the
    // write completes and the client records version 1.
    expect(await client.notify.open({ path: filePath })).toBe(1);

    // Version 2 does not fit: its write parks, the bound gives up, and the
    // caller sees a rejection.
    await writeFile(filePath, OVERSIZED_TEXT);
    await expect(client.notify.open({ path: filePath })).rejects.toThrow(
      /didChange .*timed out/,
    );

    // Let the server work through its backlog. The abandoned write was never
    // cancelled, so version 2 lands.
    proc.kill("SIGUSR2");
    expect(await versionsReceived(client)).toEqual([
      "didOpen v1",
      "didChange v2",
    ]);

    // The server holds version 2 now, so the client must not compute 2 again.
    await expect(client.notify.open({ path: filePath })).rejects.toBeInstanceOf(
      LSPDocumentOutOfSyncError,
    );

    // Anything that call put on the wire parses before this answer comes
    // back, so the reading is conclusive.
    expect(await versionsReceived(client)).toEqual([
      "didOpen v1",
      "didChange v2",
    ]);
  }, 60_000);
});
