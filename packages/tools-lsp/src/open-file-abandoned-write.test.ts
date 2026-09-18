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

// Far larger than the pipe and the stream buffers behind it can absorb, so
// the didChange carrying it parks mid-write against a server that has stopped
// reading. The didChangeWatchedFiles sent immediately before it is a couple
// of hundred bytes and still fits, which is what makes the parked write
// deterministically the version-carrying one.
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
 * The document versions the server has parsed, as it reports them.
 *
 * The JSON-RPC writer serializes writes behind one semaphore, so this request
 * is written after every notification issued before it and the server parses
 * it after them too. Its answer is therefore a barrier as well as a reading:
 * it cannot come back until the server has worked through that backlog.
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
  // `NOTIFY_TIMEOUT_MS` bounds how long the client waits for a notification
  // write, not the write itself: the JSON-RPC writer cannot cancel it, so an
  // abandoned write stays queued and lands whenever the server resumes
  // draining. The version it carries is then one the server holds, and
  // reusing it would send that version twice -- which LSP forbids, and which
  // makes `waitForDiagnostics` settle on the first send's publish and report
  // diagnostics for stale text as current.
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

    // The server stopped reading after the handshake, but this document fits
    // the pipe, so its write completes and the client records version 1.
    expect(await client.notify.open({ path: filePath })).toBe(1);

    // Version 2 does not fit. Its write parks, the client's bound gives up on
    // it, and the caller sees that as a rejection.
    await writeFile(filePath, OVERSIZED_TEXT);
    await expect(client.notify.open({ path: filePath })).rejects.toThrow(
      /didChange .*timed out/,
    );

    // Let the server work through its backlog. The abandoned write was never
    // cancelled, so version 2 is delivered after all.
    proc.kill("SIGUSR2");
    expect(await versionsReceived(client)).toEqual([
      "didOpen v1",
      "didChange v2",
    ]);

    // The server holds version 2 now, so the client must not compute 2 again.
    await expect(client.notify.open({ path: filePath })).rejects.toBeInstanceOf(
      LSPDocumentOutOfSyncError,
    );

    // Anything that call had put on the wire would be parsed before this
    // answer came back, so the reading is conclusive rather than premature.
    expect(await versionsReceived(client)).toEqual([
      "didOpen v1",
      "didChange v2",
    ]);
  }, 60_000);
});
