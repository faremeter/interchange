import { describe, test, expect, afterAll } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { createLSPClient, type LSPClient } from "./client";

const STALLING_SERVER = join(import.meta.dir, "stalling-lsp-server.ts");

let dir: string;
const clients: LSPClient[] = [];

afterAll(async () => {
  for (const c of clients) c.connection.dispose();
  if (dir !== undefined) await rm(dir, { recursive: true, force: true });
});

describe("notify.open against a server that stops draining stdin", () => {
  // `openFile` awaits its didOpen/didChange notification writes. A write
  // resolves on the pipe flush, so a server that completes the handshake and
  // then stops reading stdin leaves the write pending once the OS pipe buffer
  // fills. `sendDocumentNotification` wraps these writes in
  // `NOTIFY_TIMEOUT_MS`, so the caller sees a rejection rather than a hang;
  // this test pins that.
  test("settles instead of hanging once the pipe buffer fills", async () => {
    dir = await mkdtemp(join(tmpdir(), "lsp-stalled-open-"));
    const filePath = join(dir, "big.ts");
    await writeFile(filePath, `const x = 1;\n${"// pad\n".repeat(20_000)}`);

    const proc = spawn("bun", ["run", STALLING_SERVER], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    const client = await createLSPClient({
      serverID: "stalled",
      server: { process: proc },
      root: dir,
    });
    clients.push(client);

    // Fill the pipe. Early calls resolve; once the buffer is full the write
    // parks and the notification bound turns that into a rejection. The
    // first rejection is the proof, so stop there.
    let outcome: "rejected" | "hung" | undefined;
    for (let i = 1; i <= 40 && outcome === undefined; i++) {
      outcome = await Promise.race([
        client.notify.open({ path: filePath }).then(
          () => undefined,
          () => "rejected" as const,
        ),
        // Only has to outlast the client's bound, so it sits well clear of
        // it: the invariant is that the call settles at all.
        new Promise<"hung">((r) => setTimeout(() => r("hung"), 20_000)),
      ]);
    }

    proc.kill("SIGKILL");
    // Unbounded, the parked write never settles and this reads "hung".
    expect(outcome).toBe("rejected");
  }, 60_000);
});
