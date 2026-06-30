// This module is Node-bound: it spawns subprocesses through node:child_process
// and is not portable to environments without that API.

import { spawn } from "node:child_process";

export type RunShellArgs = {
  command: string;
  timeout?: number;
  cwd?: string;
};

const DEFAULT_TIMEOUT_MS = 30_000;

export async function runShell(
  args: RunShellArgs,
  signal: AbortSignal,
): Promise<{ output: string; exitCode: number }> {
  signal.throwIfAborted();

  const timeoutMs = args.timeout ?? DEFAULT_TIMEOUT_MS;

  return new Promise((resolve, reject) => {
    const chunks: string[] = [];

    const child = spawn(args.command, {
      shell: true,
      stdio: ["ignore", "pipe", "pipe"],
      cwd: args.cwd,
    });

    if (child.stdout === null || child.stderr === null) {
      reject(new Error("child process streams are null; stdio misconfigured"));
      return;
    }

    // Interleave stdout and stderr in temporal order by writing both to the
    // same collector as data arrives. Node emits data events from both streams
    // on the same event loop tick order that the OS delivers them, so appending
    // to a shared array preserves temporal ordering.
    child.stdout.on("data", (chunk: Uint8Array) => {
      chunks.push(new TextDecoder().decode(chunk));
    });

    child.stderr.on("data", (chunk: Uint8Array) => {
      chunks.push(new TextDecoder().decode(chunk));
    });

    let settled = false;

    const settle = (err?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      abortCleanup();
      if (err !== undefined) {
        reject(err);
      }
    };

    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      settle(
        new Error(`command timed out after ${timeoutMs}ms: ${args.command}`),
      );
    }, timeoutMs);

    const onAbort = () => {
      child.kill("SIGKILL");
      settle(new Error(`command aborted: ${args.command}`));
    };

    signal.addEventListener("abort", onAbort, { once: true });

    const abortCleanup = () => {
      signal.removeEventListener("abort", onAbort);
    };

    child.on("error", (err) => {
      settle(
        new Error(`failed to spawn command: ${args.command}`, { cause: err }),
      );
    });

    child.on("close", (code, sig) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      abortCleanup();

      const output = chunks.join("");
      const exitCode = code ?? (sig !== null ? 128 : 1);
      resolve({ output, exitCode });
    });
  });
}
