// CLI entrypoint for the coding-agent example.
//
//   bun run start "list the markdown files in the repo root"
//
// Reads ANTHROPIC_API_KEY from the environment, instantiates an agent
// pointed at the example's default context directory, sends a single
// prompt, and prints the model's reply. History is committed to the
// context directory; running the script again resumes the same
// conversation.
//
// The reply is written to stdout. Reactor events (inference deltas,
// tool calls, checkpoints) are streamed to stderr as they happen so the
// example exercises `agent.stream()` alongside `agent.send()`.

import { parseArgs } from "node:util";

import type { Dependencies, ReactorEmittedEvent } from "@intx/inference";
import type { InferenceSource } from "@intx/types/runtime";

import { createCodingAgent } from "./agent";
import { defaultContextDir, defaultRepoRoot } from "./paths";

type CliArgs = {
  prompt: string;
  cwd: string;
  contextDir: string;
  model: string | undefined;
};

export type MainOptions = {
  /** Replace stdout (defaults to `process.stdout.write`). */
  stdout?: (chunk: string) => void;
  /** Replace stderr (defaults to `process.stderr.write`). */
  stderr?: (chunk: string) => void;
  /** Inject inference deps (for tests using `@intx/inference-testing`). */
  deps?: Dependencies;
  /** Skip credential parsing and use this inference source directly. */
  sourceOverride?: InferenceSource;
};

export function parseCliArgs(argv: string[]): CliArgs {
  const { values, positionals } = parseArgs({
    args: argv,
    allowPositionals: true,
    options: {
      cwd: { type: "string" },
      "context-dir": { type: "string" },
      model: { type: "string" },
    },
  });
  const prompt = positionals.join(" ").trim();
  if (prompt === "") {
    throw new Error(
      "usage: bun run start [--cwd <dir>] [--context-dir <dir>] [--model <name>] <prompt>",
    );
  }
  return {
    prompt,
    cwd: values.cwd ?? defaultRepoRoot(),
    contextDir: values["context-dir"] ?? defaultContextDir(),
    model: values.model,
  };
}

async function pumpStreamToStderr(
  events: AsyncIterable<ReactorEmittedEvent>,
  stderr: (chunk: string) => void,
): Promise<void> {
  for await (const event of events) {
    stderr(`[${event.type}] seq=${String(event.seq)}\n`);
  }
}

/**
 * Execute one CLI run. Returns an exit code (0 success, non-zero failure).
 * Pure with respect to globals when callers supply `stdout`/`stderr`/
 * `sourceOverride`/`deps` — that's the seam tests use.
 */
export async function main(
  argv: string[],
  env: NodeJS.ProcessEnv,
  opts: MainOptions = {},
): Promise<number> {
  const stdout = opts.stdout ?? ((s) => void process.stdout.write(s));
  const stderr = opts.stderr ?? ((s) => void process.stderr.write(s));

  const apiKey = env["ANTHROPIC_API_KEY"];
  if (
    opts.sourceOverride === undefined &&
    (apiKey === undefined || apiKey === "")
  ) {
    stderr("ANTHROPIC_API_KEY is required\n");
    return 1;
  }

  let args: CliArgs;
  try {
    args = parseCliArgs(argv);
  } catch (err) {
    stderr((err instanceof Error ? err.message : String(err)) + "\n");
    return 1;
  }

  const { agent, close } = await createCodingAgent({
    contextDir: args.contextDir,
    cwd: args.cwd,
    ...(apiKey !== undefined ? { apiKey } : {}),
    ...(args.model !== undefined ? { model: args.model } : {}),
    ...(opts.sourceOverride !== undefined
      ? { sourceOverride: opts.sourceOverride }
      : {}),
    ...(opts.deps !== undefined ? { deps: opts.deps } : {}),
  });

  // Mirror the reactor's event stream to stderr so the user can watch
  // tool calls, inference deltas, and checkpoints land in real time
  // while the send() resolves on stdout. The pump runs concurrently
  // with the send and terminates when agent.close() is called.
  const streamPump = pumpStreamToStderr(agent.stream(), stderr);

  try {
    const { reply } = await agent.send(args.prompt);
    stdout(reply + "\n");
    return 0;
  } finally {
    await close();
    await streamPump;
  }
}

// Top-level invocation when run directly (not when imported by tests).
// Bun sets `import.meta.main` true for the entry module.
if (import.meta.main) {
  const code = await main(process.argv.slice(2), process.env);
  if (code !== 0) process.exit(code);
}
