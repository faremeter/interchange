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

import type { ReactorEmittedEvent } from "@interchange/inference";

import { createCodingAgent } from "./agent";
import { defaultContextDir, defaultRepoRoot } from "./paths";

type CliArgs = {
  prompt: string;
  cwd: string;
  contextDir: string;
  model: string | undefined;
};

function parseCliArgs(argv: string[]): CliArgs {
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

async function main(argv: string[]): Promise<void> {
  const apiKey = process.env["ANTHROPIC_API_KEY"];
  if (apiKey === undefined || apiKey === "") {
    process.stderr.write("ANTHROPIC_API_KEY is required\n");
    process.exit(1);
  }

  let args: CliArgs;
  try {
    args = parseCliArgs(argv);
  } catch (err) {
    process.stderr.write(
      (err instanceof Error ? err.message : String(err)) + "\n",
    );
    process.exit(1);
  }

  const { agent, close } = await createCodingAgent({
    contextDir: args.contextDir,
    cwd: args.cwd,
    apiKey,
    ...(args.model !== undefined ? { model: args.model } : {}),
  });

  // Mirror the reactor's event stream to stderr so the user can watch
  // tool calls, inference deltas, and checkpoints land in real time
  // while the send() resolves on stdout. The pump runs concurrently
  // with the send and terminates when agent.close() is called.
  const streamPump = pumpStreamToStderr(agent.stream());

  try {
    const { reply } = await agent.send(args.prompt);
    process.stdout.write(reply + "\n");
  } finally {
    await close();
    await streamPump;
  }
}

async function pumpStreamToStderr(
  events: AsyncIterable<ReactorEmittedEvent>,
): Promise<void> {
  for await (const event of events) {
    // Format one event per line; keep the payload compact so the
    // example output is readable.
    process.stderr.write(`[${event.type}] seq=${String(event.seq)}\n`);
  }
}

await main(process.argv.slice(2));
