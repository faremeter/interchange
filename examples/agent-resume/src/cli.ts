// agent-resume: demonstrate that `contextDir` is the durable boundary
// for an @intx/agent conversation.
//
// The CLI opens an agent against a fixed `contextDir`, prints whatever
// turns the store already contains, sends one new prompt, prints the
// reply, and exits. Run the script twice with the same `contextDir`
// and the second run sees the first run's turns — no checkpoint
// management, no explicit serialisation, no fork-handling needed. The
// agent committed each cycle to the isogit-backed context store on
// exit; reopening the store on the same directory replays the
// history.
//
// This is the resume-from-crash story: if you SIGKILL the process
// mid-cycle, the worst case is that the current cycle's checkpoint
// did not flush, and the next run continues from the prior cycle.

import {
  openExampleAgent,
  resolveAgentSource,
  resolveStdio,
  type SingleSourceMainOptions,
} from "@intx/example-agent-common";
import type { ConversationTurn } from "@intx/types/runtime";

const EXAMPLE_NAME = "agent-resume";

export type MainOptions = SingleSourceMainOptions;

function summarizeTurn(turn: ConversationTurn): string {
  const parts: string[] = [];
  for (const block of turn.content) {
    if (block.type === "text") parts.push(block.text);
  }
  const text = parts.join(" ").trim();
  const oneLine = text.replace(/\s+/g, " ");
  const truncated =
    oneLine.length > 80 ? oneLine.slice(0, 77) + "..." : oneLine;
  return `${turn.role}: ${truncated}`;
}

export async function main(
  argv: string[],
  env: NodeJS.ProcessEnv,
  opts: MainOptions = {},
): Promise<number> {
  const { stdout, stderr } = resolveStdio(opts);

  const prompt = argv.join(" ").trim();
  if (prompt === "") {
    stderr("usage: bun run start <prompt>\n");
    return 1;
  }

  const source = resolveAgentSource(opts, env, EXAMPLE_NAME, stderr);
  if (source === null) return 1;

  const agent = await openExampleAgent(opts, {
    exampleName: EXAMPLE_NAME,
    systemPrompt: "You are a helpful assistant. Keep replies concise.",
    tools: [],
    sources: [source],
    defaultSource: source.id,
  });
  try {
    // history() reads straight off the store; on the first run it's
    // empty and on subsequent runs it carries the conversation from
    // every prior process invocation against this directory.
    const prior = await agent.history();
    if (prior.length === 0) {
      stdout("(no prior turns — this is a fresh conversation)\n");
    } else {
      stdout(`(${String(prior.length)} prior turns)\n`);
      for (const turn of prior) {
        stdout(`  ${summarizeTurn(turn)}\n`);
      }
    }

    const result = await agent.send(prompt);
    if (result.type !== "reply") {
      throw new Error(
        `agent send suspended on correlationId ${result.correlationId}; this example drives a single prompt and has no resume path`,
      );
    }
    stdout(`\nassistant: ${result.reply}\n`);
    return 0;
  } finally {
    await agent.close();
  }
}

if (import.meta.main) {
  const code = await main(process.argv.slice(2), process.env);
  if (code !== 0) process.exit(code);
}
