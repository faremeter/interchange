// agent-audit-log: show that an @intx/agent contextDir is a
// real git repository, so the audit story is just "git log + git
// show". The example sends N prompts (defaulting to 2), then walks
// the resulting commits with isomorphic-git and prints a summary per
// commit so the reader can see exactly what gets persisted per cycle
// and how to navigate it from outside the agent.

import {
  defaultContextDir,
  openExampleAgent,
  resolveAgentSource,
  resolveStdio,
  type SingleSourceMainOptions,
} from "@intx/example-agent-common";

import { summarizeAuditLog } from "./inspect";

const EXAMPLE_NAME = "agent-audit-log";

export type MainOptions = SingleSourceMainOptions;

export async function main(
  argv: string[],
  env: NodeJS.ProcessEnv,
  opts: MainOptions = {},
): Promise<number> {
  const { stdout, stderr } = resolveStdio(opts);

  if (argv.length === 0) {
    stderr("usage: bun run start <prompt-1> [prompt-2] [prompt-3] ...\n");
    return 1;
  }

  const source = resolveAgentSource(opts, env, EXAMPLE_NAME, stderr);
  if (source === null) return 1;

  const contextDir = opts.contextDir ?? defaultContextDir(EXAMPLE_NAME);

  const agent = await openExampleAgent(
    { ...opts, contextDir },
    {
      exampleName: EXAMPLE_NAME,
      systemPrompt: "You are a helpful assistant. Keep replies concise.",
      tools: [],
      sources: [source],
      defaultSource: source.id,
    },
  );
  try {
    for (const prompt of argv) {
      const result = await agent.send(prompt);
      if (result.type !== "reply") {
        throw new Error(
          `agent send suspended on correlationId ${result.correlationId}; this example drives single prompts and has no resume path`,
        );
      }
      stdout(`> ${prompt}\nassistant: ${result.reply}\n\n`);
    }
  } finally {
    await agent.close();
  }

  // After close() the contextDir is fully quiesced. Walking it with
  // isomorphic-git from a separate code path proves the audit data
  // is readable by anything that speaks git — no special agent API
  // required.
  const commits = await summarizeAuditLog(contextDir);
  stdout(`audit log (${String(commits.length)} commit(s), newest first):\n`);
  for (const c of commits) {
    const ts = new Date(c.timestamp).toISOString();
    stdout(`  ${c.hash.slice(0, 8)}  ${ts}  ${c.message}\n`);
    stdout(`    files: ${c.files.join(", ")}\n`);
    if (c.manifestStrategies.length > 0) {
      stdout(`    transforms: ${c.manifestStrategies.join(", ")}\n`);
    }
  }
  stdout("\n");
  stdout(`To inspect further, run plain git inside ${contextDir}:\n`);
  stdout("  git log --oneline\n");
  stdout("  git show <hash> -- turns.jsonl\n");
  stdout("  git show <hash> -- manifest.jsonl\n");
  stdout("  git show <hash> -- response.jsonl\n");
  return 0;
}

if (import.meta.main) {
  const code = await main(process.argv.slice(2), process.env);
  if (code !== 0) process.exit(code);
}
