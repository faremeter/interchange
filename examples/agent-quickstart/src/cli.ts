// agent-quickstart: the smallest possible @intx/agent program.
//
// The body of `main()` below is the answer to "what does an agent
// look like?" — construct, send a prompt, print the reply, close.
// This is the only example that calls `createAgent` inline; the
// other seven agent-* examples go through
// `openExampleAgent` in @intx/example-agent-common so they
// can keep their focus on the feature they demonstrate. Read this
// file when you want to see the full `createAgent` surface.

import { createAgent } from "@intx/agent";
import {
  defaultContextDir,
  optional,
  resolveAgentSource,
  resolveStdio,
  type SingleSourceMainOptions,
} from "@intx/example-agent-common";

const EXAMPLE_NAME = "agent-quickstart";

export type MainOptions = SingleSourceMainOptions;

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

  const agent = await createAgent({
    contextDir: opts.contextDir ?? defaultContextDir(EXAMPLE_NAME),
    sources: [source],
    defaultSource: source.id,
    systemPrompt: "You are a helpful assistant. Keep replies concise.",
    tools: [],
    ...optional("deps", opts.deps),
  });
  try {
    const { reply } = await agent.send(prompt);
    stdout(reply + "\n");
    return 0;
  } finally {
    await agent.close();
  }
}

if (import.meta.main) {
  const code = await main(process.argv.slice(2), process.env);
  if (code !== 0) process.exit(code);
}
