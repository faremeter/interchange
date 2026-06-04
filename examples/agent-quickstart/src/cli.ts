// agent-quickstart: the smallest possible @intx/agent program.
//
// The body of `main()` below is the answer to "what does an agent look
// like?" -- define, build an env, instantiate, send a prompt, print
// the reply, close. This is the only example that spells the
// definition + env shape inline; the other seven agent-* examples go
// through `openExampleAgent` in @intx/example-agent-common so they can
// keep their focus on the feature they demonstrate. Read this file
// when you want to see the *required* `defineAgent` / `createAgent`
// surface.
//
// The env below sets only the required `BaseEnv` keys (`source`,
// `storage`, `workdir`, `audit`, `authorize`, `directors`). The
// optional tuning knobs documented on `BaseEnv` -- `closeTimeoutMs`,
// `sendQueueMax`, `streamBufferMax`, `sizeCapMaxChars`, `sessionId`,
// `deps` -- are intentionally omitted from this example to keep the
// minimum-shape surface obvious. Each optional field has its own
// demonstrating example in the agent-* set; see
// `examples/agent-blob-spill` for `sizeCapMaxChars` and the agent-*
// READMEs for the others.

import { mkdirSync } from "node:fs";

import {
  createAgent,
  createDefaultDirectorRegistry,
  defineAgent,
  type BaseEnv,
} from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import {
  defaultContextDir,
  optional,
  resolveAgentSource,
  resolveStdio,
  type SingleSourceMainOptions,
} from "@intx/example-agent-common";
import { createIsogitStore } from "@intx/storage-isogit";

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

  const contextDir = opts.contextDir ?? defaultContextDir(EXAMPLE_NAME);
  mkdirSync(contextDir, { recursive: true });
  const storage = await createIsogitStore(contextDir);

  const def = defineAgent({
    id: EXAMPLE_NAME,
    systemPrompt: "You are a helpful assistant. Keep replies concise.",
    tools: [],
    capabilities: [],
    inference: {
      sources: [{ provider: source.provider, model: source.model }],
    },
  });

  const agentEnv: BaseEnv = {
    source,
    storage,
    workdir: contextDir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
    ...optional("deps", opts.deps),
  };

  const agent = await createAgent(def, agentEnv);
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
