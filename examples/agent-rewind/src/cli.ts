// agent-rewind: build a multi-turn conversation, then root a fresh
// agent at an older commit so its `history()` only sees the turns
// that existed at that point in time.
//
// The CLI takes two prompts. It sends both against a primary
// `contextDir`, walks `checkpoints()` to find the commit that lands
// at the end of the first send (one before the most-recent
// checkpoint), clones the directory into a sibling path, rewinds
// the clone's HEAD to that older commit, and opens a new agent on
// the clone. The new agent sees only the first prompt's turns —
// proof that the rewind is rooted at the older state rather than
// merely reading historical commits.

import {
  defaultContextDir,
  openExampleAgent,
  resolveAgentProvider,
  resolveStdio,
  type SingleProviderMainOptions,
} from "@intx/example-agent-common";

import { defaultRewindDir, EXAMPLE_NAME } from "./paths";
import { clearRewindDir, cloneAndRewind } from "./rewind";

export type MainOptions = SingleProviderMainOptions & {
  rewindDir?: string;
};

const SYSTEM_PROMPT = "You are a helpful assistant. Keep replies concise.";

export async function main(
  argv: string[],
  env: NodeJS.ProcessEnv,
  opts: MainOptions = {},
): Promise<number> {
  const { stdout, stderr } = resolveStdio(opts);

  // `noUncheckedIndexedAccess` widens index access to `T | undefined`,
  // so we destructure once and check both bindings to placate the
  // type system in a single place.
  const [first, second, ...rest] = argv;
  if (first === undefined || second === undefined || rest.length > 0) {
    stderr("usage: bun run start <first-prompt> <second-prompt>\n");
    return 1;
  }

  const resolved = resolveAgentProvider(opts, env, EXAMPLE_NAME, stderr);
  if (resolved === null) return 1;

  const contextDir = opts.contextDir ?? defaultContextDir(EXAMPLE_NAME);
  const rewindDir = opts.rewindDir ?? defaultRewindDir();

  // Tear down any stale rewind directory from a previous run so the
  // copy step below sees an empty destination.
  await clearRewindDir(rewindDir);

  const agent = await openExampleAgent(
    { ...opts, contextDir },
    {
      exampleName: EXAMPLE_NAME,
      systemPrompt: SYSTEM_PROMPT,
      tools: [],
      providers: [resolved.provider],
      defaultModel: resolved.model,
    },
  );

  let latestHash: string;
  let rewindHash: string;
  try {
    await agent.send(first);
    await agent.send(second);

    // checkpoints() returns commits newest-first. After two sends the
    // log contains [post-second, post-first]. The "rewind target" is
    // the commit at the end of the first send, so index 1.
    const checkpoints = await agent.checkpoints(10);
    const latest = checkpoints[0];
    const target = checkpoints[1];
    if (latest === undefined || target === undefined) {
      stderr(
        `expected at least two checkpoints, got ${String(checkpoints.length)}\n`,
      );
      return 1;
    }
    latestHash = latest.hash;
    rewindHash = target.hash;
  } finally {
    // Must close before copying the directory: the original agent
    // still holds the singleton-per-contextDir lock and the isogit
    // store may have an open packfile reader. Letting close() drain
    // ensures the copy below sees a quiesced repository.
    await agent.close();
  }

  await cloneAndRewind({
    sourceDir: contextDir,
    destDir: rewindDir,
    hash: rewindHash,
  });

  const rewound = await openExampleAgent(
    { ...opts, contextDir: rewindDir },
    {
      exampleName: EXAMPLE_NAME,
      systemPrompt: SYSTEM_PROMPT,
      tools: [],
      providers: [resolved.provider],
      defaultModel: resolved.model,
    },
  );
  try {
    const rewoundHistory = await rewound.history();
    stdout(`original HEAD: ${latestHash}\n`);
    stdout(`rewound HEAD:  ${rewindHash}\n`);
    stdout(`rewound history turns: ${String(rewoundHistory.length)}\n`);
    for (const turn of rewoundHistory) {
      const text = turn.content
        .filter((b) => b.type === "text")
        .map((b) => (b.type === "text" ? b.text : ""))
        .join(" ")
        .trim()
        .replace(/\s+/g, " ");
      const truncated = text.length > 80 ? text.slice(0, 77) + "..." : text;
      stdout(`  ${turn.role}: ${truncated}\n`);
    }
    return 0;
  } finally {
    await rewound.close();
  }
}

if (import.meta.main) {
  const code = await main(process.argv.slice(2), process.env);
  if (code !== 0) process.exit(code);
}
