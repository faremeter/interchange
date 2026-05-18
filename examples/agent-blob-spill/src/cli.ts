// agent-blob-spill: prove that oversized tool output never has to
// sit inline in the conversation. The agent's default size-cap
// transform writes the payload to a blob under `tool-output/<callId>`
// and rewrites the in-history block to a `tool-output:///<callId>`
// URI; `agent.blobReader` resolves the URI back to the original
// bytes on demand.
//
// The CLI sends a single prompt that asks the model to call the
// `fetch_full_logs` tool, watches the cycle complete, then walks
// `history()` to find the spill URI and reads the blob back via the
// agent's BlobReader. The output reports the in-history marker, the
// resolved blob length, and the first few lines of the blob so the
// reader can confirm the round-trip succeeded.

import {
  openExampleAgent,
  resolveAgentProvider,
  resolveStdio,
  type SingleProviderMainOptions,
} from "@interchange/example-agent-common";
import type {
  ContentBlock,
  ConversationTurn,
} from "@interchange/types/runtime";

import { createNoisyTool, DEFAULT_PAYLOAD_CHARS } from "./noisy-tool";

const EXAMPLE_NAME = "agent-blob-spill";

export type MainOptions = SingleProviderMainOptions & {
  /** Override the synthetic payload size (defaults to 25k chars). */
  payloadChars?: number;
};

const SPILL_URI_RE = /tool-output:\/\/\/[A-Za-z0-9_-]+/;
const TRUNCATION_NOTICE_RE = /\[Tool output truncated:[^\]]+\]/;

type ToolResultBlock = Extract<ContentBlock, { type: "tool_result" }>;

function blockText(block: ToolResultBlock): string {
  return block.content.map((c) => (c.type === "text" ? c.text : "")).join("");
}

function findSpillBlock(
  turns: ConversationTurn[],
):
  | { block: ToolResultBlock; uri: string; truncationNotice: string }
  | undefined {
  for (const turn of turns) {
    for (const block of turn.content) {
      if (block.type !== "tool_result") continue;
      const text = blockText(block);
      const noticeMatch = TRUNCATION_NOTICE_RE.exec(text);
      if (noticeMatch === null) continue;
      const uriMatch = SPILL_URI_RE.exec(noticeMatch[0]);
      if (uriMatch === null) continue;
      return {
        block,
        uri: uriMatch[0],
        truncationNotice: noticeMatch[0],
      };
    }
  }
  return undefined;
}

export async function main(
  argv: string[],
  env: NodeJS.ProcessEnv,
  opts: MainOptions = {},
): Promise<number> {
  const { stdout, stderr } = resolveStdio(opts);

  const prompt = argv.join(" ").trim();
  if (prompt === "") {
    stderr(
      'usage: bun run start "<a request that should invoke fetch_full_logs>"\n',
    );
    return 1;
  }

  const resolved = resolveAgentProvider(opts, env, EXAMPLE_NAME, stderr);
  if (resolved === null) return 1;

  const noisy = createNoisyTool(opts.payloadChars ?? DEFAULT_PAYLOAD_CHARS);

  const agent = await openExampleAgent(opts, {
    exampleName: EXAMPLE_NAME,
    systemPrompt:
      "You are a log-summarising assistant. Use the fetch_full_logs tool when asked.",
    tools: [noisy],
    providers: [resolved.provider],
    defaultModel: resolved.model,
  });
  try {
    const { reply } = await agent.send(prompt);
    stdout(`assistant: ${reply}\n\n`);

    const turns = await agent.history();
    const spill = findSpillBlock(turns);
    if (spill === undefined) {
      stderr(
        "no spilled tool_result block found; the size cap may not have triggered\n",
      );
      return 2;
    }
    const blobBytes = await agent.blobReader.read(spill.uri);
    const blob = new TextDecoder().decode(blobBytes);
    const firstLines = blob.split("\n").slice(0, 3).join("\n");
    const inlineBlockChars = blockText(spill.block).length;

    stdout(`spill URI:               ${spill.uri}\n`);
    stdout(`in-history truncation:   ${spill.truncationNotice}\n`);
    stdout(`in-history block chars:  ${String(inlineBlockChars)}\n`);
    stdout(`resolved blob bytes:     ${String(blobBytes.length)}\n`);
    stdout("first lines of blob:\n");
    stdout(
      firstLines
        .split("\n")
        .map((line) => `  ${line}`)
        .join("\n") + "\n",
    );
    return 0;
  } finally {
    await agent.close();
  }
}

if (import.meta.main) {
  const code = await main(process.argv.slice(2), process.env);
  if (code !== 0) process.exit(code);
}
