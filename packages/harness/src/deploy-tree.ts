// Deploy tree reader: extracts the system prompt from the deploy
// directory of an agent's git repository.
//
// The deploy tree is written by `applyDeployPack` and contains:
//   deploy/prompt.md           — system prompt for inference

import fs from "node:fs";
import path from "node:path";

export type DeployTree = {
  systemPrompt: string | undefined;
};

/**
 * Read the system prompt from the deploy directory. Returns undefined for
 * systemPrompt when deploy/prompt.md does not exist (agent has not yet
 * received a deploy pack).
 */
export async function readDeployTree(dir: string): Promise<DeployTree> {
  const promptPath = path.join(dir, "deploy", "prompt.md");

  let systemPrompt: string | undefined;
  try {
    const raw = await fs.promises.readFile(promptPath, "utf-8");
    systemPrompt = raw.trim() === "" ? undefined : raw;
  } catch (e) {
    if (e instanceof Error && "code" in e && e.code === "ENOENT") {
      systemPrompt = undefined;
    } else {
      throw e;
    }
  }

  return { systemPrompt };
}
