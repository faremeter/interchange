// Deploy tree reader: extracts prompt and tool definitions from the deploy
// directory of an agent's git repository.
//
// The deploy tree is written by `applyDeployPack` and contains:
//   deploy/prompt.md           — system prompt for inference
//   deploy/skills/<name>/tool.json — tool definitions

import fs from "node:fs";
import path from "node:path";
import { type } from "arktype";
import type { ToolDefinition } from "@interchange/types/runtime";

const ToolManifest = type({
  "name?": "string",
  "description?": "string",
  "inputSchema?": "Record<string, unknown>",
});

export type DeployToolInfo = {
  definition: ToolDefinition;
  hasHandler: boolean;
};

export type DeployTree = {
  systemPrompt: string | undefined;
  tools: DeployToolInfo[];
};

/**
 * Read tool definitions and system prompt from the deploy directory.
 * Returns undefined for systemPrompt when deploy/prompt.md does not exist
 * (agent has not yet received a deploy pack). Throws on malformed tool.json.
 */
export async function readDeployTree(dir: string): Promise<DeployTree> {
  const promptPath = path.join(dir, "deploy", "prompt.md");
  const skillsDir = path.join(dir, "deploy", "skills");

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

  const tools: DeployToolInfo[] = [];

  let skillDirs: string[];
  try {
    skillDirs = await fs.promises.readdir(skillsDir);
  } catch (e) {
    if (e instanceof Error && "code" in e && e.code === "ENOENT") {
      return { systemPrompt, tools };
    }
    throw e;
  }

  for (const skillName of skillDirs) {
    const toolJsonPath = path.join(skillsDir, skillName, "tool.json");
    let raw: string;
    try {
      raw = await fs.promises.readFile(toolJsonPath, "utf-8");
    } catch (e) {
      if (
        e instanceof Error &&
        "code" in e &&
        (e.code === "ENOENT" || e.code === "ENOTDIR")
      ) {
        continue;
      }
      throw e;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch (e) {
      throw new Error(
        `Malformed tool.json in skill "${skillName}": ${e instanceof Error ? e.message : String(e)}`,
      );
    }

    if (
      typeof parsed !== "object" ||
      parsed === null ||
      Array.isArray(parsed)
    ) {
      throw new Error(
        `Invalid tool.json in skill "${skillName}": expected an object`,
      );
    }

    const manifest = ToolManifest(parsed);
    if (manifest instanceof type.errors) {
      throw new Error(
        `Invalid tool.json in skill "${skillName}": ${manifest.summary}`,
      );
    }

    if (manifest.name === undefined || manifest.name.trim() === "") {
      throw new Error(
        `Invalid tool.json in skill "${skillName}": missing or empty "name" field`,
      );
    }
    if (
      manifest.description === undefined ||
      manifest.description.trim() === ""
    ) {
      throw new Error(
        `Invalid tool.json in skill "${skillName}": missing or empty "description" field`,
      );
    }
    if (manifest.inputSchema === undefined) {
      throw new Error(
        `Invalid tool.json in skill "${skillName}": missing or invalid "inputSchema" field`,
      );
    }

    const handlerPath = path.join(skillsDir, skillName, "handler.ts");
    let hasHandler = false;
    try {
      const stat = await fs.promises.stat(handlerPath);
      hasHandler = stat.isFile();
    } catch (e) {
      if (!(e instanceof Error && "code" in e && e.code === "ENOENT")) {
        throw e;
      }
    }

    tools.push({
      definition: {
        name: manifest.name,
        description: manifest.description,
        inputSchema: manifest.inputSchema,
      },
      hasHandler,
    });
  }

  return { systemPrompt, tools };
}
