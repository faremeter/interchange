// Source-entry builder for the map fan-out workflow fixture (F6):
// `seed -> fanout = map({ over: literal, step })`. A leading tool-less agent
// step runs first; the map fans out over a literal item list, running a
// per-item agent step under a scoped id `<mapStepId>[<index>]`. The returned
// string is a `@intx/*`-importing entry module; `bundleWorkflowEntry` inlines
// it to a self-contained `.mjs` the sidecar evaluates in-child.
//
// Parameterised by the mail trigger address, the step ids, the per-step system
// prompts, the item count, and whether the per-item agent carries the inline
// `mail_send` tool. When `withTool` is set the item agent lists
// `mailSendTool("fs")` from the sibling `mail-tool.ts` module, so the mock
// inference reply lists the tool name -- the proof that a map iteration
// resolves its base step's staged tool tree.

import path from "node:path";

// Absolute path to the sibling tool module, so the bundled entry resolves it
// through `Bun.build` (which inlines the module) rather than a bare specifier
// the sidecar closure could not resolve.
const MAIL_TOOL_MODULE = path.join(import.meta.dir, "mail-tool.ts");

export type MapFanOutFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The leading step's key in the workflow's `steps` map. Defaults to `seed`. */
  seedStepId?: string;
  /** The map step's base key in the workflow's `steps` map. Defaults to `fanout`. */
  mapStepId?: string;
  /** The leading step agent's system prompt. */
  seedSystemPrompt: string;
  /** The per-item step agent's system prompt. */
  itemSystemPrompt: string;
  /** The number of literal items the map fans out over. Defaults to 2. */
  itemCount?: number;
  /** When true, the per-item agent carries the inline `mail_send` tool. */
  withTool?: boolean;
  /** The leading agent's `defineAgent` id. Defaults to a stable fixture-local id. */
  seedAgentId?: string;
  /** The per-item agent's `defineAgent` id. Defaults to a stable fixture-local id. */
  itemAgentId?: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
};

export function mapFanOutEntry(params: MapFanOutFixtureParams): string {
  const seedStepId = params.seedStepId ?? "seed";
  const mapStepId = params.mapStepId ?? "fanout";
  const seedAgentId = params.seedAgentId ?? "map-fan-out-seed-agent";
  const itemAgentId = params.itemAgentId ?? "map-fan-out-item-agent";
  const workflowId = params.workflowId ?? "wf_map_fan_out";
  const itemCount = params.itemCount ?? 2;
  const withTool = params.withTool ?? false;

  const items = Array.from({ length: itemCount }, (_unused, i) => ({
    id: String.fromCharCode(97 + i),
  }));

  const toolImport = withTool
    ? `import { mailSendTool } from ${JSON.stringify(MAIL_TOOL_MODULE)};\n`
    : "";
  const itemTools = withTool ? `[mailSendTool("fs")]` : `[]`;

  return `
import { defineWorkflow, map, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
${toolImport}
const seedAgent = defineAgent({
  id: ${JSON.stringify(seedAgentId)},
  systemPrompt: ${JSON.stringify(params.seedSystemPrompt)},
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

const itemAgent = defineAgent({
  id: ${JSON.stringify(itemAgentId)},
  systemPrompt: ${JSON.stringify(params.itemSystemPrompt)},
  tools: ${itemTools},
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(seedStepId)}]: step({ agent: seedAgent }),
    [${JSON.stringify(mapStepId)}]: map({
      over: { literal: ${JSON.stringify(items)} },
      step: step({ agent: itemAgent }),
      after: [${JSON.stringify(seedStepId)}],
    }),
  },
});
`;
}
