// Planner-shape agent fixture for the reactor-once tests.
//
// A pure in-process agent: no transport, no connector, no mail. Used by
// `planner.test.ts` to assert that `createAgent(def, env)` wraps the
// reactor exactly once per instantiation against a bare `BaseEnv`.

import type { ContextStore, InferenceSource } from "@intx/types/runtime";

import { defineAgent, type AgentDefinition } from "../definition";
import { createDefaultDirectorRegistry } from "../director-registry";
import type { BaseEnv } from "../env";
import { noopAuditStore } from "../testing/audit-noop";
import { permissiveAuthorize } from "../testing/authorize-allow";

export const PLANNER_SOURCE: InferenceSource = {
  id: "anthropic:claude-opus-4-6",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-planner",
  model: "claude-opus-4-6",
};

/**
 * Planner-shape definition: no tool factories, capabilities, or
 * director ref. The default director from the registry handles the
 * loop; the bare env covers every required `BaseEnv` key.
 */
export const plannerAgentDefinition: AgentDefinition<BaseEnv> = defineAgent({
  id: "planner",
  description: "Decomposes goals into a plan",
  systemPrompt: "You are the planner.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [
      { provider: PLANNER_SOURCE.provider, model: PLANNER_SOURCE.model },
    ],
  },
});

/**
 * Build a bare `BaseEnv` for the planner fixture. The caller supplies
 * the storage and workdir; everything else is filled from
 * `@intx/agent/testing` no-ops.
 */
export function createPlannerEnv(opts: {
  storage: ContextStore;
  workdir: string;
}): BaseEnv {
  return {
    source: PLANNER_SOURCE,
    storage: opts.storage,
    workdir: opts.workdir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
  };
}
