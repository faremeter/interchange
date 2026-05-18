// Public surface of the coding-agent example.
//
// The example is primarily run from `src/cli.ts`, but `createCodingAgent`
// and the path helpers are exported so other consumers (including tests)
// can construct the same agent shape.

export {
  createCodingAgent,
  DEFAULT_ANTHROPIC_BASE_URL,
  DEFAULT_MODEL,
  type CodingAgent,
  type CodingAgentOptions,
} from "./agent";
export { CODING_AGENT_SYSTEM_PROMPT } from "./prompt";
export { defaultContextDir, defaultRepoRoot } from "./paths";
