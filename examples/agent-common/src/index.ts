// Shared helpers for the agent-* examples.
//
// Every agent-* example needs to build a `ProviderConfig` from the
// surrounding environment (so users with an `ANTHROPIC_API_KEY` set can
// just run the binary) while also accepting a `providerOverride` that
// tests use to bypass the env entirely. Without a common helper each
// example would re-derive the same boilerplate and the failure mode
// (missing env vars) would drift in wording from one example to the
// next.
//
// This package is consumed only by the other examples; it is not part
// of the @interchange public surface.

export {
  DEFAULT_ANTHROPIC_BASE_URL,
  DEFAULT_ANTHROPIC_MODEL,
  resolveProvider,
  type ResolveProviderOpts,
  type ResolveProviderResult,
} from "./env-provider";

export { defaultContextDir, defaultRepoRoot } from "./paths";

export { optional } from "./optional";

export {
  resolveStdio,
  type CommonMainOptions,
  type SingleProviderMainOptions,
} from "./main-options";

export {
  openExampleAgent,
  resolveAgentProvider,
  type OpenExampleAgentSpec,
} from "./agent-setup";
