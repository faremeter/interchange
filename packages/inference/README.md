# @intx/inference

Provider-agnostic inference runtime. Adapters for Anthropic,
OpenAI-compatible relays (including OpenCode Zen), and Google
GenAI; SSE parsing; retry and error classification; message
transforms; and the reactor harness that drives a turn from
prompt to settled tool calls.

Consumed by `@intx/harness`, which composes the reactor with tool
runners, directors, and runtime capabilities.

```ts
import { createReactorAssembly, createDefaultDirector } from "@intx/inference";

const director = createDefaultDirector(
  systemPrompt,
  toolDefinitions, // ToolDefinition[] — the tool surface advertised to the model
  policy,
);

const assembly = createReactorAssembly({
  sessionId,
  director,
  source, // InferenceSource: id, provider, model, apiKey, baseURL
  toolRunner, // ToolRunner — dispatches the tool calls the director emits
  contextStore,
  onEvent: (event) => {
    // event: ReactorEmittedEvent — persist or forward
  },
});

assembly.reactor.start();
```

Provider selection is driven by `source.provider`; the assembly
resolves the matching adapter internally. `ReactorAssemblyConfig`
in `src/assembly.ts` documents the optional fields (`authorize`,
`auditStore`, transforms, compactors, correlation, gate
timeouts).

The package is the lower half of the agent stack: it knows how to
talk to model providers, how to drive a multi-turn exchange to
completion, and how to compose extensions (gates, audit,
correlation, authz) into the reactor pipeline. The higher-level
agent surface — config validation, tool runners, deploy trees,
runtime capabilities — lives in `@intx/harness`.
