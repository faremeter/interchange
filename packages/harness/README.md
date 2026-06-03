# @intx/harness

Agent harness runtime. Composes the inference reactor, tool
runners, deploy-tree reader, default director, runtime
capabilities, and connector router into a single supervisor that
sits between the message transport and the reactor.

The harness watches the agent's INBOX, routes inbound messages by
thread, sends connector replies with the right threading headers,
and drives the reactor loop to completion. Consumed by
`@intx/agent` for in-process agents and by `@intx/hub-agent` for
sidecar-hosted agents.

```ts
import { createHarness, mergeToolRunners } from "@intx/harness";

const harness = createHarness({
  address: "agent@tenant.interchange.network",
  systemPrompt,
  source, // InferenceSource: id, provider, model, apiKey, baseURL
  transport, // MessageTransport
  crypto, // CryptoProvider
  storage, // ContextStore
  tools: mergeToolRunners([mailTools, posixTools]),
  onEvent: (event) => {
    // event: InferenceEvent — persist or forward
  },
});

harness.start();
```

`HarnessConfig` in `src/config.ts` lists the optional fields:
`director` or `defaultDirectorPolicy` (mutually exclusive),
`beforeToolExtensions`, `auditStore`, `authorize`, and
`onConnectorStateChanged`. `mergeToolRunners` composes multiple
`ToolRunner` implementations into a single runner that dispatches
by tool definition name.
