# @intx/harness

Composition layer over `@intx/agent` that adds the mail-transport
surface: INBOX watch, connector router, connector-reply outbound
forwarding, and connector-state persistence layered on top of the
agent's context store. The reactor is wrapped exactly once -- inside
`@intx/agent`'s `createAgent` -- and the harness composes around that.

Consumed by `apps/sidecar` for sidecar-hosted agents and by demo
examples that need a transport-bearing agent.

```ts
import {
  createDefaultDirectorRegistry,
  defineAgent,
  defineTool,
} from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { createHarness, defineMailTools } from "@intx/harness";
import { createIsogitStore } from "@intx/storage-isogit";

const mailFactory = defineMailTools(
  () => ({
    definitions: myMailTools.definitions,
    run: (call, signal) => myMailTools.run(call, signal),
  }),
  myMailTools.definitions.map((def) => ({ name: def.name })),
);

const posixFactory = defineTool({
  id: "@my-org/agent/posix",
  definitions: myPosixTools.definitions.map((def) => ({ name: def.name })),
  factory: () => ({
    definitions: myPosixTools.definitions,
    run: (call, signal) => myPosixTools.run(call, signal),
  }),
});

const def = defineAgent({
  id: "agent@tenant.interchange.network",
  systemPrompt,
  tools: [mailFactory, posixFactory],
  capabilities: [],
  inference: { sources: [{ provider: source.provider, model: source.model }] },
});

const harness = await createHarness(def, {
  source,
  storage: await createIsogitStore(workdir),
  workdir,
  audit: noopAuditStore(),
  authorize: permissiveAuthorize(),
  directors: createDefaultDirectorRegistry(),
  transport,
  address: "agent@tenant.interchange.network",
});

// Subscribe to events via harness.stream(); compose with your own
// downstream observability sink.
for await (const event of harness.stream()) {
  // ...
}

await harness.close();
```

The narrowed `Harness` surface is `close()`, `deliver(message)`,
`setSource(source)`, `stream()`, and `blobReader`. Tool composition
flows through `defineMailTools` and `defineTool` from `@intx/agent`;
the agent's own `resolveTools` aggregates definitions and dispatches
calls.
