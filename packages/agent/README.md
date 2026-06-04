# @intx/agent

In-process agent runtime built on `createReactorAssembly` from
`@intx/inference`. Construct an agent, `send()` it a message,
get a reply.

Use this package when you want an agent you can drive from inside
your own program — a CLI, a worker, a test, an embedded assistant.
If you want an agent that lives behind a mailbox instead, see
`@intx/harness`.

```ts
import {
  createAgent,
  createDefaultDirectorRegistry,
  defineAgent,
} from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { createIsogitStore } from "@intx/storage-isogit";

// `apiKey` and `model` come from the caller's env / config; pick the
// shape that fits the deployment. The snippet below uses literals so
// it copy-pastes cleanly.
const apiKey = process.env.ANTHROPIC_API_KEY ?? "";
const model = "claude-sonnet-4-6";
const source = {
  id: `anthropic:${model}`,
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey,
  model,
};

const workdir = "./tmp/my-agent";
const storage = await createIsogitStore(workdir);

const def = defineAgent({
  id: "my-agent",
  systemPrompt: "...",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: source.provider, model: source.model }],
  },
});

const agent = await createAgent(def, {
  source,
  storage,
  workdir,
  audit: noopAuditStore(),
  authorize: permissiveAuthorize(),
  directors: createDefaultDirectorRegistry(),
});

const { reply } = await agent.send("hello");
await agent.close();
```

## Where to start

Read [`examples/agent-quickstart`](../../examples/agent-quickstart/README.md).
It is the minimum runnable program against this package.

Then, depending on what you're trying to do:

- Persistence and time travel — `agent-resume`, `agent-rewind`, `agent-audit-log`
- Tool I/O — `agent-blob-spill`, `agent-rich-tool`, `agent-structured-payload`
- Multi inference provider routing — `agent-multi-provider`
- End-to-end with real tools — `coding-agent`

See [`examples/README.md`](../../examples/README.md) for the full index.
