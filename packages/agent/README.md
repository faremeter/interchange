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

## Compactors

A compactor rewrites the conversation history when the director asks for
it. The deployer registers compactors on `env.compactors` keyed by name;
the director picks a name at construction and emits
`caps.compact(name, reason)` when it wants the history rewritten.

```ts
const agent = await createAgent(def, {
  source,
  storage,
  workdir,
  audit: noopAuditStore(),
  authorize: permissiveAuthorize(),
  directors: createDefaultDirectorRegistry(),
  compactors: { "tail-only": tailOnlyCompactor() },
});
```

The director author sees the registered names through
`agentContext.compactorNames`, the same way `agentContext.toolDefinitions`
surfaces the resolved tools:

```ts
import { defineDirector } from "@intx/agent";
import { type } from "arktype";

defineDirector({
  id: "my-pkg/planner",
  configSchema: type({}),
  factory: (_config, _env, agent) => {
    const compactor = agent.compactorNames[0];
    if (compactor === undefined) {
      throw new Error("planner needs a compactor; none registered on env");
    }
    return makePlanner({ compactor });
  },
});
```

The reactor resolves the name against `env.compactors` and runs the
compactor's `apply()` on the conversation turns. A `caps.compact(name, …)`
call against a name the deployer did not register produces a fatal
"no compactor registered" reactor error -- the director-author contract
is "pick from `compactorNames`," not "trust the deployer by convention."

`env.compactors` is optional. A deployer that omits the field hands an
agent whose director never calls `caps.compact(...)` an environment that
matches an empty registry.

## Where to start

Read [`examples/agent-quickstart`](../../examples/agent-quickstart/README.md).
It is the minimum runnable program against this package.

Then, depending on what you're trying to do:

- Persistence and time travel — `agent-resume`, `agent-rewind`, `agent-audit-log`
- Tool I/O — `agent-blob-spill`, `agent-rich-tool`, `agent-structured-payload`
- Multi inference provider routing — `agent-multi-provider`
- End-to-end with real tools — `coding-agent`

See [`examples/README.md`](../../examples/README.md) for the full index.
