# @intx/agent

In-process agent runtime built on `createReactorAssembly` from
`@intx/inference`. Construct an agent, `send()` it a message,
get a reply.

Workflow packages can import `defineAgent`, tool and director declarations,
and director registries from `@intx/agent/authoring`. That entry point keeps
the reactor and agent execution runtime out of definition loading.

`@intx/agent/canonicalize` exports `canonicalizeForHash` and
`CanonicalizationError` for deterministic serialization of deploy-hash inputs
without loading the agent execution runtime.

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
import { createIsogitStore } from "@intx/storage-isogit/node";

// `apiKey` and `model` come from the caller's env / config; pick the
// shape that fits the deployment. The snippet below uses literals so
// it copy-pastes cleanly.
const apiKey = process.env.ANTHROPIC_API_KEY ?? "";
const model = "claude-sonnet-5";
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

## Sends and turns

Each `send()` belongs to one turn: one inbound request, which can span
several model calls and resumes. Pass `SendOptions.turnId` to name the
turn; without it the agent mints a fresh id. The send settles only on the
events of the turn it follows, so another turn's reply or park never
settles it.

- A reply of the turn resolves the send as `reply`.
- A park of the turn resolves it as `suspended`, with the correlation id
  to resume against.
- A send that carries a correlated decision for a parked turn joins that
  turn: it then follows the parked turn and settles on its reply or its
  next park.
- A turn that ends with no reply for the send rejects it with
  `TurnEndedWithoutReplyError`, for example when the director waits, when
  a correlated send resumes nothing, or when the send joined a parked turn
  whose only reply came before the send joined it.
- A turn that ends `failed` with no fatal reactor error before it rejects
  the send with `TurnFailedError`.
- A send that names a turn that already ended rejects. Naming another live
  turn, other than a parked turn the send resumes, is misuse. For example,
  a queued send that names the same turn as the send before it starts when
  that send's reply settles it, before the turn ends. Its message then runs
  under a fresh turn, and the named turn's end rejects the send with
  `TurnEndedWithoutReplyError`.

Both `reply` and `suspended` results carry `turnId`: the turn the send
followed, which after a join is the parked turn. See INFERENCE.md § Agent
Sends.

An aborted send (`SendOptions.signal`) rejects with the signal's reason
at once, and the agent abandons the turn the send follows: its model call
or tool batch is aborted, the director sees no more of its events, its
open tool calls get an error result, and it ends `abandoned`. The next
send starts once that turn has ended. The turn the send follows is also
a turn the send names (`SendOptions.turnId`): an abort abandons that
turn, even a parked one. A turn stays parked only when the send uses its
own fresh turn id and the abort lands before the delivery is admitted:
the delivery is then dropped and resumes nothing. A send aborted while
still queued never reaches the reactor.

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
