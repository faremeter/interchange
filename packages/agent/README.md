# @interchange/agent

In-process agent runtime built on `createReactorAssembly` from
`@interchange/inference`. Construct an agent, `send()` it a message,
get a reply.

Use this package when you want an agent you can drive from inside
your own program — a CLI, a worker, a test, an embedded assistant.
If you want an agent that lives behind a mailbox instead, see
`@interchange/harness`.

```ts
import { createAgent } from "@interchange/agent";

const agent = await createAgent({
  contextDir: "./tmp/my-agent",
  providers: [{ provider: "anthropic", apiKey, model }],
  defaultModel: model,
  systemPrompt: "...",
  tools: [],
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
- Provider routing — `agent-multi-provider`
- End-to-end with real tools — `coding-agent`

See [`examples/README.md`](../../examples/README.md) for the full index.
