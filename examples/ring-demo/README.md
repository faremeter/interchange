# ring-demo

A "braintrust" of five agents that pass a question around a ring over an
in-process mailbox, each adding its perspective, until the discussion
returns to the start and the first agent synthesizes a recommendation.

Like `posix-demo`, this is a standalone script — `src/cli.ts` runs at
module scope with no `main()` to import.

## What it shows

- A five-agent ring — **alpha** (Facilitator), **bravo** (Devil's
  Advocate), **charlie** (Technical Architect), **delta** (UX Lead), and
  **echo** (Synthesizer) — plus a **user**, all on one
  `@intx/mail-memory` transport with per-agent Ed25519 identities.
- Ring routing built entirely from system prompts: each agent's prompt
  names the next address and tells it to `mail_send` the running
  discussion onward. Alpha frames the question, sends it to bravo, and
  uses `mail_wait` to block until echo closes the ring.
- Two director modes side by side: agents past the first use
  `buildDefaultDirectorRef({ mode: "reactive" })` — one tool call per
  inbound message, no follow-up inference — while alpha uses the default
  conversational director so it can compose the final reply.
- Each agent carries mail and POSIX tools, an isogit store, and a
  permissive authorize, constructed in a loop over the ring.

## Running

Defaults to an OpenAI-compatible provider at `http://localhost:4096/v1`;
all five agents share one source.

```bash
cd examples/ring-demo

export OPENAI_API_KEY=sk-...
export OPENAI_MODEL=...
bun run start
```

| Variable        | Default                                                                                     |
| --------------- | ------------------------------------------------------------------------------------------- |
| `RING_PROVIDER` | `openai-compatible`                                                                         |
| `RING_BASE_URL` | `OPENAI_BASE_URL`, else `http://localhost:4096/v1` (Anthropic: `https://api.anthropic.com`) |
| `RING_API_KEY`  | `OPENAI_API_KEY` / `OPENCODE_API_KEY` (Anthropic: `ANTHROPIC_API_KEY`)                      |
| `RING_MODEL`    | `OPENAI_MODEL` (Anthropic: `claude-sonnet-5`)                                               |
| `RING_PROMPT`   | "Should we build our own authentication system or use a third-party provider?"              |

To run against Anthropic:

```bash
export RING_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-ant-...
bun run start
```

Per-agent context stores are written under `tmp/ring/<name>` inside the
package, covered by the repo-wide `tmp/` gitignore.

## What the script does

`src/cli.ts` runs top to bottom:

1. **Resolve one source** from `RING_*` (fatal on a missing key or
   model).
2. **Build the ring.** Five names, each mapped to a role with a title
   and perspective, plus a `user` identity — all registered on one
   in-process transport with their own crypto.
3. **Construct one harness per agent** in a loop: mail and POSIX tools,
   an isogit store under `tmp/ring/<name>`, and a system prompt built by
   `buildRingPrompt` that hard-codes the next hop. Agents past index 0
   run in reactive director mode; alpha runs conversational.
4. **Watch and log.** The user's INBOX is watched for alpha's final
   braintrust recommendation; each harness's stream is drained to a
   labelled logger.
5. **Seed the ring.** The user sends `RING_PROMPT` to alpha. The message
   circulates alpha → bravo → charlie → delta → echo, echo hands back to
   alpha (unblocking its `mail_wait`), and alpha writes the synthesized
   recommendation to the user's INBOX, which triggers shutdown. A
   300-second safety timer bounds the run.

## Next

- [`posix-demo`](../posix-demo/README.md) — the two-agent version of the
  same in-process-mail wiring.
- [`agent-multi-provider`](../agent-multi-provider/README.md) — routing
  policies (model-per-task, failover) on a single agent.
