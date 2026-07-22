# posix-demo

Two agents holding a conversation over an in-process mailbox. Alpha
relays a question from the user to Beta and reports Beta's answer back.
It wires the POSIX and mail tool packages onto `@intx/agent` through the
`@intx/harness` composition layer.

Unlike the `agent-*` examples, this is a standalone script, not an
importable module: `src/cli.ts` runs its setup at module scope and has
no `main()` to call. Run it; don't import it.

## What it shows

- Two agents — **Alpha** (a relay) and **Beta** (a responder) — plus a
  **user** identity, all registered on one in-process
  `@intx/mail-memory` transport, each with its own Ed25519 key.
- Each agent built with `createHarness` over an `@intx/agent`
  definition, wired with mail tools (`@intx/tools-mail`) and POSIX tools
  (`@intx/tools-posix`), an isogit context store, and a permissive
  authorize.
- Cross-agent messaging: Alpha's system prompt tells it to `mail_send`
  the user's question to Beta and relay Beta's reply back to the user.
- Per-agent provider config: Alpha and Beta read independent `ALPHA_*`
  and `BETA_*` env vars, so they can run on different providers.
- Draining each harness's event `stream()` to a per-agent console
  logger, and a clean shutdown once Alpha's reply lands in the user's
  INBOX.

## Running

Both agents default to an OpenAI-compatible provider at
`http://localhost:4096/v1`. Point them at whatever you have:

```bash
cd examples/posix-demo

export OPENAI_API_KEY=sk-...
export OPENAI_MODEL=...
bun run start
```

Each agent is configured independently through its own prefix; unset
values fall back to the shared `OPENAI_*` / `OPENCODE_API_KEY` /
`ANTHROPIC_API_KEY` variables:

| Variable                           | Default                                                                                     |
| ---------------------------------- | ------------------------------------------------------------------------------------------- |
| `ALPHA_PROVIDER` / `BETA_PROVIDER` | `openai-compatible`                                                                         |
| `ALPHA_BASE_URL` / `BETA_BASE_URL` | `OPENAI_BASE_URL`, else `http://localhost:4096/v1` (Anthropic: `https://api.anthropic.com`) |
| `ALPHA_API_KEY` / `BETA_API_KEY`   | `OPENAI_API_KEY` / `OPENCODE_API_KEY` (Anthropic: `ANTHROPIC_API_KEY`)                      |
| `ALPHA_MODEL` / `BETA_MODEL`       | `OPENAI_MODEL` (Anthropic: `claude-sonnet-5`)                                               |
| `DEMO_SEED`                        | the built-in "ask Beta what it wants to be when it grows up" prompt                         |

To run the two agents on different providers — say Beta on Anthropic:

```bash
export OPENAI_API_KEY=sk-...           # alpha
export OPENAI_MODEL=...
export BETA_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-ant-...
bun run start
```

Agent context stores are written under `tmp/agent-alpha` and
`tmp/agent-beta` inside the package, covered by the repo-wide `tmp/`
gitignore. Delete them for a clean run.

## What the script does

`src/cli.ts` runs top to bottom:

1. **Resolve two sources.** `readAgentSource("ALPHA")` and
   `readAgentSource("BETA")` read the prefixed env vars; a missing API
   key or model is fatal.
2. **Build identities and transport.** One `createInMemoryTransport`
   hosts three addresses — `alpha@`, `beta@`, and `user@local.interchange`
   — each registered with its own Ed25519 crypto.
3. **Construct two harnesses.** Each agent gets mail and POSIX tool
   runners, an isogit store under `tmp/`, and a `MailEnv`. Alpha's
   prompt makes it a relay; Beta's makes it a thoughtful responder.
4. **Watch and log.** The user's INBOX is watched for Alpha's final
   reply; each harness's event stream is drained to a labelled logger.
5. **Seed the conversation.** The user sends `DEMO_SEED` to Alpha. Alpha
   messages Beta, Beta answers, Alpha relays it to the user, and the
   INBOX watcher triggers shutdown. A 120-second safety timer bounds the
   run.

## Next

- [`agent-quickstart`](../agent-quickstart/README.md) — the single-agent
  minimum, with no tools or mail.
- [`ring-demo`](../ring-demo/README.md) — the same in-process-mail
  pattern scaled to a five-agent ring.
- [`coding-agent`](../coding-agent/README.md) — a single agent wired to
  the POSIX and LSP tool packages for real file work.
