# agent-resume

Demonstrate that an `@intx/agent` conversation survives process
death. The CLI prints whatever turns the `contextDir` already contains,
sends a new prompt, prints the reply, and exits. Run it twice with the
same `contextDir` and the second run sees the first run's turns.

This is the "resume from crash" story for `@intx/agent`. There
is nothing to opt in to: the agent commits each cycle to its
isogit-backed context store on exit, so reopening the store on the
same directory replays the conversation automatically.

## What it shows

- `agent.history()` returns the persistent `ConversationTurn[]` for
  whatever lives in `contextDir` today, regardless of which process
  wrote it.
- Re-running the same binary on the same `contextDir` produces a
  longer history each time — there is no explicit "session" to open
  or close, and no checkpoint to manage.
- `agent.close()` releases the singleton-per-`contextDir` lock so the
  next process can open the directory immediately.

## Running

```bash
export ANTHROPIC_API_KEY=sk-...
cd examples/agent-resume

# First run: history is empty, the agent gets a fresh conversation.
bun run start "my name is alex"

# Second run: history contains turn 1's user/assistant pair. The
# model sees the prior turns because they're in the persistent
# context store, not because the example passed them in manually.
bun run start "what is my name?"
```

The output of the second run starts with a `(2 prior turns)` summary
followed by the new exchange. To start over, delete the context
directory:

```bash
rm -rf ../../tmp/agent-resume
```

## Walkthrough

1. **Resume is automatic.** `createAgent({ contextDir })` is the only
   thing the example does to opt in. The context store materialises
   inside `contextDir` on first use and replays prior commits on
   subsequent opens.

2. **`history()` is cheap and side-effect free.** It returns the
   already-loaded turn projection from the store. The example calls
   it before `send()` purely to pretty-print prior context for the
   reader; the agent itself does not need to be told what came
   before — the store handles that as part of `createAgent`.

3. **`close()` is what makes resume work.** The agent commits each
   reactor cycle to git. Closing flushes any in-flight commit and
   releases the singleton-per-`contextDir` lock. Crashing without
   close just loses the most recent uncommitted work; the prior
   cycles are already on disk.

4. **No special crash recovery code.** The integration test in
   `tests/agent-resume/cli.test.ts` proves the point by literally
   running `main()` twice against the same directory and observing
   that the second run sees the first run's turns. There is no
   reconciliation step in the example — the durability is inherent
   to the store.

## Resume vs. branching

This example shows linear resume: every new run extends the same
conversation. To root a fresh agent at an older state — branching off
a historical commit while leaving the current conversation untouched —
see [`agent-rewind`](../agent-rewind/README.md).

## What about audit?

Each cycle's checkpoint also includes the audit record (the prompt
sent to the provider, the response received, the transform manifest).
Run `git log --oneline` inside `contextDir` to see one commit per
turn pair, and `git show <hash>` to inspect a specific cycle's audit.
See [`agent-audit-log`](../agent-audit-log/README.md) for a focused
example of that workflow.
