# agent-audit-log

Show that an `@interchange/agent` audit record is not a separate
artifact you have to query through a special API — it is a real git
repository at `contextDir`, with one commit per reactor cycle, and
the audit data is stored in plain JSON-lines files in each commit's
tree.

This example accepts one or more prompts, drives the agent through a
cycle per prompt, and then walks the resulting commits with
`isomorphic-git` to print a summary of each. The same data is
reachable from the command line with `git log` and `git show` — the
example just uses `isomorphic-git` so the demonstration is
self-contained.

## What it shows

- The `contextDir` is initialised as a real git repository on first
  use. Every `agent.send()` cycle produces one commit.
- Each commit's tree contains a fixed set of files:
  - `turns.jsonl` — the durable conversation history (one
    `ConversationTurn` per line).
  - `prompt.jsonl` — the materialised prompt fed to the model for
    this cycle. Captures the state of the conversation _as seen by
    the model_ after transforms.
  - `response.jsonl` — the single assistant turn the model produced
    for this cycle.
  - `manifest.jsonl` — one `TransformRecord` per transform invocation
    (`size-cap`, `context-transform`, etc.) so the cycle's transform
    pipeline is fully reconstructible.
  - `tool-output/` — blob spill directory (only populated when the
    size-cap transform fires; see
    [`agent-blob-spill`](../agent-blob-spill/README.md)).
- Pointing plain `git` (or `isomorphic-git`, or anything else that
  understands the git format) at `contextDir` is the audit interface.
  No bespoke audit reader is required.

## Running

```bash
export ANTHROPIC_API_KEY=sk-...
cd examples/agent-audit-log
bun run start "what is the boiling point of water" "and at altitude?"
```

Output looks like:

```
> what is the boiling point of water
assistant: 100°C at standard atmospheric pressure.

> and at altitude?
assistant: Roughly 3°C lower per 1000 m of elevation.

audit log (3 commit(s), newest first):
  3f4a2b1c  2026-05-18T05:55:01.234Z  reactor cycle
    files: manifest.jsonl, prompt.jsonl, response.jsonl, turns.jsonl
    transforms: size-cap
  82a3df11  2026-05-18T05:54:58.100Z  reactor cycle
    files: manifest.jsonl, prompt.jsonl, response.jsonl, turns.jsonl
    transforms: size-cap
  19c0b7e0  2026-05-18T05:54:55.000Z  initial
    files:

To inspect further, run plain git inside <repo>/tmp/agent-audit-log/context:
  git log --oneline
  git show <hash> -- turns.jsonl
  git show <hash> -- manifest.jsonl
  git show <hash> -- response.jsonl
```

To start over:

```bash
rm -rf ../../tmp/agent-audit-log
```

## Walkthrough

`main()` does two passes:

1. **Drive the agent.** One `agent.send()` per CLI argument. Each
   `send()` triggers a reactor cycle that ends with `commit()` on the
   isogit store; the commit's tree contains the four JSONL files plus
   any `tool-output/` blobs from this cycle.

2. **Walk the resulting commits.** After `agent.close()` the
   `contextDir` is fully quiesced. `summarizeAuditLog(contextDir)` in
   [`src/inspect.ts`](./src/inspect.ts) uses `isomorphic-git`'s
   `git.log` and `git.readTree` to enumerate commits and pull the
   file listing per commit, then reads `manifest.jsonl` to extract
   the strategy names recorded for that cycle.

You can do exactly the same walk with system git:

```bash
cd <repo>/tmp/agent-audit-log/context
git log --oneline
git show HEAD -- manifest.jsonl
git show HEAD -- response.jsonl
git diff <oldhash> <newhash> -- turns.jsonl
```

## Why expose the audit as a git repo?

- **Existing tooling works.** Any git GUI, bisect, or diff viewer can
  read the audit. There is no proprietary on-disk format and no
  "audit reader" library to keep in sync with the writer.
- **Branchable.** Combine with
  [`agent-rewind`](../agent-rewind/README.md) to root a fresh agent
  at an older commit; the audit history for that branch is the same
  data structure, just with a different `HEAD`.
- **Distributable.** A `git clone` of the `contextDir` produces a
  complete audit record — useful for handing a debugging session to
  a teammate, or feeding the audit into a compliance pipeline.

## Inspecting transforms

`manifest.jsonl` is the file to look at when you want to know what
happened to a cycle's data on the way through the reactor. Each line
is a `TransformRecord`:

```json
{
  "strategy": "size-cap",
  "version": "1",
  "parameters": { "maxChars": 10000 },
  "reason": "exceeded-cap",
  "decisions": {
    "callId": "fetch_full_logs_1",
    "originalLength": 25020,
    "kept": 10000,
    "spillKey": "fetch_full_logs_1",
    "spillURI": "tool-output:///fetch_full_logs_1"
  }
}
```

`reason` is "within-cap" or "exceeded-cap" for the size-cap transform;
context transforms and compactors record their own reasons. The
record is self-describing — `strategy` + `version` are stable
identifiers, and `parameters`/`decisions` are open JSON objects.
