# agent-rewind

Walk an `@interchange/agent` conversation backwards: clone the
`contextDir`, roll the clone's `HEAD` back to an older commit, and
open a fresh agent rooted at that earlier state. The original
conversation is left untouched.

This is the "branchable state" story for `@interchange/agent`. The
context store is a real git repository; every checkpoint is a real
commit; "rewind" is just "copy the repo, then move HEAD".

## What it shows

- `agent.checkpoints(limit)` returns recent commits in newest-first
  order. After N sends you have at least N commits to choose from.
- `agent.readAt(hash)` is the read-only view of an older state. It is
  useful for inspection but does not give you an agent you can keep
  talking to.
- To get an agent that _acts as if_ it is at the older state, you copy
  the repo and roll `HEAD` back. `cloneAndRewind` (exported from
  `./src/rewind.ts`) does both steps with `node:fs/promises.cp` plus
  `isomorphic-git`'s `checkout({ ref, force: true })`.
- The new agent's `history()` returns only the turns reachable from
  the rewound `HEAD`. The original agent's history is unaffected.

## Running

```bash
export ANTHROPIC_API_KEY=sk-...
cd examples/agent-rewind
bun run start "name a planet" "now name a moon of that planet"
```

The example sends both prompts against `tmp/agent-rewind/context/`,
then creates a sibling `tmp/agent-rewind/context-rewound/` whose
`HEAD` points at the commit after the first send. The rewound agent's
history will contain a user/assistant pair for "name a planet" and
nothing else.

To start from scratch:

```bash
rm -rf ../../tmp/agent-rewind
```

## Walkthrough

`main()` in [`src/cli.ts`](./src/cli.ts) does five things:

1. **Build some history.** Two `agent.send()` calls produce two
   checkpoints — one after each cycle — in the primary `contextDir`.

2. **Pick a rewind target.** `agent.checkpoints(10)` returns the
   commits newest-first. Index 0 is the latest (post-second-send),
   index 1 is the commit at the end of the first send. The example
   uses index 1 as the rewind target.

3. **Close the original agent.** The singleton-per-`contextDir` lock
   has to be released before the copy step, and any in-flight commit
   in the isogit store needs to flush. `agent.close()` handles both.

4. **Copy the directory and roll HEAD back.** `cloneAndRewind()` uses
   `fs/promises.cp` to recursively duplicate the contextDir into the
   sibling path, then `isomorphic-git`'s `git.checkout({ ref, force:
true })` to move the clone's `HEAD` to the rewind target. The
   working tree (turns.jsonl, manifest.jsonl, response.jsonl) is
   rewritten to match the target commit's tree.

5. **Open a new agent on the rewound directory.** The new agent reads
   `history()` from the store at its current `HEAD`, which is the
   older commit. The reply count and assistant turns show only the
   first cycle.

## Why a copy, not in-place rollback?

You could in principle `git.checkout` on the original `contextDir`,
but that would discard the second send's commit from the active
branch. The whole point of rewind is to **branch off** an older
state without destroying the current one — copying the repo is the
cheapest way to express "I want both timelines to keep existing".

If you have many rewinds and the storage cost of repeated copies
matters, the next iteration would be to use a git worktree (real
git, not isomorphic-git) so the rewound view shares the object
database with the original. That is outside the scope of an example
whose job is to demonstrate the shape of the operation.

## Why `fs.cp` and not `isomorphic-git.clone`?

`isomorphic-git.clone` expects an HTTP-fetched remote and a shimmed
`http` transport object — not the right primitive for cloning a
local directory. Recursive `fs.cp` plus `git.checkout({ force:
true })` produces the same observable result without extra plumbing:

```
git clone <local> <local> && git -C <copy> reset --hard <hash>
```

is what we are emulating; the two-step JavaScript form is the closest
analogue.

## Combine with audit

After running the example, both directories are inspectable with
plain git:

```bash
cd tmp/agent-rewind/context && git log --oneline
cd tmp/agent-rewind/context-rewound && git log --oneline
```

The first shows three commits (the initial commit plus one per send).
The second shows the initial plus the first send only — exactly the
trees the rewound agent reads from. See
[`agent-audit-log`](../agent-audit-log/README.md) for what to look at
in each commit.
