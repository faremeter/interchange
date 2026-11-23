# Workflow-run latency benches

Two off-by-default measurement benches profile the sustained per-turn
latency of the workflow-run substrate. They are `*.bench.ts`, not
`*.test.ts`, so `make test` never runs them; `make build` type-checks
them via this directory's tsconfig. Both drive one interactive mail
agent back-to-back with inference mocked -- message N+1 fires only after
reply N (no pipelining). The agent is an `onTrigger({ on: mail, body })`
deployment whose body is one agent step, so each mail is a new body run.
No agent stays live between turns: every turn builds the agent, restores
the conversation, runs one turn and closes. The body step continues its
conversation at its place, so it grows by one turn per message, and a
late turn pays the restore of a long conversation. The per-turn
round-trip is what they measure. The bench agents have no tools, so the
round-trip does not include the start cost of tools or of an LSP server.

## Running

Each bench takes `--messages N` (default in-file) and `--out <dir>` and
prints its table to stdout. Send output to a throwaway `tmp/` dir
(gitignored):

```
bun run tests/workflow-deploy/latency-gate.bench.ts --messages 200 --out tmp/latency/gate
bun run tests/workflow-deploy/latency-d2-attribution.bench.ts --messages 200 --out tmp/latency/d2
```

A bench flushes its result JSON and prints its table before teardown,
and teardown can hang; once the table has printed (the JSON is already
written), interrupt the process.

## What each measures

- **`latency-gate.bench.ts`** -- the dispatch->reply gate. Runs a
  baseline (in-process agent) and the unified path (subprocess child +
  IPC-proxied commits); writes `raw-baseline.csv`, `raw-unified.csv`,
  and `results.json`. The baseline also builds a fresh agent each turn,
  over one persistent store. Both paths discard the first message, which
  pays one-time process costs. Read the `unified`/`baseline` percentiles,
  `trend.unified.slopeMsPerMessage` (ms added per earlier turn in the
  conversation), and `conversationLength` (the first against the last
  10% of turns).
- **`latency-d2-attribution.bench.ts`** -- splits the unified path's
  per-message substrate cost across the individual git-commit legs
  (dequeue, run-event, step-state WAL, ...); writes `d2-results.json`.
  Read `total.slopeMsPerMessage` and `perLeg[<leg>].slopeMsPerMessage`.

A healthy substrate holds the slopes small as `--messages` grows. Each
turn restores the whole conversation, so some growth is expected; a
steep slope is the per-turn growth these benches exist to catch.
