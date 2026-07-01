# Workflow-run latency benches

Two off-by-default measurement benches profile the sustained per-message
latency of the workflow-run substrate. They are `*.bench.ts`, not
`*.test.ts`, so `make test` never runs them; `make build` type-checks
them via this directory's tsconfig. Both drive one warm agent
back-to-back with inference mocked -- message N+1 fires only after reply
N (no pipelining) -- and discard the first (cold) message so agent
build, tool materialization, and LSP spawn cost are excluded. The
steady-state per-message round-trip is what they measure.

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
  and `results.json`. Read the `unified`/`baseline` percentiles and
  `trend.unified.slopeMsPerMessage` (ms added per sustained message).
- **`latency-d2-attribution.bench.ts`** -- splits the unified path's
  per-message substrate cost across the individual git-commit legs
  (dequeue, run-event, agent-state WAL, ...); writes `d2-results.json`.
  Read `total.slopeMsPerMessage` and `perLeg[<leg>].slopeMsPerMessage`.

A healthy substrate holds the slopes flat (near zero) as `--messages`
grows; a rising slope is the per-message growth these benches exist to
catch.
