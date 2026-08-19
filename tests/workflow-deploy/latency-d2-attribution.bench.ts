// D2 PER-LEG SUBSTRATE ATTRIBUTION benchmark (NOT a CI test).
//
// Splits the unified path's per-message substrate tax across the five
// substrate commit legs, as a function of message index, so each leg's
// per-message OLS slope (ms added per sustained message) and floor
// (intercept ms) can be read independently -- the same analysis 4.7/D1
// did for the whole round-trip, now split by commit. This is the D2
// re-attribution the rev-3 durability design (§§8-13) calls for: D1's
// conversation-WAL change removed only ~10% of the slope, so the residual
// ~90% must be attributed across the OTHER legs before any fix is chosen.
//
// The five legs (design §10a), each a single git
// `writeTreePreservingPrefix` commit against the growing workflow-run
// repo, all emitted through the supervisor's off-by-default
// `onDispatchTiming` seam to `SIDECAR_LATENCY_BENCH_FILE`:
//
//   enqueue       inbox claim-check WRITE in onMailMessage, BEFORE
//                 dispatch (paid OUTSIDE the 4.7 measured window).
//   dequeue       claim-check READ (dequeueToProcessing), inside window.
//   runevent      run-event bracket commit(s) (runs/<runId>/events/),
//                 inside window. A message may produce SEVERAL; this bench
//                 SUMS them per message and reports the COUNT.
//   markconsumed  consumed/ dedup WRITE, AFTER reply-produced (paid
//                 OUTSIDE the window).
//   wal           D1 conversation WAL append (agent-state/...), the
//                 control leg; should be small + flat post-D1.
//
// The TRUE per-message substrate tax is the SUM of all five, including
// the two out-of-window legs (enqueue, markconsumed) -- higher than 4.7's
// in-window 54.5 ms/msg slope.
//
// Plus per-commit STRUCTURAL COUNTERS (design §10b), sampled at each
// leg's commit time: runs/ fan-out, addresses/<addr>/consumed/ fan-out,
// loose git-object count, and .git byte size -- so we know WHY a leg
// grows (tree-rewrite vs pack growth), not merely that it does.
//
// Plus the §10c discriminating A/B: set SIDECAR_REPACK_EVERY_MESSAGES to
// force a `git gc`/repack every M messages. If the slope FLATTENS with
// forced repack, the cost is pack/loose-object growth (cheap pack/gc
// fix); if it does NOT, the cost is the per-commit root-tree rewrite
// scaling with runs/+consumed/ fan-out (run-model change).
//
// Run:
//   bun run tests/workflow-deploy/latency-d2-attribution.bench.ts \
//     [--messages N] [--out <dir>] [--repack-every M]
//
// Writes <out>/d2-leg-timing.log (raw), <out>/d2-per-message.csv (the
// per-message per-leg matrix), and <out>/d2-results.json (the per-leg
// OLS slopes/floors + counters). Only the UNIFIED path is instrumented
// per-leg; the in-process baseline is already known flat (4.7/D1).

import fs from "node:fs";
import path from "node:path";
import os from "node:os";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  startDeployFlowEnv,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_latency-d2-bench";
const STEP_ID = "step1";
const BODY = "Latency-d2 per-leg attribution probe body 0xD2D2D2.";

// The definition's own tenant, the caller principal that creates the
// deployment's definition asset, and that asset id. The install/approve freeze
// and the anchor `workflow_run` insert write against these, so they must exist
// in the real DB before the unified deploy runs.
const TENANT_ID = "tnt_latency_d2_bench";
const CALLER_PRINCIPAL_ID = "prn_latency_d2_bench";
const DEFINITION_ASSET_ID = "ast_latency_d2_wf";

const LEGS = ["enqueue", "dequeue", "runevent", "markconsumed", "wal"] as const;
type Leg = (typeof LEGS)[number];

type BenchOpts = {
  messages: number;
  outDir: string;
  repackEvery: number | null;
};

function parseArgs(argv: string[]): BenchOpts {
  let messages = 200;
  let outDir = path.resolve(
    import.meta.dir,
    "../../dispatch/workflow-launch-and-converge/p4-7-latency/d2",
  );
  let repackEvery: number | null = null;
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--messages") {
      const next = argv[i + 1];
      if (next === undefined) throw new Error("--messages requires a value");
      const parsed = Number.parseInt(next, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`--messages must be a positive integer, got ${next}`);
      }
      messages = parsed;
      i += 1;
    } else if (arg === "--out") {
      const next = argv[i + 1];
      if (next === undefined) throw new Error("--out requires a value");
      outDir = path.resolve(next);
      i += 1;
    } else if (arg === "--repack-every") {
      const next = argv[i + 1];
      if (next === undefined)
        throw new Error("--repack-every requires a value");
      const parsed = Number.parseInt(next, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(
          `--repack-every must be a positive integer, got ${next}`,
        );
      }
      repackEvery = parsed;
      i += 1;
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  return { messages, outDir, repackEvery };
}

// --- OLS ------------------------------------------------------------------

type Trend = { slope: number; intercept: number; first: number; last: number };

/** OLS fit of `ys` against `xs` (parallel arrays). */
function ols(xs: number[], ys: number[]): Trend {
  const n = xs.length;
  if (n === 0) return { slope: 0, intercept: 0, first: 0, last: 0 };
  let sumX = 0;
  let sumY = 0;
  let sumXY = 0;
  let sumXX = 0;
  for (let i = 0; i < n; i += 1) {
    const x = xs[i];
    const y = ys[i];
    if (x === undefined || y === undefined) continue;
    sumX += x;
    sumY += y;
    sumXY += x * y;
    sumXX += x * x;
  }
  const denom = n * sumXX - sumX * sumX;
  const slope = denom === 0 ? 0 : (n * sumXY - sumX * sumY) / denom;
  const intercept = (sumY - slope * sumX) / n;
  const first = ys[0];
  const last = ys[n - 1];
  return {
    slope,
    intercept,
    first: first ?? 0,
    last: last ?? 0,
  };
}

// --- leg-timing-file parsing ----------------------------------------------

type Counters = {
  runsFanOut: number;
  consumedFanOut: number;
  looseObjects: number;
  gitBytes: number;
};

type LegSample = {
  start: number | null;
  end: number | null;
  endCounters: Counters | null;
  /** count of commits for this (runId, leg) -- runevent may be > 1. */
  count: number;
};

type PerRun = Map<Leg, LegSample>;

/**
 * Parse the supervisor's mixed timing channel. Round-trip lines
 * (`<runId> <marker> <atMs>`) are ignored here; leg lines
 * (`<runId> leg <leg> <phase> <atMs> [runsFanOut consumedFanOut looseObjects gitBytes]`)
 * are accumulated per (runId, leg). Multiple commits for the same
 * (runId, leg) -- the run-event bracket fires several -- are summed into
 * one duration and counted; the LAST commit's counters are retained
 * (the freshest fan-out snapshot for that message).
 */
function parseLegFile(file: string): Map<string, PerRun> {
  const text = fs.readFileSync(file, "utf8");
  const byRun = new Map<string, PerRun>();
  // Pending start timestamps per (runId, leg) so each start pairs with
  // its next end. The run-event bracket interleaves start/end pairs, so a
  // stack per key is the robust pairing.
  const pendingStarts = new Map<string, number[]>();

  function ensureRun(runId: string): PerRun {
    let run = byRun.get(runId);
    if (run === undefined) {
      run = new Map<Leg, LegSample>();
      byRun.set(runId, run);
    }
    return run;
  }

  function ensureLeg(run: PerRun, leg: Leg): LegSample {
    let sample = run.get(leg);
    if (sample === undefined) {
      sample = { start: null, end: null, endCounters: null, count: 0 };
      run.set(leg, sample);
    }
    return sample;
  }

  for (const rawLine of text.split("\n")) {
    const line = rawLine.trim();
    if (line === "") continue;
    const parts = line.split(" ");
    if (parts[1] !== "leg") continue;
    const runId = parts[0];
    const legRaw = parts[2];
    const phase = parts[3];
    const atRaw = parts[4];
    if (
      runId === undefined ||
      legRaw === undefined ||
      phase === undefined ||
      atRaw === undefined
    ) {
      throw new Error(`malformed leg line: ${JSON.stringify(rawLine)}`);
    }
    const leg = LEGS.find((l) => l === legRaw);
    if (leg === undefined) {
      throw new Error(`unknown leg in line: ${JSON.stringify(rawLine)}`);
    }
    const at = Number.parseFloat(atRaw);
    if (!Number.isFinite(at)) {
      throw new Error(
        `non-numeric atMs in leg line: ${JSON.stringify(rawLine)}`,
      );
    }
    const key = `${runId} ${leg}`;
    if (phase === "start") {
      const stack = pendingStarts.get(key) ?? [];
      stack.push(at);
      pendingStarts.set(key, stack);
      continue;
    }
    if (phase !== "end") {
      throw new Error(`unknown phase in leg line: ${JSON.stringify(rawLine)}`);
    }
    const stack = pendingStarts.get(key);
    const start = stack !== undefined ? stack.pop() : undefined;
    if (start === undefined) continue; // end without a matching start; skip.
    const run = ensureRun(runId);
    const sample = ensureLeg(run, leg);
    const dur = at - start;
    // Sum durations across the (possibly several) commits for this leg.
    sample.end = (sample.end ?? 0) + dur;
    sample.start = 0; // marker that the leg produced at least one pair
    sample.count += 1;
    // Retain the freshest counters present on this end line (6 trailing
    // fields => counters were attached).
    if (parts.length >= 9) {
      const runsFanOut = Number.parseInt(parts[5] ?? "", 10);
      const consumedFanOut = Number.parseInt(parts[6] ?? "", 10);
      const looseObjects = Number.parseInt(parts[7] ?? "", 10);
      const gitBytes = Number.parseInt(parts[8] ?? "", 10);
      if (
        Number.isFinite(runsFanOut) &&
        Number.isFinite(consumedFanOut) &&
        Number.isFinite(looseObjects) &&
        Number.isFinite(gitBytes)
      ) {
        sample.endCounters = {
          runsFanOut,
          consumedFanOut,
          looseObjects,
          gitBytes,
        };
      }
    }
  }
  return byRun;
}

// --- unified driver -------------------------------------------------------

async function waitForFirstRoutable(
  env: DeployFlowEnv,
  address: string,
): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (env.hub.router.getRoutableAddresses().includes(address)) return;
    if (Date.now() - start > 30_000) {
      throw new Error(
        `d2 bench: deployment address ${address} never became routable\n${env.sidecarDiagnostics()}`,
      );
    }
    await new Promise((r) => setTimeout(r, 25));
  }
}

/**
 * Wait until a leg line for `runId` with the `markconsumed end` phase is
 * present -- the last leg of the message's dispatch, so its arrival means
 * every leg for this message has been written to the file. Paces the
 * driver one message at a time (no pipelining).
 */
async function waitForMessageComplete(
  file: string,
  runId: string,
  timeoutMs: number,
  diagnostics: () => string,
): Promise<void> {
  const start = Date.now();
  const needle = `${runId} leg markconsumed end `;
  for (;;) {
    if (fs.existsSync(file)) {
      const text = fs.readFileSync(file, "utf8");
      if (text.includes(needle)) return;
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error(
        `d2 bench: timed out after ${String(timeoutMs)}ms waiting for markconsumed end for ${runId}\n${diagnostics()}`,
      );
    }
    await new Promise((r) => setTimeout(r, 20));
  }
}

async function runUnifiedD2(opts: {
  messages: number;
  timingFile: string;
  repackEvery: number | null;
  db: TestDb["db"];
}): Promise<string[]> {
  const sidecarEnv: Record<string, string> = {
    SIDECAR_LATENCY_BENCH_FILE: opts.timingFile,
  };
  if (opts.repackEvery !== null) {
    sidecarEnv["SIDECAR_REPACK_EVERY_MESSAGES"] = String(opts.repackEvery);
  }
  const env: DeployFlowEnv = await startDeployFlowEnv({
    inferenceEchoUserMessage: true,
    sidecarEnv,
  });

  try {
    const deploymentMailAddress = deriveRunAddress({
      runId: DEPLOYMENT_ID,
      domain: DEPLOYMENT_DOMAIN,
    });

    const inferenceSource: InferenceSource = {
      id: "anthropic:mock-model",
      provider: "anthropic",
      baseURL: `http://localhost:${String(env.inference.server.port)}`,
      apiKey: "sk-mock",
      model: "mock-model",
    };

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the definition)",
      tools: [],
      grants: [],
      sources: [inferenceSource],
      defaultSource: "anthropic:mock-model",
    };

    const entryModule = singleStepAgentEntry({
      stepId: STEP_ID,
      systemPrompt: "You are the latency-d2 attribution agent.",
      address: deploymentMailAddress,
      agentId: `agent-${DEPLOYMENT_ID}-${STEP_ID}`,
      workflowId: `wf_${DEPLOYMENT_ID}`,
    });

    // Deploy the single warm agent BY SOURCE-REF, the same code-sourced front
    // production drives. The helper registers the deployment on the env, so the
    // fire loop resolves it by `anchorRunId` with no manual registration.
    const handle = await deployWorkflowSourceForTest(env, {
      entryModule,
      db: opts.db,
      tenantId: TENANT_ID,
      definitionAssetId: DEFINITION_ASSET_ID,
      anchorRunId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      agentAddress: deploymentMailAddress,
      approvals: "approve-probed",
      config,
      sources: { [STEP_ID]: [inferenceSource] },
    });
    if (!handle.publicKey) {
      throw new Error("d2 bench: deploy did not return a public key");
    }

    await waitForFirstRoutable(env, deploymentMailAddress);

    const orderedRunIds: string[] = [];
    for (let i = 0; i < opts.messages + 1; i += 1) {
      const messageId = `<latency-d2-${String(i)}@integration.interchange>`;
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId,
        content: BODY,
      });
      await waitForMessageComplete(opts.timingFile, messageId, 120_000, () =>
        env.sidecarDiagnostics(),
      );
      orderedRunIds.push(messageId);
    }
    return orderedRunIds;
  } finally {
    await env.teardown();
  }
}

// --- analysis + output ----------------------------------------------------

type LegStats = {
  slopeMsPerMessage: number;
  floorMs: number;
  firstSampleMs: number;
  lastSampleMs: number;
  meanMs: number;
  totalCommits: number;
  meanCommitsPerMessage: number;
};

function summarizeLeg(
  perMessage: (number | null)[],
  counts: number[],
): LegStats {
  const xs: number[] = [];
  const ys: number[] = [];
  let sum = 0;
  let nonNull = 0;
  for (let i = 0; i < perMessage.length; i += 1) {
    const y = perMessage[i];
    if (y === null || y === undefined) continue;
    xs.push(i);
    ys.push(y);
    sum += y;
    nonNull += 1;
  }
  const trend = ols(xs, ys);
  const totalCommits = counts.reduce((a, b) => a + b, 0);
  return {
    slopeMsPerMessage: trend.slope,
    floorMs: trend.intercept,
    firstSampleMs: trend.first,
    lastSampleMs: trend.last,
    meanMs: nonNull === 0 ? 0 : sum / nonNull,
    totalCommits,
    meanCommitsPerMessage:
      perMessage.length === 0 ? 0 : totalCommits / perMessage.length,
  };
}

async function main(): Promise<void> {
  if (!harnessDbEnvAvailable()) {
    throw new Error(
      "latency-d2 bench: no database env (.env + .env.migrate); the " +
        "code-sourced unified deploy requires a real Postgres schema",
    );
  }

  const opts = parseArgs(process.argv.slice(2));
  fs.mkdirSync(opts.outDir, { recursive: true });

  const loadBefore = os.loadavg();
  const timingFile = path.join(
    opts.outDir,
    opts.repackEvery !== null
      ? "d2-leg-timing-repack.log"
      : "d2-leg-timing.log",
  );
  if (fs.existsSync(timingFile)) fs.rmSync(timingFile);

  // The unified deploy is BY SOURCE-REF, so it needs a real migrated schema:
  // the install/approve freeze and the anchor `workflow_run` insert write
  // through it. Seed the definition's tenant, the caller principal, and the
  // `workflow`-kind definition asset before the deploy runs.
  const h = await createTestDb();
  await h.db.insert(tenantTable).values({
    id: TENANT_ID,
    name: TENANT_ID,
    slug: TENANT_ID,
    domain: DEPLOYMENT_DOMAIN,
    parentId: null,
  });
  await seedPrincipal(h.db, {
    id: CALLER_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
  });
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "latency-d2-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  let orderedRunIds: string[];
  try {
    orderedRunIds = await runUnifiedD2({
      messages: opts.messages,
      timingFile,
      repackEvery: opts.repackEvery,
      db: h.db,
    });
  } finally {
    await h.close();
  }
  const loadAfter = os.loadavg();

  const byRun = parseLegFile(timingFile);

  // Build the per-message matrix (steady-state: discard the cold first
  // message). Index 0 here is message 2 of the run.
  const steady = orderedRunIds.slice(1);
  const perMessageByLeg = new Map<Leg, (number | null)[]>();
  const countsByLeg = new Map<Leg, number[]>();
  const countersByMessage: (Counters | null)[] = [];
  for (const leg of LEGS) {
    perMessageByLeg.set(leg, []);
    countsByLeg.set(leg, []);
  }
  for (const runId of steady) {
    const run = byRun.get(runId);
    let bestCounters: Counters | null = null;
    for (const leg of LEGS) {
      const sample = run?.get(leg);
      const dur = sample?.end ?? null;
      perMessageByLeg.get(leg)?.push(dur);
      countsByLeg.get(leg)?.push(sample?.count ?? 0);
      if (sample?.endCounters != null) {
        // Prefer the markconsumed leg's counters as the end-of-message
        // snapshot (largest fan-out for the message); else take whatever
        // is present.
        if (leg === "markconsumed" || bestCounters === null) {
          bestCounters = sample.endCounters;
        }
      }
    }
    countersByMessage.push(bestCounters);
  }

  const legStats: Record<string, LegStats> = {};
  for (const leg of LEGS) {
    const pm = perMessageByLeg.get(leg) ?? [];
    const counts = countsByLeg.get(leg) ?? [];
    legStats[leg] = summarizeLeg(pm, counts);
  }

  // Per-message total substrate tax (sum across all five legs), and its
  // own OLS, so the true tax slope/floor (including the two out-of-window
  // legs) is reported as a single number.
  const totalPerMessage: number[] = [];
  for (let i = 0; i < steady.length; i += 1) {
    let total = 0;
    for (const leg of LEGS) {
      const v = perMessageByLeg.get(leg)?.[i];
      if (v !== null && v !== undefined) total += v;
    }
    totalPerMessage.push(total);
  }
  const totalTrend = ols(
    totalPerMessage.map((_v, i) => i),
    totalPerMessage,
  );

  // Fan-out growth: first vs last message's counters.
  const firstCounters = countersByMessage.find((c) => c !== null) ?? null;
  let lastCounters: Counters | null = null;
  for (let i = countersByMessage.length - 1; i >= 0; i -= 1) {
    const c = countersByMessage[i];
    if (c != null) {
      lastCounters = c;
      break;
    }
  }

  // Per-message CSV (one row per steady-state message; columns per leg +
  // counters), so the raw matrix is auditable.
  const header = [
    "message_index",
    ...LEGS.map((l) => `${l}_ms`),
    ...LEGS.map((l) => `${l}_commits`),
    "total_ms",
    "runs_fanout",
    "consumed_fanout",
    "loose_objects",
    "git_bytes",
  ].join(",");
  const rows: string[] = [header];
  for (let i = 0; i < steady.length; i += 1) {
    const c = countersByMessage[i];
    const cells: string[] = [String(i)];
    for (const leg of LEGS) {
      const v = perMessageByLeg.get(leg)?.[i];
      cells.push(v === null || v === undefined ? "" : v.toFixed(6));
    }
    for (const leg of LEGS) {
      cells.push(String(countsByLeg.get(leg)?.[i] ?? 0));
    }
    cells.push((totalPerMessage[i] ?? 0).toFixed(6));
    cells.push(c != null ? String(c.runsFanOut) : "");
    cells.push(c != null ? String(c.consumedFanOut) : "");
    cells.push(c != null ? String(c.looseObjects) : "");
    cells.push(c != null ? String(c.gitBytes) : "");
    rows.push(cells.join(","));
  }
  const csvName =
    opts.repackEvery !== null
      ? "d2-per-message-repack.csv"
      : "d2-per-message.csv";
  fs.writeFileSync(path.join(opts.outDir, csvName), rows.join("\n") + "\n");

  const results = {
    generatedAt: new Date().toISOString(),
    variant: opts.repackEvery !== null ? "with-repack" : "without-repack",
    repackEveryMessages: opts.repackEvery,
    messagesPerPath: opts.messages,
    steadyStateSamples: steady.length,
    machine: {
      platform: `${os.type()} ${os.release()} ${os.arch()}`,
      cpus: os.cpus().length,
      loadavgBefore: loadBefore,
      loadavgAfter: loadAfter,
    },
    units: "milliseconds",
    perLeg: legStats,
    total: {
      slopeMsPerMessage: totalTrend.slope,
      floorMs: totalTrend.intercept,
      firstSampleMs: totalTrend.first,
      lastSampleMs: totalTrend.last,
    },
    fanOut: {
      first: firstCounters,
      last: lastCounters,
    },
  };
  const jsonName =
    opts.repackEvery !== null ? "d2-results-repack.json" : "d2-results.json";
  fs.writeFileSync(
    path.join(opts.outDir, jsonName),
    JSON.stringify(results, null, 2) + "\n",
  );

  // Console summary.
  process.stdout.write(
    `\nD2 per-leg attribution (${results.variant}); messages=${String(opts.messages)} steady=${String(steady.length)}\n`,
  );
  process.stdout.write(
    `loadavg before=${loadBefore.map((v) => v.toFixed(2)).join(",")} after=${loadAfter.map((v) => v.toFixed(2)).join(",")}\n\n`,
  );
  process.stdout.write(
    `${"leg".padEnd(14)}${"slope(ms/msg)".padStart(16)}${"floor(ms)".padStart(12)}${"mean(ms)".padStart(12)}${"commits/msg".padStart(14)}\n`,
  );
  for (const leg of LEGS) {
    const s = legStats[leg];
    if (s === undefined) continue;
    process.stdout.write(
      `${leg.padEnd(14)}${s.slopeMsPerMessage.toFixed(4).padStart(16)}${s.floorMs.toFixed(2).padStart(12)}${s.meanMs.toFixed(2).padStart(12)}${s.meanCommitsPerMessage.toFixed(2).padStart(14)}\n`,
    );
  }
  process.stdout.write(
    `${"TOTAL".padEnd(14)}${totalTrend.slope.toFixed(4).padStart(16)}${totalTrend.intercept.toFixed(2).padStart(12)}\n\n`,
  );
  process.stdout.write(
    `fan-out: runs ${String(firstCounters?.runsFanOut ?? "?")} -> ${String(lastCounters?.runsFanOut ?? "?")}; consumed ${String(firstCounters?.consumedFanOut ?? "?")} -> ${String(lastCounters?.consumedFanOut ?? "?")}; looseObjects ${String(firstCounters?.looseObjects ?? "?")} -> ${String(lastCounters?.looseObjects ?? "?")}; gitBytes ${String(firstCounters?.gitBytes ?? "?")} -> ${String(lastCounters?.gitBytes ?? "?")}\n`,
  );
  process.stdout.write(`\nwrote ${jsonName}, ${csvName} to ${opts.outDir}\n`);
}

if (import.meta.main) {
  await main();
  // Exit explicitly instead of letting the event loop drain. A source-ref
  // deploy makes the sidecar spawn a child that outlives the sidecar kill in
  // `env.teardown()`; the child keeps the sidecar's inherited stdout/stderr
  // pipe open, so the fixture's pipe-reader loops never see EOF and the loop
  // never drains. `main()` has already torn down the hub, sidecar, and
  // database, so this exit only bypasses that orphaned pipe handle. A failure
  // in `main()` rejects this top-level await, which exits non-zero with the
  // stack, so the error still surfaces.
  process.exit(0);
}
