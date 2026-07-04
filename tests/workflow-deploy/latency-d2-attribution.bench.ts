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

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  type ApprovalSet,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  fireMailTrigger,
  startDeployFlowEnv,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "latency-d2-bench";
const STEP_ID = "step1";
const BODY = "Latency-d2 per-leg attribution probe body 0xD2D2D2.";

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
    const deploymentMailAddress = deriveDeploymentAddress({
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    const agent = defineAgent({
      id: "latency-d2-agent",
      systemPrompt: "You are the latency-d2 attribution agent.",
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });

    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: { [STEP_ID]: step({ agent }) },
    });

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the orchestrator)",
      tools: [],
      grants: [],
      sources: [
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${String(env.inference.server.port)}`,
          apiKey: "sk-mock",
          model: "mock-model",
        },
      ],
      defaultSource: "anthropic:mock-model",
    };

    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    const launchSession: LaunchSessionFn = async (orchestratorParams) => {
      await env.hub.sessionService.stageWorkflowStep({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        instanceId: orchestratorParams.instanceId,
        config: orchestratorParams.config,
        deployContent: toLaunchDeployContent(orchestratorParams.deployContent),
        ...(orchestratorParams.toolPackagePins !== undefined
          ? { toolPackagePins: orchestratorParams.toolPackagePins }
          : {}),
      });
    };

    const sendMultiStepDeploy: SendMultiStepDeployFn = async (params) =>
      env.hub.router.sendAgentDeploy(params.agentAddress, params.config, {
        definition: {
          id: params.definition.id,
          triggers: [...params.definition.triggers],
          stepOrder: [...params.definition.stepOrder],
          steps: params.definition.steps as Record<string, unknown>,
          ...(params.definition.state !== undefined
            ? { state: params.definition.state }
            : {}),
        },
        sources: params.sources,
      });

    const workflowRepo: WorkflowRepoWriter = {
      async writeWorkflowRepo(args) {
        const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
        const principal: WorkflowRunHubPrincipal = { kind: "hub" };
        const files: Record<string, string> = {};
        for (const [k, v] of args.files) {
          files[k] = v;
        }
        await env.hub.agentRepoStore.repoStore.writeTree(
          principal,
          repoId,
          DEFAULT_ASSET_REF,
          { files, message: `latency-d2 bench: write workflow repo` },
        );
      },
    };

    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry: createDefaultDirectorRegistry(),
      workflowRepo,
      launchSession,
      sendMultiStepDeploy,
    });

    const result = await orchestrator.deployWorkflow({
      workflow,
      config,
      deployContent: { systemPrompt: config.systemPrompt },
      operatorApprovals,
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    if (!result.publicKey) {
      throw new Error("d2 bench: deploy did not return a public key");
    }

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveDeploymentId(deploymentMailAddress),
    };
    env.registerDeployment({
      deploymentId: DEPLOYMENT_ID,
      workflowDefinition: workflow,
      workflowRunRepoId,
      workflowRunRef: "refs/heads/main",
      mailAddress: deploymentMailAddress,
    });

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

  const orderedRunIds = await runUnifiedD2({
    messages: opts.messages,
    timingFile,
    repackEvery: opts.repackEvery,
  });
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

await main();
