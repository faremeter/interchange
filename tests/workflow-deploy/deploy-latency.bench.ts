// Deploy-latency benchmark (NOT a CI test).
//
// Measures the wall-clock of one CODE-SOURCED workflow deploy, so a
// regression in the source-ref deploy front can be observed. The measured
// operation is `deployWorkflowSourceForTest(...)`: it bundles a workflow
// entry module to a self-contained `.mjs`, seeds it as a `workflow`-kind
// source asset, installs + probes + approves + freezes it against a real
// database, emits the source-ref deploy frame, and writes the deployment's
// anchor `workflow_run` row. The interval brackets ONLY that call --
// nothing else in the iteration is timed.
//
// The stack is the real deploy stack stood up by `startDeployFlowEnv` (real
// hub WebSocket server, real sidecar subprocess, mock echo inference so
// inference cost is ~0 and does not confound the deploy timing) plus a real
// migrated Postgres schema (`createTestDb`) the install/approve freeze and
// the anchor `workflow_run` insert both write through. Each measured deploy
// uses a fresh anchorRunId (hence a fresh derived mail address and source
// asset) and a fresh definition asset id, so per-deploy state never collides
// across iterations.
//
// The FIRST (cold) iteration is discarded: the first deploy on a fresh env
// pays one-time warm costs (sidecar link warm-up, repo-store directory
// materialization) the steady-state samples must exclude. The remaining
// iterations are the reported samples.
//
// Run:
//   bun run tests/workflow-deploy/deploy-latency.bench.ts \
//     [--iterations N] [--out <dir>]
//
// Writes <out>/results.json and prints a summary to stdout. Not matched by
// `bun test` (it is a `.bench.ts`, not a `.test.ts`), so `make test` never
// runs it; it is type-checked by `make build` via this directory's tsconfig.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

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
  startDeployFlowEnv,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const STEP_ID = "step1";

// The definition's own tenant and the caller principal that creates each
// deploy's definition asset. The install/approve freeze and the anchor
// `workflow_run` insert both write against these, so they must exist in the
// real DB before the first deploy runs.
const TENANT_ID = "tnt_deploy_latency_bench";
const CALLER_PRINCIPAL_ID = "prn_deploy_latency_bench";

type BenchOpts = {
  iterations: number;
  outDir: string;
};

function parseArgs(argv: string[]): BenchOpts {
  let iterations = 6;
  let outDir = path.resolve(
    import.meta.dir,
    "../../dispatch/workflow-launch-and-converge/deploy-latency",
  );
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--iterations") {
      const next = argv[i + 1];
      if (next === undefined) throw new Error("--iterations requires a value");
      const parsed = Number.parseInt(next, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`--iterations must be a positive integer, got ${next}`);
      }
      iterations = parsed;
      i += 1;
    } else if (arg === "--out") {
      const next = argv[i + 1];
      if (next === undefined) throw new Error("--out requires a value");
      outDir = path.resolve(next);
      i += 1;
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  return { iterations, outDir };
}

// --- percentile statistics -------------------------------------------------

type Stats = {
  n: number;
  min: number;
  p50: number;
  p95: number;
  p99: number;
  mean: number;
  max: number;
};

/**
 * Nearest-rank percentile on a copy sorted ascending. `p` in [0,100].
 * Nearest-rank (rather than interpolation) keeps the reported value an
 * actually-observed sample.
 */
function percentile(sortedAsc: number[], p: number): number {
  if (sortedAsc.length === 0) throw new Error("percentile of empty sample");
  const rank = Math.ceil((p / 100) * sortedAsc.length);
  const idx = Math.min(Math.max(rank, 1), sortedAsc.length) - 1;
  const value = sortedAsc[idx];
  if (value === undefined) throw new Error("percentile index out of range");
  return value;
}

function computeStats(samples: number[]): Stats {
  if (samples.length === 0) throw new Error("computeStats of empty sample");
  const sorted = [...samples].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, v) => acc + v, 0);
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  if (min === undefined || max === undefined) {
    throw new Error("unreachable: non-empty sample has no min/max");
  }
  return {
    n: sorted.length,
    min,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    mean: sum / sorted.length,
    max,
  };
}

function fmt(ms: number): string {
  return ms.toFixed(3);
}

// --- one measured deploy ---------------------------------------------------

/**
 * Deploy one single-step workflow BY SOURCE-REF against the env's hub and
 * return the measured `deployWorkflowSourceForTest` wall-clock in
 * milliseconds. The caller seeds `definitionAssetId` in the DB before this
 * runs; the interval brackets ONLY the deploy call.
 */
async function measureOneDeploy(
  env: DeployFlowEnv,
  db: TestDb["db"],
  anchorRunId: string,
  definitionAssetId: string,
): Promise<number> {
  const mailAddress = deriveRunAddress({
    runId: anchorRunId,
    domain: DEPLOYMENT_DOMAIN,
  });

  const inferenceSource: InferenceSource = {
    id: "anthropic:mock-model",
    provider: "anthropic",
    baseURL: `http://localhost:${env.inference.server.port}`,
    apiKey: "sk-mock",
    model: "mock-model",
  };

  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: `${anchorRunId}`,
    tenantId: "tenant-1",
    principalId: "prin_integration-1",
    agentAddress: mailAddress,
    systemPrompt: "Fallback prompt (overridden per step by the definition)",
    tools: [],
    grants: [],
    sources: [inferenceSource],
    defaultSource: "anthropic:mock-model",
  };

  const entryModule = singleStepAgentEntry({
    stepId: STEP_ID,
    systemPrompt: "You are the single-step agent of the deploy-latency bench.",
    address: mailAddress,
    agentId: `agent-${anchorRunId}-${STEP_ID}`,
    workflowId: `wf_${anchorRunId}`,
  });

  const t0 = performance.now();
  const handle = await deployWorkflowSourceForTest(env, {
    entryModule,
    db,
    tenantId: TENANT_ID,
    definitionAssetId,
    anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: mailAddress,
    approvals: "approve-probed",
    config,
    sources: { [STEP_ID]: [inferenceSource] },
  });
  const elapsed = performance.now() - t0;

  if (!handle.publicKey) {
    throw new Error(
      `deploy-latency bench: deploy of ${anchorRunId} did not return a public key`,
    );
  }
  return elapsed;
}

// --- main ------------------------------------------------------------------

function statsRow(label: string, s: Stats): string {
  return [
    label.padEnd(12),
    String(s.n).padStart(6),
    fmt(s.p50).padStart(10),
    fmt(s.p95).padStart(10),
    fmt(s.mean).padStart(10),
    fmt(s.min).padStart(10),
    fmt(s.max).padStart(10),
  ].join("  ");
}

async function main(): Promise<void> {
  if (!harnessDbEnvAvailable()) {
    throw new Error(
      "deploy-latency bench: no database env (.env + .env.migrate); the " +
        "code-sourced deploy front requires a real Postgres schema",
    );
  }

  const opts = parseArgs(process.argv.slice(2));
  fs.mkdirSync(opts.outDir, { recursive: true });

  const loadBefore = os.loadavg();

  // One migrated schema and one env (one hub + one sidecar subprocess) drive
  // every measured deploy. A fresh env per iteration would fold sidecar spawn
  // cost into the samples; the deploy path under measurement does not spawn a
  // sidecar, so reusing the env keeps the measured interval to the deploy
  // itself. Each deploy gets a unique anchorRunId and definition asset id so
  // nothing collides.
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

  const env: DeployFlowEnv = await startDeployFlowEnv({
    inferenceEchoUserMessage: true,
  });

  const samples: number[] = [];
  try {
    // iterations + 1 deploys; the first (cold) sample is discarded.
    for (let i = 0; i < opts.iterations + 1; i += 1) {
      const anchorRunId = `run_deploy-latency-i${String(i)}`;
      const definitionAssetId = `ast_deploy_latency_wf_${String(i)}`;
      await seedAsset(h.db, {
        id: definitionAssetId,
        tenantId: TENANT_ID,
        kind: "workflow",
        name: `deploy-latency-wf-${String(i)}`,
        creatorPrincipalId: CALLER_PRINCIPAL_ID,
      });
      const elapsed = await measureOneDeploy(
        env,
        h.db,
        anchorRunId,
        definitionAssetId,
      );
      if (i > 0) samples.push(elapsed);
    }
  } finally {
    await env.teardown();
    await h.close();
  }

  if (samples.length === 0) {
    throw new Error("deploy-latency bench: zero steady-state samples");
  }
  const stats = computeStats(samples);

  const loadAfter = os.loadavg();

  const results = {
    generatedAt: new Date().toISOString(),
    iterations: opts.iterations,
    sampleNote:
      "first (cold) deploy discarded; each measured deploy uses a fresh anchorRunId and definition asset on one shared hub+sidecar env",
    inference: "HTTP mock (echo mode); inference cost fixed/~0 (deploy path)",
    measured:
      "deployWorkflowSourceForTest: bundle entry, seed source asset, install/approve/freeze against DB, emit source-ref deploy frame, write anchor workflow_run",
    machine: {
      platform: `${os.type()} ${os.release()} ${os.arch()}`,
      cpus: os.cpus().length,
      loadavgBefore: loadBefore,
      loadavgAfter: loadAfter,
    },
    units: "milliseconds",
    stats,
    samples,
  };
  fs.writeFileSync(
    path.join(opts.outDir, "results.json"),
    JSON.stringify(results, null, 2) + "\n",
  );

  const header = [
    "deploy".padEnd(12),
    "n".padStart(6),
    "p50".padStart(10),
    "p95".padStart(10),
    "mean".padStart(10),
    "min".padStart(10),
    "max".padStart(10),
  ].join("  ");
  process.stdout.write(`\nCode-sourced workflow deploy latency (ms)\n`);
  process.stdout.write(
    `iterations=${String(opts.iterations)} (cold first deploy discarded)\n`,
  );
  process.stdout.write(
    `loadavg before=${loadBefore.map((v) => v.toFixed(2)).join(",")} after=${loadAfter.map((v) => v.toFixed(2)).join(",")}\n\n`,
  );
  process.stdout.write(header + "\n");
  process.stdout.write(statsRow("single-step", stats) + "\n");
  process.stdout.write(`\nresults.json written to ${opts.outDir}\n`);
}

if (import.meta.main) {
  await main();
}
