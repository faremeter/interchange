#!/usr/bin/env bun
/* eslint-disable no-console */

// CI test-pass wiring guard.
//
// check-test-enumeration.ts proves the Makefile <-> filesystem half: every
// `*.test.ts` is reachable from an enumerating target. This guard proves
// the complementary Makefile <-> CI half: every enumerating pass (except
// the standalone load lane) is actually invoked by a CI job, AND every job
// is gated by the required "make all" check.
//
// The parallel-job split that made this necessary also reintroduced the
// exact silent-drop failure check-test-enumeration.ts exists to prevent,
// one layer up: with a single `make all` job the Makefile-to-CI mapping was
// trivially true, but once the passes run as separate jobs a mis-wired job
// -- one that runs `make test-unit` twice and never runs `make test-core`,
// or a worker left out of the gate's `needs` -- goes green while a whole
// pass silently never runs. This guard closes that gap at lint time.

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { ENUMERATING_TARGETS } from "./check-test-enumeration";

// The required status-check context branch protection gates on. The
// aggregation job carries this name; every worker job must be in its
// `needs` so the gate cannot go green while a worker is red or skipped.
const GATE_JOB_NAME = "make all";

// test-load runs on its own cadence, not in the per-PR job graph, so it is
// exempt from the "must run in CI" assertion.
const CI_EXEMPT_TARGETS = new Set<string>(["test-load"]);

type Step = { run?: string };
type Job = {
  name?: string;
  if?: string;
  needs?: string | string[];
  steps?: Step[];
};
type Workflow = { jobs?: Record<string, Job> };

function normalizeNeeds(needs: string | string[] | undefined): string[] {
  if (needs === undefined) return [];
  return Array.isArray(needs) ? needs : [needs];
}

// A job runs a target when one of its steps' `run` script invokes `make`
// with that target as a whitespace-delimited token. Exact-token match so
// `make test` does not read as running `make test-core`.
function jobRunsTarget(job: Job, target: string): boolean {
  for (const step of job.steps ?? []) {
    const run = step.run;
    if (run === undefined) continue;
    const tokens = run.split(/\s+/);
    if (tokens.includes("make") && tokens.includes(target)) return true;
  }
  return false;
}

export type CiJobsReport = { errors: string[] };

export function checkCiTestJobs(repoRoot: string): CiJobsReport {
  const raw = readFileSync(
    join(repoRoot, ".github", "workflows", "ci.yml"),
    "utf8",
  );
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Bun.YAML.parse yields unknown-shaped data; every field access below is guarded
  const workflow = Bun.YAML.parse(raw) as Workflow;
  const jobs = workflow.jobs ?? {};
  const jobEntries = Object.entries(jobs);
  const jobIds = jobEntries.map(([id]) => id);
  const errors: string[] = [];

  // Locate the aggregation gate by its effective check-context name (a
  // job's context is its `name`, defaulting to the job id).
  const gateEntries = jobEntries.filter(
    ([id, job]) => (job.name ?? id) === GATE_JOB_NAME,
  );
  if (gateEntries.length === 0) {
    errors.push(
      `no CI job produces the required "${GATE_JOB_NAME}" status check; ` +
        "branch protection gates on that context, so it must exist",
    );
    return { errors };
  }
  if (gateEntries.length > 1) {
    errors.push(
      `more than one CI job resolves to the "${GATE_JOB_NAME}" status ` +
        "check; exactly one job may carry that context",
    );
    return { errors };
  }
  const gateEntry = gateEntries[0];
  if (gateEntry === undefined) {
    errors.push("internal: gate entry vanished after a length check");
    return { errors };
  }
  const [gateId, gate] = gateEntry;

  // The gate must run even when a dependency fails, or a failed worker
  // leaves the gate skipped -- the required context then never reports and
  // the PR blocks forever. `if: always()` forces it to report a conclusion.
  const gateIf = gate.if ?? "";
  if (!gateIf.includes("always()")) {
    errors.push(
      `the "${GATE_JOB_NAME}" gate must carry \`if: always()\` so a failed ` +
        "or skipped worker still produces a failing required check instead " +
        "of leaving it pending",
    );
  }

  // The gate must depend on every other job, so no worker can be red or
  // skipped while the required check is green.
  const gateNeeds = new Set(normalizeNeeds(gate.needs));
  for (const id of jobIds) {
    if (id === gateId) continue;
    if (!gateNeeds.has(id)) {
      errors.push(
        `CI job "${id}" is not in the "${GATE_JOB_NAME}" gate's \`needs\`; ` +
          "its failure would not block the required check",
      );
    }
  }
  // A `needs` entry that names no real job is a typo that silently drops
  // the dependency.
  for (const need of gateNeeds) {
    if (!jobIds.includes(need)) {
      errors.push(
        `the "${GATE_JOB_NAME}" gate \`needs\` "${need}", which is not a job ` +
          "in this workflow",
      );
    }
  }

  // Every enumerating pass (except the load lane) must be invoked by some
  // CI job, or that whole pass silently never runs.
  for (const target of ENUMERATING_TARGETS) {
    if (CI_EXEMPT_TARGETS.has(target)) continue;
    const runners = jobEntries.filter(([, job]) => jobRunsTarget(job, target));
    if (runners.length === 0) {
      errors.push(
        `Makefile target "${target}" enumerates tests but no CI job runs ` +
          `\`make ${target}\`; that whole pass would silently never run`,
      );
    }
  }

  return { errors };
}

if (import.meta.main) {
  if (import.meta.dirname === undefined) {
    throw new Error(
      "check-ci-test-jobs: import.meta.dirname is undefined; cannot locate the repository root",
    );
  }
  const repoRoot = join(import.meta.dirname, "..");
  const { errors } = checkCiTestJobs(repoRoot);
  if (errors.length > 0) {
    console.error(
      `check-ci-test-jobs: ${String(errors.length)} problem(s) with the CI ` +
        "test-job wiring\n",
    );
    for (const e of errors) console.error(`  - ${e}`);
    console.error(
      "\nThe CI job graph must run every enumerated test pass and gate all " +
        `jobs behind the required "${GATE_JOB_NAME}" check. Fix ` +
        ".github/workflows/ci.yml (or the Makefile targets) so the two agree.",
    );
    process.exit(1);
  }
  console.log(
    "check-ci-test-jobs: ok (every test pass runs in CI and every job is gated)",
  );
}
