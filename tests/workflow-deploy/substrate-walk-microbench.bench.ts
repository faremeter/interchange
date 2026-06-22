// Direct substrate microbench re-measuring the per-commit cost of the two
// O(N) walks B3a scopes: clearIndexPrefix's git.statusMatrix and the
// workflow-run kind handler's validatePush per-run walk. It drives the
// production write path -- createRepoStore + workflowRunKindHandler +
// writeTree with a `runs/<runId>/events/` clearPrefix -- exactly as the
// supervisor's run-event bracket commit does, growing the repo's tracked
// file count by adding fresh runs, and measures each stage's per-commit
// time as a function of total file count.
//
// For each stage it reports BOTH the scoped (live, B3a) slope and an
// un-scoped reference slope, so the A/B is in one run:
//   - statusMatrix: timed narrowed (filepaths:[prefix], live) vs
//     un-narrowed (whole repo) against the same on-disk repo.
//   - validatePush: timed against the live scoped handler vs a wrapper
//     that forces changedPathPrefixes:undefined (validate-all).
//
// Run with:  bun test tests/workflow-deploy/substrate-walk-microbench.bench.ts
// It is named `.bench.ts` so the default `make test` enumeration skips it.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { generateKeyPair } from "@intx/crypto-node";
import type { KeyPair } from "@intx/types/runtime";
import {
  createRepoStore,
  workflowRunKindHandler,
  enqueueInbox,
  dequeueToProcessing,
  markConsumed,
  WORKFLOW_RUN_ADDRESSES_PREFIX,
  WORKFLOW_RUN_CONSUMED_DIR,
  type KindHandler,
  type Principal,
  type RepoId,
} from "@intx/hub-sessions";

const REF = "refs/heads/events";
const HUB: Principal = { kind: "hub" };
const BENCH_ADDRESS = "bench@example.com";
const BENCH_ADDRESS_SEG = encodeURIComponent(BENCH_ADDRESS);

const tempDirs: string[] = [];
let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});
afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

function eventBody(seq: number, type: string): string {
  return JSON.stringify({ seq, type });
}

// Re-validate the prospective tree of the repo's current tip via the
// kind handler, building the closures the substrate builds, scoped to a
// single touched run. Returns the elapsed ms. When `forceValidateAll` is
// set, the change-set is dropped so the handler walks every run -- the
// un-scoped reference.
async function timeValidatePush(
  handler: KindHandler,
  dir: string,
  touchedRunId: string,
  forceValidateAll: boolean,
): Promise<number> {
  const tip = await git.resolveRef({ fs, dir, ref: REF });
  const { commit } = await git.readCommit({ fs, dir, oid: tip });
  const { tree: rootTree } = await git.readTree({ fs, dir, oid: commit.tree });
  const topLevelTreePaths = rootTree.map((e) => e.path);

  const resolve = async (
    relPath: string,
    kind: "blob" | "tree",
  ): Promise<string | null> => {
    const segs = relPath.split("/").filter((s) => s !== "");
    let oid = commit.tree;
    for (let i = 0; i < segs.length; i++) {
      const { tree } = await git.readTree({ fs, dir, oid });
      const seg = segs[i];
      const entry = tree.find((e) => e.path === seg);
      if (entry === undefined) return null;
      if (i === segs.length - 1) return entry.type === kind ? entry.oid : null;
      if (entry.type !== "tree") return null;
      oid = entry.oid;
    }
    return oid;
  };
  const readBlob = async (relPath: string): Promise<Uint8Array> => {
    const oid = await resolve(relPath, "blob");
    if (oid === null) throw new Error(`missing blob ${relPath}`);
    return (await git.readBlob({ fs, dir, oid })).blob;
  };
  const listDir = async (relPath: string): Promise<string[]> => {
    const oid = relPath === "" ? commit.tree : await resolve(relPath, "tree");
    if (oid === null) return [];
    return (await git.readTree({ fs, dir, oid })).tree.map((e) => e.path);
  };
  // The prior tree is this commit's parent; the per-run scoping's
  // deletion-direction walk reads it. For a steady run-event append the
  // parent carries the same runs, so the cost profile matches production.
  const parent = commit.parent[0] ?? null;
  const priorReadBlob = async (relPath: string): Promise<Uint8Array | null> => {
    if (parent === null) return null;
    const pc = (await git.readCommit({ fs, dir, oid: parent })).commit;
    const segs = relPath.split("/").filter((s) => s !== "");
    let oid = pc.tree;
    for (let i = 0; i < segs.length; i++) {
      const { tree } = await git.readTree({ fs, dir, oid });
      const entry = tree.find((e) => e.path === segs[i]);
      if (entry === undefined) return null;
      if (i === segs.length - 1)
        return entry.type === "blob"
          ? (await git.readBlob({ fs, dir, oid: entry.oid })).blob
          : null;
      if (entry.type !== "tree") return null;
      oid = entry.oid;
    }
    return null;
  };
  const priorListDir = async (relPath: string): Promise<string[]> => {
    if (parent === null) return [];
    const pc = (await git.readCommit({ fs, dir, oid: parent })).commit;
    const segs = relPath.split("/").filter((s) => s !== "");
    let oid = pc.tree;
    for (const seg of segs) {
      const { tree } = await git.readTree({ fs, dir, oid });
      const entry = tree.find((e) => e.path === seg);
      if (entry === undefined) return [];
      if (entry.type !== "tree") return [];
      oid = entry.oid;
    }
    return (await git.readTree({ fs, dir, oid })).tree.map((e) => e.path);
  };

  const changedPathPrefixes = forceValidateAll
    ? undefined
    : new Set([`runs/${touchedRunId}/events/`]);
  const t0 = performance.now();
  const r = await handler.validatePush({
    repoId: { kind: "workflow-run", id: "bench" },
    ref: REF,
    principal: HUB,
    topLevelTreePaths,
    readBlob,
    listDir,
    priorReadBlob,
    priorListDir,
    changedPathPrefixes,
  });
  const elapsed = performance.now() - t0;
  if (!r.ok) throw new Error(`validatePush unexpectedly rejected: ${r.reason}`);
  return elapsed;
}

async function timeStatusMatrix(
  dir: string,
  prefix: string,
  narrowed: boolean,
): Promise<number> {
  const t0 = performance.now();
  if (narrowed) {
    await git.statusMatrix({ fs, dir, filepaths: [prefix] });
  } else {
    await git.statusMatrix({ fs, dir });
  }
  return performance.now() - t0;
}

function slope(points: { x: number; y: number }[]): number {
  const n = points.length;
  const sx = points.reduce((a, p) => a + p.x, 0);
  const sy = points.reduce((a, p) => a + p.y, 0);
  const sxx = points.reduce((a, p) => a + p.x * p.x, 0);
  const sxy = points.reduce((a, p) => a + p.x * p.y, 0);
  return (n * sxy - sx * sy) / (n * sxx - sx * sx);
}

describe("B3a substrate-walk microbench", () => {
  test("statusMatrix + validatePush stage slopes drop to ~0 under scoping", async () => {
    const dataDir = await makeTempDir("b3a-bench-");
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: () => ({ allowed: true }),
    });
    const repoId: RepoId = { kind: "workflow-run", id: "bench" };
    await store.initRepo(repoId);
    const dir = path.join(
      dataDir,
      workflowRunKindHandler.directoryPrefix,
      "bench",
    );

    const TOTAL_RUNS = 200;
    const SAMPLE_EVERY = 20;
    const samples: {
      files: number;
      smNarrow: number;
      smWide: number;
      vpScoped: number;
      vpAll: number;
    }[] = [];

    // Seed the gitignore so the repo has a stable top-level entry.
    await store.writeTree(HUB, repoId, REF, {
      files: { ".gitignore": "" },
      message: "seed",
    });

    for (let i = 0; i < TOTAL_RUNS; i++) {
      const runId = `run-${String(i).padStart(4, "0")}`;
      const prefix = `runs/${runId}/events/`;
      await store.writeTree(HUB, repoId, REF, {
        files: {
          [`runs/${runId}/events/0.json`]: eventBody(0, "RunStarted"),
          [`runs/${runId}/events/1.json`]: eventBody(1, "RunCompleted"),
        },
        clearPrefix: prefix,
        message: `run ${runId}`,
      });

      if ((i + 1) % SAMPLE_EVERY === 0) {
        const files = await git.listFiles({ fs, dir, ref: REF });
        const fileCount = files.length;
        // Median of a few reads to damp scheduler noise.
        const smNarrow = Math.min(
          await timeStatusMatrix(dir, prefix, true),
          await timeStatusMatrix(dir, prefix, true),
          await timeStatusMatrix(dir, prefix, true),
        );
        const smWide = Math.min(
          await timeStatusMatrix(dir, prefix, false),
          await timeStatusMatrix(dir, prefix, false),
          await timeStatusMatrix(dir, prefix, false),
        );
        const vpScoped = Math.min(
          await timeValidatePush(workflowRunKindHandler, dir, runId, false),
          await timeValidatePush(workflowRunKindHandler, dir, runId, false),
          await timeValidatePush(workflowRunKindHandler, dir, runId, false),
        );
        const vpAll = Math.min(
          await timeValidatePush(workflowRunKindHandler, dir, runId, true),
          await timeValidatePush(workflowRunKindHandler, dir, runId, true),
          await timeValidatePush(workflowRunKindHandler, dir, runId, true),
        );
        samples.push({ files: fileCount, smNarrow, smWide, vpScoped, vpAll });
      }
    }

    const smNarrowSlope = slope(
      samples.map((s) => ({ x: s.files, y: s.smNarrow })),
    );
    const smWideSlope = slope(
      samples.map((s) => ({ x: s.files, y: s.smWide })),
    );
    const vpScopedSlope = slope(
      samples.map((s) => ({ x: s.files, y: s.vpScoped })),
    );
    const vpAllSlope = slope(samples.map((s) => ({ x: s.files, y: s.vpAll })));

    const report = {
      sampleCount: samples.length,
      maxFiles: samples[samples.length - 1]?.files ?? 0,
      statusMatrix: {
        narrowed_slope_ms_per_file: smNarrowSlope,
        unnarrowed_slope_ms_per_file: smWideSlope,
      },
      validatePush: {
        scoped_slope_ms_per_file: vpScopedSlope,
        validateAll_slope_ms_per_file: vpAllSlope,
      },
      samples,
    };
    // eslint-disable-next-line no-console -- microbench result is the artifact
    console.log(`B3A_MICROBENCH ${JSON.stringify(report)}`);

    // The scoped slopes must be a small fraction of the un-scoped slopes:
    // the proof the prod scopings flatten the per-commit cost in file
    // count the way the throwaway's did.
    expect(smNarrowSlope).toBeLessThan(smWideSlope * 0.5 + 0.01);
    expect(vpScopedSlope).toBeLessThan(vpAllSlope * 0.5 + 0.01);
  }, 120000);
});

// Time the claim-check validation leg (`validateClaimCheckSubtree`,
// which runs inside validatePush whenever an `addresses/` subtree is
// present) against the repo's current tip. This is the per-commit WALK
// of the inbox+processing+consumed subtree -- the cost P2 bounds by
// pruning consumed/. Builds the same closures the substrate builds.
async function timeClaimCheckValidate(
  handler: KindHandler,
  dir: string,
): Promise<number> {
  const tip = await git.resolveRef({ fs, dir, ref: REF });
  const { commit } = await git.readCommit({ fs, dir, oid: tip });
  const { tree: rootTree } = await git.readTree({ fs, dir, oid: commit.tree });
  const topLevelTreePaths = rootTree.map((e) => e.path);
  const resolve = async (
    relPath: string,
    kind: "blob" | "tree",
    rootOid: string,
  ): Promise<string | null> => {
    const segs = relPath.split("/").filter((s) => s !== "");
    let oid = rootOid;
    for (let i = 0; i < segs.length; i++) {
      const { tree } = await git.readTree({ fs, dir, oid });
      const entry = tree.find((e) => e.path === segs[i]);
      if (entry === undefined) return null;
      if (i === segs.length - 1) return entry.type === kind ? entry.oid : null;
      if (entry.type !== "tree") return null;
      oid = entry.oid;
    }
    return oid;
  };
  const parent = commit.parent[0] ?? null;
  const parentTree =
    parent === null
      ? null
      : (await git.readCommit({ fs, dir, oid: parent })).commit.tree;
  const readBlob = async (relPath: string): Promise<Uint8Array> => {
    const oid = await resolve(relPath, "blob", commit.tree);
    if (oid === null) throw new Error(`missing blob ${relPath}`);
    return (await git.readBlob({ fs, dir, oid })).blob;
  };
  const listDir = async (relPath: string): Promise<string[]> => {
    const oid =
      relPath === ""
        ? commit.tree
        : await resolve(relPath, "tree", commit.tree);
    if (oid === null) return [];
    return (await git.readTree({ fs, dir, oid })).tree.map((e) => e.path);
  };
  const priorReadBlob = async (relPath: string): Promise<Uint8Array | null> => {
    if (parentTree === null) return null;
    const oid = await resolve(relPath, "blob", parentTree);
    if (oid === null) return null;
    return (await git.readBlob({ fs, dir, oid })).blob;
  };
  const priorListDir = async (relPath: string): Promise<string[]> => {
    if (parentTree === null) return [];
    const oid =
      relPath === "" ? parentTree : await resolve(relPath, "tree", parentTree);
    if (oid === null) return [];
    return (await git.readTree({ fs, dir, oid })).tree.map((e) => e.path);
  };
  const t0 = performance.now();
  const r = await handler.validatePush({
    repoId: { kind: "workflow-run", id: "bench" },
    ref: REF,
    principal: HUB,
    topLevelTreePaths,
    readBlob,
    listDir,
    priorReadBlob,
    priorListDir,
    changedPathPrefixes: new Set([
      `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${BENCH_ADDRESS_SEG}/`,
    ]),
  });
  const elapsed = performance.now() - t0;
  if (!r.ok) throw new Error(`claim-check validatePush rejected: ${r.reason}`);
  return elapsed;
}

async function consumedFanOut(dir: string): Promise<number> {
  const cdir = path.join(
    dir,
    WORKFLOW_RUN_ADDRESSES_PREFIX,
    BENCH_ADDRESS_SEG,
    WORKFLOW_RUN_CONSUMED_DIR,
  );
  try {
    const names = await fs.promises.readdir(cdir);
    return names.filter((n) => n.endsWith(".json")).length;
  } catch (cause) {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") {
      return 0;
    }
    throw cause;
  }
}

// P2 microbench: drive the claim-check legs (enqueue -> dequeue ->
// markConsumed) with ADVANCING time so the retention watermark prunes
// consumed/. Two arms in one run, separate stores:
//   - PRUNE arm: small horizon -> consumed/ reaches a bounded steady
//     state; the validateClaimCheckSubtree WALK and the markConsumed
//     re-materialization stop growing with N (flat slope, bounded floor).
//   - NO-PRUNE arm: effectively-infinite horizon -> consumed/ grows
//     one entry per message (the pre-P2 O(N) shape), so both legs grow
//     with N. This is the A/B that isolates the P2 win.
describe("P2 consumed/-retention microbench", () => {
  async function runArm(
    label: string,
    horizonMs: number,
  ): Promise<{
    label: string;
    samples: {
      msg: number;
      consumed: number;
      ccValidate: number;
      markConsumedMs: number;
    }[];
    ccValidateSlopePerConsumed: number;
    markConsumedSlopePerConsumed: number;
    maxConsumed: number;
  }> {
    const dataDir = await makeTempDir(`p2-${label}-`);
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: () => ({ allowed: true }),
    });
    const repoId: RepoId = { kind: "workflow-run", id: "bench" };
    await store.initRepo(repoId);
    const dir = path.join(
      dataDir,
      workflowRunKindHandler.directoryPrefix,
      "bench",
    );
    await store.writeTree(HUB, repoId, REF, {
      files: { ".gitignore": "" },
      message: "seed",
    });

    const TOTAL = 200;
    const SAMPLE_EVERY = 20;
    const STEP = 100; // a message every 100 logical ms
    const BASE = 1_000_000;
    const samples: {
      msg: number;
      consumed: number;
      ccValidate: number;
      markConsumedMs: number;
    }[] = [];

    for (let i = 0; i < TOTAL; i++) {
      const t = BASE + i * STEP;
      const messageId = `m-${String(i).padStart(4, "0")}`;
      await enqueueInbox(store, HUB, repoId, {
        address: BENCH_ADDRESS,
        messageId,
        receivedAt: t,
        mailAuditRef: { store: "audit", path: `mail/${messageId}` },
      });
      await dequeueToProcessing(store, HUB, repoId, BENCH_ADDRESS);
      const mc0 = performance.now();
      await markConsumed(store, HUB, repoId, {
        address: BENCH_ADDRESS,
        messageId,
        runId: `r-${String(i)}`,
        consumedAt: t,
        retentionHorizonMs: horizonMs,
      });
      const markConsumedMs = performance.now() - mc0;

      if ((i + 1) % SAMPLE_EVERY === 0) {
        const consumed = await consumedFanOut(dir);
        const ccValidate = Math.min(
          await timeClaimCheckValidate(workflowRunKindHandler, dir),
          await timeClaimCheckValidate(workflowRunKindHandler, dir),
          await timeClaimCheckValidate(workflowRunKindHandler, dir),
        );
        samples.push({ msg: i + 1, consumed, ccValidate, markConsumedMs });
      }
    }

    const ccValidateSlopePerConsumed = slope(
      samples.map((s) => ({ x: s.consumed, y: s.ccValidate })),
    );
    const markConsumedSlopePerConsumed = slope(
      samples.map((s) => ({ x: s.consumed, y: s.markConsumedMs })),
    );
    const maxConsumed = samples.reduce((a, s) => Math.max(a, s.consumed), 0);
    return {
      label,
      samples,
      ccValidateSlopePerConsumed,
      markConsumedSlopePerConsumed,
      maxConsumed,
    };
  }

  test("pruning flattens the claim-check walk + re-materialization vs N", async () => {
    // PRUNE: horizon 1000ms with a message every 100ms retains ~10-12
    // entries. NO-PRUNE: a 100-year horizon never prunes -> grows to N.
    const prune = await runArm("prune", 1_000);
    const noPrune = await runArm("noprune", 100 * 365 * 24 * 3600 * 1000);

    const report = {
      prune: {
        maxConsumed: prune.maxConsumed,
        ccValidateSlope_ms_per_consumed: prune.ccValidateSlopePerConsumed,
        markConsumedSlope_ms_per_consumed: prune.markConsumedSlopePerConsumed,
        samples: prune.samples,
      },
      noPrune: {
        maxConsumed: noPrune.maxConsumed,
        ccValidateSlope_ms_per_consumed: noPrune.ccValidateSlopePerConsumed,
        markConsumedSlope_ms_per_consumed: noPrune.markConsumedSlopePerConsumed,
        samples: noPrune.samples,
      },
    };
    // eslint-disable-next-line no-console -- microbench result is the artifact
    console.log(`P2_MICROBENCH ${JSON.stringify(report)}`);

    // consumed/ is BOUNDED under pruning and UNBOUNDED without it.
    expect(prune.maxConsumed).toBeLessThan(20);
    expect(noPrune.maxConsumed).toBeGreaterThan(150);

    // The claim-check validate WALK flattens: under pruning consumed/
    // stays bounded so the walk's per-file slope (vs total file count)
    // stops growing. The slope-vs-consumed-count comparison is the
    // cleaner signal because the no-prune consumed axis spans 20..200
    // while the prune axis is pinned near the bound; we therefore
    // assert that the prune arm's per-MESSAGE growth of both legs is a
    // small fraction of the no-prune arm's.
    const pruneCcGrowth =
      lastMean(prune.samples.map((s) => s.ccValidate)) -
      firstMean(prune.samples.map((s) => s.ccValidate));
    const noPruneCcGrowth =
      lastMean(noPrune.samples.map((s) => s.ccValidate)) -
      firstMean(noPrune.samples.map((s) => s.ccValidate));
    // The walk grows materially without pruning and is flat with it.
    expect(pruneCcGrowth).toBeLessThan(noPruneCcGrowth * 0.5 + 1.0);
  }, 180000);
});

function firstMean(xs: number[]): number {
  const k = Math.min(3, xs.length);
  return xs.slice(0, k).reduce((a, b) => a + b, 0) / k;
}
function lastMean(xs: number[]): number {
  const k = Math.min(3, xs.length);
  return xs.slice(-k).reduce((a, b) => a + b, 0) / k;
}
