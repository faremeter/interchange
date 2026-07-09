// BOOT-RESTORE benchmark (NOT a CI test).
//
// Measures the sidecar's boot-time restore cost -- the wall-clock the
// `restoreWorkflowDeployments()` driver takes to bring a batch of persisted
// deployments back to ready -- as a function of the number of restored
// deployments. Restore runs once at boot, before `hubLink.connect()`, and
// spawns each deployment's supervisor SERIALLY; this bench quantifies the
// per-deployment restore cost that serial spawn imposes.
//
// The measured operation is `SidecarDeployRouter.restoreWorkflowDeployments()`
// in `apps/sidecar/src/workflow-host-wiring.ts`. For each batch size N the
// bench:
//
//   1. SETUP (not timed): stands up N single-step deployments through a first
//      router over a scratch data dir, so N `deployment.json` records plus
//      their `assets/workflow/<id>/workflow.json` and per-step grants land on
//      disk exactly as a live deploy would leave them.
//   2. RESTORE (timed): builds a SECOND router with a FRESH transport (an
//      empty registration table -- the sidecar-restart model) over the SAME
//      data dir, then brackets `restoreWorkflowDeployments()` with
//      `performance.now()`. The child handshakes are driven concurrently (the
//      driver blocks on each `supervisor.spawn` until its child signals
//      `ready`), so the measured interval is the real serial restore-to-ready
//      path: scan -> per-record re-validate -> spawn -> ready, N times.
//   3. READINESS: confirms all N restored addresses are live via
//      `activeAddresses()` before recording the sample.
//
// The subprocess spawner is a deterministic in-memory ready-driver (no real
// `Bun.spawn`), so the sample isolates the restore driver's own per-deployment
// cost -- scan, validation, supervisor construction, transport registration --
// rather than OS process-spawn latency. The FIRST batch's sample is discarded
// (cold: first isogit/module warm), matching the standalone latency gate.
//
// The measured interval ends when `restoreWorkflowDeployments()` resolves --
// i.e. once every supervisor has spawned and handshaked `ready`. AFTER that,
// each supervisor's dispatch loop runs against the disk-backed stub RepoStore
// (which mirrors the wiring test's fixture and implements only the handful of
// methods the deploy/restore-to-ready path exercises), so its
// `replayProcessingToInbox` / dispatch iteration logs a WRN/ERR for the
// unimplemented `writeTreeDelta`. Those lines are post-measurement background
// noise, not a restore failure: readiness is asserted via `activeAddresses()`
// before the sample is recorded.
//
// Run:
//   bun run tests/workflow-deploy/boot-restore.bench.ts \
//     [--sizes 1,5,10,20] [--out <dir>]
//
// Writes <out>/results.json and prints a per-N table to stdout. Not matched by
// `bun test` (it is a `.bench.ts`, not a `.test.ts`), so `make test` never
// runs it; it is type-checked by this directory's tsconfig.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { RepoId, RepoStore } from "@intx/hub-sessions";
import {
  createControlChannelSender,
  type FrameReader,
  type NdjsonReader,
  type NdjsonWriter,
  type SubprocessHandle,
  type SubprocessSpawner,
} from "@intx/workflow-host";
import type { AgentDeployFrame } from "@intx/types/sidecar";
import {
  createSidecarDeployRouter,
  type SidecarDeployRouter,
} from "@intx/sidecar-app/src/workflow-host-wiring";

const DEFAULT_SIZES = [1, 5, 10, 20];

type BenchOpts = {
  sizes: number[];
  outDir: string;
};

function parseArgs(argv: string[]): BenchOpts {
  let sizes = [...DEFAULT_SIZES];
  let outDir = path.resolve(
    import.meta.dir,
    "../../dispatch/intr-277-reconnect/1g-boot_restore_bench",
  );
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--sizes") {
      const next = argv[i + 1];
      if (next === undefined) throw new Error("--sizes requires a value");
      const parsed = next.split(",").map((s) => {
        const n = Number.parseInt(s.trim(), 10);
        if (!Number.isFinite(n) || n <= 0) {
          throw new Error(
            `--sizes entries must be positive integers, got ${s}`,
          );
        }
        return n;
      });
      if (parsed.length === 0)
        throw new Error("--sizes must list at least one N");
      sizes = parsed;
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
  return { sizes, outDir };
}

// --- in-memory IPC streams (mirrors the wiring test's fixtures) -----------

function createMemoryNdjsonStream() {
  const buffer: string[] = [];
  let waiter: (() => void) | null = null;
  let done = false;
  function wake() {
    const w = waiter;
    waiter = null;
    if (w) w();
  }
  const reader: NdjsonReader = {
    read(): AsyncIterableIterator<string> {
      return (async function* () {
        while (true) {
          if (buffer.length > 0) {
            const next = buffer.shift();
            if (next === undefined) throw new Error("buffer shift undefined");
            yield next;
            continue;
          }
          if (done) return;
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      })();
    },
  };
  const writer: NdjsonWriter = {
    write(line: string) {
      buffer.push(line.replace(/\n$/, ""));
      wake();
      return Promise.resolve();
    },
  };
  return {
    writer,
    reader,
    inject(line: string) {
      buffer.push(line.replace(/\n$/, ""));
      wake();
    },
    close() {
      done = true;
      wake();
    },
  };
}

function createMemoryFrameStream() {
  const buffer: Uint8Array[] = [];
  let waiter: (() => void) | null = null;
  let done = false;
  function wake() {
    const w = waiter;
    waiter = null;
    if (w) w();
  }
  const reader: FrameReader = {
    read(): AsyncIterableIterator<Uint8Array> {
      return (async function* () {
        while (true) {
          if (buffer.length > 0) {
            const next = buffer.shift();
            if (next === undefined) throw new Error("frame shift undefined");
            yield next;
            continue;
          }
          if (done) return;
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      })();
    },
  };
  return {
    reader,
    close() {
      done = true;
      wake();
    },
  };
}

// --- ready-driving spawner (mirrors makeReadyDrivingSpawner) --------------

/**
 * A deterministic in-memory subprocess spawner that records each spawn and
 * exposes `driveReadyFor(index)` to complete a child's ready handshake. The
 * restore driver blocks on `supervisor.spawn` until `ready` lands, so every
 * spawned child needs its handshake driven for restore to progress.
 */
function makeReadyDrivingSpawner(pidBase: number) {
  type Spawn = {
    env: Record<string, string>;
    childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
    eventChildToSupervisor: ReturnType<typeof createMemoryFrameStream>;
  };
  const spawns: Spawn[] = [];
  const spawner: SubprocessSpawner = ({ env }) => {
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    spawns.push({ env, childToSupervisor, eventChildToSupervisor });
    const handle: SubprocessHandle = {
      pid: pidBase + spawns.length,
      controlWriter: supervisorToChild.writer,
      controlReader: childToSupervisor.reader,
      eventReader: eventChildToSupervisor.reader,
      kill: () => {
        childToSupervisor.close();
        eventChildToSupervisor.close();
        resolveExit?.(0);
      },
      exited,
    };
    return handle;
  };
  async function driveReadyFor(index: number): Promise<void> {
    while (spawns.length <= index) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const spawn = spawns[index];
    if (spawn === undefined) throw new Error(`spawn ${String(index)} missing`);
    const channelId = spawn.env.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID missing in spawn env");
    }
    const childIpcKeyPair = await generateKeyPair();
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          spawn.childToSupervisor.inject(line);
          return Promise.resolve();
        },
      },
    });
    await childSender.send({
      type: "ready",
      data: {
        childPid: pidBase + index,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });
  }
  return { spawner, driveReadyFor, spawnCount: () => spawns.length };
}

// --- router fixture wiring (mirrors buildMultistepFixture) ----------------

function createSpawnTestRepoStore(tempBase: string): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(tempBase, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(_p, _id, _ref, args) {
      await args.merge(new Map());
      return { commitSha: "stub-sha", newlyTerminalRuns: [] };
    },
    async writeTree(_p, repoId, _ref, content) {
      const dir = path.join(tempBase, repoId.kind, repoId.id);
      for (const [relPath, contents] of Object.entries(content.files)) {
        const full = path.join(dir, relPath);
        fs.mkdirSync(path.dirname(full), { recursive: true });
        fs.writeFileSync(full, contents);
      }
      return { commitSha: "stub-sha", newlyTerminalRuns: [] };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- bench stub; only getRepoDir + writeTree(PreservingPrefix) are exercised on the restore spawn path
  return new Proxy(stub as RepoStore, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (value !== undefined) return value;
      return () => {
        throw new Error(`stub RepoStore: ${String(prop)} not implemented`);
      };
    },
  });
}

/**
 * Build a `SidecarDeployRouter` over `dataDir` with the same stubbed
 * host bindings the wiring test's `buildMultistepFixture` uses: a stub
 * `sessions`/`keyStore` (the single-step head deploy only inits its repo and
 * records the hub key), a disk-backed spawn-test RepoStore, a permissive
 * source-admission gate, and the supplied in-memory spawner + transport.
 */
async function buildRouter(args: {
  dataDir: string;
  spawner: SubprocessSpawner;
  transport: ReturnType<typeof createInMemoryTransport>;
}): Promise<SidecarDeployRouter> {
  const signingKeyPair = await generateKeyPair();
  const tempBase = fs.mkdtempSync(path.join(os.tmpdir(), "boot-restore-repo-"));
  const repoStore = createSpawnTestRepoStore(tempBase);
  return createSidecarDeployRouter({
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the workflow path never invokes provisionAgent/persistHubPublicKey; single-step uses the narrow initRepo
    sessions: {
      provisionAgent: async () => {
        throw new Error("workflow branch must not invoke provisionAgent");
      },
      persistHubPublicKey: async () => {
        throw new Error("workflow branch must not invoke persistHubPublicKey");
      },
      initRepo: async () => undefined,
    } as unknown as Parameters<typeof createSidecarDeployRouter>[0]["sessions"],
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- bench stub; the single-step head deploy records the hub key for pack verification
    keyStore: {
      recordHubKey: () => undefined,
      loadOrGenerateKey: async () => ({
        keyPair: await generateKeyPair(),
        isNew: false,
      }),
      forgetAgent: () => undefined,
    } as unknown as Parameters<typeof createSidecarDeployRouter>[0]["keyStore"],
    transport: args.transport,
    repoStore,
    signingKeySeed: signingKeyPair.privateKey,
    createAgentCrypto: createEd25519Crypto,
    assertSourceBuildable: () => undefined,
    registerDeployment: () => undefined,
    unregisterDeployment: () => undefined,
    multistepSubprocessSpawner: args.spawner,
    multistepSubstrateEnv: { SIDECAR_DATA_DIR: args.dataDir },
  });
}

function singleStepFrame(
  agentAddress: string,
  definitionId: string,
): AgentDeployFrame {
  return {
    type: "agent.deploy",
    agentAddress,
    agentId: "boot-restore-agent",
    hubPublicKey: "hub-pk",
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the workflow path reads only config.sessionId/config.grants, which tolerate undefined
    config: {} as AgentDeployFrame["config"],
    workflow: {
      definition: {
        id: definitionId,
        triggers: [{ type: "manual" }],
        stepOrder: ["step-1"],
        steps: { "step-1": { kind: "step" } },
      },
      sources: {
        "step-1": [
          {
            id: "step-1",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-step-1",
            model: "claude-3-5",
          },
        ],
      },
    },
  };
}

// --- one batch: setup N records on disk, then time restore-to-ready --------

/**
 * Stand up N single-step deployments through a first router (SETUP, untimed),
 * then time a fresh router's `restoreWorkflowDeployments()` bringing all N
 * back to ready (RESTORE). Returns the measured restore wall-clock in ms.
 * Confirms every restored address is live before returning.
 */
async function measureRestore(n: number): Promise<number> {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "boot-restore-data-"));
  const addresses: string[] = [];
  for (let i = 0; i < n; i += 1) {
    addresses.push(`ins_boot_${String(i)}@example.com`);
  }

  // SETUP: deploy each address through the first router so its restore record,
  // workflow.json, and step grants land on disk. Not part of the measured
  // interval.
  const setupTransport = createInMemoryTransport();
  const setup = makeReadyDrivingSpawner(20000);
  const routerA = await buildRouter({
    dataDir,
    spawner: setup.spawner,
    transport: setupTransport,
  });
  for (let i = 0; i < n; i += 1) {
    const address = addresses[i];
    if (address === undefined) throw new Error("address hole in setup");
    const deployPromise = routerA.deploy(
      singleStepFrame(address, `wf-boot-${String(i)}`),
    );
    await setup.driveReadyFor(i);
    await deployPromise;
  }

  // RESTORE: a fresh transport (empty registration table -- the restart model)
  // and fresh router state over the SAME data dir. Drive all N child
  // handshakes concurrently with the serial restore driver, which blocks on
  // each spawn until ready.
  const restoreTransport = createInMemoryTransport();
  const restore = makeReadyDrivingSpawner(30000);
  const routerB = await buildRouter({
    dataDir,
    spawner: restore.spawner,
    transport: restoreTransport,
  });

  const readyDrivers: Promise<void>[] = [];
  for (let i = 0; i < n; i += 1) {
    readyDrivers.push(restore.driveReadyFor(i));
  }

  const t0 = performance.now();
  await routerB.restoreWorkflowDeployments();
  const elapsed = performance.now() - t0;

  await Promise.all(readyDrivers);

  const active = new Set(routerB.activeAddresses());
  for (const address of addresses) {
    if (!active.has(address)) {
      throw new Error(
        `restore left ${address} inactive: activeAddresses did not include it after restoreWorkflowDeployments resolved`,
      );
    }
  }
  if (restore.spawnCount() !== n) {
    throw new Error(
      `restore spawned ${String(restore.spawnCount())} children, expected ${String(n)}`,
    );
  }

  fs.rmSync(dataDir, { recursive: true, force: true });
  return elapsed;
}

// --- OLS fit (ms/deployment slope + floor) --------------------------------

type Fit = { slopeMsPerDeployment: number; interceptMs: number };

/** Ordinary-least-squares fit of restore-ms against deployment count. */
function computeFit(points: { n: number; ms: number }[]): Fit {
  const k = points.length;
  if (k === 0) throw new Error("computeFit of empty sample");
  let sumX = 0;
  let sumY = 0;
  let sumXY = 0;
  let sumXX = 0;
  for (const p of points) {
    sumX += p.n;
    sumY += p.ms;
    sumXY += p.n * p.ms;
    sumXX += p.n * p.n;
  }
  const denom = k * sumXX - sumX * sumX;
  const slope = denom === 0 ? 0 : (k * sumXY - sumX * sumY) / denom;
  const intercept = (sumY - slope * sumX) / k;
  return { slopeMsPerDeployment: slope, interceptMs: intercept };
}

function fmt(ms: number): string {
  return ms.toFixed(3);
}

// --- main ------------------------------------------------------------------

async function main(): Promise<void> {
  const opts = parseArgs(process.argv.slice(2));
  fs.mkdirSync(opts.outDir, { recursive: true });

  const loadBefore = os.loadavg();

  // A cold warm-up batch (discarded): the first restore pays one-time
  // isogit/module/crypto warm costs the steady-state must exclude. Use the
  // smallest requested size for the warm-up.
  const warmSize = Math.min(...opts.sizes);
  await measureRestore(warmSize);

  const samples: { n: number; ms: number; msPerDeployment: number }[] = [];
  for (const n of opts.sizes) {
    const ms = await measureRestore(n);
    samples.push({ n, ms, msPerDeployment: ms / n });
  }

  const loadAfter = os.loadavg();
  const fit = computeFit(samples.map((s) => ({ n: s.n, ms: s.ms })));

  const results = {
    generatedAt: new Date().toISOString(),
    measured:
      "SidecarDeployRouter.restoreWorkflowDeployments() wall-clock to bring N persisted single-step deployments back to ready",
    sizes: opts.sizes,
    sampleNote:
      "first (cold) batch discarded; each sample is one fresh router restoring N records over a scratch data dir with an in-memory ready-driving spawner (no real Bun.spawn)",
    machine: {
      platform: `${os.type()} ${os.release()} ${os.arch()}`,
      cpus: os.cpus().length,
      loadavgBefore: loadBefore,
      loadavgAfter: loadAfter,
    },
    units: "milliseconds",
    samples,
    fit: {
      note: "OLS fit of total restore-ms vs deployment count N: slope is the marginal per-deployment restore cost, intercept is the fixed restore floor",
      slopeMsPerDeployment: fit.slopeMsPerDeployment,
      interceptMs: fit.interceptMs,
    },
  };
  fs.writeFileSync(
    path.join(opts.outDir, "results.json"),
    JSON.stringify(results, null, 2) + "\n",
  );

  const header = [
    "N".padStart(6),
    "restore_ms".padStart(14),
    "ms/deployment".padStart(16),
  ].join("  ");
  process.stdout.write(
    `\nBoot-restore time to ready vs number of restored deployments\n`,
  );
  process.stdout.write(
    `loadavg before=${loadBefore.map((v) => v.toFixed(2)).join(",")} after=${loadAfter.map((v) => v.toFixed(2)).join(",")}\n\n`,
  );
  process.stdout.write(header + "\n");
  for (const s of samples) {
    process.stdout.write(
      [
        String(s.n).padStart(6),
        fmt(s.ms).padStart(14),
        fmt(s.msPerDeployment).padStart(16),
      ].join("  ") + "\n",
    );
  }
  process.stdout.write(
    `\nOLS fit: slope=${fmt(fit.slopeMsPerDeployment)} ms/deployment, floor(intercept)=${fmt(fit.interceptMs)} ms\n\n`,
  );
  process.stdout.write(`results.json written to ${opts.outDir}\n`);
}

await main();
