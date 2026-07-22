import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import git from "isomorphic-git";
import { eq } from "drizzle-orm";

import { generateKeyPair } from "@intx/crypto";
import { configureSync, getConfig, resetSync } from "@intx/log";
import { collectReachableObjects } from "@intx/storage-isogit";
import type { KeyPair } from "@intx/types/runtime";
import type { DB } from "@intx/db";
import { createWorkflowRunStore } from "@intx/db";
import { principal, workflowRun } from "@intx/db/schema";
import {
  createAgentRepoStore,
  createHubSessionLookups,
  WORKFLOW_RUN_RUNS_PREFIX,
} from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  seedTenants,
  seedWorkflowDeployment,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const ASSET = "ast";
// The workflow-run repo slug the pack is received under. It is the id the
// substrate maps to a `WorkflowRunSupervisorPrincipal`; it does not need to
// match a `workflow_deployment.id`, since the terminal flip keys off the run
// id carried in the event tree, not the deployment.
const DEPLOYMENT = "dep";
const WFR_REF = "refs/heads/events";

function eventBody(seq: number, type: string): string {
  return JSON.stringify({ seq, type });
}

// Route the package logger's error-level records into `sink` until the
// returned restore is called, so a test can assert the terminal seam surfaces
// a missing anchor loudly rather than swallowing it.
function installErrorCapture(sink: string[]): () => void {
  const savedConfig = getConfig();
  configureSync({
    reset: true,
    sinks: {
      capture: (record) => {
        if (record.level !== "error") return;
        const message = Array.isArray(record.message)
          ? record.message
              .map((part) =>
                typeof part === "string" ? part : JSON.stringify(part),
              )
              .join("")
          : String(record.message);
        sink.push(message);
      },
    },
    loggers: [{ category: [], lowestLevel: "error", sinks: ["capture"] }],
  });
  return () => {
    if (savedConfig) {
      configureSync({ reset: true, ...savedConfig });
    } else {
      resetSync();
    }
  };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "workflow-run terminal flip on pack receive (real DB)",
  () => {
    let h: TestDb;
    let signingKey: KeyPair;
    const tempDirs: string[] = [];

    beforeAll(async () => {
      h = await createTestDb();
      signingKey = await generateKeyPair();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: ASSET,
      });
      await seedWorkflowDeployment(h.db, {
        id: DEPLOYMENT,
        tenantId: TENANT,
        definitionAssetId: ASSET,
      });
    });

    afterEach(async () => {
      for (const d of tempDirs.splice(0)) {
        await fs.promises
          .rm(d, { recursive: true, force: true })
          .catch((_e) => {
            /* best effort cleanup */
          });
      }
    });

    async function makeTempDir(prefix: string): Promise<string> {
      const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
      tempDirs.push(d);
      return d;
    }

    // Build a workflow-run pack whose tip commit adds each run's event log
    // ending in its terminal event. The genesis commit carries a
    // `.gitignore`-only tree (the kind handler's accepted initial commit); the
    // tip adds every run's RunStarted + terminal events in one commit, so a
    // single pack carries a batch of newly-terminal runs.
    async function buildPack(
      runs: { runId: string; terminalType: string }[],
    ): Promise<{ pack: Uint8Array; tip: string }> {
      const srcDir = await makeTempDir("wfr-terminal-src-");
      await git.init({ fs, dir: srcDir, defaultBranch: "events" });

      const author = { name: "probe", email: "probe@example.com" };
      await fs.promises.writeFile(path.join(srcDir, ".gitignore"), "");
      await git.add({ fs, dir: srcDir, filepath: ".gitignore" });
      const genesis = await git.commit({
        fs,
        dir: srcDir,
        message: "genesis",
        author,
        ref: WFR_REF,
      });

      const files: Record<string, string> = {};
      for (const { runId, terminalType } of runs) {
        files[`${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/events/0.json`] = eventBody(
          0,
          "RunStarted",
        );
        files[`${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/events/1.json`] = eventBody(
          1,
          terminalType,
        );
      }
      for (const [rel, body] of Object.entries(files)) {
        const full = path.join(srcDir, rel);
        await fs.promises.mkdir(path.dirname(full), { recursive: true });
        await fs.promises.writeFile(full, body);
        await git.add({ fs, dir: srcDir, filepath: rel });
      }
      const tip = await git.commit({
        fs,
        dir: srcDir,
        message: "runs reach terminal",
        author,
        parent: [genesis],
        ref: WFR_REF,
      });

      const oids = Array.from(
        new Set(
          (
            await Promise.all(
              [genesis, tip].map((s) => collectReachableObjects(srcDir, s)),
            )
          ).flat(),
        ),
      );
      const packResult = await git.packObjects({
        fs,
        dir: srcDir,
        oids,
        write: false,
      });
      if (packResult.packfile === undefined) {
        throw new Error("git.packObjects returned no packfile");
      }
      return { pack: packResult.packfile, tip };
    }

    // Receive a pack through the real lookups seam wired to `db` (the real
    // handle unless a test injects a wrapper), returning the pack verdict.
    async function receiveWith(
      db: DB["db"],
      pack: Uint8Array,
      tip: string,
    ): Promise<{ accepted: true } | { accepted: false; reason: string }> {
      const dataDir = await makeTempDir("wfr-terminal-data-");
      const repoStore = createAgentRepoStore({ dataDir, signingKey });
      const lookups = createHubSessionLookups({
        db,
        agentRepoStore: repoStore,
      });
      return lookups.receiveWorkflowRunPack(
        { kind: "workflow-run", id: DEPLOYMENT },
        pack,
        WFR_REF,
        tip,
      );
    }

    async function buildAndReceive(
      runId: string,
      terminalType: string,
    ): Promise<void> {
      const { pack, tip } = await buildPack([{ runId, terminalType }]);
      const verdict = await receiveWith(h.db, pack, tip);
      expect(verdict).toEqual({ accepted: true });
    }

    test("flips the run to its terminal status and deactivates its principal", async () => {
      await seedPrincipal(h.db, {
        id: "prn-run",
        tenantId: TENANT,
        kind: "workflow",
        refId: "run-ext",
        status: "active",
      });
      await seedWorkflowRun(h.db, {
        id: "run-ext",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
        principalId: "prn-run",
      });

      await buildAndReceive("run-ext", "RunCompleted");

      const [run] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-ext"));
      expect(run?.status).toBe("completed");
      expect(run?.endedAt).not.toBeNull();

      const [prn] = await h.db
        .select()
        .from(principal)
        .where(eq(principal.id, "prn-run"));
      expect(prn?.status).toBe("deactivated");
    });

    test("maps RunFailed and RunCancelled to their statuses", async () => {
      await seedWorkflowRun(h.db, {
        id: "run-failed",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
      });
      await buildAndReceive("run-failed", "RunFailed");
      const [failed] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-failed"));
      expect(failed?.status).toBe("failed");

      await seedWorkflowRun(h.db, {
        id: "run-cancelled",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
      });
      await buildAndReceive("run-cancelled", "RunCancelled");
      const [cancelled] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-cancelled"));
      expect(cancelled?.status).toBe("cancelled");
    });

    test("deactivates only the run's own principal, not a bystander", async () => {
      // The run's own principal, plus an unrelated bystander principal on the
      // same tenant. The flip must deactivate only the run's own principal
      // (matched by `won.principalId`), leaving the bystander active. (The
      // bystander cannot share the run's refId: the principal table's
      // (tenantId, kind, refId) uniqueness forbids two workflow principals with
      // the same refId, so scoping is proven through the id match.)
      await seedPrincipal(h.db, {
        id: "prn-owner",
        tenantId: TENANT,
        kind: "workflow",
        refId: "run-scoped",
        status: "active",
      });
      await seedPrincipal(h.db, {
        id: "prn-other",
        tenantId: TENANT,
        kind: "workflow",
        refId: "run-unrelated",
        status: "active",
      });
      await seedWorkflowRun(h.db, {
        id: "run-scoped",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
        principalId: "prn-owner",
      });

      await buildAndReceive("run-scoped", "RunCompleted");

      const [owner] = await h.db
        .select()
        .from(principal)
        .where(eq(principal.id, "prn-owner"));
      const [other] = await h.db
        .select()
        .from(principal)
        .where(eq(principal.id, "prn-other"));
      expect(owner?.status).toBe("deactivated");
      expect(other?.status).toBe("active");
    });

    test("an internal run with no principal is flipped and touches no principal", async () => {
      // A second active principal on the tenant that shares the run's id as its
      // refId, so a mis-scoped deactivation would visibly hit it. The internal
      // run carries principalId = null, so nothing must be deactivated.
      await seedPrincipal(h.db, {
        id: "prn-bystander",
        tenantId: TENANT,
        kind: "workflow",
        refId: "run-internal",
        status: "active",
      });
      await seedWorkflowRun(h.db, {
        id: "run-internal",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
        principalId: null,
      });

      await buildAndReceive("run-internal", "RunCompleted");

      const [run] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-internal"));
      expect(run?.status).toBe("completed");
      expect(run?.endedAt).not.toBeNull();

      // The run had no principal of its own; the guarded flip won but the
      // principal branch never ran, so the bystander stays active.
      const [bystander] = await h.db
        .select()
        .from(principal)
        .where(eq(principal.id, "prn-bystander"));
      expect(bystander?.status).toBe("active");
    });

    test("a terminal event with no run row logs loudly and still acks", async () => {
      // The run reached a terminal event before any anchor committed, so no
      // workflow_run row exists. The flip matches nothing, but this is a defect
      // (a lost anchor), not a benign replay: the seam must log at ERROR rather
      // than silently succeed. The pack is still acked -- the git ref advanced.
      const errors: string[] = [];
      const restore = installErrorCapture(errors);
      try {
        const { pack, tip } = await buildPack([
          { runId: "run-orphan", terminalType: "RunCompleted" },
        ]);
        const verdict = await receiveWith(h.db, pack, tip);
        expect(verdict).toEqual({ accepted: true });
      } finally {
        restore();
      }

      // No row was silently created; the flip stayed a no-op on the DB.
      const rows = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-orphan"));
      expect(rows).toHaveLength(0);
      // The missing anchor surfaced loudly, naming the run.
      expect(errors.some((m) => m.includes("run-orphan"))).toBe(true);
    });

    test("markTerminal flips a running run once and is a no-op thereafter", async () => {
      // Direct store test of the `status = 'running'` guard -- the property the
      // pack-receive seam relies on for idempotency. This is the test that dies
      // if the guard is removed: a second flip on an already-completed row must
      // match nothing and mutate nothing.
      await seedWorkflowRun(h.db, {
        id: "run-guard",
        deploymentId: DEPLOYMENT,
        tenantId: TENANT,
      });
      const store = createWorkflowRunStore(h.db);

      const endedAt = new Date();
      const won = await store.markTerminal("run-guard", "completed", endedAt);
      expect(won).not.toBeNull();
      expect(won?.status).toBe("completed");
      expect(won?.endedAt).toEqual(endedAt);

      const [afterFirst] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-guard"));
      const settledEndedAt = afterFirst?.endedAt ?? null;
      expect(settledEndedAt).not.toBeNull();

      // A second flip -- with a DIFFERENT status and a later timestamp -- must
      // find no running row, return null, and leave the settled row untouched.
      const secondEndedAt = new Date(endedAt.getTime() + 60_000);
      const again = await store.markTerminal(
        "run-guard",
        "failed",
        secondEndedAt,
      );
      expect(again).toBeNull();

      const [afterSecond] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-guard"));
      expect(afterSecond?.status).toBe("completed");
      expect(afterSecond?.endedAt ?? null).toEqual(settledEndedAt);
    });

    test("a failed flip for one run does not block the batch or the ack", async () => {
      // The regression that proves the withdrawn crash-window is closed: with
      // two newly-terminal runs in one pack, if ONE run's DB flip throws,
      // (a) the pack is still acked (verdict accepted), and (b) the OTHER run is
      // still flipped and its principal deactivated. A throw that escaped the
      // per-run loop would drop the ack and abort the batch.
      //
      // The kind handler's run-enumeration order is not contractually fixed, so
      // the injected failure targets whichever run the seam happens to process
      // first; the assertions below are order-agnostic (exactly one run stuck,
      // exactly one flipped).
      for (const runId of ["run-a", "run-b"]) {
        await seedPrincipal(h.db, {
          id: `prn-${runId}`,
          tenantId: TENANT,
          kind: "workflow",
          refId: runId,
          status: "active",
        });
        await seedWorkflowRun(h.db, {
          id: runId,
          deploymentId: DEPLOYMENT,
          tenantId: TENANT,
          principalId: `prn-${runId}`,
        });
      }

      // Wrap the real db so the FIRST db.transaction the seam opens throws,
      // standing in for a transient DB failure on one run's flip. Every other
      // property forwards to the real handle, and later transactions run for
      // real, so the second run's flip commits normally.
      let transactionCalls = 0;
      const failFirstTxDb = new Proxy(h.db, {
        get(target, prop, receiver) {
          if (prop === "transaction") {
            return (
              ...args: Parameters<DB["db"]["transaction"]>
            ): ReturnType<DB["db"]["transaction"]> => {
              transactionCalls += 1;
              if (transactionCalls === 1) {
                throw new Error("injected terminal-flip failure");
              }
              return target.transaction(...args);
            };
          }
          return Reflect.get(target, prop, receiver);
        },
      });

      const { pack, tip } = await buildPack([
        { runId: "run-a", terminalType: "RunCompleted" },
        { runId: "run-b", terminalType: "RunCompleted" },
      ]);
      const verdict = await receiveWith(failFirstTxDb, pack, tip);

      // The pack was acked despite the mid-loop throw.
      expect(verdict).toEqual({ accepted: true });
      // Exactly the two per-run transactions were attempted -- one failure did
      // not short-circuit the loop.
      expect(transactionCalls).toBe(2);

      const runs = await h.db.select().from(workflowRun);
      const principals = await h.db.select().from(principal);
      const statusById = new Map(runs.map((r) => [r.id, r.status]));
      const principalStatusById = new Map(
        principals.map((p) => [p.id, p.status]),
      );

      // Exactly one run was left running (the injected failure) and exactly one
      // flipped to completed (the sibling the loop still processed).
      const runStatuses = [
        statusById.get("run-a"),
        statusById.get("run-b"),
      ].sort();
      expect(runStatuses).toEqual(["completed", "running"]);

      // The stuck run's principal stays active; the flipped run's is
      // deactivated. They move in lockstep with their run, whichever order the
      // seam processed them in.
      for (const runId of ["run-a", "run-b"]) {
        if (statusById.get(runId) === "completed") {
          expect(principalStatusById.get(`prn-${runId}`)).toBe("deactivated");
        } else {
          expect(principalStatusById.get(`prn-${runId}`)).toBe("active");
        }
      }
    });
  },
);
