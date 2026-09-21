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
import { eq, inArray, sql } from "drizzle-orm";

import { generateKeyPair } from "@intx/crypto";
import { configureSync, getConfig } from "@intx/log";
import { collectReachableObjects } from "@intx/storage-isogit/node";
import type { KeyPair } from "@intx/types/runtime";
import type { DB } from "@intx/db";
import {
  createSidecarAllocationStore,
  createWorkflowRunDispatchStore,
  createWorkflowRunStore,
} from "@intx/db";
import {
  principal,
  sidecar,
  sidecarAllocation,
  workflowPendingProjection,
  workflowRun,
} from "@intx/db/schema";
import {
  createWorkflowHistoryReceiveTracker,
  createAgentRepoStore,
  createHubSessionLookups,
  createWorkflowLifecycleService,
  createWorkflowDispatchProjection,
  createWorkflowRunReader,
  enqueueInbox,
  dequeueToProcessing,
  markConsumed,
  type RepoStore,
  WORKFLOW_RUN_RUNS_PREFIX,
} from "@intx/hub-sessions";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const ASSET = "ast";
const DEPLOYMENT = "dep";
const DEPLOYMENT_ADDRESS = "run_dep@tnt.example";
const DEPLOYMENT_REPO_ID = deriveWorkflowRunRepoId(DEPLOYMENT_ADDRESS);
// A second deployment on the same tenant, so a run anchored elsewhere can be
// offered to this deployment's pack-receive seam.
const FOREIGN_DEPLOYMENT = "dep-foreign";
const FOREIGN_DEPLOYMENT_ADDRESS = "run_dep_foreign@tnt.example";
const WFR_REF = "refs/heads/main";

function eventBody(seq: number, type: string, at?: string): string {
  return JSON.stringify({ seq, type, ...(at !== undefined ? { at } : {}) });
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
    // A null capture means this file loaded without `@intx/log` having
    // installed its default sink, which cannot happen -- importing the
    // package runs the install. Resetting here instead would leave the
    // worker with no logging configuration at all, and the install
    // cannot re-fire to repair it.
    if (!savedConfig) {
      throw new Error(
        "no logging configuration was captured before this suite replaced it",
      );
    }
    configureSync({ reset: true, ...savedConfig });
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
      await seedWorkflowRun(h.db, {
        id: DEPLOYMENT,
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        address: DEPLOYMENT_ADDRESS,
      });
      await h.db.insert(sidecarAllocation).values({
        id: "allocation-terminal-flip",
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        provisionerId: "test",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "test:v1",
        status: "allocated",
        generation: 1,
        ensureAcceptedGeneration: 1,
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

    // Build a workflow-run pack whose tip commit adds each run's event log.
    // The genesis commit carries a
    // `.gitignore`-only tree (the kind handler's accepted initial commit); the
    // tip adds every requested event in one commit.
    async function buildPack(
      runs: {
        runId: string;
        terminalType?: string;
        terminalAt?: string;
        signalId?: string;
        consumedMessageId?: string;
        // When true, omit the seq-0 RunStarted and emit the terminal event
        // alone at seq 1 -- the exact artifact the crash-loop guard's
        // supervisor-authored RunFailed produces on an anchor run whose event
        // log is otherwise empty.
        terminalOnlyAtSeq1?: boolean;
      }[],
    ): Promise<{ pack: Uint8Array; tip: string }> {
      const srcDir = await makeTempDir("wfr-terminal-src-");
      await git.init({ fs, dir: srcDir, defaultBranch: "main" });

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
      for (const {
        runId,
        terminalType,
        terminalAt,
        signalId,
        consumedMessageId,
        terminalOnlyAtSeq1,
      } of runs) {
        if (terminalOnlyAtSeq1 === true) {
          if (terminalType === undefined) {
            throw new Error("terminalOnlyAtSeq1 requires a terminalType");
          }
          if (signalId !== undefined) {
            throw new Error(
              "terminalOnlyAtSeq1 cannot be combined with a signalId",
            );
          }
          files[`${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/events/1.json`] =
            eventBody(1, terminalType, terminalAt);
          continue;
        }
        files[`${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/events/0.json`] =
          JSON.stringify({
            seq: 0,
            type: "RunStarted",
            ...(consumedMessageId === undefined ? {} : { consumedMessageId }),
          });
        let seq = 1;
        if (signalId !== undefined) {
          files[
            `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/events/${String(seq)}.json`
          ] = JSON.stringify({ seq, type: "SignalReceived", signalId });
          seq += 1;
        }
        if (terminalType !== undefined) {
          files[
            `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/events/${String(seq)}.json`
          ] = eventBody(seq, terminalType, terminalAt);
        }
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
      repoStore?: ReturnType<typeof createAgentRepoStore>,
    ): Promise<{ accepted: true } | { accepted: false; reason: string }> {
      repoStore ??= createAgentRepoStore({
        dataDir: await makeTempDir("wfr-terminal-data-"),
        signingKey,
      });
      await repoStore.repoStore.initRepo({
        kind: "workflow-run",
        id: DEPLOYMENT_REPO_ID,
      });
      const lookups = createHubSessionLookups({
        db,
        agentRepoStore: repoStore,
        historyReceives: createWorkflowHistoryReceiveTracker(),
      });
      return lookups.receiveWorkflowRunPack(
        { kind: "workflow-run", id: DEPLOYMENT_REPO_ID },
        pack,
        WFR_REF,
        tip,
        {
          kind: "allocated",
          agentAddress: DEPLOYMENT_ADDRESS,
          allocationId: "allocation-terminal-flip",
          anchorRunId: DEPLOYMENT,
          generation: 1,
        },
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

    async function recordMailRejection(store: RepoStore, messageId: string) {
      const repoId = { kind: "workflow-run", id: DEPLOYMENT_REPO_ID } as const;
      const writer = { kind: "supervisor", anchorRunId: DEPLOYMENT_REPO_ID };
      const now = Date.now();
      await enqueueInbox(store, writer, repoId, {
        address: DEPLOYMENT_ADDRESS,
        messageId,
        receivedAt: now,
        mailAuditRef: { store: "audit", path: `mail/${messageId}` },
      });
      await dequeueToProcessing(store, writer, repoId, DEPLOYMENT_ADDRESS);
      await markConsumed(store, writer, repoId, {
        address: DEPLOYMENT_ADDRESS,
        messageId,
        runId: DEPLOYMENT,
        consumedAt: now,
        rejection: { code: "malformed_mail", message: "Missing MIME bytes" },
      });
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
        anchorRunId: DEPLOYMENT,
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
      expect(await h.db.select().from(workflowPendingProjection)).toEqual([]);
    });

    test("delayed terminal delivery uses the same retention deadline as projection recovery", async () => {
      const createdAt = new Date("2026-01-01T11:00:00.000Z");
      const terminalAt = "2026-01-01T12:00:00.000Z";
      const observedAt = new Date("2026-01-01T12:10:00.000Z");
      const releaseAt = "2026-01-01T12:15:00.000Z";
      await h.db
        .update(workflowRun)
        .set({
          createdAt,
          lifecyclePolicy: { capacityRetention: { failed: "15m" } },
        })
        .where(eq(workflowRun.id, DEPLOYMENT));
      const { pack, tip } = await buildPack([
        { runId: DEPLOYMENT, terminalType: "RunFailed", terminalAt },
      ]);
      expect(await receiveWith(h.db, pack, tip)).toEqual({ accepted: true });
      const projected = await h.db.query.workflowRun.findFirst({
        where: eq(workflowRun.id, DEPLOYMENT),
      });
      expect(projected?.endedAt?.toISOString()).toBe(terminalAt);

      let current = observedAt;
      const lifecycle = createWorkflowLifecycleService({
        db: h.db,
        historyReceives: createWorkflowHistoryReceiveTracker(),
        now: () => current,
        runReader: {
          listRunIds: async () => [DEPLOYMENT],
          readRunEvents: async () => [
            {
              seq: 1,
              type: "RunFailed",
              body: { at: terminalAt },
            },
          ],
          readLatestRunEvents: async () => ({
            tip: "test-tip",
            events: new Map([
              [
                DEPLOYMENT,
                { seq: 1, type: "RunFailed", body: { at: terminalAt } },
              ],
            ]),
          }),
          resolveRefTip: async () => "test-tip",
          hasRepository: async () => true,
        },
      });
      await lifecycle.reconcileNext();
      expect(
        (await lifecycle.getStatus(TENANT, DEPLOYMENT))?.capacityReleaseAt,
      ).toBe(releaseAt);

      // Recreate an accepted pack whose database projection failed.
      current = new Date(current.getTime() + 1_000);
      await h.db
        .update(workflowRun)
        .set({ status: "running", endedAt: null, capacityReleaseAt: null })
        .where(eq(workflowRun.id, DEPLOYMENT));
      await h.db.insert(workflowPendingProjection).values({
        id: "wpp_failed_projection",
        anchorRunId: DEPLOYMENT,
        createdAt: new Date(current.getTime() - 60_000),
      });
      await lifecycle.reconcileNext();
      const recovered = await h.db.query.workflowRun.findFirst({
        where: eq(workflowRun.id, DEPLOYMENT),
      });
      expect(recovered?.endedAt).toEqual(projected?.endedAt);
      expect(
        (await lifecycle.getStatus(TENANT, DEPLOYMENT))?.capacityReleaseAt,
      ).toBe(releaseAt);

      current = new Date("2026-01-01T12:14:00.000Z");
      await lifecycle.reconcileNext();
      expect(
        (await lifecycle.getStatus(TENANT, DEPLOYMENT))?.capacityReleaseAt,
      ).toBe(releaseAt);
      current = new Date(releaseAt);
      await lifecycle.reconcileNext();
      expect(
        (await lifecycle.getStatus(TENANT, DEPLOYMENT))?.allocation?.status,
      ).toBe("releasing");
    });

    test("maps RunFailed and RunCancelled to their statuses", async () => {
      await seedWorkflowRun(h.db, {
        id: "run-failed",
        anchorRunId: DEPLOYMENT,
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
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
      });
      await buildAndReceive("run-cancelled", "RunCancelled");
      const [cancelled] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-cancelled"));
      expect(cancelled?.status).toBe("cancelled");
    });

    test("flips a deployed run to failed from a lone seq-1 RunFailed tombstone", async () => {
      // The crash-loop guard's supervisor-authored RunFailed is the sole event
      // on the deployment's anchor run: seq 1, no preceding RunStarted, on a run
      // still in its pre-trigger `deployed` window. This drives that exact
      // artifact through the full pack-receive path -- not validatePush in
      // isolation -- and asserts the run's status flips to `failed`.
      await seedWorkflowRun(h.db, {
        id: "run-crashloop",
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        status: "deployed",
      });

      const { pack, tip } = await buildPack([
        {
          runId: "run-crashloop",
          terminalType: "RunFailed",
          terminalOnlyAtSeq1: true,
        },
      ]);
      const verdict = await receiveWith(h.db, pack, tip);
      expect(verdict).toEqual({ accepted: true });

      const [row] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-crashloop"));
      expect(row?.status).toBe("failed");
      expect(row?.endedAt).not.toBeNull();
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
        anchorRunId: DEPLOYMENT,
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
        anchorRunId: DEPLOYMENT,
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

    test("settles a retained signal from its internal run event log", async () => {
      const dispatchStore = createWorkflowRunDispatchStore(h.db);
      await dispatchStore.enqueueSignal({
        id: "dispatch-internal-signal",
        anchorRunId: DEPLOYMENT,
        signal: {
          agentAddress: DEPLOYMENT_ADDRESS,
          runId: "run-internal-signal",
          signalName: "approval:resolved",
          signalId: "signal-internal",
          payload: { approved: true },
        },
      });

      const { pack, tip } = await buildPack([
        { runId: "run-internal-signal", signalId: "signal-internal" },
      ]);
      const verdict = await receiveWith(h.db, pack, tip);

      expect(verdict).toEqual({ accepted: true });
      expect(
        (await dispatchStore.findById("dispatch-internal-signal"))?.status,
      ).toBe("settled");
    });

    test("preserves child acceptance across failed terminal projection and a stale pin", async () => {
      const dispatches = createWorkflowRunDispatchStore(h.db);
      await dispatches.enqueueSignal({
        id: "child-after-anchor",
        anchorRunId: DEPLOYMENT,
        signal: {
          agentAddress: DEPLOYMENT_ADDRESS,
          runId: "run-child",
          signalName: "continue",
          signalId: "child-after-anchor",
          payload: null,
        },
      });
      const before = await buildPack([
        { runId: DEPLOYMENT, terminalType: "RunCompleted" },
        { runId: "run-child" },
      ]);
      const after = await buildPack([
        { runId: DEPLOYMENT, terminalType: "RunCompleted" },
        { runId: "run-child", signalId: "child-after-anchor" },
      ]);
      const beforeStore = createAgentRepoStore({
        dataDir: await makeTempDir("dispatch-anchor-terminal-before-"),
        signingKey,
      });
      const afterStore = createAgentRepoStore({
        dataDir: await makeTempDir("dispatch-anchor-terminal-after-"),
        signingKey,
      });
      for (const [store, { pack, tip }] of [
        [beforeStore, before],
        [afterStore, after],
      ] as const) {
        await store.repoStore.initRepo({
          kind: "workflow-run",
          id: DEPLOYMENT_REPO_ID,
        });
        await store.receiveWorkflowRunPack(
          { kind: "workflow-run", id: DEPLOYMENT_REPO_ID },
          pack,
          WFR_REF,
          tip,
        );
      }

      let currentStore = beforeStore;
      let switchOnRecheck = false;
      let mainReads = 0;
      const projection = createWorkflowDispatchProjection({
        db: h.db,
        repoStore: {
          openCommittedReads: (...args) => {
            if (switchOnRecheck && args[2] === WFR_REF && ++mainReads === 2)
              currentStore = afterStore;
            return currentStore.repoStore.openCommittedReads(...args);
          },
        },
      });
      await projection.project(DEPLOYMENT);
      expect((await dispatches.findById("child-after-anchor"))?.status).toBe(
        "pending",
      );

      await h.db
        .update(workflowRun)
        .set({ status: "completed" })
        .where(eq(workflowRun.id, DEPLOYMENT));
      switchOnRecheck = true;
      mainReads = 0;
      await projection.project(DEPLOYMENT);
      expect((await dispatches.findById("child-after-anchor"))?.status).toBe(
        "pending",
      );

      switchOnRecheck = false;
      await projection.project(DEPLOYMENT);
      expect((await dispatches.findById("child-after-anchor"))?.status).toBe(
        "settled",
      );
    });

    test("fails a signal when its child run is terminal", async () => {
      const dispatches = createWorkflowRunDispatchStore(h.db);
      await dispatches.enqueueSignal({
        id: "terminal-child-signal",
        anchorRunId: DEPLOYMENT,
        signal: {
          agentAddress: DEPLOYMENT_ADDRESS,
          runId: "run-terminal-child",
          signalName: "continue",
          signalId: "terminal-child-signal",
          payload: null,
        },
      });
      const { pack, tip } = await buildPack([
        { runId: DEPLOYMENT },
        { runId: "run-terminal-child", terminalType: "RunCompleted" },
      ]);
      const store = createAgentRepoStore({
        dataDir: await makeTempDir("dispatch-terminal-child-"),
        signingKey,
      });
      await store.repoStore.initRepo({
        kind: "workflow-run",
        id: DEPLOYMENT_REPO_ID,
      });
      await store.receiveWorkflowRunPack(
        { kind: "workflow-run", id: DEPLOYMENT_REPO_ID },
        pack,
        WFR_REF,
        tip,
      );

      await createWorkflowDispatchProjection({
        db: h.db,
        repoStore: store.repoStore,
      }).project(DEPLOYMENT);
      expect(await dispatches.findById("terminal-child-signal")).toMatchObject({
        status: "failed",
        failureCode: "workflow_run_terminal",
      });
    });

    test("mail on a terminal run keeps a rejection recorded before history is final", async () => {
      const dispatches = createWorkflowRunDispatchStore(h.db);
      await dispatches.enqueue({
        id: "late-rejected-mail",
        anchorRunId: DEPLOYMENT,
        messageId: "late-rejected-mail",
        senderAddress: "sender@example.test",
        rawMessage: new Uint8Array([1]),
        stepGrants: [],
      });
      const { pack, tip } = await buildPack([
        { runId: DEPLOYMENT, terminalType: "RunCompleted" },
      ]);
      const store = createAgentRepoStore({
        dataDir: await makeTempDir("dispatch-late-rejection-"),
        signingKey,
      });
      const repoId = { kind: "workflow-run", id: DEPLOYMENT_REPO_ID } as const;
      await store.repoStore.initRepo(repoId);
      await store.receiveWorkflowRunPack(repoId, pack, WFR_REF, tip);
      // The terminal status flip has not landed, so the worker can still push
      // the events ref that records how it handled the mail.
      const projection = createWorkflowDispatchProjection({
        db: h.db,
        repoStore: store.repoStore,
      });
      await projection.project(DEPLOYMENT);
      expect((await dispatches.findById("late-rejected-mail"))?.status).toBe(
        "pending",
      );

      await recordMailRejection(store.repoStore, "late-rejected-mail");
      await projection.project(DEPLOYMENT);
      expect(await dispatches.findById("late-rejected-mail")).toMatchObject({
        status: "failed",
        failureCode: "malformed_mail",
        failureMessage: "Missing MIME bytes",
      });
    });

    test.each(["pending", "abandoned"] as const)(
      "recovers rejected mail without a workflow event ref (%s)",
      async (status) => {
        const dispatches = createWorkflowRunDispatchStore(h.db);
        await dispatches.enqueue({
          id: "rejected-mail",
          anchorRunId: DEPLOYMENT,
          messageId: "rejected-mail",
          senderAddress: "sender@example.test",
          rawMessage: new Uint8Array([1]),
          stepGrants: [],
        });
        const store = createAgentRepoStore({
          dataDir: await makeTempDir("dispatch-rejection-recovery-"),
          signingKey,
        });
        await recordMailRejection(store.repoStore, "rejected-mail");
        // The claim-check history is independent of the workflow event ref.
        await git.deleteRef({
          fs,
          dir: store.repoStore.getRepoDir({
            kind: "workflow-run",
            id: DEPLOYMENT_REPO_ID,
          }),
          ref: WFR_REF,
        });
        if (status === "abandoned") {
          await h.db.update(workflowRun).set({ status: "cancelled" });
          await h.db.update(sidecarAllocation).set({ status: "released" });
          await dispatches.abandonUnsettled(
            DEPLOYMENT,
            "workflow_cancelled",
            "Stopped before projection recovered",
          );
        }
        expect(
          await store.repoStore.openCommittedReads(
            { kind: "hub" },
            { kind: "workflow-run", id: DEPLOYMENT_REPO_ID },
            WFR_REF,
          ),
        ).toBeNull();
        const projection = createWorkflowDispatchProjection({
          db: h.db,
          repoStore: store.repoStore,
        });
        await projection.reconcileNext();
        expect(await dispatches.findById("rejected-mail")).toMatchObject({
          status: "failed",
          failureCode: "malformed_mail",
          failureMessage: "Missing MIME bytes",
        });
      },
    );

    test.each(["without", "with"] as const)(
      "scans an abandoned delivery once after final history %s pushed events",
      async (history) => {
        const dispatches = createWorkflowRunDispatchStore(h.db);
        await dispatches.enqueue({
          id: "lost-mail",
          anchorRunId: DEPLOYMENT,
          messageId: "lost-mail",
          senderAddress: "sender@example.test",
          rawMessage: new Uint8Array([1]),
          stepGrants: [],
        });
        const store = createAgentRepoStore({
          dataDir: await makeTempDir("dispatch-final-scan-"),
          signingKey,
        });
        if (history === "with") {
          const { pack, tip } = await buildPack([{ runId: DEPLOYMENT }]);
          const repoId = {
            kind: "workflow-run",
            id: DEPLOYMENT_REPO_ID,
          } as const;
          await store.repoStore.initRepo(repoId);
          await store.receiveWorkflowRunPack(repoId, pack, WFR_REF, tip);
        }
        await h.db.update(workflowRun).set({ status: "cancelled" });
        await h.db.update(sidecarAllocation).set({ status: "released" });
        await dispatches.abandonUnsettled(
          DEPLOYMENT,
          "workflow_cancelled",
          "Stopped before the delivery was consumed",
        );
        await h.db.insert(workflowPendingProjection).values({
          id: "unreconciled-receive",
          anchorRunId: DEPLOYMENT,
        });
        let reads = 0;
        const projection = createWorkflowDispatchProjection({
          db: h.db,
          repoStore: {
            openCommittedReads: (...args) => {
              reads += 1;
              return store.repoStore.openCommittedReads(...args);
            },
          },
        });

        await projection.reconcileNext();
        expect(
          (await dispatches.findById("lost-mail"))?.nextAttemptAt,
        ).not.toBeNull();

        await h.db.delete(workflowPendingProjection);
        await projection.reconcileNext();
        expect(await dispatches.findById("lost-mail")).toMatchObject({
          status: "abandoned",
          failureCode: "workflow_cancelled",
          nextAttemptAt: null,
        });

        reads = 0;
        await projection.reconcileNext();
        expect(reads).toBe(0);
      },
    );

    test.each(["completed", "running"] as const)(
      "recovers delivery outcomes after release when the database run is %s",
      async (status) => {
        const dispatches = createWorkflowRunDispatchStore(h.db);
        for (const messageId of [
          "accepted-mail",
          "resumed-mail",
          "unconsumed-mail",
          "rejected-mail",
        ]) {
          await dispatches.enqueue({
            id: messageId,
            anchorRunId: DEPLOYMENT,
            messageId,
            senderAddress: "sender@example.test",
            rawMessage: new TextEncoder().encode(messageId),
            stepGrants: [],
          });
        }
        await dispatches.enqueueSignal({
          id: "accepted-signal",
          anchorRunId: DEPLOYMENT,
          signal: {
            agentAddress: DEPLOYMENT_ADDRESS,
            runId: "internal",
            signalName: "continue",
            signalId: "accepted-signal",
            payload: null,
          },
        });
        const { pack, tip } = await buildPack([
          {
            runId: DEPLOYMENT,
            consumedMessageId: "accepted-mail",
            signalId: "resumed-mail",
            terminalType: "RunCompleted",
          },
          {
            runId: "internal",
            signalId: "accepted-signal",
            terminalType: "RunCompleted",
          },
        ]);
        const store = createAgentRepoStore({
          dataDir: await makeTempDir("dispatch-recovery-"),
          signingKey,
        });
        await recordMailRejection(store.repoStore, "rejected-mail");
        // Git was accepted before the crash, but delivery outcomes have not
        // been projected when capacity is released.
        await store.receiveWorkflowRunPack(
          { kind: "workflow-run", id: DEPLOYMENT_REPO_ID },
          pack,
          WFR_REF,
          tip,
        );
        await h.db
          .update(workflowRun)
          .set({ status })
          .where(eq(workflowRun.id, DEPLOYMENT));
        const allocations = createSidecarAllocationStore(h.db);
        expect(
          await allocations.beginRelease({
            allocationId: "allocation-terminal-flip",
            expectedStatus: "allocated",
            expectedGeneration: 1,
          }),
        ).toMatchObject({ status: "releasing", generation: 2 });
        expect(
          await allocations.markReleased({
            allocationId: "allocation-terminal-flip",
            generation: 2,
          }),
        ).toMatchObject({ status: "released" });
        const abandoned = await dispatches.listUnsettled(DEPLOYMENT);
        expect(abandoned).toHaveLength(5);
        expect(
          abandoned.every((dispatch) => dispatch.status === "abandoned"),
        ).toBe(true);
        let failRead = true;
        const projection = createWorkflowDispatchProjection({
          db: h.db,
          repoStore: {
            async openCommittedReads(...args) {
              if (failRead && args[2] === "refs/heads/events")
                throw new Error("Consumed history temporarily unreadable");
              return store.repoStore.openCommittedReads(...args);
            },
          },
        });
        await projection.reconcileNext();
        expect((await dispatches.listUnsettled(DEPLOYMENT)).length).toBe(5);
        failRead = false;
        await Promise.all([
          projection.reconcileNext(),
          projection.reconcileNext(),
        ]);
        expect((await dispatches.findById("accepted-mail"))?.status).toBe(
          "settled",
        );
        expect((await dispatches.findById("resumed-mail"))?.status).toBe(
          "settled",
        );
        expect((await dispatches.findById("accepted-signal"))?.status).toBe(
          "settled",
        );
        expect(await dispatches.findById("unconsumed-mail")).toMatchObject({
          status: "failed",
          failureCode: "workflow_run_terminal",
        });
        expect(await dispatches.findById("rejected-mail")).toMatchObject({
          status: "failed",
          failureCode: "malformed_mail",
          failureMessage: "Missing MIME bytes",
        });
        expect(await dispatches.listUnsettled(DEPLOYMENT)).toEqual([]);
        await projection.reconcileNext();
        expect((await dispatches.findById("accepted-mail"))?.status).toBe(
          "settled",
        );
        expect((await h.db.select().from(sidecarAllocation))[0]?.status).toBe(
          "released",
        );
      },
    );

    test.each(["before", "during", "after"] as const)(
      "confirmed stop %s acceptance projection preserves the actual delivery outcome",
      async (order) => {
        const dispatches = createWorkflowRunDispatchStore(h.db);
        for (const messageId of [
          "accepted-before-stop",
          "unknown-after-stop",
        ]) {
          await dispatches.enqueue({
            id: messageId,
            anchorRunId: DEPLOYMENT,
            messageId,
            senderAddress: "sender@example.test",
            rawMessage: new Uint8Array([1]),
            stepGrants: [],
          });
        }
        const { pack, tip } = await buildPack([
          { runId: DEPLOYMENT, consumedMessageId: "accepted-before-stop" },
        ]);
        const store = createAgentRepoStore({
          dataDir: await makeTempDir("dispatch-stop-race-"),
          signingKey,
        });
        const repoId = {
          kind: "workflow-run",
          id: DEPLOYMENT_REPO_ID,
        } as const;
        await store.repoStore.initRepo(repoId);
        await store.receiveWorkflowRunPack(repoId, pack, WFR_REF, tip);
        await h.db
          .insert(sidecar)
          .values({ id: "stop-worker", tokenHashSha256: new Uint8Array(32) });
        await h.db.update(sidecarAllocation).set({ sidecarId: "stop-worker" });
        const lifecycle = createWorkflowLifecycleService({
          db: h.db,
          historyReceives: createWorkflowHistoryReceiveTracker(),
          runReader: createWorkflowRunReader(store.repoStore),
          cancelGraceMs: 0,
          sendControl: async (_target, command) => {
            expect(command.action).toBe("stop");
          },
        });
        const stop = async () => {
          await lifecycle.requestCancellation(
            TENANT,
            DEPLOYMENT,
            "Stop confirmed",
          );
          await lifecycle.reconcileNext();
          expect((await lifecycle.getStatus(TENANT, DEPLOYMENT))?.status).toBe(
            "cancelled",
          );
        };
        const reading = Promise.withResolvers<undefined>();
        const release = Promise.withResolvers<undefined>();
        const projection = createWorkflowDispatchProjection({
          db: h.db,
          repoStore: {
            async openCommittedReads(...args) {
              const reads = await store.repoStore.openCommittedReads(...args);
              reading.resolve(undefined);
              if (order === "during") await release.promise;
              return reads;
            },
          },
        });
        if (order === "before") await stop();
        const projecting = projection.reconcileNext();
        try {
          if (order === "during") {
            await Promise.race([reading.promise, projecting]);
            await stop();
            expect(
              (await dispatches.findById("accepted-before-stop"))?.status,
            ).toBe("abandoned");
            release.resolve(undefined);
          }
          await projecting;
          if (order === "after") await stop();
          expect(
            (await dispatches.findById("accepted-before-stop"))?.status,
          ).toBe("settled");
          expect(await dispatches.findById("unknown-after-stop")).toMatchObject(
            { status: "abandoned", failureCode: "workflow_cancelled" },
          );
          expect(await dispatches.requeueUnsettled(DEPLOYMENT)).toBe(0);
          expect(
            await dispatches.claimNextPending({
              leaseId: "must-not-retry",
              leaseDurationMs: 1000,
            }),
          ).toBeNull();
          await projection.reconcileNext();
          expect(
            (await dispatches.findById("unknown-after-stop"))?.status,
          ).toBe("abandoned");
        } finally {
          release.resolve(undefined);
          await projecting;
        }
      },
    );

    test("recovery resumes after a database failure partway through delivery updates", async () => {
      const dispatches = createWorkflowRunDispatchStore(h.db);
      for (const messageId of ["first-accepted", "second-accepted"]) {
        await dispatches.enqueue({
          id: messageId,
          anchorRunId: DEPLOYMENT,
          messageId,
          senderAddress: "sender@example.test",
          rawMessage: new Uint8Array([1]),
          stepGrants: [],
        });
      }
      const { pack, tip } = await buildPack([
        {
          runId: DEPLOYMENT,
          consumedMessageId: "first-accepted",
          signalId: "second-accepted",
          terminalType: "RunCompleted",
        },
      ]);
      const store = createAgentRepoStore({
        dataDir: await makeTempDir("dispatch-write-retry-"),
        signingKey,
      });
      const repoId = { kind: "workflow-run", id: DEPLOYMENT_REPO_ID } as const;
      await store.repoStore.initRepo(repoId);
      await store.receiveWorkflowRunPack(repoId, pack, WFR_REF, tip);
      const projection = createWorkflowDispatchProjection({
        db: h.db,
        repoStore: store.repoStore,
      });
      await h.db
        .execute(sql`create function reject_second_settlement() returns trigger language plpgsql as $$
        begin
          if new.message_id = 'second-accepted' and new.status = 'settled' then
            raise exception 'injected settlement failure';
          end if;
          return new;
        end
      $$`);
      await h.db
        .execute(sql`create trigger reject_second_settlement before update on workflow_run_dispatch
        for each row execute function reject_second_settlement()`);
      try {
        await expect(projection.project(DEPLOYMENT)).rejects.toThrow();
        expect((await dispatches.findById("first-accepted"))?.status).toBe(
          "settled",
        );
        expect((await dispatches.findById("second-accepted"))?.status).toBe(
          "pending",
        );
      } finally {
        await h.db.execute(
          sql`drop trigger reject_second_settlement on workflow_run_dispatch`,
        );
      }
      await projection.reconcileNext();
      expect((await dispatches.findById("first-accepted"))?.status).toBe(
        "settled",
      );
      expect((await dispatches.findById("second-accepted"))?.status).toBe(
        "settled",
      );
    });

    test("new arrivals cannot keep a sweep from revisiting earlier unresolved runs", async () => {
      const dispatches = createWorkflowRunDispatchStore(h.db);
      const addRun = async (id: string) => {
        await seedWorkflowRun(h.db, {
          id,
          anchorRunId: id,
          tenantId: TENANT,
          address: `${id}@tnt.example`,
        });
        await dispatches.enqueue({
          id,
          anchorRunId: id,
          messageId: id,
          senderAddress: "sender@example.test",
          rawMessage: new Uint8Array([1]),
          stepGrants: [],
        });
      };
      await addRun("run_a");
      await addRun("run_b");
      const readIds: string[] = [];
      const projection = createWorkflowDispatchProjection({
        db: h.db,
        repoStore: {
          async openCommittedReads(_principal, repoId, ref) {
            if (ref !== WFR_REF) return null;
            readIds.push(repoId.id);
            return null;
          },
        },
      });
      await projection.reconcileNext();
      await addRun("run_c");
      await projection.reconcileNext();
      await projection.reconcileNext();
      expect(readIds).toEqual(
        ["run_a", "run_b", "run_a"].map((id) =>
          deriveWorkflowRunRepoId(`${id}@tnt.example`),
        ),
      );
    });

    test("a stalled projection keeps one bounded slot while other runs recover", async () => {
      const dispatches = createWorkflowRunDispatchStore(h.db);
      const runs = [
        { id: DEPLOYMENT, address: DEPLOYMENT_ADDRESS },
        { id: "dep-b", address: "run_dep_b@tnt.example" },
        { id: "dep-c", address: "run_dep_c@tnt.example" },
      ];
      const gates = runs.map(() => ({
        entered: Promise.withResolvers<undefined>(),
        release: Promise.withResolvers<undefined>(),
        reads: 0,
      }));
      for (const run of runs) {
        if (run.id !== DEPLOYMENT)
          await seedWorkflowRun(h.db, {
            id: run.id,
            anchorRunId: run.id,
            tenantId: TENANT,
            address: run.address,
          });
        await dispatches.enqueue({
          id: run.id,
          anchorRunId: run.id,
          messageId: run.id,
          senderAddress: "sender@example.test",
          rawMessage: new Uint8Array([1]),
          stepGrants: [],
        });
      }
      const projection = createWorkflowDispatchProjection({
        db: h.db,
        maxConcurrentProjections: 2,
        repoStore: {
          async openCommittedReads(_principal, repoId, ref) {
            if (ref !== WFR_REF) return null;
            const index = runs.findIndex(
              (run) => deriveWorkflowRunRepoId(run.address) === repoId.id,
            );
            const gate = gates[index];
            if (gate === undefined)
              throw new Error("Unexpected projection target");
            gate.reads += 1;
            gate.entered.resolve(undefined);
            await gate.release.promise;
            return null;
          },
        },
      });
      const [a, b, c] = gates;
      if (a === undefined || b === undefined || c === undefined)
        throw new Error("Missing gates");
      const first = projection.reconcileNext();
      const second = projection.reconcileNext();
      try {
        await Promise.all([a.entered.promise, b.entered.promise]);
        await projection.reconcileNext();
        expect(c.reads).toBe(0);
        b.release.resolve(undefined);
        await second;
        c.release.resolve(undefined);
        await projection.reconcileNext();
        expect(c.reads).toBe(1);
        await projection.reconcileNext();
        expect(a.reads).toBe(1);
      } finally {
        for (const gate of gates) gate.release.resolve(undefined);
        await Promise.all([first, second]);
      }
    });

    test("a stalled history read permits release and does not fail a later enqueue", async () => {
      const dispatches = createWorkflowRunDispatchStore(h.db);
      const enqueue = (messageId: string) =>
        dispatches.enqueue({
          id: messageId,
          anchorRunId: DEPLOYMENT,
          messageId,
          senderAddress: "sender@example.test",
          rawMessage: new TextEncoder().encode(messageId),
          stepGrants: [],
        });
      await enqueue("old-mail");
      const { pack, tip } = await buildPack([
        {
          runId: DEPLOYMENT,
          consumedMessageId: "later-mail",
          terminalType: "RunCompleted",
        },
      ]);
      const store = createAgentRepoStore({
        dataDir: await makeTempDir("dispatch-read-race-"),
        signingKey,
      });
      await store.repoStore.initRepo({
        kind: "workflow-run",
        id: DEPLOYMENT_REPO_ID,
      });
      await store.receiveWorkflowRunPack(
        { kind: "workflow-run", id: DEPLOYMENT_REPO_ID },
        pack,
        WFR_REF,
        tip,
      );
      await h.db
        .update(workflowRun)
        .set({ status: "completed" })
        .where(eq(workflowRun.id, DEPLOYMENT));
      const reading = Promise.withResolvers<undefined>();
      const releaseRead = Promise.withResolvers<undefined>();
      const projection = createWorkflowDispatchProjection({
        db: h.db,
        repoStore: {
          async openCommittedReads(...args) {
            const reads = await store.repoStore.openCommittedReads(...args);
            reading.resolve(undefined);
            await releaseRead.promise;
            return reads;
          },
        },
      });
      const projecting = projection.project(DEPLOYMENT);
      const result = projecting.catch((cause: unknown) => cause);
      try {
        await Promise.race([reading.promise, result]);
        await enqueue("later-mail");
        const lifecycle = createWorkflowLifecycleService({
          db: h.db,
          historyReceives: createWorkflowHistoryReceiveTracker(),
          runReader: {
            listRunIds: async () => [],
            readRunEvents: async () => {
              throw new Error("Terminal release does not need a Git read");
            },
            readLatestRunEvents: async () => {
              throw new Error("Terminal release does not need a Git read");
            },
            resolveRefTip: async () => {
              throw new Error("Terminal release does not need a Git read");
            },
            hasRepository: async () => {
              throw new Error("Terminal release does not need a Git read");
            },
          },
        });
        expect(await lifecycle.releaseCapacity(TENANT, DEPLOYMENT)).toBe(
          "pending",
        );
        await lifecycle.reconcileNext();
        expect(
          (await lifecycle.getStatus(TENANT, DEPLOYMENT))?.allocation?.status,
        ).toBe("releasing");
        releaseRead.resolve(undefined);
        expect(await result).toBeUndefined();
        expect((await dispatches.findById("old-mail"))?.status).toBe("failed");
        expect((await dispatches.findById("later-mail"))?.status).toBe(
          "pending",
        );
        await projection.project(DEPLOYMENT);
        expect((await dispatches.findById("later-mail"))?.status).toBe(
          "settled",
        );
      } finally {
        releaseRead.resolve(undefined);
        await result;
      }
    });

    test("a later database terminal status cannot close deliveries from an older live Git view", async () => {
      const dispatches = createWorkflowRunDispatchStore(h.db);
      await dispatches.enqueue({
        id: "waiting-mail",
        anchorRunId: DEPLOYMENT,
        messageId: "waiting-mail",
        senderAddress: "sender@example.test",
        rawMessage: new TextEncoder().encode("waiting"),
        stepGrants: [],
      });
      const { pack, tip } = await buildPack([{ runId: DEPLOYMENT }]);
      const store = createAgentRepoStore({
        dataDir: await makeTempDir("dispatch-old-view-"),
        signingKey,
      });
      await store.repoStore.initRepo({
        kind: "workflow-run",
        id: DEPLOYMENT_REPO_ID,
      });
      await store.receiveWorkflowRunPack(
        { kind: "workflow-run", id: DEPLOYMENT_REPO_ID },
        pack,
        WFR_REF,
        tip,
      );
      const reading = Promise.withResolvers<undefined>();
      const release = Promise.withResolvers<undefined>();
      const projection = createWorkflowDispatchProjection({
        db: h.db,
        repoStore: {
          async openCommittedReads(...args) {
            const reads = await store.repoStore.openCommittedReads(...args);
            reading.resolve(undefined);
            await release.promise;
            return reads;
          },
        },
      });
      const projecting = projection
        .project(DEPLOYMENT)
        .catch((cause: unknown) => cause);
      try {
        await Promise.race([reading.promise, projecting]);
        await h.db
          .update(workflowRun)
          .set({ status: "completed" })
          .where(eq(workflowRun.id, DEPLOYMENT));
        release.resolve(undefined);
        expect(await projecting).toBeUndefined();
        expect((await dispatches.findById("waiting-mail"))?.status).toBe(
          "pending",
        );
      } finally {
        release.resolve(undefined);
        await projecting;
      }
    });

    test("a terminal event with no run row mints the row and settles it quietly", async () => {
      // A loop iteration parked on a plain signal gate never crosses the
      // correlation register, the only other path that mints an internal run
      // row, so its terminal event is the first the hub sees of the run. A
      // missing row is ordinary bookkeeping, not a deployment-boundary
      // violation: the seam must mint the row against this deployment's anchor
      // and settle it, with no ERROR.
      const errors: string[] = [];
      const restore = installErrorCapture(errors);
      try {
        const { pack, tip } = await buildPack([
          { runId: "run-plain-gate", terminalType: "RunCompleted" },
        ]);
        const verdict = await receiveWith(h.db, pack, tip);
        expect(verdict).toEqual({ accepted: true });
      } finally {
        restore();
      }

      const [minted] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-plain-gate"));
      expect(minted?.anchorRunId).toBe(DEPLOYMENT);
      expect(minted?.tenantId).toBe(TENANT);
      // An internal run inherits its deployment's grants and owns no principal.
      expect(minted?.principalId).toBeNull();
      expect(minted?.status).toBe("completed");
      expect(minted?.endedAt).not.toBeNull();

      expect(errors).toEqual([]);
    });

    test("a terminal event for another deployment's run is ignored loudly", async () => {
      // The deployment-boundary check. A run row that EXISTS and anchors on a
      // different deployment must not be settled by this deployment's pack: the
      // seam logs at ERROR and leaves the row untouched. This is the case the
      // missing-row mint above must not swallow.
      await seedWorkflowRun(h.db, {
        id: FOREIGN_DEPLOYMENT,
        anchorRunId: FOREIGN_DEPLOYMENT,
        tenantId: TENANT,
        address: FOREIGN_DEPLOYMENT_ADDRESS,
      });
      await seedWorkflowRun(h.db, {
        id: "run-foreign",
        anchorRunId: FOREIGN_DEPLOYMENT,
        tenantId: TENANT,
      });

      const errors: string[] = [];
      const restore = installErrorCapture(errors);
      try {
        const { pack, tip } = await buildPack([
          { runId: "run-foreign", terminalType: "RunCompleted" },
        ]);
        const verdict = await receiveWith(h.db, pack, tip);
        expect(verdict).toEqual({ accepted: true });
      } finally {
        restore();
      }

      // The foreign row kept its status, its end time, and its anchor.
      const [foreign] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "run-foreign"));
      expect(foreign?.status).toBe("running");
      expect(foreign?.endedAt).toBeNull();
      expect(foreign?.anchorRunId).toBe(FOREIGN_DEPLOYMENT);

      // The boundary violation surfaced loudly, naming the run and the
      // deployment that tried to settle it.
      expect(errors).toHaveLength(1);
      expect(errors[0]).toContain("run-foreign");
      expect(errors[0]).toContain(DEPLOYMENT);
    });

    test("markTerminal flips a running run once and is a no-op thereafter", async () => {
      // Direct store test of the `status = 'running'` guard -- the property the
      // pack-receive seam relies on for idempotency. This is the test that dies
      // if the guard is removed: a second flip on an already-completed row must
      // match nothing and mutate nothing.
      await seedWorkflowRun(h.db, {
        id: "run-guard",
        anchorRunId: DEPLOYMENT,
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

    test("markTerminal settles a deployed run torn down before its first trigger", async () => {
      // A deployment can be torn down while still in its "deployed" (pre-
      // trigger) window. The live-status guard must accept "deployed" so the
      // teardown settles the anchor instead of leaving it live forever.
      await seedWorkflowRun(h.db, {
        id: "run-deployed",
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        status: "deployed",
      });
      const store = createWorkflowRunStore(h.db);

      const endedAt = new Date();
      const won = await store.markTerminal(
        "run-deployed",
        "cancelled",
        endedAt,
      );
      expect(won).not.toBeNull();
      expect(won?.status).toBe("cancelled");
      expect(won?.endedAt).toEqual(endedAt);
    });

    test("a receive that fails before reaching Git clears its pending projection", async () => {
      const failLockDb = new Proxy(h.db, {
        get(target, prop, receiver) {
          if (prop === "transaction")
            return (): never => {
              throw new Error("injected lock failure");
            };
          return Reflect.get(target, prop, receiver);
        },
      });
      const { pack, tip } = await buildPack([
        { runId: DEPLOYMENT, terminalType: "RunCompleted" },
      ]);
      expect(await receiveWith(failLockDb, pack, tip)).toEqual({
        accepted: false,
        reason: "corrupt",
      });
      expect(await h.db.select().from(workflowPendingProjection)).toEqual([]);
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
          anchorRunId: DEPLOYMENT,
          tenantId: TENANT,
          principalId: `prn-${runId}`,
        });
      }

      // The first transaction fences pack ingestion under the allocation lock.
      // Throw from the following transaction, which is the first terminal-row
      // projection, then let the second run's projection commit normally.
      let transactionCalls = 0;
      const failFirstTxDb = new Proxy(h.db, {
        get(target, prop, receiver) {
          if (prop === "transaction") {
            return (
              ...args: Parameters<DB["db"]["transaction"]>
            ): ReturnType<DB["db"]["transaction"]> => {
              transactionCalls += 1;
              if (transactionCalls === 2) {
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
      const repoStore = createAgentRepoStore({
        dataDir: await makeTempDir("wfr-failed-flip-"),
        signingKey,
      });
      const verdict = await receiveWith(failFirstTxDb, pack, tip, repoStore);

      // The pack was acked despite the mid-loop throw.
      expect(verdict).toEqual({ accepted: true });
      // Ingestion and both run projections were attempted. With no deliveries
      // outstanding, delivery projection needs no write transaction.
      expect(transactionCalls).toBe(3);

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

      // The failed flip left a durable record, and recovery settles the stuck
      // run from the accepted history instead of a manual flip.
      expect(await h.db.select().from(workflowPendingProjection)).toHaveLength(
        1,
      );
      const lifecycle = createWorkflowLifecycleService({
        db: h.db,
        historyReceives: createWorkflowHistoryReceiveTracker(),
        runReader: createWorkflowRunReader(repoStore.repoStore),
      });
      expect(await lifecycle.releaseCapacity(TENANT, DEPLOYMENT)).toBe("live");
      const recovered = await h.db
        .select({ id: workflowRun.id, status: workflowRun.status })
        .from(workflowRun)
        .where(inArray(workflowRun.id, ["run-a", "run-b"]));
      expect(recovered.map((row) => row.status)).toEqual([
        "completed",
        "completed",
      ]);
      const runPrincipals = await h.db
        .select({ status: principal.status })
        .from(principal)
        .where(inArray(principal.id, ["prn-run-a", "prn-run-b"]));
      expect(runPrincipals.map((row) => row.status)).toEqual([
        "deactivated",
        "deactivated",
      ]);
      expect(await h.db.select().from(workflowPendingProjection)).toEqual([]);
    });
  },
);
