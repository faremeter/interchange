// Unit-style smoke tests for the Phase I helpers: only the hub substrate is
// stood up (no sidecar subprocess, no mock inference), so the substrate-only
// helper surfaces (repo reads, signal injection, processing-crash simulation)
// are exercised in isolation. The e2e tests exercise them against the full env.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";

import {
  PRODUCTION_RECONNECT_DELAY_MS,
  WORKFLOW_RUN_TERMINAL_TYPES,
  assertPinnedSidecarEnvReached,
  buildSidecarSubprocessEnv,
  currentWaitMark,
  injectSignal,
  readWorkflowRunEvents,
  renderOutstandingWaitReport,
  retrying,
  settleWorkflowRunPacks,
  simulateProcessingCrash,
  startHub,
  stopOutstandingWaits,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type DeploymentHandle,
  type HubEnv,
} from "./deploy-flow-env";

import fs from "node:fs";

const DEPLOYMENT_ID = "run_smoke-test";
const MAIL_ADDRESS = "run_smoke-test@integration.interchange";

// `startHub` owns just the hub-substrate + WS server; the helpers under test
// operate entirely on the substrate, so the sidecar subprocess would add
// minutes of startup without exercising any path.
async function startSmokeEnv(): Promise<{
  env: DeployFlowEnv;
  hub: HubEnv;
  tempDirs: string[];
}> {
  const tempDirs: string[] = [];
  const registerTempDir = (dir: string): void => {
    tempDirs.push(dir);
  };
  const hub = await startHub(registerTempDir);
  const deployments = new Map<string, DeploymentHandle>();
  const registerDeployment = (handle: DeploymentHandle): void => {
    if (deployments.has(handle.anchorRunId)) {
      throw new Error(
        `smoke env: deployment ${handle.anchorRunId} already registered`,
      );
    }
    deployments.set(handle.anchorRunId, handle);
  };
  const env: DeployFlowEnv = {
    hub,
    // Narrow stand-ins for subsystems the substrate-only helpers never use.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- inference is not consulted by the substrate-only helpers under test
    inference: {
      server: { stop: () => undefined },
      requests: [],
    } as unknown as DeployFlowEnv["inference"],
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- sidecar is not consulted by the substrate-only helpers under test
    sidecar: {
      proc: { kill: () => undefined },
      dataDir: "",
      stderr: [],
    } as unknown as DeployFlowEnv["sidecar"],
    sidecarDiagnostics: () => "",
    deployments,
    registerDeployment,
    // No sidecar is spawned, so there is nothing to register or report.
    registerSidecar: () => undefined,
    retrying,
    teardown: async () => {
      await hub.server.stop(true);
      for (const d of tempDirs.splice(0)) {
        await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
          /* best effort cleanup */
        });
      }
    },
  };
  return { env, hub, tempDirs };
}

let env: DeployFlowEnv;

beforeAll(async () => {
  ({ env } = await startSmokeEnv());
  env.registerDeployment({
    anchorRunId: DEPLOYMENT_ID,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the smoke tests do not exercise the workflow-definition shape; the helpers only consult `workflowRunRepoId`/`workflowRunRef`
    workflowDefinition: {
      id: "wf_smoke",
      stepOrder: [],
    } as unknown as DeploymentHandle["workflowDefinition"],
    workflowRunRepoId: { kind: "workflow-run", id: DEPLOYMENT_ID },
    workflowRunRef: "refs/heads/main",
    mailAddress: MAIL_ADDRESS,
  });
});

afterAll(async () => {
  await env.teardown();
});

describe("deploy-flow-env helpers smoke tests", () => {
  test("WORKFLOW_RUN_TERMINAL_TYPES matches the kind handler's vocabulary", () => {
    expect(WORKFLOW_RUN_TERMINAL_TYPES.has("RunCompleted")).toBe(true);
    expect(WORKFLOW_RUN_TERMINAL_TYPES.has("RunFailed")).toBe(true);
    expect(WORKFLOW_RUN_TERMINAL_TYPES.has("RunCancelled")).toBe(true);
    expect(WORKFLOW_RUN_TERMINAL_TYPES.has("RunStarted")).toBe(false);
  });

  test("readWorkflowRunEvents returns an empty array before any commit lands", async () => {
    const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, "run-1");
    expect(events).toEqual([]);
  });

  test("injectSignal routes the wire frame through the hub router to the deployment sidecar", async () => {
    // The helper routes through the production hub -> sidecar -> supervisor ->
    // workflow-process pipeline, preserving the workflow-run repo's
    // single-writer invariant on the sidecar side (a host-side write would
    // race the next pack push and surface `non_fast_forward`). The smoke env
    // has no sidecar registered, so the routing error surfaces verbatim.
    await expect(
      injectSignal(env, DEPLOYMENT_ID, "run-2", "operator.ack", { ok: true }),
    ).rejects.toThrow(/No sidecar connected/);

    const after = await readWorkflowRunEvents(env, DEPLOYMENT_ID, "run-2");
    expect(after).toEqual([]);
  });

  test("waitForWorkflowRunComplete throws on timeout when no terminal event lands", async () => {
    await expect(
      waitForWorkflowRunComplete(env, DEPLOYMENT_ID, "run-3", {
        timeoutMs: 100,
      }),
    ).rejects.toThrow(/timed out/);
  });

  test("simulateProcessingCrash composes enqueueInbox + dequeueToProcessing", async () => {
    const address = "run_smoke-test@integration.interchange";
    const messageId = "<smoke-crash-1@integration.interchange>";
    const receivedAt = 1_700_000_000_000;
    await simulateProcessingCrash(
      env,
      DEPLOYMENT_ID,
      address,
      messageId,
      receivedAt,
    );

    // Surface the tree via getRepoDir + isomorphic-git, decoupled from the
    // kind handler's private path construction.
    const repoDir = env.hub.agentRepoStore.repoStore.getRepoDir({
      kind: "workflow-run",
      id: DEPLOYMENT_ID,
    });
    const git = await import("isomorphic-git");
    // Peek at the claim-check ref (`refs/heads/events`) to assert the
    // processing entry landed.
    const oid = await git.default.resolveRef({
      fs,
      dir: repoDir,
      ref: "refs/heads/events",
    });
    const tree = await git.default.readTree({
      fs,
      dir: repoDir,
      oid,
      filepath: `addresses/${encodeURIComponent(address)}/processing`,
    });
    const filenames = tree.tree.map((e) => e.path);
    expect(filenames).toContain(`${String(receivedAt)}-${messageId}.json`);
  });
});

// `teardown()` gates its diagnostics dump on the in-flight wait registry, so
// these tests exercise the registry directly (the sidecar subprocess does not
// belong in the unit lane). A wedged test is reproduced by leaving a wait in
// flight across the assertion -- the state the runner's budget leaves behind:
// bun abandons the body's promise and nothing aborts its poll loop.
describe("in-flight wait registry", () => {
  test("reports a wait that is still in flight, naming the helper", async () => {
    const mark = currentWaitMark();
    let ready = false;
    const inFlight = waitFor(() => ready);

    const report = renderOutstandingWaitReport(mark);
    expect(report).not.toBeNull();
    expect(report).toContain("waitFor");
    // The label carries the predicate source, the only distinguisher between
    // a file's several bare `waitFor` calls.
    expect(report).toContain("ready");

    ready = true;
    await inFlight;
  });

  test("reports nothing once every wait has returned", async () => {
    const mark = currentWaitMark();
    await waitFor(() => true);
    expect(renderOutstandingWaitReport(mark)).toBeNull();
  });

  test("reports nothing for a wait that threw", async () => {
    const mark = currentWaitMark();
    await expect(
      waitFor(() => {
        throw new Error("predicate blew up");
      }),
    ).rejects.toThrow("predicate blew up");
    expect(renderOutstandingWaitReport(mark)).toBeNull();
  });

  test("reports only the waits still in flight when several overlap", async () => {
    const mark = currentWaitMark();
    let firstReady = false;
    let secondReady = false;
    const first = waitFor(() => firstReady);
    const second = waitFor(() => secondReady);

    firstReady = true;
    await first;

    const report = renderOutstandingWaitReport(mark);
    expect(report).not.toBeNull();
    expect(report).toContain("secondReady");
    expect(report).not.toContain("firstReady");

    secondReady = true;
    await second;
    expect(renderOutstandingWaitReport(mark)).toBeNull();
  });

  test("names the run an env-taking wait is blocked on", async () => {
    const mark = currentWaitMark();
    const inFlight = waitForWorkflowRunComplete(env, DEPLOYMENT_ID, "run-4", {
      timeoutMs: 100,
    });

    expect(renderOutstandingWaitReport(mark)).toContain(
      `waitForWorkflowRunComplete(${DEPLOYMENT_ID}/run-4)`,
    );

    await expect(inFlight).rejects.toThrow(/timed out/);
    expect(renderOutstandingWaitReport(mark)).toBeNull();
  });

  test("reports a retry loop still running inside env.retrying", async () => {
    const mark = currentWaitMark();
    let release = (): void => undefined;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    const inFlight = env.retrying("signal-then-read", async () => {
      await released;
      return "done";
    });

    expect(renderOutstandingWaitReport(mark)).toContain(
      "retrying(signal-then-read)",
    );

    release();
    expect(await inFlight).toBe("done");
    expect(renderOutstandingWaitReport(mark)).toBeNull();
  });

  test("reports nothing for a retry loop that threw", async () => {
    const mark = currentWaitMark();
    await expect(
      env.retrying("throws", () => Promise.reject(new Error("loop blew up"))),
    ).rejects.toThrow("loop blew up");
    expect(renderOutstandingWaitReport(mark)).toBeNull();
  });

  test("excludes a wait that was already in flight when the mark was taken", async () => {
    let ready = false;
    const inFlight = waitFor(() => ready);

    // A wedged wait from an earlier env never deregisters, and `--no-isolate`
    // shares the registry across a worker's files; the mark keeps that record
    // from being reported against a later env's teardown.
    const mark = currentWaitMark();
    expect(renderOutstandingWaitReport(mark)).toBeNull();

    ready = true;
    await inFlight;
  });
});

// `teardown()` calls `stopOutstandingWaits(waitMark)` then dismantles the env;
// these tests drive the same call in the same order and assert what an
// abandoned wait does on the far side of it.
describe("stopping in-flight waits at teardown", () => {
  test("stops a wait left in flight, naming it in the error", async () => {
    const mark = currentWaitMark();
    // Read through an object so the name survives into the label: the
    // transpiler folds a `const false` into its value, taking the name out of
    // the source the label is rendered from.
    const gate = { ready: false };
    const inFlight = waitFor(() => gate.ready);

    const report = stopOutstandingWaits(mark);

    // The report is what diagnosed the real wedge, so the call that stops the
    // waits is also the call that renders them.
    expect(report).toContain("waitFor");
    expect(report).toContain("gate.ready");
    await expect(inFlight).rejects.toThrow(
      /torn down while waitFor\(.*gate.ready.*\) was still in flight/,
    );
    // The stopped wait ran its `finally` on the way out, like every other
    // exit from a helper.
    expect(renderOutstandingWaitReport(mark)).toBeNull();
  });

  // The failure this exists for: a run whose budget lapsed inside
  // `waitForWorkflowRunComplete` kept polling through teardown, and its read
  // after `deployments.clear()` reported a missing registration. The wait is
  // stopped ahead of that read, so the error names the teardown instead.
  test("stops a run wait before it reads a deployment teardown cleared", async () => {
    const anchorRunId = "run_stopped-wait";
    const registered = env.deployments.get(DEPLOYMENT_ID);
    if (registered === undefined) {
      throw new Error(`smoke env: ${DEPLOYMENT_ID} is not registered`);
    }
    env.registerDeployment({ ...registered, anchorRunId });

    const mark = currentWaitMark();
    const inFlight = waitForWorkflowRunComplete(env, anchorRunId, "run-5");

    const report = stopOutstandingWaits(mark);
    env.deployments.delete(anchorRunId);

    expect(report).toContain(
      `waitForWorkflowRunComplete(${anchorRunId}/run-5)`,
    );
    await expect(inFlight).rejects.toThrow(
      new RegExp(
        `torn down while waitForWorkflowRunComplete\\(${anchorRunId}/run-5\\) was still in flight`,
      ),
    );
  });

  // A quiescence wait exits on "no pack for quietMs", and teardown killing the
  // sidecar produces exactly that -- so an unstopped one would report the
  // pipeline drained when nothing drained it.
  test("stops a quiescence wait instead of letting teardown satisfy it", async () => {
    const mark = currentWaitMark();
    // An hour of quiet no run reaches, so the stop below is the only thing
    // that can end the wait.
    const quietMs = 3_600_000;
    const inFlight = settleWorkflowRunPacks(env, { quietMs });

    stopOutstandingWaits(mark);

    await expect(inFlight).rejects.toThrow(
      new RegExp(
        `torn down while settleWorkflowRunPacks\\(quietMs=${String(quietMs)}\\) was still in flight`,
      ),
    );
  });

  // A loop the stop cannot reach (it consults neither the seam nor a
  // registered helper) can still be refused: a loop that finishes after
  // teardown read its answer out of an env that no longer exists.
  test("refuses the result of a retry loop that finished after the stop", async () => {
    const mark = currentWaitMark();
    let release = (): void => undefined;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    const inFlight = env.retrying("finishes after teardown", async () => {
      await released;
      return "done";
    });

    stopOutstandingWaits(mark);
    release();

    await expect(inFlight).rejects.toThrow(
      /torn down while retrying\(finishes after teardown\) was still in flight/,
    );
  });

  // The failure this exists for: a self-polling retry loop kept iterating past
  // the stop, and its read after `deployments.clear()` reported a missing
  // registration -- a cause unrelated to the failure. `checkTornDown` at the
  // top of the loop body ends it ahead of that read.
  test("stops a retry loop that checks the seam at the top of its body", async () => {
    // Stand-in for the env's `deployments`, cleared right after the stop in
    // the order `teardown()` uses.
    const deployments = new Map<string, string>([["run_x", "handle"]]);
    let iterations = 0;

    const mark = currentWaitMark();
    const inFlight = env.retrying(
      "re-fire until a run appears",
      async (checkTornDown) => {
        for (;;) {
          checkTornDown();
          iterations += 1;
          if (!deployments.has("run_x")) {
            throw new Error(
              "deploy-flow env: no deployment registered for run_x; " +
                "call deployWorkflowSourceForTest or registerDeployment first",
            );
          }
          await new Promise((r) => setTimeout(r, 20));
        }
      },
    );

    const report = stopOutstandingWaits(mark);
    expect(report).toContain("retrying(re-fire until a run appears)");
    const iterationsAtStop = iterations;

    // teardown's very next statement.
    deployments.clear();

    await expect(inFlight).rejects.toThrow(
      /torn down while retrying\(re-fire until a run appears\) was still in flight/,
    );
    // The loop performed no further pass: the seam threw ahead of the read
    // that would have reported the cleared map as a missing registration.
    expect(iterations).toBe(iterationsAtStop);
  });

  // Contrast: a retry loop whose inner poll is a registered helper is reached
  // through that helper and needs no seam of its own.
  test("stops a retry loop that polls through a registered helper", async () => {
    const gate = { ready: false };
    const mark = currentWaitMark();
    const inFlight = env.retrying("polls through waitFor", () =>
      waitFor(() => gate.ready),
    );

    stopOutstandingWaits(mark);

    await expect(inFlight).rejects.toThrow(/torn down while waitFor/);
    expect(renderOutstandingWaitReport(mark)).toBeNull();
  });

  test("reports nothing and stops nothing when every wait has returned", async () => {
    const mark = currentWaitMark();
    await waitFor(() => true);

    expect(stopOutstandingWaits(mark)).toBeNull();

    // A wait that returned is deregistered, so the stop cannot reach it --
    // and it leaves nothing behind that a later wait inherits.
    await waitFor(() => true);
    await env.retrying("after the stop", () => Promise.resolve("done"));
  });

  test("leaves a wait registered before the mark running", async () => {
    let ready = false;
    const inFlight = waitFor(() => ready);

    // The same fence the report uses: a wedged wait from an earlier env in
    // this worker is not this env's to stop.
    stopOutstandingWaits(currentWaitMark());

    ready = true;
    await inFlight;
  });
});

// The fixture shortens the reconnect backoff by default, so a test whose
// recovery must run through the production cycle depends on its `sidecarEnv`
// override reaching the subprocess env -- and nothing else reports which delay
// is in effect. Reading the env the fixture would pass needs no subprocess.
//
// The far side of the seam is pinned in `apps/sidecar/src/config.test.ts`
// (`parseReconnectDelayMs`) and `hub-link.test.ts`.
describe("spawned sidecar reconnect delay", () => {
  const baseOpts = { hubPort: 4321, dataDir: "/sidecar-data" };

  test("defaults to the short test backoff", () => {
    const env = buildSidecarSubprocessEnv(baseOpts);
    expect(env["SIDECAR_RECONNECT_DELAY_MS"]).toBe("250");
  });

  test("carries the production backoff when a test pins it", () => {
    const env = buildSidecarSubprocessEnv({
      ...baseOpts,
      extraEnv: { SIDECAR_RECONNECT_DELAY_MS: PRODUCTION_RECONNECT_DELAY_MS },
    });
    expect(env["SIDECAR_RECONNECT_DELAY_MS"]).toBe("3000");
  });
});

// Composed against the real env builder rather than a hand-written map, so a
// break in the merge reaches this test the same way it would reach a survival
// test.
describe("assertPinnedSidecarEnvReached", () => {
  const baseOpts = { hubPort: 4321, dataDir: "/sidecar-data" };
  const pinProduction = {
    SIDECAR_RECONNECT_DELAY_MS: PRODUCTION_RECONNECT_DELAY_MS,
  };

  test("accepts a pin the built env carries", () => {
    const env = buildSidecarSubprocessEnv({
      ...baseOpts,
      extraEnv: pinProduction,
    });
    expect(() =>
      assertPinnedSidecarEnvReached(pinProduction, env),
    ).not.toThrow();
  });

  test("throws when the pin never reached the env, naming both values", () => {
    // A broken `sidecarEnv` hop leaves the fixture's own default, which looks
    // deliberate -- the reason the caller's claim must be checked.
    const env = buildSidecarSubprocessEnv(baseOpts);
    expect(() => assertPinnedSidecarEnvReached(pinProduction, env)).toThrow(
      /SIDECAR_RECONNECT_DELAY_MS: pinned 3000, subprocess env has 250/,
    );
  });

  test("throws when the variable is absent from the env entirely", () => {
    expect(() => assertPinnedSidecarEnvReached(pinProduction, {})).toThrow(
      /SIDECAR_RECONNECT_DELAY_MS: pinned 3000, subprocess env has no value/,
    );
  });

  // The guard is keyed off the caller's variables, not off any particular
  // one, so pinning something the reconnect chain knows nothing about is
  // checked on the same terms.
  test("checks a pinned variable unrelated to the reconnect delay", () => {
    const pinned = { SIDECAR_WORKFLOW_RUN_SHADOW: "1" };
    expect(() =>
      assertPinnedSidecarEnvReached(
        pinned,
        buildSidecarSubprocessEnv({ ...baseOpts, extraEnv: pinned }),
      ),
    ).not.toThrow();
    expect(() =>
      assertPinnedSidecarEnvReached(
        pinned,
        buildSidecarSubprocessEnv(baseOpts),
      ),
    ).toThrow(
      /SIDECAR_WORKFLOW_RUN_SHADOW: pinned 1, subprocess env has no value/,
    );
  });

  // Files that pass no `sidecarEnv` reach the guard with no keys;
  // `startDeployFlowEnv` skips the call on that path too. An empty claim is
  // vacuous, not a failure.
  test("passes vacuously when the caller pinned nothing", () => {
    expect(() =>
      assertPinnedSidecarEnvReached({}, buildSidecarSubprocessEnv(baseOpts)),
    ).not.toThrow();
  });

  // The `extraEnv` spread is the last entry in the built map, so no
  // fixture-owned key can be written over it; overriding all of them pins that
  // ordering, which the guard passing for a real caller depends on.
  test("a pin of every fixture-owned variable reaches the env", () => {
    const pinned = {
      PATH: "/sentinel/path",
      HOME: "/sentinel/home",
      TMPDIR: "/sentinel/tmp",
      HUB_WS_URL: "ws://sentinel/ws",
      SIDECAR_ID: "sc-sentinel",
      SIDECAR_TOKEN: "token-sentinel",
      SIDECAR_DATA_DIR: "/sentinel/data",
      SIDECAR_CREDENTIAL_ENCRYPTION_KEY: "ff".repeat(32),
      SIDECAR_RECONNECT_DELAY_MS: "1234",
    };
    expect(() =>
      assertPinnedSidecarEnvReached(
        pinned,
        buildSidecarSubprocessEnv({ ...baseOpts, extraEnv: pinned }),
      ),
    ).not.toThrow();
  });

  test("reports every mismatch, not just the first", () => {
    expect(() =>
      assertPinnedSidecarEnvReached(
        { SIDECAR_RECONNECT_DELAY_MS: "3000", SIDECAR_ID: "sc-other" },
        buildSidecarSubprocessEnv(baseOpts),
      ),
    ).toThrow(/SIDECAR_RECONNECT_DELAY_MS: .*; SIDECAR_ID: /);
  });
});
