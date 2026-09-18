// Unit-style smoke tests for the Phase I helpers landed alongside the
// integration-test fixture. These tests stand up only the hub
// substrate (no sidecar subprocess, no mock inference) so the helper
// surfaces that operate purely on the substrate (workflow-run repo
// reads, signal injection, processing-crash simulation) can be
// exercised in isolation. The end-to-end integration tests in the
// Phase I commit set exercise the helpers against the full env.

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

// `startHub` is the slice of `startDeployFlowEnv` that owns just the
// hub-substrate + WS server. The helpers we smoke-test here operate
// entirely on the substrate; standing up the sidecar subprocess would
// add minutes of startup without exercising any path under test.
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
    // The fields below are unused by the helpers exercised here.
    // Construct narrow stand-ins so the env-shape type is satisfied
    // without spinning up the corresponding subsystems.
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
    // No sidecar subprocess is spawned here, so there is nothing to register
    // and no diagnostics for `teardown` to dump.
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
    // The helper now drives the production hub -> sidecar ->
    // supervisor -> workflow-process pipeline rather than writing a
    // SignalReceived blob directly to the hub substrate. Routing the
    // signal through the child preserves the workflow-run repo's
    // single-writer invariant on the sidecar side -- without it, a
    // host-side substrate write would race against the next pack push
    // from the child and surface `non_fast_forward` on the hub. The
    // smoke env has no sidecar registered against the deployment
    // address, so the helper surfaces the routing error verbatim.
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

    // Surface the resulting tree via the substrate's getRepoDir +
    // isomorphic-git so the test is decoupled from the kind handler's
    // private path-construction helpers.
    const repoDir = env.hub.agentRepoStore.repoStore.getRepoDir({
      kind: "workflow-run",
      id: DEPLOYMENT_ID,
    });
    const git = await import("isomorphic-git");
    // The claim-check primitives target the workflow-run kind
    // handler's canonical claim-check ref (`refs/heads/events`); the
    // smoke test peeks at the resulting tree on that ref to assert
    // the processing entry landed.
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

// The in-flight wait registry is what `startDeployFlowEnv`'s `teardown()`
// gates its sidecar-diagnostics dump on, so these tests are what make that
// gate trustworthy. They exercise the registry directly rather than through
// `startDeployFlowEnv`, whose sidecar subprocess does not belong in the unit
// lane; `teardown()` adds only the write of the report these tests assert on.
//
// A wedged test is reproduced here by leaving a wait in flight across the
// assertion, which is the state the runner's per-test budget leaves behind:
// bun abandons the test body's promise, and nothing aborts the poll loop the
// body was suspended in, so the helper never reaches its `finally`.
describe("in-flight wait registry", () => {
  test("reports a wait that is still in flight, naming the helper", async () => {
    const mark = currentWaitMark();
    let ready = false;
    const inFlight = waitFor(() => ready);

    const report = renderOutstandingWaitReport(mark);
    expect(report).not.toBeNull();
    expect(report).toContain("waitFor");
    // The label carries the predicate source, which is the only thing that
    // distinguishes one bare `waitFor` from the several a test file makes.
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

    // A wedged wait from an earlier env in this worker never deregisters, and
    // `--no-isolate` shares this module's registry across the files a worker
    // runs. The mark is what keeps that record from being reported against a
    // later env's teardown.
    const mark = currentWaitMark();
    expect(renderOutstandingWaitReport(mark)).toBeNull();

    ready = true;
    await inFlight;
  });
});

// Stopping the waits teardown found still in flight. `startDeployFlowEnv`'s
// `teardown()` calls `stopOutstandingWaits(waitMark)` and then dismantles the
// env, so these tests drive the same call in the same order and assert what
// the abandoned wait does on the far side of it.
describe("stopping in-flight waits at teardown", () => {
  test("stops a wait left in flight, naming it in the error", async () => {
    const mark = currentWaitMark();
    // Never set true, which is the state a wedged test leaves. Read through
    // an object so the name survives into the label: the transpiler folds a
    // `const false` into its value, taking the name out of the source the
    // label is rendered from.
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
  // `waitForWorkflowRunComplete` kept polling through teardown, and the read
  // it made after `deployments.clear()` reported that the test had forgotten
  // to register its deployment. The wait is stopped ahead of that read, so
  // the error names the teardown rather than an innocent-looking omission.
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

  // A quiescence wait is the case where running on would be worse than a
  // misleading error: teardown kills the sidecar, which is exactly the
  // "no pack for quietMs" condition the wait exits on, so an unstopped one
  // would report the pipeline drained when nothing drained it.
  test("stops a quiescence wait instead of letting teardown satisfy it", async () => {
    const mark = currentWaitMark();
    // An hour of quiet no run reaches, so the stop below is the only thing
    // that can end this wait. It is the helper's own parameter, not a bound
    // on the test: a slower machine makes nothing here fail.
    const quietMs = 3_600_000;
    const inFlight = settleWorkflowRunPacks(env, { quietMs });

    stopOutstandingWaits(mark);

    await expect(inFlight).rejects.toThrow(
      new RegExp(
        `torn down while settleWorkflowRunPacks\\(quietMs=${String(quietMs)}\\) was still in flight`,
      ),
    );
  });

  // A loop that consults neither the seam `retrying` hands it nor one of the
  // registered helpers is a loop the stop cannot reach. What it can do is
  // refuse the result: a loop that finishes after teardown read its answer out
  // of an env that no longer exists.
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

  // The failure this exists for: a retry loop that polls on its own, through
  // no registered helper, kept iterating past the stop, and the read it made
  // after `deployments.clear()` reported that the test had forgotten to
  // register its deployment -- naming a cause that has nothing to do with the
  // failure. `checkTornDown` at the top of the loop body is the seam that ends
  // it, ahead of that read.
  test("stops a retry loop that checks the seam at the top of its body", async () => {
    // Stand-in for the env's `deployments`, cleared right after the stop
    // because that is the order `startDeployFlowEnv`'s `teardown()` uses.
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

  // The contrast case: a retry loop whose inner poll IS one of the registered
  // helpers is reached through that helper, and needs no seam of its own.
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

// The link between a reconnect test's `sidecarEnv` and the delay the spawned
// sidecar's hub link actually uses. The fixture shortens the backoff by
// default, so a test whose recovery must run through the production cycle
// depends on its override reaching the subprocess env -- and nothing else
// reports which of the two delays is in effect. Reading the env the fixture
// would pass needs no subprocess and no hub.
//
// The value the sidecar does with it is pinned on the far side of the seam:
// `parseReconnectDelayMs` in `apps/sidecar/src/config.test.ts` turns the
// string into the option, and `hub-link.test.ts` asserts the option is the
// delay the reconnect scheduler receives.
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

// The guard `startDeployFlowEnv` applies to a caller's `sidecarEnv`. Composed
// against the real env builder rather than a hand-written map, so a break in
// the merge reaches this test the same way it would reach a survival test.
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
    // The env a broken `sidecarEnv` hop produces: the fixture's own default
    // survives and looks deliberate, which is the whole reason the caller's
    // claim has to be checked rather than assumed.
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

  // The seventy-plus files that pass no `sidecarEnv` at all reach the guard
  // with no keys, so it has nothing to check. `startDeployFlowEnv` skips the
  // call on that path as well; this pins that an empty claim is vacuous
  // rather than a failure, so neither layer can fire on the common path.
  test("passes vacuously when the caller pinned nothing", () => {
    expect(() =>
      assertPinnedSidecarEnvReached({}, buildSidecarSubprocessEnv(baseOpts)),
    ).not.toThrow();
  });

  // Every real caller's pin reaches the subprocess for one structural
  // reason: the `extraEnv` spread is the last entry in the built map, so no
  // fixture-owned key can be written over it. Overriding all of them at once
  // pins that ordering, which is what the guard passing for a real caller
  // depends on -- and what it would report if the spread ever moved.
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
