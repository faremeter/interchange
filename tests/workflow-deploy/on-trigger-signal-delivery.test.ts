// onTrigger body author-signal delivery integration test (signal-relay).
//
// The reachability gate for the CORE signal-relay capability: a deployed
// onTrigger section whose body parks on an author `awaitSignal` gate has that
// gate serviced by delivering the signal to the DEPLOYMENT (parent) run id --
// the body, a child run, never receives a signal directly. It proves the whole
// path end-to-end on the real deploy stack: the body's author gate surfaces up
// through the suspendable-child seam, the section proxies it as a signal-relay
// await on the container, a signal delivered to the parent run resolves that
// await, and the section relays it back down into the body, which continues.
//
// The workflow is deployed BY SOURCE-REF (bundle a source entry module into a
// hub asset, probe it, approve+freeze it against a real DB, deploy the
// source-ref frame): a single-step workflow whose one step is an `onTrigger`
// section subscribed to the deployment mail address, with a NON-AGENT body -- a
// single `awaitSignal({ name })` gate with no timeout. (Body agent-step
// execution is not wired yet, INTR-310; the signal-relay feature is non-agent
// by nature, so the body IS an awaitSignal, exercising exactly the capability
// under test.)
//
//   1. Fire mail #1 -> the container run starts and spawns the body
//      `section__0`, which parks on `awaitSignal({ name: "proceed" })`. The
//      section proxies that gate UP as a signal-relay `SignalAwaited` on the
//      container over the SAME author name; the container is now awaiting
//      "proceed" and section__0 is parked (not complete).
//   2. Deliver "proceed" to the PARENT deployment run id (the container), the
//      way an operator would via the signals path. The container's await
//      resolves, the section relays the signal down into the body, the body's
//      gate completes, section__0 completes, and the section re-arms on its
//      input park for the next event.
//
// Load-bearing assertions: the container carries a signal-relay `SignalAwaited`
// for the author name (the proxied gate) and a `SignalReceived` for it (the
// delivery to the PARENT); `section__0` completes ONLY after the delivery (the
// relay reached the child body); the long-lived container never self-completes
// and re-arms on an input park.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child driving `runOnTrigger` with the production
// suspendable-child seam. No inference (the body is pure runtime). The signal
// is delivered through the sidecar router's `signal.deliver` (the delivery step
// the hub-api /signals route performs after its authz + reserved-name guards,
// which are covered by that route's own unit tests). This is a deploy-level
// test of the signal-relay capability C built.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";
import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  injectSignal,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import {
  onTriggerBodyEntry,
  type OnTriggerBodyVariant,
} from "./fixtures/on-trigger-body";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_on-trigger-signal-delivery-1";
const DEPLOYMENT_ID_TIMED = "run_on-trigger-timed-abandon-1";
const SECTION_ID = "section";
const SIGNAL_NAME = "proceed";

// The definition's own tenant, the caller principal that creates the
// definition assets, and one `workflow`-kind asset per deploy the frozen
// definitions project over. The install/approve freeze and the anchor
// `workflow_run` insert both write against these, so they must exist in the
// real DB before each deploy runs.
const TENANT_ID = "tnt_on_trigger_signal";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_signal";
const DEFINITION_ASSET_ID = "ast_on_trigger_signal_wf";
const TIMED_DEFINITION_ASSET_ID = "ast_on_trigger_timed_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  h = await createTestDb();
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
  for (const id of [DEFINITION_ASSET_ID, TIMED_DEFINITION_ASSET_ID]) {
    await seedAsset(h.db, {
      id,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: id,
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });
  }

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

/**
 * The container run is the single run under the deployment's workflow-run repo
 * that is NOT a body child. Body children are `${SECTION_ID}__<n>`; the
 * container carries their `ChildSpawned`/`ChildCompleted` records in its own
 * log. Returns `undefined` until the container's `runs/` entry exists.
 */
async function findContainerRunId(
  target: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  const ids = await listRunIds(target, workflowRunRepoId);
  return ids.find((id) => !id.startsWith(`${SECTION_ID}__`));
}

async function readContainerEvents(
  anchorRunId: string,
  workflowRunRepoId: RepoId,
): Promise<{ type: string; body: Record<string, unknown> }[]> {
  const containerRunId = await findContainerRunId(env, workflowRunRepoId);
  if (containerRunId === undefined) return [];
  return readWorkflowRunEvents(env, anchorRunId, containerRunId);
}

const hasChildCompleted = (
  events: { type: string; body: Record<string, unknown> }[],
  childRunId: string,
): boolean =>
  events.some(
    (e) => e.type === "ChildCompleted" && e.body["childRunId"] === childRunId,
  );

/**
 * Deploy a single onTrigger section with the given body BY SOURCE-REF, fire the
 * first event, and wait for the section to proxy the body's author
 * `awaitSignal` gate UP as a signal-relay await on the container. Returns the
 * container run id so each test drives the section from there. Shared by the
 * delivery and timeout scenarios, which diverge only in what becomes of the
 * proxied await.
 */
async function deployAndTriggerSection(opts: {
  anchorRunId: string;
  definitionAssetId: string;
  body: OnTriggerBodyVariant;
}): Promise<{ workflowRunRepoId: RepoId; containerRunId: string }> {
  const { anchorRunId } = opts;
  const deploymentMailAddress = deriveRunAddress({
    runId: anchorRunId,
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
    agentId: `${anchorRunId}`,
    tenantId: "tenant-1",
    principalId: `prin_${anchorRunId}`,
    agentAddress: deploymentMailAddress,
    systemPrompt: "Fallback prompt (overridden per step by the definition)",
    tools: [],
    grants: [],
    sources: [inferenceSource],
    defaultSource: "anthropic:mock-model",
  };

  const operatorApprovals: ApprovalSet = new Set<string>([
    "inference.source:anthropic:mock-model",
    "director:@intx/agent/default",
    `mail.address:${deploymentMailAddress}`,
    `mail.send:${DEPLOYMENT_DOMAIN}`,
  ]);

  const entryModule = onTriggerBodyEntry({
    address: deploymentMailAddress,
    sectionId: SECTION_ID,
    body: opts.body,
    workflowId: `wf_${anchorRunId}`,
  });

  const handle = await deployWorkflowSourceForTest(env, {
    entryModule,
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId: opts.definitionAssetId,
    anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: deploymentMailAddress,
    approvals: operatorApprovals,
    config,
    sources: { [SECTION_ID]: [inferenceSource] },
  });
  expect(handle.publicKey).toBeTruthy();

  const workflowRunRepoId: RepoId = handle.workflowRunRepoId;

  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
    { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
  );

  // Fire the first event; the body parks on its author gate and the section
  // proxies it up as a signal-relay await on the container.
  await fireMailTrigger(env, deploymentMailAddress, {
    messageId: `<${anchorRunId}@integration.interchange>`,
    content: "trigger the section",
  });
  await waitFor(
    async () => {
      const events = await readContainerEvents(anchorRunId, workflowRunRepoId);
      return events.some(
        (e) =>
          e.type === "SignalAwaited" &&
          e.body["parkKind"] === "signal-relay" &&
          e.body["signalName"] === SIGNAL_NAME,
      );
    },
    { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
  );
  const containerRunId = await findContainerRunId(env, workflowRunRepoId);
  if (containerRunId === undefined) {
    throw new Error("no container run under the workflow-run repo");
  }
  return { workflowRunRepoId, containerRunId };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "onTrigger body author-signal delivered to the parent run reaches the body via the section",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a signal delivered to the deployment run resolves the body's awaitSignal gate", async () => {
      // A non-agent awaitSignal gate (no timeout) -- the signal-relay capability.
      const { containerRunId } = await deployAndTriggerSection({
        anchorRunId: DEPLOYMENT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        body: { variant: "awaitSignal", signalName: SIGNAL_NAME },
      });

      // The body is parked on its gate: section__0 spawned, not yet completed.
      const parked = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        containerRunId,
      );
      expect(
        parked.some(
          (e) =>
            e.type === "ChildSpawned" &&
            e.body["childRunId"] === `${SECTION_ID}__0`,
        ),
      ).toBe(true);
      expect(hasChildCompleted(parked, `${SECTION_ID}__0`)).toBe(false);

      // ---- deliver the author signal to the PARENT deployment run id ----
      // The body (a child run) never receives a signal directly; delivering to
      // the container run resolves the section's signal-relay await, and the
      // section relays it down into the body.
      await injectSignal(env, DEPLOYMENT_ID, containerRunId, SIGNAL_NAME, {
        go: true,
      });

      // The relay reached the body: its gate completed, section__0 completed, and
      // the section re-armed on its input park for the next event.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            containerRunId,
          );
          return (
            hasChildCompleted(events, `${SECTION_ID}__0`) &&
            events.some(
              (e) =>
                e.type === "SignalAwaited" && e.body["parkKind"] === "input",
            )
          );
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
      );

      const finalEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        containerRunId,
      );
      const finalTypes = finalEvents.map((e) => e.type);

      // The container proxied the body's author gate up (signal-relay await) and
      // recorded the delivery to the PARENT run (SignalReceived on the same name).
      expect(
        finalEvents.some(
          (e) =>
            e.type === "SignalAwaited" &&
            e.body["parkKind"] === "signal-relay" &&
            e.body["signalName"] === SIGNAL_NAME,
        ),
      ).toBe(true);
      expect(
        finalEvents.some(
          (e) =>
            e.type === "SignalReceived" && e.body["signalName"] === SIGNAL_NAME,
        ),
      ).toBe(true);

      // The body completed only after the delivery (the relay reached the child).
      expect(hasChildCompleted(finalEvents, `${SECTION_ID}__0`)).toBe(true);

      // One long-lived run that never self-completes.
      expect(finalTypes.filter((t) => t === "RunStarted").length).toBe(1);
      expect(finalTypes).not.toContain("RunCompleted");
      expect(finalTypes).not.toContain("RunFailed");
      expect(finalTypes).not.toContain("RunCancelled");
    }, 120_000);

    test("a body gate that times out routes via onTimeout and the section keeps working", async () => {
      // The body's gate has a timeout + onTimeout to a completing step. No signal
      // is delivered, so the timer fires: the body routes onward (does NOT fail),
      // completes, and the section abandons its now-stale signal-relay await and
      // re-arms -- the long-lived section keeps working through a timed-out body
      // gate. onTimeout's target is a non-agent sleep (agent body steps are
      // INTR-310); its timer fires through the sidecar scheduler.
      const { containerRunId } = await deployAndTriggerSection({
        anchorRunId: DEPLOYMENT_ID_TIMED,
        definitionAssetId: TIMED_DEFINITION_ASSET_ID,
        body: {
          variant: "awaitSignal",
          signalName: SIGNAL_NAME,
          timeout: 500,
          recoverId: "recover",
          recoverDuration: 10,
        },
      });

      // Deliver NOTHING. The 500ms gate times out, routes to recover, completes;
      // the section abandons its relay await and re-arms on input.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID_TIMED,
            containerRunId,
          );
          return (
            hasChildCompleted(events, `${SECTION_ID}__0`) &&
            events.some(
              (e) =>
                e.type === "SignalAwaited" && e.body["parkKind"] === "input",
            )
          );
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
      );

      const finalEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID_TIMED,
        containerRunId,
      );
      const finalTypes = finalEvents.map((e) => e.type);
      // The section proxied the gate, then ABANDONED it when the body moved on via
      // its onTimeout route (no signal ever arrived).
      expect(
        finalEvents.some(
          (e) =>
            e.type === "SignalAwaited" &&
            e.body["parkKind"] === "signal-relay" &&
            e.body["signalName"] === SIGNAL_NAME,
        ),
      ).toBe(true);
      expect(
        finalEvents.some(
          (e) =>
            e.type === "SignalAwaitAbandoned" &&
            e.body["signalName"] === SIGNAL_NAME,
        ),
      ).toBe(true);
      // The body completed via the timeout route; the section keeps working.
      expect(hasChildCompleted(finalEvents, `${SECTION_ID}__0`)).toBe(true);
      expect(finalTypes.filter((t) => t === "RunStarted").length).toBe(1);
      expect(finalTypes).not.toContain("RunCompleted");
      expect(finalTypes).not.toContain("RunFailed");
      expect(finalTypes).not.toContain("RunCancelled");
    }, 120_000);
  },
);
