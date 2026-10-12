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
// execution is exercised by the tool-invoke round-trips; this case is non-agent
// by nature, so the body IS an awaitSignal, exercising exactly the capability
// under test.)
//
//   1. Fire mail #1 -> the container run starts and spawns event 0's body,
//      which parks on `awaitSignal({ name: "proceed" })`. The
//      section proxies that gate UP as a signal-relay `SignalAwaited` on the
//      container over the SAME author name; the container is now awaiting
//      "proceed" and the body is parked (not complete).
//   2. Deliver "proceed" to the PARENT deployment run id (the container), the
//      way an operator would via the signals path. The container's await
//      resolves, the section relays the signal down into the body, the body's
//      gate completes, the body completes, and the section re-arms on its
//      input park for the next event.
//
// Load-bearing assertions: the container carries a signal-relay `SignalAwaited`
// for the author name (the proxied gate) and a `SignalReceived` for it (the
// delivery to the PARENT); the body completes ONLY after the delivery (the
// relay reached the child body); the long-lived container never self-completes
// and re-arms on an input park.
//
// Later tests in this file keep that path and pin the addressing split: a
// name-only post does not complete a new relay, a late correlation does not
// complete the next turn, two sections do not share a delivery, and a
// top-level gate on the same name still completes by name.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, real
// workflow-process child driving `runOnTrigger` with the production
// suspendable-child seam. No inference (the body is pure runtime). The signal
// is delivered through the sidecar router's `signal.deliver` (the delivery step
// the hub-api /signals route performs after its authz + reserved-name guards,
// which are covered by that route's own unit tests). This is a deploy-level
// test of the signal-relay capability C built.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { sectionBodyRunId } from "@intx/workflow";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  tenantSlugFromId,
} from "@intx/test-harness/seed";
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
import {
  onTriggerGateBesideSectionEntry,
  onTriggerTwoSectionsEntry,
} from "./fixtures/on-trigger-signal-routing";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_on-trigger-signal-delivery-1";
const DEPLOYMENT_ID_TIMED = "run_on-trigger-timed-abandon-1";
const DEPLOYMENT_ID_TWO = "run_on-trigger-two-sections-1";
const DEPLOYMENT_ID_GATE = "run_on-trigger-gate-beside-1";
const SECTION_ID = "section";
const LEFT_ID = "left";
const RIGHT_ID = "right";
const GATE_ID = "gate";
const NEXT_ID = "next";
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
const TWO_SECTIONS_ASSET_ID = "ast_on_trigger_two_sections_wf";
const GATE_BESIDE_ASSET_ID = "ast_on_trigger_gate_beside_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  h = await createTestDb();
  await h.db.insert(tenantTable).values({
    id: TENANT_ID,
    name: TENANT_ID,
    slug: tenantSlugFromId(TENANT_ID),
    domain: DEPLOYMENT_DOMAIN,
    parentId: null,
  });
  await seedPrincipal(h.db, {
    id: CALLER_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
  });
  for (const id of [
    DEFINITION_ASSET_ID,
    TIMED_DEFINITION_ASSET_ID,
    TWO_SECTIONS_ASSET_ID,
    GATE_BESIDE_ASSET_ID,
  ]) {
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
 * whose id sorts ahead of a minted section body. The
 * container carries the bodies' `ChildSpawned`/`ChildCompleted` records in its own
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

type RunEvents = { type: string; body: Record<string, unknown> }[];

const hasChildCompleted = (events: RunEvents, childRunId: string): boolean =>
  events.some(
    (e) => e.type === "ChildCompleted" && e.body["childRunId"] === childRunId,
  );

function field(body: Record<string, unknown>, key: string): string | undefined {
  const value = body[key];
  return typeof value === "string" ? value : undefined;
}

function relayCorrelationId(events: RunEvents, stepId: string): string {
  const park = events.find(
    (e) =>
      e.type === "SignalAwaited" &&
      e.body["parkKind"] === "signal-relay" &&
      e.body["stepId"] === stepId &&
      e.body["signalName"] === SIGNAL_NAME,
  );
  const correlationId =
    park === undefined ? undefined : field(park.body, "correlationId");
  if (correlationId === undefined) {
    throw new Error(`${stepId} relay park has no correlationId`);
  }
  return correlationId;
}

function receivedSignal(
  events: RunEvents,
  signalId: string,
): Record<string, unknown> | undefined {
  return events.find(
    (e) => e.type === "SignalReceived" && e.body["signalId"] === signalId,
  )?.body;
}

/**
 * Deploy a source-ref workflow, fire its first mail, and wait until `parked`
 * holds. Each section id is given the mock inference source the deploy pins,
 * matching the single-section helper. The bodies under test are not agents.
 */
async function deployAndPark(opts: {
  anchorRunId: string;
  definitionAssetId: string;
  entryModule: string;
  sectionIds: readonly string[];
  parked: (events: RunEvents) => boolean;
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
    credentialId: "sk-mock",
    model: "mock-model",
  };
  const sources: Record<string, InferenceSource[]> = {};
  for (const sectionId of opts.sectionIds) {
    sources[sectionId] = [inferenceSource];
  }

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

  const operatorApprovals: ApprovalSet = createApprovalSet([
    "inference.source:anthropic:mock-model",
    "director:@intx/agent/default",
    `mail.address:${deploymentMailAddress}`,
    `mail.send:${DEPLOYMENT_DOMAIN}`,
  ]);

  const handle = await deployWorkflowSourceForTest(env, {
    entryModule: opts.entryModule,
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId: opts.definitionAssetId,
    anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: deploymentMailAddress,
    approvals: operatorApprovals,
    config,
    sources,
  });
  expect(handle.publicKey).toBeTruthy();

  const workflowRunRepoId: RepoId = handle.workflowRunRepoId;

  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
    { diagnostics: env.sidecarDiagnostics },
  );

  await fireMailTrigger(env, deploymentMailAddress, {
    messageId: `<${anchorRunId}@integration.interchange>`,
    content: "trigger the section",
  });
  await waitFor(
    async () => {
      const events = await readContainerEvents(anchorRunId, workflowRunRepoId);
      return opts.parked(events);
    },
    { diagnostics: env.sidecarDiagnostics },
  );
  const containerRunId = await findContainerRunId(env, workflowRunRepoId);
  if (containerRunId === undefined) {
    throw new Error("no container run under the workflow-run repo");
  }
  return { workflowRunRepoId, containerRunId };
}

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
  const deploymentMailAddress = deriveRunAddress({
    runId: opts.anchorRunId,
    domain: DEPLOYMENT_DOMAIN,
  });
  return deployAndPark({
    anchorRunId: opts.anchorRunId,
    definitionAssetId: opts.definitionAssetId,
    sectionIds: [SECTION_ID],
    entryModule: onTriggerBodyEntry({
      address: deploymentMailAddress,
      sectionId: SECTION_ID,
      body: opts.body,
      workflowId: `wf_${opts.anchorRunId}`,
    }),
    parked: (events) =>
      events.some(
        (e) =>
          e.type === "SignalAwaited" &&
          e.body["parkKind"] === "signal-relay" &&
          e.body["signalName"] === SIGNAL_NAME,
      ),
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "onTrigger body author-signal delivered to the parent run reaches the body via the section",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a name-only delivery leaves the new park and the correlation completes it", async () => {
      // A non-agent awaitSignal gate (no timeout) -- the signal-relay capability.
      const { containerRunId } = await deployAndTriggerSection({
        anchorRunId: DEPLOYMENT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        body: { variant: "awaitSignal", signalName: SIGNAL_NAME },
      });

      // The body is parked on its gate: event 0's body spawned, not yet completed.
      const parked = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        containerRunId,
      );
      expect(
        parked.some(
          (e) =>
            e.type === "ChildSpawned" &&
            e.body["childRunId"] ===
              sectionBodyRunId(containerRunId, SECTION_ID, 0),
        ),
      ).toBe(true);
      expect(
        hasChildCompleted(
          parked,
          sectionBodyRunId(containerRunId, SECTION_ID, 0),
        ),
      ).toBe(false);

      // A fresh relay park is addressed by the correlationId on that
      // SignalAwaited. A name-only post is recorded and does not complete it.
      const correlationId = relayCorrelationId(parked, SECTION_ID);
      const childRunId = sectionBodyRunId(containerRunId, SECTION_ID, 0);
      const nameOnly = await injectSignal(
        env,
        DEPLOYMENT_ID,
        containerRunId,
        SIGNAL_NAME,
        { go: false },
      );
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            containerRunId,
          );
          return receivedSignal(events, nameOnly.signalId) !== undefined;
        },
        { diagnostics: env.sidecarDiagnostics },
      );
      const afterName = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        containerRunId,
      );
      const nameOnlyBody = receivedSignal(afterName, nameOnly.signalId);
      expect(nameOnlyBody).toBeDefined();
      expect(nameOnlyBody?.["correlationId"]).toBeUndefined();
      expect(hasChildCompleted(afterName, childRunId)).toBe(false);

      // The body (a child run) never receives a signal directly. Delivering
      // the correlation to the container resolves the relay, and the section
      // relays it down into the body.
      await injectSignal(
        env,
        DEPLOYMENT_ID,
        containerRunId,
        SIGNAL_NAME,
        { go: true },
        correlationId,
      );

      // The relay reached the body: its gate completed, the body completed, and
      // the section re-armed on its input park for the next event.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID,
            containerRunId,
          );
          return (
            hasChildCompleted(
              events,
              sectionBodyRunId(containerRunId, SECTION_ID, 0),
            ) &&
            events.some(
              (e) =>
                e.type === "SignalAwaited" && e.body["parkKind"] === "input",
            )
          );
        },
        { diagnostics: env.sidecarDiagnostics },
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
      expect(
        hasChildCompleted(
          finalEvents,
          sectionBodyRunId(containerRunId, SECTION_ID, 0),
        ),
      ).toBe(true);

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
      // covered elsewhere); its timer fires through the sidecar scheduler.
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
            hasChildCompleted(
              events,
              sectionBodyRunId(containerRunId, SECTION_ID, 0),
            ) &&
            events.some(
              (e) =>
                e.type === "SignalAwaited" && e.body["parkKind"] === "input",
            )
          );
        },
        { diagnostics: env.sidecarDiagnostics },
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
      expect(
        hasChildCompleted(
          finalEvents,
          sectionBodyRunId(containerRunId, SECTION_ID, 0),
        ),
      ).toBe(true);
      expect(finalTypes.filter((t) => t === "RunStarted").length).toBe(1);
      expect(finalTypes).not.toContain("RunCompleted");
      expect(finalTypes).not.toContain("RunFailed");
      expect(finalTypes).not.toContain("RunCancelled");

      // The abandoned correlation is still the only relay. Delivering it now
      // records the signal and must not satisfy the next turn, which mints
      // its own id when the second mail parks.
      const abandoned = relayCorrelationId(finalEvents, SECTION_ID);
      const late = await injectSignal(
        env,
        DEPLOYMENT_ID_TIMED,
        containerRunId,
        SIGNAL_NAME,
        { late: true },
        abandoned,
      );
      const turnAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID_TIMED,
        domain: DEPLOYMENT_DOMAIN,
      });
      await fireMailTrigger(env, turnAddress, {
        messageId: `<${DEPLOYMENT_ID_TIMED}-turn-2@integration.interchange>`,
        content: "second event",
      });
      const nextChild = sectionBodyRunId(containerRunId, SECTION_ID, 1);
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID_TIMED,
            containerRunId,
          );
          const nextRelay = events.some(
            (e) =>
              e.type === "SignalAwaited" &&
              e.body["parkKind"] === "signal-relay" &&
              e.body["signalName"] === SIGNAL_NAME &&
              field(e.body, "correlationId") !== undefined &&
              field(e.body, "correlationId") !== abandoned,
          );
          return (
            receivedSignal(events, late.signalId) !== undefined && nextRelay
          );
        },
        { diagnostics: env.sidecarDiagnostics },
      );
      const turned = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID_TIMED,
        containerRunId,
      );
      const nextCorrelation = turned.find(
        (e) =>
          e.type === "SignalAwaited" &&
          e.body["parkKind"] === "signal-relay" &&
          field(e.body, "correlationId") !== undefined &&
          field(e.body, "correlationId") !== abandoned,
      );
      const nextId =
        nextCorrelation === undefined
          ? undefined
          : field(nextCorrelation.body, "correlationId");
      expect(nextId).toBeDefined();
      expect(receivedSignal(turned, late.signalId)?.["correlationId"]).toBe(
        abandoned,
      );
      expect(
        turned.some(
          (e) =>
            e.type === "SignalReceived" &&
            field(e.body, "correlationId") === nextId,
        ),
      ).toBe(false);
      expect(hasChildCompleted(turned, nextChild)).toBe(false);
    }, 120_000);

    test("two sections awaiting the same name do not share a delivery", async () => {
      const address = deriveRunAddress({
        runId: DEPLOYMENT_ID_TWO,
        domain: DEPLOYMENT_DOMAIN,
      });
      const { containerRunId } = await deployAndPark({
        anchorRunId: DEPLOYMENT_ID_TWO,
        definitionAssetId: TWO_SECTIONS_ASSET_ID,
        sectionIds: [LEFT_ID, RIGHT_ID],
        entryModule: onTriggerTwoSectionsEntry({
          address,
          signalName: SIGNAL_NAME,
          leftId: LEFT_ID,
          rightId: RIGHT_ID,
          workflowId: `wf_${DEPLOYMENT_ID_TWO}`,
        }),
        parked: (events) => {
          const relays = events.filter(
            (e) =>
              e.type === "SignalAwaited" &&
              e.body["parkKind"] === "signal-relay" &&
              e.body["signalName"] === SIGNAL_NAME,
          );
          const steps = new Set(
            relays
              .map((e) => field(e.body, "stepId"))
              .filter((id) => id !== undefined),
          );
          return steps.has(LEFT_ID) && steps.has(RIGHT_ID);
        },
      });
      const parked = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID_TWO,
        containerRunId,
      );
      const leftCorrelation = relayCorrelationId(parked, LEFT_ID);
      const rightCorrelation = relayCorrelationId(parked, RIGHT_ID);
      expect(leftCorrelation).not.toBe(rightCorrelation);
      const leftChild = sectionBodyRunId(containerRunId, LEFT_ID, 0);
      const rightChild = sectionBodyRunId(containerRunId, RIGHT_ID, 0);

      const nameOnly = await injectSignal(
        env,
        DEPLOYMENT_ID_TWO,
        containerRunId,
        SIGNAL_NAME,
        { go: false },
      );
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID_TWO,
            containerRunId,
          );
          return receivedSignal(events, nameOnly.signalId) !== undefined;
        },
        { diagnostics: env.sidecarDiagnostics },
      );
      const afterName = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID_TWO,
        containerRunId,
      );
      expect(hasChildCompleted(afterName, leftChild)).toBe(false);
      expect(hasChildCompleted(afterName, rightChild)).toBe(false);

      await injectSignal(
        env,
        DEPLOYMENT_ID_TWO,
        containerRunId,
        SIGNAL_NAME,
        { go: true },
        leftCorrelation,
      );
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID_TWO,
            containerRunId,
          );
          return (
            hasChildCompleted(events, leftChild) &&
            events.some(
              (e) =>
                e.type === "SignalAwaited" &&
                e.body["parkKind"] === "input" &&
                e.body["stepId"] === LEFT_ID,
            )
          );
        },
        { diagnostics: env.sidecarDiagnostics },
      );
      const afterLeft = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID_TWO,
        containerRunId,
      );
      expect(hasChildCompleted(afterLeft, rightChild)).toBe(false);

      await injectSignal(
        env,
        DEPLOYMENT_ID_TWO,
        containerRunId,
        SIGNAL_NAME,
        { go: true },
        rightCorrelation,
      );
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID_TWO,
            containerRunId,
          );
          return (
            hasChildCompleted(events, rightChild) &&
            events.some(
              (e) =>
                e.type === "SignalAwaited" &&
                e.body["parkKind"] === "input" &&
                e.body["stepId"] === RIGHT_ID,
            )
          );
        },
        { diagnostics: env.sidecarDiagnostics },
      );
    }, 120_000);

    test("a name-only delivery completes the top-level gate and the correlation completes the section", async () => {
      const address = deriveRunAddress({
        runId: DEPLOYMENT_ID_GATE,
        domain: DEPLOYMENT_DOMAIN,
      });
      const { containerRunId } = await deployAndPark({
        anchorRunId: DEPLOYMENT_ID_GATE,
        definitionAssetId: GATE_BESIDE_ASSET_ID,
        // The deploy frame requires a sources entry for every step id. The
        // gates do not call a model; the mock source only fills the map.
        sectionIds: [GATE_ID, NEXT_ID, SECTION_ID],
        entryModule: onTriggerGateBesideSectionEntry({
          address,
          signalName: SIGNAL_NAME,
          sectionId: SECTION_ID,
          gateId: GATE_ID,
          nextId: NEXT_ID,
          workflowId: `wf_${DEPLOYMENT_ID_GATE}`,
        }),
        parked: (events) => {
          const gateParked = events.some(
            (e) =>
              e.type === "SignalAwaited" &&
              e.body["stepId"] === GATE_ID &&
              e.body["signalName"] === SIGNAL_NAME &&
              field(e.body, "correlationId") === undefined,
          );
          const sectionParked = events.some(
            (e) =>
              e.type === "SignalAwaited" &&
              e.body["parkKind"] === "signal-relay" &&
              e.body["stepId"] === SECTION_ID &&
              e.body["signalName"] === SIGNAL_NAME &&
              field(e.body, "correlationId") !== undefined,
          );
          return gateParked && sectionParked;
        },
      });
      const parked = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID_GATE,
        containerRunId,
      );
      const correlationId = relayCorrelationId(parked, SECTION_ID);
      const sectionChild = sectionBodyRunId(containerRunId, SECTION_ID, 0);

      const nameOnly = await injectSignal(
        env,
        DEPLOYMENT_ID_GATE,
        containerRunId,
        SIGNAL_NAME,
        { go: false },
      );
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID_GATE,
            containerRunId,
          );
          return events.some(
            (e) =>
              e.type === "SignalAwaited" &&
              e.body["stepId"] === NEXT_ID &&
              e.body["signalName"] === NEXT_ID,
          );
        },
        { diagnostics: env.sidecarDiagnostics },
      );
      const afterGate = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID_GATE,
        containerRunId,
      );
      expect(
        receivedSignal(afterGate, nameOnly.signalId)?.["correlationId"],
      ).toBeUndefined();
      expect(hasChildCompleted(afterGate, sectionChild)).toBe(false);
      expect(
        afterGate.some(
          (e) =>
            e.type === "SignalReceived" &&
            field(e.body, "correlationId") === correlationId,
        ),
      ).toBe(false);

      await injectSignal(
        env,
        DEPLOYMENT_ID_GATE,
        containerRunId,
        SIGNAL_NAME,
        { go: true },
        correlationId,
      );
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            DEPLOYMENT_ID_GATE,
            containerRunId,
          );
          return (
            hasChildCompleted(events, sectionChild) &&
            events.some(
              (e) =>
                e.type === "SignalAwaited" &&
                e.body["parkKind"] === "input" &&
                e.body["stepId"] === SECTION_ID,
            )
          );
        },
        { diagnostics: env.sidecarDiagnostics },
      );
    }, 120_000);
  },
);
