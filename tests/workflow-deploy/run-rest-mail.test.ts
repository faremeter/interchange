// A workflow run sends mail through the hub's run mail route as itself.
//
// A source-ref deployment of `fixtures/run-rest-mail-workflow.ts` runs on the
// real sidecar, against the real REST app served on a real port. The caller's
// kickoff mail starts the run's first `onTrigger` body. Its action composes a
// message to the run's own address with the deployment key, which only the
// sidecar holds, and posts it with the run's hub token:
//
//   - A copy with its signed text changed is refused: the signature does not
//     verify against the run principal's key.
//   - The real message is accepted, and the hub relays it to the run's
//     sidecar. The sidecar admits inbound mail only when the signature
//     verifies against the key the hub co-delivers for the stamped sender and
//     that sender is the visible From, so the second body running with the
//     message proves the recipient verified it as the run principal's.
//   - A run whose `mail.send` an operator denies gets 403 before the hub
//     routes its mail.
//
// The run mails itself, so its sender and recipient share one address. The
// real-DB test `tests/hub-api/run-rest-mail-relay.test.ts` posts to another
// run and pins the sender stamp and key the hub relays.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { deriveRunPrincipalId, RUN_TOKEN_PREFIX } from "@intx/hub-common";
import { harnessDbEnvAvailable } from "@intx/test-harness/db-harness";
import { seedGrant } from "@intx/test-harness/seed";
import { sectionBodyRunId } from "@intx/workflow";
import { deriveRunAddress } from "@intx/workflow-deploy";

import {
  readWorkflowRunEvents,
  waitFor,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import {
  RELAY_STEP_ID,
  SECTION_STEP_ID,
  SELF_MAIL_TEXT,
  runRestMailWorkflowEntry,
} from "./fixtures/run-rest-mail-workflow";
import {
  DEPLOYMENT_DOMAIN,
  startHubAccessEnv,
  type HubAccessEnv,
} from "./hub-access-env";

const TENANT_ID = "tnt_run_rest_mail";
const OTHER_TENANT_ID = "tnt_run_rest_mail_other";
const SELF_RUN = "run_rest-mail-self";
const DENIED_RUN = "run_rest-mail-denied";

let t: HubAccessEnv;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function hasChildEvent(
  events: readonly WorkflowRunEvent[],
  type: string,
  childRunId: string,
): boolean {
  return events.some(
    (event) => event.type === type && event.body["childRunId"] === childRunId,
  );
}

function stepOutput(
  events: readonly WorkflowRunEvent[],
  stepId: string,
): Record<string, unknown> {
  const completed = events.find(
    (event) =>
      event.type === "StepCompleted" && event.body["stepId"] === stepId,
  );
  if (completed === undefined) {
    throw new Error(`no StepCompleted event for step ${stepId}`);
  }
  const output = completed.body["output"];
  const ref = isRecord(output) ? output["ref"] : undefined;
  if (typeof ref !== "string" || !ref.startsWith("inline:")) {
    throw new Error(
      `step ${stepId} output is not an inline ref: ${JSON.stringify(output)}`,
    );
  }
  const parsed: unknown = JSON.parse(ref.slice("inline:".length));
  if (!isRecord(parsed)) {
    throw new Error(`step ${stepId} output is not an object`);
  }
  return parsed;
}

/** Deploy the fixture under `anchorRunId` and kick its run off. */
async function deployAndTrigger(anchorRunId: string, postTampered: boolean) {
  await t.deploy({
    anchorRunId,
    actions: "./workflow.mjs",
    entryModule: (address) =>
      runRestMailWorkflowEntry({
        address,
        workflowId: `wf_${anchorRunId.replaceAll("-", "_")}`,
        mailPath: `${t.selfRunsPath}/${anchorRunId}/mail`,
        postTampered,
      }),
  });
}

/** Wait for the `onTrigger` body `index` of the run to complete. */
async function waitForBody(anchorRunId: string, index: number) {
  const bodyRunId = sectionBodyRunId(anchorRunId, SECTION_STEP_ID, index);
  await waitFor(
    async () =>
      hasChildEvent(
        await readWorkflowRunEvents(t.env, anchorRunId, anchorRunId),
        "ChildCompleted",
        bodyRunId,
      ),
    { diagnostics: t.env.sidecarDiagnostics },
  );
  return stepOutput(
    await readWorkflowRunEvents(t.env, anchorRunId, bodyRunId),
    RELAY_STEP_ID,
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a run sends mail through the run mail route as itself",
  () => {
    beforeAll(async () => {
      t = await startHubAccessEnv({
        tenantId: TENANT_ID,
        callerUserId: "usr_run_rest_mail_caller",
        callerPrincipalId: "prn_run_rest_mail_caller",
      });
      await t.seedTenancy(OTHER_TENANT_ID);
    });

    afterAll(async () => {
      if (t !== undefined) await t.teardown();
    });

    test("the hub relays the run's signed mail, refuses a changed copy, and the run receives it as its own", async () => {
      const address = deriveRunAddress({
        runId: SELF_RUN,
        domain: DEPLOYMENT_DOMAIN,
      });
      await deployAndTrigger(SELF_RUN, true);
      const fromRunsBefore = t.received.length;
      await t.trigger(SELF_RUN);

      const sender = await waitForBody(SELF_RUN, 0);
      expect(sender["tampered"]).toEqual({
        status: 400,
        code: "mail_signature_invalid",
      });
      expect(sender["sent"]).toEqual({ status: 202, code: null });
      const messageId = sender["messageId"];
      expect(typeof messageId).toBe("string");

      // The second body ran with the relayed message, so the recipient
      // admitted it under the run's own address.
      const recipient = await waitForBody(SELF_RUN, 1);
      expect(recipient["from"]).toBe(address);
      expect(recipient["messageId"]).toBe(messageId);
      expect(String(recipient["text"]).trim()).toBe(SELF_MAIL_TEXT);

      // The only requests from the run are its two posts, each with its run
      // token.
      const fromRun = t.received.slice(fromRunsBefore);
      expect(fromRun.map((call) => [call.path, call.status])).toEqual([
        [`${t.selfRunsPath}/${SELF_RUN}/mail`, 400],
        [`${t.selfRunsPath}/${SELF_RUN}/mail`, 202],
      ]);
      for (const call of fromRun) {
        expect(
          call.authorization?.startsWith(`Bearer ${RUN_TOKEN_PREFIX}`),
        ).toBe(true);
      }
    });

    test("a run whose mail.send an operator denies is refused before the hub routes its mail", async () => {
      await deployAndTrigger(DENIED_RUN, false);
      // Deploy created the run principal, so the operator can deny it before
      // the first trigger stages the run's `mail.send:*:*` allow.
      await seedGrant(t.h.db, {
        id: "grant-operator-deny-send",
        tenantId: TENANT_ID,
        resource: "mail.send:*",
        action: "*",
        effect: "deny",
        origin: "system",
        principalId: await deriveRunPrincipalId(TENANT_ID, DENIED_RUN),
      });
      const fromRunsBefore = t.received.length;
      await t.trigger(DENIED_RUN);

      const sender = await waitForBody(DENIED_RUN, 0);
      expect(sender["tampered"]).toBeNull();
      expect(sender["sent"]).toEqual({
        status: 403,
        code: "mail_send_denied",
      });

      // The run's one request is its post, and the hub refuses it before it
      // routes anything.
      const fromRun = t.received.slice(fromRunsBefore);
      expect(fromRun.map((call) => [call.path, call.status])).toEqual([
        [`${t.selfRunsPath}/${DENIED_RUN}/mail`, 403],
      ]);
    });
  },
);
