// The relay's send check on a held mail, with the database-backed authorizer.
//
// A run's mail can wait in the hub until its sender's key is recorded. The
// hub checks the mail again when it re-drives it, so the check sees the
// grants as they are then. A run that ended while its mail waited has a
// deactivated principal; its grants still decide, so the mail still goes. A
// deny written while the mail waited still refuses it.

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createGrantStore, deactivateRunPrincipals } from "@intx/db";
import { tenant as tenantTable } from "@intx/db/schema";
import { createMailSendAuthorizer } from "@intx/hub-api";
import {
  createSidecarRouter,
  type SidecarAuthIdentity,
} from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedGrant,
  seedPrincipal,
  seedWorkflowRun,
  tenantSlugFromId,
} from "@intx/test-harness/seed";
import { DEFAULT_SIDECAR_CAPACITY } from "@intx/types/sidecar";
import { waitUntil } from "@intx/types/testing";

import { createMockWs } from "./sidecar-test-helpers";

const TENANT = "tnt_send_relay";
const DOMAIN = "send-relay.localhost";
const SENDER_RUN = "run_send_relay";
const SENDER_ADDRESS = `${SENDER_RUN}@${DOMAIN}`;
const SENDER_PRINCIPAL = "prn_send_relay";
const EXTERNAL = "x@example.com";
const ALLOCATION = "allocation-send-relay";

const binding: SidecarAuthIdentity = {
  kind: "allocated",
  sidecarId: "sc-send-relay",
  allocationId: ALLOCATION,
  tenantId: TENANT,
  anchorRunId: SENDER_RUN,
  workflowRunAddress: SENDER_ADDRESS,
  generation: 1,
};

describe.skipIf(!harnessDbEnvAvailable())(
  "the relay's send check on a held mail (real DB)",
  () => {
    let h: TestDb;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await h.db.insert(tenantTable).values({
        id: TENANT,
        name: TENANT,
        slug: tenantSlugFromId(TENANT),
        domain: DOMAIN,
        parentId: null,
      });
      await seedPrincipal(h.db, {
        id: SENDER_PRINCIPAL,
        tenantId: TENANT,
        kind: "workflow",
        refId: SENDER_RUN,
        status: "active",
      });
      await seedWorkflowRun(h.db, {
        id: SENDER_RUN,
        tenantId: TENANT,
        anchorRunId: SENDER_RUN,
        address: SENDER_ADDRESS,
        status: "running",
        principalId: SENDER_PRINCIPAL,
      });
      await grant("send:all", "mail.send:*:*", "allow");
    });

    async function grant(
      id: string,
      resource: string,
      effect: "allow" | "deny",
    ) {
      await seedGrant(h.db, {
        id: `grt_${id.replace(":", "_")}`,
        tenantId: TENANT,
        principalId: SENDER_PRINCIPAL,
        resource,
        action: "send",
        effect,
      });
    }

    // Hold a mail from the run to itself and to an external address until
    // the run's key is recorded, run `whileHeld`, then record the key so the
    // hub re-drives the mail. The external recipient's undelivered report
    // marks the end of the re-drive.
    async function holdThenRedrive(whileHeld: () => Promise<void>) {
      const held = Promise.withResolvers<undefined>();
      const router = createSidecarRouter({
        withExecutableWorkflowRun: async (_target, deliver) => deliver(),
        authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
        resolveSidecarBindings: async () => [binding],
        validateSidecarIdentity: async () => true,
        lookups: {
          authorizeMailSend: createMailSendAuthorizer({
            db: h.db,
            grantStoreFor: createGrantStore,
          }),
          async resolveSenderKey() {
            held.resolve(undefined);
            return null;
          },
          async materializeMailTriggeredRunGrants() {
            return { outcome: "materialized", stepGrants: [] };
          },
          readWorkflowRunRefTips: async () => ({
            "refs/heads/main": "c".repeat(40),
            "refs/heads/events": null,
          }),
        },
      });
      const undelivered = Promise.withResolvers<string[]>();
      router.events.on("mail.outbound.undelivered", ({ recipients }) => {
        undelivered.resolve(recipients);
      });

      const ws = createMockWs();
      router.fenceAllocation(ALLOCATION, 1);
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "hello",
          sidecarId: binding.sidecarId,
          token: "tok",
          capacity: DEFAULT_SIDECAR_CAPACITY,
          incarnations: [
            { address: SENDER_ADDRESS, generation: 1, state: "live" },
          ],
        }),
      );
      await waitUntil(() =>
        router.getRoutableAddresses().includes(SENDER_ADDRESS),
      );

      const attempt = {
        allocationId: ALLOCATION,
        generation: 1,
        leaseId: "send-relay-hold",
      };
      router.noteSenderDeployStarted(SENDER_ADDRESS, attempt);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "mail.outbound",
          senderAddress: SENDER_ADDRESS,
          generation: 1,
          rawMessage: btoa(
            `From: ${SENDER_ADDRESS}\r\nTo: ${EXTERNAL}\r\nMessage-ID: <held@send-relay.localhost>\r\n\r\nbody`,
          ),
          recipients: [SENDER_ADDRESS, EXTERNAL],
        }),
      );
      await held.promise;
      await whileHeld();
      router.noteSenderDeploySettled(attempt, { recorded: "ab".repeat(32) });

      const reported = await undelivered.promise;
      const routed = ws.sent.filter((raw) => raw.includes('"mail.inbound"'));
      return { reported, routed };
    }

    test("delivers a held mail from a run that ended while it waited", async () => {
      const { reported, routed } = await holdThenRedrive(async () => {
        await deactivateRunPrincipals(h.db, TENANT, [SENDER_RUN], new Date());
      });

      expect(reported).toEqual([EXTERNAL]);
      expect(routed).toHaveLength(1);
    });

    test("refuses a held mail to a recipient denied while it waited", async () => {
      const { reported, routed } = await holdThenRedrive(async () => {
        await grant(
          "send:self",
          `mail.send:principal:${SENDER_PRINCIPAL}`,
          "deny",
        );
      });

      expect(reported).toEqual([EXTERNAL]);
      expect(routed).toEqual([]);
    });
  },
);
