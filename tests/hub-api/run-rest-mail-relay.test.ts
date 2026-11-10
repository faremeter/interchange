import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { type } from "arktype";

import {
  createEd25519Crypto,
  createNoopCredentialCipher,
  generateKeyPair,
} from "@intx/crypto";
import { eq } from "drizzle-orm";

import {
  createRunTokenStore,
  createSidecarAllocationStore,
  createWorkflowRunDispatchStore,
  retireRunPrincipalKey,
} from "@intx/db";
import { workflowDefinition, workflowRunDispatch } from "@intx/db/schema";
import { createApp, type GetSession } from "@intx/hub-api";
import { deriveRunPrincipalId } from "@intx/hub-common";
import {
  createEventCollectorRegistry,
  createSidecarRouter,
  createWorkflowDispatchService,
  type AssetService,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
  type WorkflowDispatchService,
} from "@intx/hub-sessions";
import {
  assembleMessage,
  assembleSignedContent,
  createDetachedSignatureFromProvider,
  generateSenderScopedMessageId,
} from "@intx/mime";
import { base64Decode, base64Encode, hexEncode } from "@intx/types";
import type { KeyPair } from "@intx/types/runtime";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAllocation,
  seedAsset,
  seedGrant,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
  seedWorkflowRunLaunchSpec,
  tenantSlugFromId,
} from "@intx/test-harness/seed";

// A run posts a message it signed to ANOTHER run through the run mail route.
// The hub relays the bytes it verified under the sender's address and
// co-delivers the sender's key, so the recipient verifies the mail as the
// sending run's. The end-to-end test in tests/workflow-deploy has a run mail
// itself, where sender and recipient share one address; this test keeps the
// two apart, so a swap of the sender stamp and the target fails it.

const TENANT = "tnt_rest_mail_relay";
const DOMAIN = `${tenantSlugFromId(TENANT).toLowerCase()}.example.test`;
const SENDER = "run_rest_mail_sender";
const TARGET = "run_rest_mail_target";
const SENDER_ADDRESS = `${SENDER}@${DOMAIN}`;
const TARGET_ADDRESS = `${TARGET}@${DOMAIN}`;
const CREATOR = "prn_rest_mail_creator";
const CONSUMER = "action:relay";

const noSession: GetSession = async () => null;

function notImpl(name: string): never {
  throw new Error(`mock: ${name} not implemented`);
}

function mockSessionService(): SessionService {
  return { endSession: () => notImpl("endSession") };
}

// The run mail route mounts only with an asset service and a repo store. The
// trigger reads the run's durable lifecycle through the repo store, and a repo
// with no committed reads is a run that has not started.
function stubAssetService(): AssetService {
  return {
    createAsset: () => notImpl("createAsset"),
    populateAsset: () => notImpl("populateAsset"),
    readAssetBlob: () => notImpl("readAssetBlob"),
    listAssetBlobs: () => notImpl("listAssetBlobs"),
  };
}

function stubRepoStore(): RepoStore {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the trigger reads only committed reads from the repo store
  return new Proxy({} as RepoStore, {
    get: (_target, prop) =>
      prop === "openCommittedReads"
        ? async () => null
        : () => notImpl(`repoStore.${String(prop)}`),
  });
}

async function signedMail(
  keyPair: KeyPair,
  messageId: string = generateSenderScopedMessageId(SENDER_ADDRESS),
  text = "from the sender run",
): Promise<Uint8Array> {
  const signedContent = assembleSignedContent({
    kind: "conversation",
    text,
  });
  const signature = await createDetachedSignatureFromProvider(
    signedContent,
    createEd25519Crypto(keyPair),
  );
  return assembleMessage(
    {
      from: SENDER_ADDRESS,
      to: [TARGET_ADDRESS],
      cc: undefined,
      date: new Date("2026-01-15T12:00:00Z"),
      messageId,
      subject: undefined,
      inReplyTo: undefined,
      references: undefined,
      mimeVersion: "1.0",
      interchangeType: "conversation.message",
      interchangeCorrelationId: undefined,
      interchangeTenantId: undefined,
      interchangeAgentId: undefined,
      interchangeSessionId: undefined,
      interchangeOfferingId: undefined,
      interchangeSchemaVersion: undefined,
      traceparent: undefined,
      tracestate: undefined,
    },
    signedContent,
    signature,
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a run's REST mail to another run (real DB)",
  () => {
    let h: TestDb;
    let senderKey: KeyPair;
    let token: string;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedPrincipal(h.db, {
        id: CREATOR,
        tenantId: TENANT,
        kind: "user",
        refId: `${CREATOR}-user`,
      });

      // The sender: a deployment whose run principal holds its sidecar key,
      // a run token for its action, and the grants a run needs to mail
      // another run.
      senderKey = await generateKeyPair();
      await seedWorkflowRun(h.db, {
        id: SENDER,
        tenantId: TENANT,
        address: SENDER_ADDRESS,
        publicKey: hexEncode(senderKey.publicKey),
      });
      const senderPrincipal = await deriveRunPrincipalId(TENANT, SENDER);
      const tokens = createRunTokenStore({
        db: h.db,
        cipher: createNoopCredentialCipher(),
      });
      await tokens.mintIfAbsent(
        { principalId: senderPrincipal, consumers: [CONSUMER] },
        h.db,
      );
      const [delivered] = await tokens.loadDelivery(senderPrincipal);
      if (delivered === undefined) throw new Error("expected a run token");
      token = delivered.token;
      await seedGrant(h.db, {
        id: "grt_sender_manage_target",
        tenantId: TENANT,
        resource: `workflow-run:${TARGET}`,
        action: "manage",
        effect: "allow",
        origin: "creator",
        principalId: senderPrincipal,
      });
      await seedGrant(h.db, {
        id: "grt_sender_send",
        tenantId: TENANT,
        resource: "mail.send:*:*",
        action: "send",
        effect: "allow",
        origin: "creator",
        principalId: senderPrincipal,
      });

      // The target: a deployment that has not fired.
      await seedAsset(h.db, {
        id: "ast_rest_mail_target",
        tenantId: TENANT,
        kind: "workflow",
        name: "ast_rest_mail_target",
        creatorPrincipalId: CREATOR,
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_rest_mail_target",
        tenantId: TENANT,
        name: "wfd_rest_mail_target",
        assetId: "ast_rest_mail_target",
      });
      await seedWorkflowRun(h.db, {
        id: TARGET,
        tenantId: TENANT,
        anchorRunId: TARGET,
        definitionId: "wfd_rest_mail_target",
        address: TARGET_ADDRESS,
        status: "deployed",
        // Initialization recorded a credential resolution with nothing bound.
        credentialRefs: { credentialIds: [], bindings: [] },
      });
      await seedPrincipal(h.db, {
        id: await deriveRunPrincipalId(TENANT, TARGET),
        tenantId: TENANT,
        kind: "workflow",
        refId: TARGET,
      });
      await seedWorkflowRunLaunchSpec(h.db, {
        anchorRunId: TARGET,
        sourceAuthorityPrincipalId: CREATOR,
        grantSnapshot: { perStep: [], grantRequirements: [] },
      });
    });

    function stubRouter() {
      return createSidecarRouter({
        withExecutableWorkflowRun: async (_target, send) => send(),
        authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
        resolveSidecarBindings: async () => [],
        validateSidecarIdentity: async () => true,
      });
    }

    function makeApp(
      routed: Parameters<SidecarRouter["routeMail"]>[],
      workflowDispatchService?: WorkflowDispatchService,
    ) {
      const router = stubRouter();
      return createApp({
        getSession: noSession,
        authHandler: () => new Response("", { status: 404 }),
        db: h.db,
        credentialCipher: createNoopCredentialCipher(),
        sidecarRouter: {
          ...router,
          async routeMail(...args) {
            routed.push(args);
            return true;
          },
        },
        sessionService: mockSessionService(),
        eventCollectors: createEventCollectorRegistry({ db: h.db }),
        assetService: stubAssetService(),
        repoStore: stubRepoStore(),
        maxTarballBytes: 10_000_000,
        ...(workflowDispatchService !== undefined
          ? { workflowDispatchService }
          : {}),
      });
    }

    async function postMail(
      app: ReturnType<typeof makeApp>,
      raw: Uint8Array,
    ): Promise<Response> {
      return app.request(
        `/api/tenants/${TENANT}/workflows/runs/${TARGET}/mail`,
        {
          method: "POST",
          headers: {
            authorization: `Bearer ${token}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({ rawMessage: base64Encode(raw) }),
        },
      );
    }

    test("relays the verified bytes to the target under the sender's address and key", async () => {
      const routed: Parameters<SidecarRouter["routeMail"]>[] = [];
      const raw = await signedMail(senderKey);
      const res = await postMail(makeApp(routed), raw);
      if (res.status !== 202) {
        throw new Error(
          `expected 202, got ${String(res.status)}: ${await res.text()}`,
        );
      }
      const body = type({ address: "string" }).assert(await res.json());
      expect(body.address).toBe(TARGET_ADDRESS);

      expect(routed).toHaveLength(1);
      const [to, base64, authenticatedSender, , options] = routed[0] ?? [];
      expect(to).toBe(TARGET_ADDRESS);
      expect(authenticatedSender).toBe(SENDER_ADDRESS);
      expect(base64Decode(String(base64))).toEqual(raw);
      expect(options?.senderIdentities).toEqual([
        {
          address: SENDER_ADDRESS,
          publicKey: hexEncode(senderKey.publicKey),
        },
      ]);
    });

    test("answers a retryable 503 while the sender has no active key, and relays nothing", async () => {
      const routed: Parameters<SidecarRouter["routeMail"]>[] = [];
      const raw = await signedMail(senderKey);
      // A sidecar replacement retires the run's key until the new sidecar
      // records its own.
      await retireRunPrincipalKey(h.db, TENANT, SENDER);
      const res = await postMail(makeApp(routed), raw);
      expect(res.status).toBe(503);
      expect(res.headers.get("retry-after")).toBe("5");
      const body = type({ error: { code: "string" } }).assert(await res.json());
      expect(body.error.code).toBe("sender_key_unavailable");
      expect(routed).toEqual([]);
    });

    describe("to a provisioned deployment", () => {
      // The durable dispatches the service handed to the sidecar.
      let dispatched: string[];
      // Every drain the trigger started, so no test ends with one in flight.
      let drains: Promise<number>[];

      beforeEach(async () => {
        dispatched = [];
        drains = [];
        await seedAllocation(h.db, {
          tenantId: TENANT,
          anchorRunId: TARGET,
          sidecarId: "sidecar_rest_mail_target",
          allocationId: "alloc_rest_mail_target",
          generation: 1,
        });
      });

      afterEach(async () => {
        await Promise.all(drains);
      });

      // The production dispatch service over the real stores. Its own enqueue
      // starts a drain that a caller cannot await, so the trigger gets the
      // store's enqueue and a wake that records the drain it starts.
      function makeDispatchService(): WorkflowDispatchService {
        const router = stubRouter();
        const dispatchStore = createWorkflowRunDispatchStore(h.db);
        const service = createWorkflowDispatchService({
          dispatchStore,
          allocationStore: createSidecarAllocationStore(h.db, {
            getSidecarInventory: router.getSidecarInventory,
          }),
          router: {
            ...router,
            async sendWorkflowRunDispatchToAllocation(
              _target,
              _agentAddress,
              _runId,
              _stepGrants,
              _rawMessage,
              _authenticatedSender,
              messageId,
            ) {
              dispatched.push(messageId);
            },
          },
          resolveAnchorAddress: async () => TARGET_ADDRESS,
        });
        return {
          ...service,
          enqueue: (args, tx) => dispatchStore.enqueue(args, tx),
          wake: () => {
            drains.push(service.reconcileUntilIdle());
          },
        };
      }

      async function dispatchRows() {
        return h.db
          .select({ messageId: workflowRunDispatch.messageId })
          .from(workflowRunDispatch)
          .where(eq(workflowRunDispatch.anchorRunId, TARGET));
      }

      test("queues a re-sent Message-ID once, and refuses it with other bytes", async () => {
        const routed: Parameters<SidecarRouter["routeMail"]>[] = [];
        const app = makeApp(routed, makeDispatchService());
        // A caller-signed Message-ID names the sender's own scope.
        const messageId = `<durable-1@${SENDER}.${DOMAIN}>`;
        const raw = await signedMail(senderKey, messageId);

        for (const attempt of [1, 2]) {
          const res = await postMail(app, raw);
          if (res.status !== 202) {
            throw new Error(
              `attempt ${String(attempt)}: expected 202, got ${String(res.status)}: ${await res.text()}`,
            );
          }
          const body = type({ messageId: "string" }).assert(await res.json());
          expect(body.messageId).toBe(messageId);
        }
        expect(await dispatchRows()).toEqual([{ messageId }]);

        const conflicting = await signedMail(
          senderKey,
          messageId,
          "other bytes under the same Message-ID",
        );
        const res = await postMail(app, conflicting);
        expect(res.status).toBe(409);
        const body = type({ error: { code: "string" } }).assert(
          await res.json(),
        );
        expect(body.error.code).toBe("message_id_conflict");
        expect(await dispatchRows()).toEqual([{ messageId }]);

        await Promise.all(drains);
        expect(dispatched).toEqual([messageId]);
        // The durable path never takes the live relay.
        expect(routed).toEqual([]);
      });
    });
  },
);
