import {
  afterAll,
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
import { createRunTokenStore, retireRunPrincipalKey } from "@intx/db";
import { workflowDefinition } from "@intx/db/schema";
import { createApp, type GetSession } from "@intx/hub-api";
import { deriveRunPrincipalId } from "@intx/hub-common";
import {
  createEventCollectorRegistry,
  createSidecarRouter,
  type AssetService,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
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

async function signedMail(keyPair: KeyPair): Promise<Uint8Array> {
  const signedContent = assembleSignedContent({
    kind: "conversation",
    text: "from the sender run",
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
      messageId: generateSenderScopedMessageId(SENDER_ADDRESS),
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

    function makeApp(routed: Parameters<SidecarRouter["routeMail"]>[]) {
      const router = createSidecarRouter({
        withExecutableWorkflowRun: async (_target, send) => send(),
        authenticateSidecar: async ({ sidecarId }) => ({ sidecarId }),
        resolveSidecarBindings: async () => [],
        validateSidecarIdentity: async () => true,
      });
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
      });
    }

    test("relays the verified bytes to the target under the sender's address and key", async () => {
      const routed: Parameters<SidecarRouter["routeMail"]>[] = [];
      const raw = await signedMail(senderKey);
      const res = await makeApp(routed).request(
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
      const res = await makeApp(routed).request(
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
      expect(res.status).toBe(503);
      expect(res.headers.get("retry-after")).toBe("5");
      const body = type({ error: { code: "string" } }).assert(await res.json());
      expect(body.error.code).toBe("sender_key_unavailable");
      expect(routed).toEqual([]);
    });
  },
);
