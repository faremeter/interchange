import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  createDetachedSignatureWithSigner,
  createEd25519Crypto,
  createNoopCredentialCipher,
  generateKeyPair,
} from "@intx/crypto";
import { createPrincipalKeyStore, retireRunPrincipalKey } from "@intx/db";
import { admitCallerSignedMail } from "@intx/hub-api";
import {
  assembleMessage,
  assembleSignedContent,
  createDetachedSignatureFromProvider,
  generateMessageId,
  generateSenderScopedMessageId,
  type MessageHeaders,
} from "@intx/mime";
import { base64Encode, hexEncode } from "@intx/types";
import type { KeyPair } from "@intx/types/runtime";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedPrincipal,
  seedPrincipalKey,
  seedTenants,
  seedWorkflowRun,
  tenantSlugFromId,
} from "@intx/test-harness/seed";

// A caller whose key the hub does not hold posts a message it signed. The hub
// checks the routing headers, resolves the key it vouches for at the caller's
// address, and verifies the signature against it. This drives that against a
// real schema, so the run and user key resolution is exercised, not mocked.

const TENANT = "tnt_caller_signed";
const DOMAIN = `${tenantSlugFromId(TENANT).toLowerCase()}.example.test`;
const SENDER_RUN = "run_caller_signed_sender";
const TARGET_RUN = "run_caller_signed_target";
const SENDER = `${SENDER_RUN}@${DOMAIN}`;
const TARGET = `${TARGET_RUN}@${DOMAIN}`;
const USER_PRINCIPAL = "prn_caller_signed_user";
const USER_REF = "usr_caller_signed";
// The id-right a sender's Message-IDs must carry.
const SCOPE = `${SENDER_RUN}.${DOMAIN}`;

type Overrides = Partial<
  Pick<
    MessageHeaders,
    | "from"
    | "to"
    | "cc"
    | "messageId"
    | "interchangeType"
    | "interchangeTenantId"
  >
>;

async function signedMessage(
  sign: (content: Uint8Array) => Promise<Uint8Array>,
  overrides: Overrides = {},
): Promise<Uint8Array> {
  const headers: MessageHeaders = {
    from: SENDER,
    to: [TARGET],
    cc: undefined,
    date: new Date("2026-01-15T12:00:00Z"),
    messageId: generateSenderScopedMessageId(SENDER),
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
    ...overrides,
  };
  const signedContent = assembleSignedContent({
    kind: "conversation",
    text: "from the run",
  });
  return assembleMessage(headers, signedContent, await sign(signedContent));
}

function signerFor(keyPair: KeyPair) {
  const crypto = createEd25519Crypto(keyPair);
  return (content: Uint8Array) =>
    createDetachedSignatureFromProvider(content, crypto);
}

describe.skipIf(!harnessDbEnvAvailable())(
  "admitCallerSignedMail (real DB)",
  () => {
    let h: TestDb;
    let runKey: KeyPair;
    const principalKeyStore = () =>
      createPrincipalKeyStore({
        db: h.db,
        cipher: createNoopCredentialCipher(),
      });

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT }]);
      runKey = await generateKeyPair();
      await seedWorkflowRun(h.db, {
        id: SENDER_RUN,
        tenantId: TENANT,
        address: SENDER,
        publicKey: hexEncode(runKey.publicKey),
      });
    });

    function admit(raw: Uint8Array, callerAddress = SENDER) {
      return admitCallerSignedMail(h.db, principalKeyStore(), {
        rawMessageBase64: base64Encode(raw),
        callerAddress,
        targetAddress: TARGET,
        tenantId: TENANT,
      });
    }

    test("admits a run's message signed with its recorded key", async () => {
      const raw = await signedMessage(signerFor(runKey));
      const admitted = await admit(raw);
      if ("status" in admitted) {
        throw new Error(`refused: ${admitted.code} ${admitted.message}`);
      }
      expect(admitted.rawMessage).toEqual(raw);
      expect(admitted.publicKey).toBe(hexEncode(runKey.publicKey));
    });

    test("admits a user's message signed with its hub-held key", async () => {
      await seedPrincipal(h.db, {
        id: USER_PRINCIPAL,
        tenantId: TENANT,
        kind: "user",
        refId: USER_REF,
        status: "active",
      });
      await seedPrincipalKey(h.db, USER_PRINCIPAL);
      const user = `${USER_REF}@${DOMAIN}`;
      const raw = await signedMessage(
        (content) =>
          createDetachedSignatureWithSigner(content, (input) =>
            principalKeyStore().sign(USER_PRINCIPAL, input),
          ),
        { from: user, messageId: generateSenderScopedMessageId(user) },
      );
      const admitted = await admit(raw, user);
      expect("status" in admitted).toBe(false);
    });

    test("refuses a signature by another key", async () => {
      const raw = await signedMessage(signerFor(await generateKeyPair()));
      expect(await admit(raw)).toMatchObject({
        status: 400,
        code: "mail_signature_invalid",
      });
    });

    test("refuses a message with no signature", async () => {
      const unsigned = new TextEncoder().encode(
        `From: ${SENDER}\r\nTo: ${TARGET}\r\nMessage-ID: <a@${SENDER_RUN}.${DOMAIN}>\r\nInterchange-Type: conversation.message\r\n\r\nhi`,
      );
      expect(await admit(unsigned)).toMatchObject({
        status: 400,
        code: "mail_signature_invalid",
      });
    });

    test("answers a retryable 503 when the run's key is retired", async () => {
      await retireRunPrincipalKey(h.db, TENANT, SENDER_RUN);
      const raw = await signedMessage(signerFor(runKey));
      expect(await admit(raw)).toMatchObject({
        status: 503,
        code: "sender_key_unavailable",
        retryAfterSeconds: 5,
      });
    });

    test("refuses routing headers that do not match the caller and target", async () => {
      const sign = signerFor(runKey);
      const cases: [string, Overrides][] = [
        ["another sender", { from: `run_other@${DOMAIN}` }],
        ["another target", { to: [`run_other@${DOMAIN}`] }],
        ["two recipients", { to: [TARGET, `run_other@${DOMAIN}`] }],
        ["a cc", { cc: [`run_other@${DOMAIN}`] }],
        ["another type", { interchangeType: "conversation.join" }],
        ["another tenant", { interchangeTenantId: "tnt_other" }],
        [
          "an oversized message id",
          { messageId: `<${"x".repeat(300)}@b.test>` },
        ],
        [
          "a message id in the plain domain",
          { messageId: generateMessageId(SENDER) },
        ],
        ["a message id with a NUL", { messageId: `<a\u0000b@${SCOPE}>` }],
        [
          "a message id with a control character",
          { messageId: `<a\u0007b@${SCOPE}>` },
        ],
        ["a message id with DEL", { messageId: `<a\u007fb@${SCOPE}>` }],
        [
          "a message id with non-ASCII text",
          { messageId: `<caf\u00e9@${SCOPE}>` },
        ],
        [
          "another sender's message id",
          { messageId: generateSenderScopedMessageId(`run_other@${DOMAIN}`) },
        ],
        [
          "a message id scoped in upper case",
          {
            messageId: `<x@${SENDER_RUN.toUpperCase()}.${DOMAIN}>`,
          },
        ],
      ];
      for (const [label, overrides] of cases) {
        const result = await admit(await signedMessage(sign, overrides));
        expect({ label, result }).toMatchObject({
          label,
          result: { status: 400, code: "mail_header_invalid" },
        });
      }
    });

    test("accepts a tenant header that names this tenant", async () => {
      const raw = await signedMessage(signerFor(runKey), {
        interchangeTenantId: TENANT,
      });
      expect("status" in (await admit(raw))).toBe(false);
    });

    test("refuses a repeated From header", async () => {
      const raw = await signedMessage(signerFor(runKey));
      const text = new TextDecoder().decode(raw);
      const doubled = new TextEncoder().encode(
        text.replace(
          `From: ${SENDER}\r\n`,
          `From: ${SENDER}\r\nFrom: run_other@${DOMAIN}\r\n`,
        ),
      );
      expect(await admit(doubled)).toMatchObject({
        status: 400,
        code: "mail_header_invalid",
      });
    });

    test("refuses a body that is not base64", async () => {
      const result = await admitCallerSignedMail(h.db, principalKeyStore(), {
        rawMessageBase64: "not base64!",
        callerAddress: SENDER,
        targetAddress: TARGET,
        tenantId: TENANT,
      });
      expect(result).toMatchObject({
        status: 400,
        code: "malformed_raw_message",
      });
    });
  },
);
