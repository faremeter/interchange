import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq } from "drizzle-orm";

import { createGrantStore, deactivateRunPrincipals } from "@intx/db";
import {
  principal,
  principalRole,
  role,
  tenant as tenantTable,
  workflowDefinition,
} from "@intx/db/schema";
import { deriveRunPrincipalId } from "@intx/hub-common";
import { createMailSendAuthorizer } from "@intx/hub-api";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedGrant,
  seedPrincipal,
  seedWorkflowRun,
  tenantSlugFromId,
} from "@intx/test-harness/seed";

const TENANT = "tnt_send";
const DOMAIN = "send.localhost";
const OTHER_TENANT = "tnt_other";
const OTHER_DOMAIN = "other.localhost";
const SENDER_RUN = "run_sender";
const SENDER_ADDRESS = `${SENDER_RUN}@${DOMAIN}`;
const SENDER_PRINCIPAL = "prn_sender";

describe.skipIf(!harnessDbEnvAvailable())(
  "createMailSendAuthorizer (real DB)",
  () => {
    let h: TestDb;
    let grantCounter = 0;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      for (const [id, domain] of [
        [TENANT, DOMAIN],
        [OTHER_TENANT, OTHER_DOMAIN],
      ] as const) {
        await h.db.insert(tenantTable).values({
          id,
          name: id,
          slug: tenantSlugFromId(id),
          domain,
          parentId: null,
        });
      }
      await seedPrincipal(h.db, {
        id: SENDER_PRINCIPAL,
        tenantId: TENANT,
        kind: "workflow",
        refId: SENDER_RUN,
        status: "active",
      });
      await seedAnchor(TENANT, SENDER_RUN, { principalId: SENDER_PRINCIPAL });
    });

    async function seedAnchor(
      tenantId: string,
      id: string,
      opts: {
        status?: "deployed" | "running" | "completed";
        principalId?: string;
        definitionId?: string;
      } = {},
    ) {
      const domain = tenantId === TENANT ? DOMAIN : OTHER_DOMAIN;
      await seedWorkflowRun(h.db, {
        id,
        tenantId,
        anchorRunId: id,
        address: `${id}@${domain}`,
        status: opts.status ?? "running",
        ...(opts.principalId === undefined
          ? {}
          : { principalId: opts.principalId }),
        ...(opts.definitionId === undefined
          ? {}
          : { definitionId: opts.definitionId }),
      });
    }

    async function grantSender(
      resource: string,
      effect: "allow" | "deny" = "allow",
      extra: { expiresAt?: Date } = {},
    ) {
      grantCounter += 1;
      await seedGrant(h.db, {
        id: `grt_send_${String(grantCounter)}`,
        tenantId: TENANT,
        principalId: SENDER_PRINCIPAL,
        resource,
        action: "send",
        effect,
        ...extra,
      });
    }

    const authorize = createMailSendAuthorizer({
      get db() {
        return h.db;
      },
      grantStoreFor: createGrantStore,
    });

    async function decide(recipients: string[]) {
      const result = await authorize({
        sender: { kind: "address", address: SENDER_ADDRESS },
        recipients,
      });
      if (!result.ok) throw new Error(`sender refused: ${result.reason}`);
      return result.decisions.map((d) => ({
        recipient: d.recipient,
        allowed: d.allowed,
      }));
    }

    test("refuses an unknown sender address", async () => {
      expect(
        await authorize({
          sender: { kind: "address", address: `run_absent@${DOMAIN}` },
          recipients: ["x@example.com"],
        }),
      ).toEqual({ ok: false, reason: "sender_unresolved" });
    });

    // A run deactivates its principal when it ends. Mail it sent before then
    // can still wait in the hub, so its grants decide, not its status.
    test("decides a run that has ended by its grants", async () => {
      await grantSender("mail.send:*:*");
      await grantSender("mail.send:domain:evil.example", "deny");
      await deactivateRunPrincipals(h.db, TENANT, [SENDER_RUN], new Date());

      expect(await decide(["x@example.com", "x@evil.example"])).toEqual([
        { recipient: "x@example.com", allowed: true },
        { recipient: "x@evil.example", allowed: false },
      ]);
    });

    test("decides a suspended sender by its grants", async () => {
      await grantSender("mail.send:*:*");
      await h.db
        .update(principal)
        .set({ status: "suspended" })
        .where(eq(principal.id, SENDER_PRINCIPAL));

      expect(await decide(["x@example.com"])).toEqual([
        { recipient: "x@example.com", allowed: true },
      ]);
    });

    test("refuses every recipient when the sender holds no send grant", async () => {
      await seedAnchor(TENANT, "run_peer");
      expect(await decide(["x@example.com", `run_peer@${DOMAIN}`])).toEqual([
        { recipient: "x@example.com", allowed: false },
        { recipient: `run_peer@${DOMAIN}`, allowed: false },
      ]);
    });

    test("a namespace-wide allow allows an external recipient by its domain", async () => {
      await grantSender("mail.send:*:*");
      expect(await decide(["Someone@Example.COM"])).toEqual([
        { recipient: "Someone@Example.COM", allowed: true },
      ]);
    });

    test("an operator's namespace-wide deny narrows a run's own send grant", async () => {
      await grantSender("mail.send:*:*");
      await seedGrant(h.db, {
        id: "grt_send_operator_deny",
        tenantId: TENANT,
        principalId: SENDER_PRINCIPAL,
        resource: "mail.send:*",
        action: "*",
        effect: "deny",
      });
      await seedAnchor(TENANT, "run_peer");
      expect(await decide(["x@example.com", `run_peer@${DOMAIN}`])).toEqual([
        { recipient: "x@example.com", allowed: false },
        { recipient: `run_peer@${DOMAIN}`, allowed: false },
      ]);
    });

    // The send check has no consumer, so it cannot evaluate a `tool`
    // condition. A stored row that carries one throws, and the caller then
    // refuses every recipient, instead of the deny refusing nothing.
    test("a stored tool-conditioned deny fails the check closed", async () => {
      await grantSender("mail.send:*:*");
      await seedGrant(h.db, {
        id: "grt_send_tool_deny",
        tenantId: TENANT,
        principalId: SENDER_PRINCIPAL,
        resource: "mail.send:domain:example.com",
        action: "send",
        effect: "deny",
        conditions: { tool: "tool:pkg" },
      });
      await expect(decide(["x@example.com"])).rejects.toThrow("tool");
    });

    test("a principal deny reaches every spelling and state of a run recipient", async () => {
      await grantSender("mail.send:*:*");
      await seedAnchor(TENANT, "run_fresh", { status: "deployed" });
      await seedAnchor(TENANT, "run_done", { status: "completed" });
      for (const id of ["run_fresh", "run_done"]) {
        await grantSender(
          `mail.send:principal:${await deriveRunPrincipalId(TENANT, id)}`,
          "deny",
        );
      }

      const fresh = `RUN_FRESH@${DOMAIN.toUpperCase()}.`;
      const done = `run_done@${DOMAIN}`;
      const external = "x@example.com";
      expect(await decide([fresh, done, external])).toEqual([
        { recipient: fresh, allowed: false },
        { recipient: done, allowed: false },
        { recipient: external, allowed: true },
      ]);
    });

    test("an asset allow reaches a run of that asset only", async () => {
      await seedAsset(h.db, {
        id: "ast_send",
        tenantId: TENANT,
        kind: "workflow",
        name: "ast_send",
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_send",
        tenantId: TENANT,
        name: "wfd_send",
        assetId: "ast_send",
      });
      await seedAnchor(TENANT, "run_asset", { definitionId: "wfd_send" });
      await seedAnchor(TENANT, "run_plain");
      await grantSender("mail.send:asset:ast_send");

      expect(
        await decide([`run_asset@${DOMAIN}`, `run_plain@${DOMAIN}`]),
      ).toEqual([
        { recipient: `run_asset@${DOMAIN}`, allowed: true },
        { recipient: `run_plain@${DOMAIN}`, allowed: false },
      ]);
    });

    test("a tenant allow reaches a user and a run in another tenant", async () => {
      await seedPrincipal(h.db, {
        id: "prn_other_user",
        tenantId: OTHER_TENANT,
        kind: "user",
        refId: "usr_other",
        status: "active",
      });
      await seedAnchor(OTHER_TENANT, "run_other");
      await grantSender(`mail.send:tenant:${OTHER_TENANT}`);

      expect(
        await decide([
          `usr_other@${OTHER_DOMAIN}`,
          `run_other@${OTHER_DOMAIN}`,
          `run_sender@${DOMAIN}`,
        ]),
      ).toEqual([
        { recipient: `usr_other@${OTHER_DOMAIN}`, allowed: true },
        { recipient: `run_other@${OTHER_DOMAIN}`, allowed: true },
        { recipient: `run_sender@${DOMAIN}`, allowed: false },
      ]);
    });

    test("an ambiguous user recipient is refused and its co-recipient decided", async () => {
      await grantSender("mail.send:*:*");
      for (const [id, refId] of [
        ["prn_v1", "Usr_Alice"],
        ["prn_v2", "USR_ALICE"],
      ] as const) {
        await seedPrincipal(h.db, {
          id,
          tenantId: TENANT,
          kind: "user",
          refId,
          status: "active",
        });
      }

      const result = await authorize({
        sender: { kind: "address", address: SENDER_ADDRESS },
        recipients: [`usr_alice@${DOMAIN}`, "x@example.com"],
      });
      if (!result.ok) throw new Error(`sender refused: ${result.reason}`);
      // The ambiguous recipient is a fault, not a grant decision, so a caller
      // can tell it from a refusal.
      expect(
        result.decisions.map((d) => ({
          recipient: d.recipient,
          outcome: d.outcome,
          allowed: d.allowed,
        })),
      ).toEqual([
        {
          recipient: `usr_alice@${DOMAIN}`,
          outcome: "unresolved",
          allowed: false,
        },
        { recipient: "x@example.com", outcome: "evaluated", allowed: true },
      ]);
    });

    test("reads role grants and ignores expired grants", async () => {
      await h.db.insert(role).values({
        id: "rol_senders",
        tenantId: TENANT,
        name: "senders",
        isSystem: false,
      });
      await h.db.insert(principalRole).values({
        principalId: SENDER_PRINCIPAL,
        roleId: "rol_senders",
      });
      await seedGrant(h.db, {
        id: "grt_role_send",
        tenantId: TENANT,
        roleId: "rol_senders",
        resource: "mail.send:domain:example.com",
        action: "send",
      });
      await grantSender("mail.send:domain:expired.example", "allow", {
        expiresAt: new Date("2000-01-01T00:00:00.000Z"),
      });

      expect(await decide(["a@example.com", "b@expired.example"])).toEqual([
        { recipient: "a@example.com", allowed: true },
        { recipient: "b@expired.example", allowed: false },
      ]);
    });

    test("decides for an authenticated principal sender", async () => {
      await seedPrincipal(h.db, {
        id: "prn_owner",
        tenantId: TENANT,
        kind: "user",
        refId: "usr_owner",
        status: "active",
      });
      await seedGrant(h.db, {
        id: "grt_owner_send",
        tenantId: TENANT,
        principalId: "prn_owner",
        resource: `mail.send:tenant:${TENANT}`,
        action: "send",
      });

      const result = await authorize({
        sender: {
          kind: "principal",
          principalId: "prn_owner",
          tenantId: TENANT,
        },
        recipients: [SENDER_ADDRESS],
      });
      expect(result.ok && result.decisions[0]?.allowed).toBe(true);
    });

    test("a domain deny reaches every spelling of an external recipient", async () => {
      await grantSender("mail.send:*:*");
      await grantSender("mail.send:domain:example.com", "deny");
      const spellings = ["a@example.com", "B@Example.COM", "c@EXAMPLE.com."];
      expect(await decide([...spellings, "d@other.example"])).toEqual([
        ...spellings.map((recipient) => ({ recipient, allowed: false })),
        { recipient: "d@other.example", allowed: true },
      ]);
    });

    test("a domain deny reaches a local user and a local run recipient", async () => {
      await grantSender("mail.send:*:*");
      await grantSender(`mail.send:domain:${OTHER_DOMAIN}`, "deny");
      await seedPrincipal(h.db, {
        id: "prn_u",
        tenantId: OTHER_TENANT,
        kind: "user",
        refId: "usr_u",
        status: "active",
      });
      await seedAnchor(OTHER_TENANT, "run_o");
      expect(
        await decide([`usr_u@${OTHER_DOMAIN}`, `run_o@${OTHER_DOMAIN}`]),
      ).toEqual([
        { recipient: `usr_u@${OTHER_DOMAIN}`, allowed: false },
        { recipient: `run_o@${OTHER_DOMAIN}`, allowed: false },
      ]);
    });

    test("a tenant deny reaches a user and a run of that tenant", async () => {
      await grantSender("mail.send:*:*");
      await grantSender(`mail.send:tenant:${OTHER_TENANT}`, "deny");
      await seedPrincipal(h.db, {
        id: "prn_u",
        tenantId: OTHER_TENANT,
        kind: "user",
        refId: "usr_u",
        status: "active",
      });
      await seedAnchor(OTHER_TENANT, "run_o");
      expect(
        await decide([
          `usr_u@${OTHER_DOMAIN}`,
          `run_o@${OTHER_DOMAIN}`,
          "x@example.com",
        ]),
      ).toEqual([
        { recipient: `usr_u@${OTHER_DOMAIN}`, allowed: false },
        { recipient: `run_o@${OTHER_DOMAIN}`, allowed: false },
        { recipient: "x@example.com", allowed: true },
      ]);
    });

    test("a principal deny reaches a user recipient", async () => {
      await grantSender("mail.send:*:*");
      await seedPrincipal(h.db, {
        id: "prn_u",
        tenantId: OTHER_TENANT,
        kind: "user",
        refId: "usr_u",
        status: "active",
      });
      await grantSender("mail.send:principal:prn_u", "deny");
      expect(await decide([`USR_U@${OTHER_DOMAIN}`])).toEqual([
        { recipient: `USR_U@${OTHER_DOMAIN}`, allowed: false },
      ]);
    });

    test("a recipient with no coordinate is refused under a namespace-wide allow", async () => {
      await grantSender("mail.send:*:*");
      const odd = [
        "no-at-sign",
        "x@",
        "x@[127.0.0.1]",
        "a@b@example.com",
        "Name <x@example.com>",
        "x@example.com ",
      ];
      expect(await decide(odd)).toEqual(
        odd.map((recipient) => ({ recipient, allowed: false })),
      );
    });

    test("an address that names a user or a child run is not a sender", async () => {
      await seedPrincipal(h.db, {
        id: "prn_u",
        tenantId: TENANT,
        kind: "user",
        refId: "usr_u",
        status: "active",
      });
      expect(
        await authorize({
          sender: { kind: "address", address: `usr_u@${DOMAIN}` },
          recipients: ["x@example.com"],
        }),
      ).toEqual({ ok: false, reason: "sender_unresolved" });
      await seedWorkflowRun(h.db, {
        id: "run_child",
        tenantId: TENANT,
        anchorRunId: SENDER_RUN,
        address: `run_child@${DOMAIN}`,
        status: "running",
      });
      expect(
        await authorize({
          sender: { kind: "address", address: `run_child@${DOMAIN}` },
          recipients: ["x@example.com"],
        }),
      ).toEqual({ ok: false, reason: "sender_unresolved" });
    });

    test("a sender whose run principal row is missing is unresolved", async () => {
      await seedAnchor(TENANT, "run_noprn");
      expect(
        await authorize({
          sender: { kind: "address", address: `run_noprn@${DOMAIN}` },
          recipients: ["x@example.com"],
        }),
      ).toEqual({ ok: false, reason: "sender_unresolved" });
    });

    test("an unfired sender uses its derived run principal grants", async () => {
      const derived = await deriveRunPrincipalId(TENANT, "run_unfired");
      await seedPrincipal(h.db, {
        id: derived,
        tenantId: TENANT,
        kind: "workflow",
        refId: "run_unfired",
        status: "active",
      });
      await seedGrant(h.db, {
        id: "grt_unfired",
        tenantId: TENANT,
        principalId: derived,
        resource: "mail.send:domain:example.com",
        action: "send",
        effect: "allow",
      });
      await seedAnchor(TENANT, "run_unfired", { status: "deployed" });
      const result = await authorize({
        sender: { kind: "address", address: `RUN_UNFIRED@${DOMAIN}.` },
        recipients: ["x@example.com"],
      });
      expect(result.ok && result.decisions[0]?.allowed).toBe(true);
    });

    test("a sender grant row in another tenant does not count", async () => {
      await seedGrant(h.db, {
        id: "grt_cross",
        tenantId: OTHER_TENANT,
        principalId: SENDER_PRINCIPAL,
        resource: "mail.send:*:*",
        action: "send",
        effect: "allow",
      });
      expect(await decide(["x@example.com"])).toEqual([
        { recipient: "x@example.com", allowed: false },
      ]);
    });

    test("a broad star grant does not allow a send", async () => {
      await grantSender("*");
      await grantSender("mail.*");
      expect(await decide(["x@example.com"])).toEqual([
        { recipient: "x@example.com", allowed: false },
      ]);
    });

    test("an ask grant does not allow a send", async () => {
      await seedGrant(h.db, {
        id: "grt_ask",
        tenantId: TENANT,
        principalId: SENDER_PRINCIPAL,
        resource: "mail.send:*:*",
        action: "send",
        effect: "ask",
      });
      expect(await decide(["x@example.com"])).toEqual([
        { recipient: "x@example.com", allowed: false },
      ]);
    });

    test("an empty recipient list gives no decisions", async () => {
      expect(await decide([])).toEqual([]);
    });

    test("duplicate recipients each get a decision in order", async () => {
      await grantSender("mail.send:domain:example.com");
      expect(
        await decide(["x@example.com", "y@other.example", "x@example.com"]),
      ).toEqual([
        { recipient: "x@example.com", allowed: true },
        { recipient: "y@other.example", allowed: false },
        { recipient: "x@example.com", allowed: true },
      ]);
    });
  },
);
