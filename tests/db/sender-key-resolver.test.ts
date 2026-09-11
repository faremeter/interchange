import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  auditSenderKeys,
  createPrincipalKeyStore,
  resolveSenderKey,
  resolveFrameSenderKey,
  type PrincipalKeyStore,
} from "@intx/db";
import { tenant as tenantTable } from "@intx/db/schema";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedWorkflowRun } from "@intx/test-harness/seed";

const cipher = createTestCredentialCipher();
const RUN_PUBLIC_KEY = "ab".repeat(32);

describe.skipIf(!harnessDbEnvAvailable())("resolveSenderKey (real DB)", () => {
  let h: TestDb;
  let store: PrincipalKeyStore;

  beforeAll(async () => {
    h = await createTestDb();
  });

  afterAll(async () => {
    await h.close();
  });

  beforeEach(async () => {
    await h.reset();
    store = createPrincipalKeyStore({ db: h.db, cipher });
  });

  async function seedTenant(id: string, domain: string): Promise<void> {
    await h.db.insert(tenantTable).values({
      id,
      name: id,
      slug: id,
      domain,
      parentId: null,
    });
  }

  test("resolves a run address to the key recorded on its deployment anchor", async () => {
    await seedTenant("tnt_run", "run.localhost");
    const address = "run_deadbeef@run.localhost";
    await seedWorkflowRun(h.db, {
      id: "run_deadbeef",
      tenantId: "tnt_run",
      address,
      publicKey: RUN_PUBLIC_KEY,
      status: "running",
    });

    expect(await resolveSenderKey(h.db, store, address)).toEqual({
      source: "run",
      publicKey: RUN_PUBLIC_KEY,
    });
  });

  test("returns null for a run whose deploy has not been acked yet", async () => {
    await seedTenant("tnt_preack", "preack.localhost");
    const address = "run_preack01@preack.localhost";
    // A deployed-but-not-yet-acked anchor carries an address with a null
    // public_key; that is the expected pre-ack state, not an error.
    await seedWorkflowRun(h.db, {
      id: "run_preack01",
      tenantId: "tnt_preack",
      address,
      publicKey: null,
      status: "deployed",
    });

    expect(await resolveSenderKey(h.db, store, address)).toBeNull();
  });

  test("returns null for an unknown run address", async () => {
    expect(
      await resolveSenderKey(h.db, store, "run_missing@nowhere.localhost"),
    ).toBeNull();
  });

  test("resolves a user address to the principal's hub key", async () => {
    await seedTenant("tnt_user", "user.localhost");
    await seedPrincipal(h.db, {
      id: "prn_user",
      tenantId: "tnt_user",
      kind: "user",
      refId: "usr_alice",
      status: "active",
    });
    const publicKey = await store.generate("prn_user");

    expect(
      await resolveSenderKey(h.db, store, "usr_alice@user.localhost"),
    ).toEqual({ source: "user", publicKey });
  });

  test("returns null for an address that matches no user principal", async () => {
    await seedTenant("tnt_nouser", "nouser.localhost");
    expect(
      await resolveSenderKey(h.db, store, "usr_ghost@nouser.localhost"),
    ).toBeNull();
  });

  test("returns null for a malformed address", async () => {
    expect(await resolveSenderKey(h.db, store, "no-at-sign")).toBeNull();
    expect(await resolveSenderKey(h.db, store, "@empty-local")).toBeNull();
    expect(await resolveSenderKey(h.db, store, "empty-domain@")).toBeNull();
  });

  test("matches the stored domain and refId case-insensitively", async () => {
    // The stored domain derives from an unnormalized slug (mixed case), while an
    // inbound From is lowercased when parsed. Resolution must still succeed.
    await seedTenant("tnt_case", "Acme.Localhost");
    await seedPrincipal(h.db, {
      id: "prn_case",
      tenantId: "tnt_case",
      kind: "user",
      refId: "USR_Case",
      status: "active",
    });
    const publicKey = await store.generate("prn_case");
    await seedWorkflowRun(h.db, {
      id: "run_caseabc0",
      tenantId: "tnt_case",
      address: "run_caseabc0@Acme.Localhost",
      publicKey: RUN_PUBLIC_KEY,
      status: "running",
    });

    expect(
      await resolveSenderKey(h.db, store, "usr_case@acme.localhost"),
    ).toEqual({ source: "user", publicKey });
    expect(
      await resolveSenderKey(h.db, store, "run_caseabc0@acme.localhost"),
    ).toEqual({ source: "run", publicKey: RUN_PUBLIC_KEY });
  });

  test("prefers the exact canonical refId when a case-variant refId collides", async () => {
    // A refId is a case-sensitively-unique betterAuth id that can carry
    // uppercase, so two principals in one tenant can hold case-variant refIds.
    // Seed the mixed-case variant first; the lowercased inbound address must
    // still resolve to the exact (canonical lowercase) principal's key.
    await seedTenant("tnt_case", "case.localhost");
    await seedPrincipal(h.db, {
      id: "prn_variant",
      tenantId: "tnt_case",
      kind: "user",
      refId: "USR_Alice",
      status: "active",
    });
    await store.generate("prn_variant");
    await seedPrincipal(h.db, {
      id: "prn_exact",
      tenantId: "tnt_case",
      kind: "user",
      refId: "usr_alice",
      status: "active",
    });
    const keyExact = await store.generate("prn_exact");

    expect(
      await resolveSenderKey(h.db, store, "usr_alice@case.localhost"),
    ).toEqual({ source: "user", publicKey: keyExact });
  });

  test("prefers the exact refId even when the tenant's stored domain is mixed-case", async () => {
    // A legacy tenant whose stored domain was never lowercased (creation
    // lowercases new ones; existing rows are left as stored). The exact-refId
    // preference must still fire -- the domain is matched case-insensitively --
    // so a mixed-case stored domain does not force the ambiguous ci fallback.
    await seedTenant("tnt_legacy", "Acme.Localhost");
    await seedPrincipal(h.db, {
      id: "prn_legacy_variant",
      tenantId: "tnt_legacy",
      kind: "user",
      refId: "USR_Alice",
      status: "active",
    });
    await store.generate("prn_legacy_variant");
    await seedPrincipal(h.db, {
      id: "prn_legacy_exact",
      tenantId: "tnt_legacy",
      kind: "user",
      refId: "usr_alice",
      status: "active",
    });
    const keyExact = await store.generate("prn_legacy_exact");

    expect(
      await resolveSenderKey(h.db, store, "usr_alice@acme.localhost"),
    ).toEqual({ source: "user", publicKey: keyExact });
  });

  test("throws when a user address is ambiguous with no canonical refId", async () => {
    // Two principals in one tenant with case-variant refIds, neither equal to
    // the lowercased inbound localPart, so no canonical row disambiguates them.
    // Rather than attribute the sender to an arbitrary principal, it fails loud.
    await seedTenant("tnt_amb", "amb.localhost");
    await seedPrincipal(h.db, {
      id: "prn_v1",
      tenantId: "tnt_amb",
      kind: "user",
      refId: "Usr_Alice",
      status: "active",
    });
    await store.generate("prn_v1");
    await seedPrincipal(h.db, {
      id: "prn_v2",
      tenantId: "tnt_amb",
      kind: "user",
      refId: "USR_ALICE",
      status: "active",
    });
    await store.generate("prn_v2");

    await expect(
      resolveSenderKey(h.db, store, "usr_alice@amb.localhost"),
    ).rejects.toThrow(/ambiguous/);
  });

  test("throws for a user principal with no active key", async () => {
    await seedTenant("tnt_keyless", "keyless.localhost");
    await seedPrincipal(h.db, {
      id: "prn_keyless",
      tenantId: "tnt_keyless",
      kind: "user",
      refId: "usr_keyless",
      status: "active",
    });
    // The principal exists but was never minted a key -- an INTR-164 invariant
    // break -- so resolution fails loudly rather than returning null.
    await expect(
      resolveSenderKey(h.db, store, "usr_keyless@keyless.localhost"),
    ).rejects.toThrow(/no active key/);
  });

  test("resolveFrameSenderKey returns the key, and degrades a fault to null", async () => {
    // A resolvable sender yields its hex key. A resolution FAULT -- here a
    // keyless principal, the INTR-164 break that makes resolveSenderKey throw --
    // degrades to null instead, so a resolution fault never blocks the send
    // path (the recipient then resolves the sender as unverifiable).
    await seedTenant("tnt_frame", "frame.localhost");
    await seedPrincipal(h.db, {
      id: "prn_frame_ok",
      tenantId: "tnt_frame",
      kind: "user",
      refId: "usr_ok",
      status: "active",
    });
    const publicKey = await store.generate("prn_frame_ok");
    expect(
      await resolveFrameSenderKey(h.db, store, "usr_ok@frame.localhost"),
    ).toBe(publicKey);

    await seedPrincipal(h.db, {
      id: "prn_frame_keyless",
      tenantId: "tnt_frame",
      kind: "user",
      refId: "usr_keyless2",
      status: "active",
    });
    await expect(
      resolveSenderKey(h.db, store, "usr_keyless2@frame.localhost"),
    ).rejects.toThrow(/no active key/);
    expect(
      await resolveFrameSenderKey(h.db, store, "usr_keyless2@frame.localhost"),
    ).toBeNull();
  });

  test("audit: reports no unresolved senders when every sender has a key", async () => {
    // Stand up a mixed population of senders that COULD sign -- two acked runs,
    // one deployed-but-not-yet-acked run (the legitimate address-set, key-null
    // row), and two active user principals across a lower- and a mixed-case
    // domain -- then sweep. The bin/ audit runs this same sweep against live
    // data; here it runs against seeds.
    await seedTenant("tnt_a", "a.localhost");
    await seedTenant("tnt_b", "B.Localhost");

    await seedPrincipal(h.db, {
      id: "prn_a1",
      tenantId: "tnt_a",
      kind: "user",
      refId: "usr_a1",
      status: "active",
    });
    await store.generate("prn_a1");
    await seedPrincipal(h.db, {
      id: "prn_b1",
      tenantId: "tnt_b",
      kind: "user",
      refId: "usr_b1",
      status: "active",
    });
    await store.generate("prn_b1");

    await seedWorkflowRun(h.db, {
      id: "run_a1",
      tenantId: "tnt_a",
      address: "run_a1@a.localhost",
      publicKey: RUN_PUBLIC_KEY,
      status: "running",
    });
    await seedWorkflowRun(h.db, {
      id: "run_a2",
      tenantId: "tnt_a",
      address: "run_a2@a.localhost",
      publicKey: RUN_PUBLIC_KEY,
      status: "completed",
    });
    await seedWorkflowRun(h.db, {
      id: "run_a3",
      tenantId: "tnt_a",
      address: "run_a3@a.localhost",
      publicKey: null,
      status: "deployed",
    });

    const report = await auditSenderKeys(h.db, store);
    // The pre-ack run (run_a3) is excluded from the sweep, so two runs and two
    // users are checked and none is unresolvable.
    expect(report).toEqual({
      runsChecked: 2,
      usersChecked: 2,
      unresolved: [],
    });
  });

  test("audit: reports a signing sender that has no durable key", async () => {
    await seedTenant("tnt_hole", "hole.localhost");
    // A run past the pre-ack window whose key was never recorded: the exact
    // unresolvable-signed-sender hole the audit exists to find.
    await seedWorkflowRun(h.db, {
      id: "run_hole",
      tenantId: "tnt_hole",
      address: "run_hole@hole.localhost",
      publicKey: null,
      status: "running",
    });

    const report = await auditSenderKeys(h.db, store);
    expect(report.unresolved).toEqual([
      { address: "run_hole@hole.localhost", kind: "run" },
    ]);
  });

  test("audit: ignores a run that failed before its deploy was acked", async () => {
    await seedTenant("tnt_failed", "failed.localhost");
    // A deploy that fails before ack flips the anchor deployed -> failed while
    // the public key is still null and the address is kept. It never signed, so
    // the audit must NOT flag it -- the boundary is not merely "past deployed".
    await seedWorkflowRun(h.db, {
      id: "run_failedanchor",
      tenantId: "tnt_failed",
      address: "run_failedanchor@failed.localhost",
      publicKey: null,
      status: "failed",
    });

    const report = await auditSenderKeys(h.db, store);
    expect(report).toEqual({
      runsChecked: 0,
      usersChecked: 0,
      unresolved: [],
    });
  });
});
