import { and, eq, isNotNull, or, sql } from "drizzle-orm";

import { parseAddress } from "@intx/types";
import { getLogger } from "@intx/log";

import type { DBExecutor } from "./client";
import type { PrincipalKeyStore } from "./principal-key-store";
import { principal } from "./schema/principals";
import { tenant } from "./schema/tenants";
import { workflowRun } from "./schema/workflow-run";

const RUN_PREFIX = "run_";

const logger = getLogger(["db", "sender-key-resolver"]);

/**
 * The durable public key that authenticates a signed mail sender, resolved from
 * the hub's own storage. `source` records which store the key came from.
 */
export type SenderKeyResolution =
  | { source: "run"; publicKey: string }
  | { source: "user"; publicKey: string };

/**
 * Resolve the durable public key a signed mail sender must verify against: a
 * run address (`run_<id>@<domain>`) resolves to the run's `workflow_run.public_key`;
 * any other address resolves to the user principal's hub-custodied key.
 *
 * Returns `null` when the sender has no durable key (malformed address, unknown
 * run, pre-ack deploy, or no matching user principal). A `null` is a legitimate
 * "unresolvable sender" answer, NOT an error. Addresses match case-insensitively:
 * the resolver prefers the exact lowercase refId row and falls back to a
 * case-insensitive refId match. An ambiguous case-insensitive match with no exact
 * row throws rather than attribute the sender to the wrong principal.
 */
export async function resolveSenderKey(
  db: DBExecutor,
  principalKeyStore: PrincipalKeyStore,
  address: string,
): Promise<SenderKeyResolution | null> {
  const normalized = address.toLowerCase();
  const parsed = parseAddress(normalized);
  if (parsed === null) return null;
  const { localPart, domain } = parsed;

  if (localPart.startsWith(RUN_PREFIX)) {
    // A row whose `public_key` is still null is a deployed-but-not-yet-acked run
    // -- the expected pre-ack state -- so it resolves to nothing rather than
    // erroring, unlike the keyless-principal invariant break below.
    const [row] = await db
      .select({ publicKey: workflowRun.publicKey })
      .from(workflowRun)
      .where(eq(sql`lower(${workflowRun.address})`, normalized))
      .limit(1);
    if (row === undefined || row.publicKey === null) return null;
    return { source: "run", publicKey: row.publicKey };
  }

  const principalId = await resolveUserPrincipalId(db, domain, localPart);
  if (principalId === null) return null;
  // A principal with no active key violates the invariant that every principal
  // is minted with one, so `getPublicKey` throws rather than defaulting. Do not
  // soften that to null -- unlike the pre-ack run key above, a keyless
  // principal is a real breakage that must surface.
  const publicKey = await principalKeyStore.getPublicKey(principalId, db);
  return { source: "user", publicKey };
}

/**
 * Best-effort frame sender key that never blocks delivery: `null` on an
 * unresolvable sender OR on a resolution fault. `resolveSenderKey` throws on a
 * genuine fault (ambiguous address, keyless principal); those must fail loud for
 * {@link auditSenderKeys} but must not break the send path, so a throw here
 * degrades to `null` and is logged at ERROR -- a degraded fault, kept distinct
 * from the ordinary unresolvable-sender `null`, which stays silent.
 */
export async function resolveFrameSenderKey(
  db: DBExecutor,
  principalKeyStore: PrincipalKeyStore,
  address: string,
): Promise<string | null> {
  try {
    return (
      (await resolveSenderKey(db, principalKeyStore, address))?.publicKey ??
      null
    );
  } catch (cause) {
    logger.error`Degraded to a null frame sender key for ${address}: resolving its public key failed (a fault, not an unresolvable sender): ${cause instanceof Error ? cause.message : String(cause)}`;
    return null;
  }
}

/**
 * Resolve the user principal a normalized `<localPart>@<domain>` address names,
 * or `null` when none matches. The domain is matched case-insensitively (legacy
 * rows may store a mixed-case domain); within the tenant, an exact refId match
 * is preferred, with a case-insensitive refId fallback for mixed-case-stored
 * refIds. A non-unique case-insensitive match with no exact row is genuinely
 * ambiguous, so it throws rather than return an arbitrary principal.
 */
async function resolveUserPrincipalId(
  db: DBExecutor,
  domain: string,
  localPart: string,
): Promise<string | null> {
  const [exact] = await db
    .select({ principalId: principal.id })
    .from(principal)
    .innerJoin(tenant, eq(principal.tenantId, tenant.id))
    .where(
      and(
        eq(sql`lower(${tenant.domain})`, domain),
        eq(principal.kind, "user"),
        eq(principal.refId, localPart),
      ),
    )
    .limit(1);
  if (exact !== undefined) return exact.principalId;

  const caseInsensitive = await db
    .select({ principalId: principal.id })
    .from(principal)
    .innerJoin(tenant, eq(principal.tenantId, tenant.id))
    .where(
      and(
        eq(sql`lower(${tenant.domain})`, domain),
        eq(principal.kind, "user"),
        eq(sql`lower(${principal.refId})`, localPart),
      ),
    )
    .limit(2);
  if (caseInsensitive.length === 0) return null;
  if (caseInsensitive.length > 1) {
    throw new Error(
      `resolveSenderKey: user sender ${localPart}@${domain} is ambiguous; it ` +
        `matches multiple principals with case-variant refIds and no canonical ` +
        `row -- the colliding principal refIds must be reconciled`,
    );
  }
  const [only] = caseInsensitive;
  return only === undefined ? null : only.principalId;
}

/**
 * Outcome of sweeping every sender that could sign mail: how many of each kind
 * were checked, and any whose address did not resolve to a durable key. An
 * empty `unresolved` is the state a durable-signature verifier depends on.
 */
export type SenderKeyAuditReport = {
  runsChecked: number;
  usersChecked: number;
  unresolved: { address: string; kind: "run" | "user" }[];
};

/**
 * Confirm every sender that could sign mail resolves to a durable public key
 * via {@link resolveSenderKey}. Read-only.
 *
 * The senders are every workflow run that holds a signing key or is live and
 * carries a sending address, plus every active user principal. A never-signed
 * run (pre-ack deploy, or failed/cancelled before ack) is excluded. Any checked
 * sender that does not resolve is collected in `unresolved`; a keyless user
 * principal makes `resolveSenderKey` throw and the sweep fail loudly -- a more
 * serious fault than an unresolvable address.
 */
export async function auditSenderKeys(
  db: DBExecutor,
  principalKeyStore: PrincipalKeyStore,
): Promise<SenderKeyAuditReport> {
  const unresolved: { address: string; kind: "run" | "user" }[] = [];

  const runs = await db
    .select({ address: workflowRun.address })
    .from(workflowRun)
    .where(
      and(
        isNotNull(workflowRun.address),
        // A key-null row is a non-signer EXCEPT a live "running" run, the
        // genuine can-sign-but-keyless hole to flag. Match "running" literally,
        // NOT isLiveWorkflowRunStatus, which also admits "deployed" and would
        // re-open the pre-ack false positive.
        or(isNotNull(workflowRun.publicKey), eq(workflowRun.status, "running")),
      ),
    );
  for (const run of runs) {
    if (run.address === null) continue;
    if ((await resolveSenderKey(db, principalKeyStore, run.address)) === null) {
      unresolved.push({ address: run.address, kind: "run" });
    }
  }

  const users = await db
    .select({ refId: principal.refId, domain: tenant.domain })
    .from(principal)
    .innerJoin(tenant, eq(principal.tenantId, tenant.id))
    .where(and(eq(principal.kind, "user"), eq(principal.status, "active")));
  for (const user of users) {
    const address = `${user.refId}@${user.domain}`;
    if ((await resolveSenderKey(db, principalKeyStore, address)) === null) {
      unresolved.push({ address, kind: "user" });
    }
  }

  return { runsChecked: runs.length, usersChecked: users.length, unresolved };
}
