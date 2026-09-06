import { and, eq, isNotNull, or, sql } from "drizzle-orm";

import { parseAddress } from "@intx/types";

import type { DBExecutor } from "./client";
import type { PrincipalKeyStore } from "./principal-key-store";
import { principal } from "./schema/principals";
import { tenant } from "./schema/tenants";
import { workflowRun } from "./schema/workflow-run";

const RUN_PREFIX = "run_";

/**
 * The durable public key that authenticates a signed mail sender, resolved from
 * the hub's own storage. `source` records which store the key came from -- a
 * run sender's key is the sidecar-minted key recorded on the deployment anchor,
 * a user sender's key is the hub-custodied principal key -- so a consumer can
 * bucket a resolution without re-parsing the address.
 */
export type SenderKeyResolution =
  | { source: "run"; publicKey: string }
  | { source: "user"; publicKey: string };

/**
 * Resolve the durable public key a signed mail sender must verify against,
 * unioning the two existing durable-key sources: a run address
 * (`run_<id>@<domain>`) resolves to the run's `workflow_run.public_key`; any
 * other address (`<refId>@<domain>`) is treated as a user sender and resolves
 * to that principal's hub-custodied key.
 *
 * Read-only. Returns `null` when the sender has no durable key to resolve -- a
 * malformed address, an unknown run, a run whose deploy has not been acked yet,
 * or an address that matches no user principal. A `null` is a legitimate
 * "unresolvable sender" answer the caller acts on; it is NOT an error.
 *
 * Addresses are matched case-insensitively. Inbound `From` addresses are
 * lowercased when parsed (see `@intx/mime` `extractAddrSpec`), but a stored
 * tenant domain derives from an unnormalized slug, so the address is normalized
 * here and stored values are compared under `lower(...)`. Because the tenant
 * domain and principal `refId` uniqueness constraints are case-sensitive,
 * case-variant rows can coexist and a case-insensitive user-address match can
 * hit more than one. The resolver prefers the exact (already-lowercase,
 * canonical) row; only when no exact row exists AND the case-insensitive match
 * is not unique is the sender genuinely ambiguous, and the resolver throws
 * rather than silently return one row's key -- which could be another tenant's.
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
    // A run sender resolves to the sidecar-minted key the hub recorded on the
    // deployment anchor at `agent.deploy.ack`. `workflow_run.address` is unique
    // among the runs that set it, so there is at most one row. A row whose
    // `public_key` is still null is a deployed-but-not-yet-acked run -- the
    // expected pre-ack state -- so it resolves to nothing rather than erroring,
    // unlike the keyless-principal invariant break below.
    const [row] = await db
      .select({ publicKey: workflowRun.publicKey })
      .from(workflowRun)
      .where(eq(sql`lower(${workflowRun.address})`, normalized))
      .limit(1);
    if (row === undefined || row.publicKey === null) return null;
    return { source: "run", publicKey: row.publicKey };
  }

  // A user sender resolves to its hub-custodied principal key, keyed by the
  // `(tenant domain, user refId)` its From address carries. Prefer the exact
  // (already-lowercase, canonical) row: the normalized address equals it, and
  // the tenant domain is case-sensitively unique, so at most one tenant holds
  // exactly this domain.
  const principalId = await resolveUserPrincipalId(db, domain, localPart);
  if (principalId === null) return null;
  // The principal exists; a principal with no active key violates INTR-164's
  // invariant that every principal is minted with one, so `getPublicKey` throws
  // rather than defaulting. Do not soften that to null -- unlike the pre-ack run
  // key above, a keyless principal is a real breakage that must surface.
  const publicKey = await principalKeyStore.getPublicKey(principalId, db);
  return { source: "user", publicKey };
}

/**
 * Resolve the user principal a normalized `<localPart>@<domain>` address names,
 * or `null` when none matches. Prefers the exact (already-lowercase) row; only
 * when no exact row exists does it fall back to a case-insensitive match, and a
 * non-unique case-insensitive match with no exact row is genuinely ambiguous --
 * two case-variant senders with no canonical row -- so it throws rather than
 * return an arbitrary one, which could be another tenant's principal.
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
        eq(tenant.domain, domain),
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
        `matches multiple case-variant principals with no canonical row -- ` +
        `case-variant tenant domains or refIds must be reconciled`,
    );
  }
  const [only] = caseInsensitive;
  return only === undefined ? null : only.principalId;
}

/**
 * The outcome of sweeping every sender that could sign a piece of mail: how many
 * of each kind were checked, and any whose address did not resolve to a durable
 * key. An empty `unresolved` is the "no unresolvable signed senders" state a
 * durable-signature verifier depends on.
 */
export type SenderKeyAuditReport = {
  runsChecked: number;
  usersChecked: number;
  unresolved: { address: string; kind: "run" | "user" }[];
};

/**
 * Sweep every mail sender that could sign and confirm each resolves to a durable
 * public key via {@link resolveSenderKey}. Read-only.
 *
 * The senders that can sign are every workflow run that holds a signing key or
 * is live ("running") and carries a sending address, plus every active user
 * principal. A never-signed run -- deployed-but-not-yet-acked, or failed or
 * cancelled before ack (address set, key null) -- is excluded. Any checked
 * sender that does not resolve is collected in `unresolved`. A keyless user
 * principal is an INTR-164 invariant break, so `resolveSenderKey` throws and the
 * sweep fails loudly rather than folding it into `unresolved` -- it is a
 * different, more serious fault than an unresolvable address.
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
        // A run can only have signed if it holds a key, so every key-null row is
        // a non-signer EXCEPT a live "running" run, which is the genuine
        // can-sign-but-keyless hole to flag. This includes all key-bearing rows
        // (any status -- a terminal run's in-flight mail is still verifiable)
        // and excludes both the pre-ack "deployed" window and a run that failed
        // or was cancelled before ack (address set, key null, never signed).
        // Match "running" literally, NOT isLiveWorkflowRunStatus, which also
        // admits "deployed" and would re-open the pre-ack false positive.
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
