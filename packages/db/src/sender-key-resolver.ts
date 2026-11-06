import { and, eq, isNotNull, or } from "drizzle-orm";

import type { MailAcceptCoordinate } from "@intx/authz";
import { getLogger } from "@intx/log";

import type { DBExecutor } from "./client";
import { resolveLocalMailParty } from "./local-mail-party";
import type { PrincipalKeyStore } from "./principal-key-store";
import {
  loadRunPrincipalActiveKey,
  runPrincipalHasActiveKey,
} from "./run-principal-key";
import { principal } from "./schema/principals";
import { tenant } from "./schema/tenants";
import { workflowRun } from "./schema/workflow-run";

const logger = getLogger(["db", "sender-key-resolver"]);

/**
 * The durable public key that authenticates a signed mail sender, resolved from
 * the hub's own storage, together with the sender's admission coordinates.
 * `source` records which store the key came from -- a run sender's key is the
 * sidecar-minted key recorded on the deployment anchor, a user sender's key is
 * the hub-custodied principal key -- so a consumer can bucket a resolution
 * without re-parsing the address.
 *
 * Every resolution carries the durable ids the admission gate keys on: the
 * sender's own `principalId` and its `tenantId`. A run sender additionally
 * carries the `assetId` of the workflow asset that backs its definition,
 * because a `mail.accept` grant can target every deployment of an asset. The
 * `assetId` is null when the definition has no asset. `senderCoordinates`
 * renders these ids as the `MailAcceptCoordinate` set the gate compares against
 * with no translation.
 *
 * A run's `principalId` is nullable: a deployment's run principal holds its
 * signing key (recorded when its initialization completes) before the first
 * trigger links that principal to the anchor, so a run that CAN sign mail may
 * not yet carry a principal link.
 * The key still resolves for signature checks; `senderCoordinates` returns `null` for
 * such a sender, which the admission gate treats as the `unknown` outcome. A
 * user sender always resolves to a concrete principal, so its `principalId` is
 * never null.
 */
export type SenderKeyResolution =
  | {
      source: "run";
      publicKey: string;
      principalId: string | null;
      assetId: string | null;
      tenantId: string;
    }
  | {
      source: "user";
      publicKey: string;
      principalId: string;
      tenantId: string;
    };

/**
 * Render a resolved sender's admission coordinates as the `MailAcceptCoordinate`
 * set the admission gate matches `mail.accept` grants against. A user sender
 * yields `principal` + `tenant`; a run sender additionally yields `asset`, since
 * a run belongs to a definition whose workflow asset a grant can target. A run
 * whose definition has no asset yields no `asset` coordinate, so an asset grant
 * never matches it. The ids are returned in the `@intx/authz` coordinate shape
 * so the gate compares without translating.
 *
 * Returns `null` when the sender has no principal to key on -- a run that holds
 * a signing key but has not yet minted its principal (the acked-but-not-yet-
 * reconciled anchor window). This is the fail-closed signal: the gate treats a
 * `null` coordinate set as the `unknown` sender outcome rather than admitting on
 * a partial (principal-less) set.
 */
export function senderCoordinates(
  resolution: SenderKeyResolution,
): MailAcceptCoordinate[] | null {
  if (resolution.principalId === null) return null;
  const coordinates: MailAcceptCoordinate[] = [
    { coordType: "principal", id: resolution.principalId },
  ];
  // Do not fall back to the definition id when the asset id is null: an
  // asset-less run has no asset coordinate, so it fails closed on `asset`
  // grants and still resolves its principal and tenant coordinates.
  if (resolution.source === "run" && resolution.assetId !== null) {
    coordinates.push({ coordType: "asset", id: resolution.assetId });
  }
  coordinates.push({ coordType: "tenant", id: resolution.tenantId });
  return coordinates;
}

/**
 * Resolve the durable public key a signed mail sender must verify against, plus
 * the sender's admission coordinates, unioning the two existing durable-key
 * sources: a run address (`run_<id>@<domain>`) resolves to the active key of
 * the run's principal, which the sidecar running the deployment minted, and
 * its `(principal, asset, tenant)` ids; any
 * other address (`<refId>@<domain>`) is treated as a user sender and resolves
 * to that principal's hub-custodied key and its `(principal, tenant)` ids. The
 * sender is identified only from the passed address; the MIME `From` is never
 * consulted.
 *
 * Read-only. Returns `null` when the sender has no durable key to resolve -- a
 * malformed address, an unknown run, a run whose deploy has not been acked yet,
 * or an address that matches no user principal. A `null` is a legitimate
 * "unresolvable sender" answer the caller acts on; it is NOT an error. An
 * admission caller treats a `null` as the `unknown` sender outcome, which its
 * policy rejects by default.
 *
 * A resolved run may carry a null `principalId` -- an anchor that acked its
 * deploy (so it holds a signing key and CAN sign) but has not yet minted and
 * reconciled its principal. The key still resolves, so signature checks and the
 * send path are unaffected; the fail-closed for admission lives in
 * `senderCoordinates`, which returns `null` for such a sender. This keeps the
 * send path identical while a principal-less signer never yields a partial
 * coordinate set. A keyless USER principal is different: it is an invariant
 * break, so `getPublicKey` throws rather than defaulting (see below).
 *
 * The address names its sender through `resolveLocalMailParty`. Addresses are
 * matched case-insensitively and without a trailing root dot. Inbound `From`
 * addresses are lowercased when parsed (see `@intx/mime` `extractAddrSpec`), so
 * the address is normalized and stored values are compared in lower case. Tenant
 * domains are `lower(domain)`-unique (`tenant_domain_lower_idx`), so a
 * normalized domain matches at most one tenant. A user `refId` is a
 * case-sensitively-unique betterAuth id that can carry uppercase; lowercasing it
 * to reconcile with the parser is lossy, so the resolver prefers the exact
 * (already-lowercase) row and falls back to a case-insensitive match to find a
 * mixed-case-stored refId. When two principals in one tenant hold case-variant
 * refIds the case-insensitive match is not unique and no exact row disambiguates
 * it: the resolver throws rather than silently return one, which would attribute
 * the sender to the wrong principal.
 */
export async function resolveSenderKey(
  db: DBExecutor,
  principalKeyStore: PrincipalKeyStore,
  address: string,
): Promise<SenderKeyResolution | null> {
  const party = await resolveLocalMailParty(db, address);
  if (party === null) return null;

  if (party.kind === "run") {
    // A run sender resolves to the sidecar-minted key the hub recorded as the
    // active key of the run's principal when the deployment's initialization
    // completed. A run whose principal holds no active key has not finished
    // initializing, or its sidecar was replaced and the replacement has not
    // recorded a key yet -- an expected state -- so it resolves to nothing
    // rather than erroring, unlike the keyless-principal invariant break
    // below. A retired key no longer resolves, so the hub vouches for no key
    // for mail its replaced sidecar signed. The principal's status is not
    // read: a terminal run's in-flight mail still verifies.
    const publicKey = await loadRunPrincipalActiveKey(
      db,
      party.tenantId,
      party.anchorRunId,
    );
    if (publicKey === null) return null;
    // `principalId` may be null: an acked anchor holds a signing key before its
    // first trigger links its run principal, so it CAN sign yet has no linked
    // principal. The key still resolves (the send path is unaffected);
    // `senderCoordinates` fails closed on the null principal for admission.
    // `assetId` is null when the definition has no asset.
    return {
      source: "run",
      publicKey,
      principalId: party.linkedPrincipalId,
      assetId: party.assetId,
      tenantId: party.tenantId,
    };
  }

  // A user sender resolves to its hub-custodied principal key, keyed by the
  // `(tenant domain, user refId)` its address carries. The principal exists; a
  // user principal with no active key violates the invariant that every user
  // principal is minted with one, so `getPublicKey` throws rather than
  // defaulting. Do not soften that to null -- unlike the pre-ack run key
  // above, a keyless principal is a real breakage that must surface.
  const publicKey = await principalKeyStore.getPublicKey(party.principalId, db);
  return {
    source: "user",
    publicKey,
    principalId: party.principalId,
    tenantId: party.tenantId,
  };
}

/**
 * The strict, coordinate-carrying resolution a mail-triggered run's admission
 * needs: the sender's durable `principalId`, its `tenantId`, and its admission
 * `coordinates`.
 */
export type ResolvedSenderPrincipal = {
  principalId: string;
  tenantId: string;
  coordinates: MailAcceptCoordinate[];
};

/**
 * Resolve a signed mail sender to the durable identity a mail-triggered run
 * admits it by. Consumes the strict {@link resolveSenderKey} and keeps
 * the widened resolution rather than unwrapping to the key, so the sender's
 * principal, tenant, and admission coordinates carry through.
 *
 * Returns `null` when there is no durable identity to bind: an unresolvable
 * address, or a run with no principal link (an initialized anchor before its
 * first trigger links its run principal, for which `senderCoordinates`
 * returns `null`). A `null`
 * leaves the sender unadmitted rather than admitting a partial identity. The
 * strict resolver's throw on an ambiguous address or a keyless-principal
 * invariant break is preserved for the caller to catch, so a fault never
 * silently binds an identity.
 */
export async function resolveSenderPrincipal(
  db: DBExecutor,
  principalKeyStore: PrincipalKeyStore,
  address: string,
): Promise<ResolvedSenderPrincipal | null> {
  const resolution = await resolveSenderKey(db, principalKeyStore, address);
  if (resolution === null) return null;
  const coordinates = senderCoordinates(resolution);
  if (coordinates === null || resolution.principalId === null) return null;
  return {
    principalId: resolution.principalId,
    tenantId: resolution.tenantId,
    coordinates,
  };
}

/**
 * Resolve the hex-encoded public key to stamp on an outbound mail frame, as a
 * BEST-EFFORT value that never blocks delivery. Returns the key, or `null` when
 * the sender has no resolvable key -- OR when resolution FAILS.
 *
 * {@link resolveSenderKey} throws on a genuine fault (an ambiguous user address,
 * or a principal with no active key -- an INTR-164 invariant break). Those
 * throws must fail loud for {@link auditSenderKeys}, but they must not break
 * the send path: the frame key is nullable, so a recipient with no co-delivered
 * key resolves the sender as `unknown`, which its admission policy rejects by
 * default (a workflow may relax `unknown` to admit).
 * Coupling delivery to key resolution would let a data-integrity fault strand a
 * run or drop mail. So a throw here degrades to `null` and is logged at ERROR
 * with its cause -- a degraded fault, kept distinct from the ordinary
 * unresolvable-sender `null`, which stays silent.
 *
 * The principal key store is real in production (INTR-164 mints every principal
 * a key), so the throw path is a defensive safety net, not the common case: the
 * normal outcome is a resolved, populated key.
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
        or(
          runPrincipalHasActiveKey(workflowRun.tenantId, workflowRun.id),
          eq(workflowRun.status, "running"),
        ),
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
