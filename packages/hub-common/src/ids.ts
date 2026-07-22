import { hexEncode } from "@intx/types";

const PREFIXES = {
  tenant: "tnt_",
  principal: "prn_",
  role: "rol_",
  grant: "grt_",
  agent: "agt_",
  agentVersion: "avr_",
  federationTrust: "ftr_",
  provider: "prv_",
  oauthClient: "ocl_",
  credential: "crd_",
  wallet: "wlt_",
  transaction: "txn_",
  offering: "ofr_",
  model: "mdl_",
  modelProvider: "mpv_",
  modelOffering: "mof_",
  modelPricing: "mpr_",
  instance: "ins_",
  session: "ses_",
  sessionMail: "sml_",
  inferenceTurn: "itn_",
  turnPart: "tp_",
  asset: "ast_",
  agentAsset: "aas_",
  gitToken: "gtk_",
  deployment: "dep_",
  approval: "apr_",
  signal: "sig_",
} as const;

type IDKind = keyof typeof PREFIXES;

export function generateId(kind: IDKind): string {
  const prefix = PREFIXES[kind];
  const bytes = hexEncode(crypto.getRandomValues(new Uint8Array(16)));
  return `${prefix}${bytes}`;
}

/**
 * Derive the DETERMINISTIC principal id a workflow run's principal is
 * minted under, keyed on `(tenantId, runId)`. Both the external trigger
 * route and the mail-triggered run path derive the id this way, so a run
 * has ONE principal id regardless of how many times its birth is
 * attempted.
 *
 * Determinism is load-bearing for redelivery idempotency: a mail run's
 * `runId` is the mail's Message-ID (stable across a redelivery), and the
 * `principal` table's `unique(tenantId, kind, refId=runId)` makes the
 * insert an `onConflictDoNothing` no-op on the second attempt. A RANDOM
 * id would leave that no-op pointing the fresh id nowhere while the grant
 * rows referenced it, breaking the principal foreign key. Deriving the id
 * from the run means the second attempt reuses the id already written, so
 * the grant rows resolve against the principal that is actually present.
 *
 * The id is `prn_` + the first 16 bytes of `SHA-256(tenantId "\0" runId)`
 * as hex, matching the shape `generateId("principal")` produces. The NUL
 * separator keeps the two fields unambiguous so no `(tenantId, runId)`
 * pair collides with another by concatenation.
 */
export async function deriveRunPrincipalId(
  tenantId: string,
  runId: string,
): Promise<string> {
  const digest = new Uint8Array(
    await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(`${tenantId}\0${runId}`),
    ),
  );
  return `${PREFIXES.principal}${hexEncode(digest.subarray(0, 16))}`;
}

/**
 * Prefixes for the two flavours of git bearer-token secret. Personal
 * access tokens (`PAT_PREFIX`) belong to a user; service tokens
 * (`SVC_PREFIX`) are minted under a tenant on behalf of a principal.
 * The single source of truth: both the mint endpoint and the bearer
 * middleware import these so the secret shape cannot drift between
 * the issuer and the validator.
 */
export const PAT_PREFIX = "itx_pat_";
export const SVC_PREFIX = "itx_svc_";
