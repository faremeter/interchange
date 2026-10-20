import { base64urlEncode } from "@intx/types/base64url";
import { hexEncode } from "@intx/types/hex";

const PREFIXES = {
  tenant: "tnt_",
  principal: "prn_",
  principalKey: "pky_",
  role: "rol_",
  grant: "grt_",
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
  session: "ses_",
  sessionMail: "sml_",
  inferenceTurn: "itn_",
  turnPart: "tp_",
  asset: "ast_",
  gitToken: "gtk_",
  runToken: "rtk_",
  workflowRun: "run_",
  approval: "apr_",
  signal: "sig_",
  workflowDefinition: "wfd_",
  workflowDefinitionVersion: "wdv_",
  workflowPendingProjection: "wpp_",
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
 * Determinism is load-bearing for idempotency: a run's `runId` is the
 * deployment's mail address (stable across a redelivery AND across every
 * trigger of the deployment), and the `principal` table's
 * `unique(tenantId, kind, refId=runId)` makes the insert an
 * `onConflictDoNothing` no-op on every attempt after the first. A RANDOM
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

/**
 * Prefix of a workflow run token: the bearer a workflow run's consumer (a tool
 * package or an action handler) presents to the hub REST API as its run
 * principal. The prefix lets a middleware route the secret without a lookup.
 */
export const RUN_TOKEN_PREFIX = "itx_run_";

const BEARER_SECRET_BYTES = 32;

/**
 * Mint the plaintext of a hub bearer token: the given prefix followed by 32
 * random bytes, base64url-encoded. The prefix names the token's kind so a
 * middleware can route a secret without a lookup; the hub stores only what it
 * needs to verify or deliver the secret, never this string in the clear.
 */
export function generateBearerSecret(prefix: string): string {
  const bytes = crypto.getRandomValues(new Uint8Array(BEARER_SECRET_BYTES));
  return `${prefix}${base64urlEncode(bytes)}`;
}
