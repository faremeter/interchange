import { hexEncode } from "@intx/types";

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
 * Deterministic principal id for a workflow run's principal, keyed on
 * `(tenantId, runId)` so every birth attempt lands on the same row. The
 * `principal` table's `unique(tenantId, kind, refId)` makes the insert an
 * `onConflictDoNothing` no-op; a random id would orphan the grant rows
 * that reference it.
 *
 * `prn_` + the first 16 bytes of `SHA-256(tenantId "\0" runId)` as hex;
 * the NUL separator keeps the pair unambiguous.
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
 * Git bearer-token secret prefixes: user tokens (`PAT_PREFIX`) vs
 * tenant-minted service tokens (`SVC_PREFIX`). Mint endpoint and bearer
 * middleware both import these, so the shapes cannot drift.
 */
export const PAT_PREFIX = "itx_pat_";
export const SVC_PREFIX = "itx_svc_";
