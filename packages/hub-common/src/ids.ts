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
} as const;

type IDKind = keyof typeof PREFIXES;

export function generateId(kind: IDKind): string {
  const prefix = PREFIXES[kind];
  const bytes = hexEncode(crypto.getRandomValues(new Uint8Array(16)));
  return `${prefix}${bytes}`;
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
