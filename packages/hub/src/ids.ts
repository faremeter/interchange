import { randomBytes } from "crypto";

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
  instance: "ins_",
  session: "ses_",
  message: "msg_",
  messagePart: "mpt_",
  sessionMail: "sml_",
} as const;

type IDKind = keyof typeof PREFIXES;

export function generateId(kind: IDKind): string {
  const prefix = PREFIXES[kind];
  const bytes = randomBytes(16).toString("hex");
  return `${prefix}${bytes}`;
}
