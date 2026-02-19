import { randomBytes } from "crypto";

const PREFIXES = {
  tenant: "tnt_",
  principal: "prn_",
  role: "rol_",
  grant: "grt_",
  agent: "agt_",
  agentVersion: "avr_",
  federationTrust: "ftr_",
  credential: "crd_",
  wallet: "wlt_",
  transaction: "txn_",
  capability: "cap_",
} as const;

type IDKind = keyof typeof PREFIXES;

export function generateId(kind: IDKind): string {
  const prefix = PREFIXES[kind];
  const bytes = randomBytes(16).toString("hex");
  return `${prefix}${bytes}`;
}
