import { type } from "arktype";

export const grantEffects = ["allow", "deny", "ask"] as const;
export type GrantEffect = (typeof grantEffects)[number];

export const grantOrigins = ["system", "role", "creator", "invoker"] as const;
export type GrantOrigin = (typeof grantOrigins)[number];

const Effect = type.enumerated(...grantEffects);
const Origin = type.enumerated(...grantOrigins);

export const CreateGrant = type({
  "roleId?": "string | null",
  "principalId?": "string | null",
  resource: "string",
  action: "string",
  effect: Effect,
  "conditions?": "Record<string, unknown> | null",
  origin: Origin,
  "expiresAt?": "string | null",
});

export const UpdateGrant = type({
  "effect?": Effect,
  "conditions?": "Record<string, unknown> | null",
  "expiresAt?": "string | null",
});

export const GrantResponse = type({
  id: "string",
  tenantId: "string",
  "roleId?": "string | null",
  "roleName?": "string | null",
  "principalId?": "string | null",
  "principalName?": "string | null",
  resource: "string",
  action: "string",
  effect: Effect,
  "conditions?": "Record<string, unknown> | null",
  origin: Origin,
  "expiresAt?": "string | null",
  createdAt: "string",
  updatedAt: "string",
});

export const EvaluateRequest = type({
  resource: "string",
  action: "string",
});

export const MatchedGrant = type({
  id: "string",
  resource: "string",
  action: "string",
  effect: Effect,
  origin: Origin,
  "specificity?": "number",
});
export type MatchedGrant = typeof MatchedGrant.infer;

export const EvaluateResult = type({
  effect: Effect,
  matchingGrants: MatchedGrant.array(),
});
