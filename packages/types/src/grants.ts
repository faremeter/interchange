import { type } from "arktype";

export const CreateGrant = type({
  "roleId?": "string | null",
  "principalId?": "string | null",
  resource: "string",
  action: "string",
  effect: "'allow' | 'deny' | 'ask'",
  "conditions?": "Record<string, unknown> | null",
  source: "'system' | 'role' | 'creator' | 'invoker'",
  "expiresAt?": "string | null",
});

export const UpdateGrant = type({
  "effect?": "'allow' | 'deny' | 'ask'",
  "conditions?": "Record<string, unknown> | null",
  "expiresAt?": "string | null",
});

export const GrantResponse = type({
  id: "string",
  tenantId: "string",
  "roleId?": "string | null",
  "principalId?": "string | null",
  resource: "string",
  action: "string",
  effect: "'allow' | 'deny' | 'ask'",
  "conditions?": "Record<string, unknown> | null",
  source: "'system' | 'role' | 'creator' | 'invoker'",
  "expiresAt?": "string | null",
  createdAt: "string",
  updatedAt: "string",
});

export const EvaluateRequest = type({
  resource: "string",
  action: "string",
});

export const EvaluateResult = type({
  effect: "'allow' | 'deny' | 'ask'",
  matchingGrants: type({
    id: "string",
    resource: "string",
    action: "string",
    effect: "'allow' | 'deny' | 'ask'",
    source: "'system' | 'role' | 'creator' | 'invoker'",
  }).array(),
});
