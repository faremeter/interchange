import { type } from "arktype";

export const PrincipalResponse = type({
  id: "string",
  tenantId: "string",
  kind: "'user' | 'agent'",
  refId: "string",
  status: "'active' | 'suspended' | 'invited' | 'deactivated'",
  roles: type({
    id: "string",
    name: "string",
  }).array(),
  createdAt: "string",
  updatedAt: "string",
});

export const UpdatePrincipal = type({
  status: "'active' | 'suspended' | 'deactivated'",
});

export const InviteMember = type({
  email: "string",
  "roleId?": "string",
});
