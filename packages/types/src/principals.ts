import { type } from "arktype";

export const principalKinds = ["user", "agent"] as const;
export type PrincipalKind = (typeof principalKinds)[number];

export const principalStatuses = [
  "active",
  "suspended",
  "invited",
  "deactivated",
] as const;
export type PrincipalStatus = (typeof principalStatuses)[number];

export const updatablePrincipalStatuses = [
  "active",
  "suspended",
  "deactivated",
] as const;
export type UpdatablePrincipalStatus =
  (typeof updatablePrincipalStatuses)[number];

const Kind = type.enumerated(...principalKinds);
const Status = type.enumerated(...principalStatuses);
const UpdatableStatus = type.enumerated(...updatablePrincipalStatuses);

export const PrincipalResponse = type({
  id: "string",
  tenantId: "string",
  kind: Kind,
  refId: "string",
  displayName: "string",
  "email?": "string",
  status: Status,
  roles: type({
    id: "string",
    name: "string",
  }).array(),
  createdAt: "string",
  updatedAt: "string",
});

export const UpdatePrincipal = type({
  status: UpdatableStatus,
});

export const InviteMember = type({
  email: "string",
  "roleId?": "string",
});
