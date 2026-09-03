import { type } from "arktype";

export const principalKinds = ["user", "agent", "workflow", "host"] as const;
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
  kind: Kind.describe(
    "Whether this principal represents a `user` (a human account), an `agent`, a `workflow` run or definition, or an execution `host`.",
  ),
  refId: type("string").describe(
    "Identifier of the underlying entity this principal stands for: the auth user id when `kind` is `user`, an agent-instance id when `kind` is `agent`, a workflow run (`run_...`) or workflow definition (`wfd_...`) id when `kind` is `workflow`, or a stable host id when `kind` is `host`. Unique per tenant and kind.",
  ),
  displayName: "string",
  "email?": "string",
  status: Status.describe(
    "Principal state: `active`, `suspended`, `invited` (user membership pending acceptance), or `deactivated`.",
  ),
  roles: type({
    id: "string",
    name: "string",
  }).array(),
  createdAt: "string",
  updatedAt: "string",
});

export const UpdatePrincipal = type({
  status: UpdatableStatus.describe(
    "New account state for the principal. Only `active`, `suspended`, and `deactivated` are settable; `invited` is reached only through the invitation flow.",
  ),
});

export const InviteMember = type({
  email: "string",
  "roleId?": "string",
});
