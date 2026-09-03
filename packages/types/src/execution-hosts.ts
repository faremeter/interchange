import { type } from "arktype";

import { SidecarCapabilityDeclaration } from "./sidecar-capabilities";

const HostDisplayName = type("string > 0").narrow(
  (value, ctx) => value.trim() !== "" || ctx.mustBe("non-blank"),
);

export const CreateExecutionHost = type({
  displayName: HostDisplayName,
});
export type CreateExecutionHost = typeof CreateExecutionHost.infer;

export const ExecutionHostResponse = type({
  id: "string",
  tenantId: "string",
  principalId: "string",
  ownerPrincipalId: "string",
  displayName: "string",
  createdAt: "string",
  updatedAt: "string",
});
export type ExecutionHostResponse = typeof ExecutionHostResponse.infer;

export const ExecutionHostEnrollmentResponse = type({
  host: ExecutionHostResponse,
  secret: "string",
});
export type ExecutionHostEnrollmentResponse =
  typeof ExecutionHostEnrollmentResponse.infer;

export const ExecutionHostRegisterFrame = type({
  type: "'host.register'",
  hostId: "string",
  token: "string",
  capabilities: SidecarCapabilityDeclaration.array(),
});
export type ExecutionHostRegisterFrame =
  typeof ExecutionHostRegisterFrame.infer;

export const ExecutionHostPingFrame = type({ type: "'ping'" });
export type ExecutionHostPingFrame = typeof ExecutionHostPingFrame.infer;

export const ExecutionHostFrame = ExecutionHostRegisterFrame.or(
  ExecutionHostPingFrame,
);
export type ExecutionHostFrame = typeof ExecutionHostFrame.infer;

export const ExecutionHostRegisteredFrame = type({
  type: "'host.registered'",
  hostId: "string",
  principalId: "string",
  sessionId: "string",
  sessionGeneration: "number.integer >= 1",
  leaseExpiresAt: "string",
});
export type ExecutionHostRegisteredFrame =
  typeof ExecutionHostRegisteredFrame.infer;

export const ExecutionHostPongFrame = type({ type: "'pong'" });
export type ExecutionHostPongFrame = typeof ExecutionHostPongFrame.infer;

export const ExecutionHostHubFrame = ExecutionHostRegisteredFrame.or(
  ExecutionHostPongFrame,
);
export type ExecutionHostHubFrame = typeof ExecutionHostHubFrame.infer;
