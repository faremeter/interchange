import { type } from "arktype";

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
