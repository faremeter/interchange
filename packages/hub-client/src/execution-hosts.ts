import { type } from "arktype";

import {
  ExecutionHostEnrollmentResponse,
  type CreateExecutionHost,
} from "@intx/types";

import type { Transport } from "./transport";

export async function enrollExecutionHost(
  transport: Transport,
  tenantId: string,
  input: CreateExecutionHost,
) {
  const raw = await transport.fetch<unknown>(
    "POST",
    `/api/tenants/${tenantId}/hosts`,
    input,
  );
  const enrollment = ExecutionHostEnrollmentResponse(raw);
  if (enrollment instanceof type.errors) {
    throw new Error(
      `Invalid execution host enrollment response: ${enrollment.summary}`,
    );
  }
  return enrollment;
}
