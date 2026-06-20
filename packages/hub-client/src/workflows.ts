import { type } from "arktype";

import { InferenceSource } from "@intx/types/runtime";

import type { Transport } from "./transport";

export const WorkflowDeployment = type({
  id: "string",
  tenantId: "string",
  definitionAssetId: "string",
  status: "string",
  createdAt: "string",
});
export type WorkflowDeployment = typeof WorkflowDeployment.infer;

const WorkflowDeploymentList = WorkflowDeployment.array();

export type DeployWorkflowInput = {
  assetId: string;
  sources: InferenceSource[];
  defaultSource: string;
};

export type DeliverSignalInput = {
  runId: string;
  signalName: string;
  signalId: string;
  payload?: unknown;
};

function workflowsBasePath(tenantId: string): string {
  return `/api/tenants/${tenantId}/workflows`;
}

export async function listWorkflowDeployments(
  transport: Transport,
  tenantId: string,
): Promise<WorkflowDeployment[]> {
  const raw = await transport.fetch<unknown>(
    "GET",
    `${workflowsBasePath(tenantId)}/instances`,
  );
  const deployments = WorkflowDeploymentList(raw);
  if (deployments instanceof type.errors) {
    throw new Error(
      `Invalid workflow deployment list response: ${deployments.summary}`,
    );
  }
  return deployments;
}

export async function deployWorkflow(
  transport: Transport,
  tenantId: string,
  input: DeployWorkflowInput,
): Promise<WorkflowDeployment> {
  const raw = await transport.fetch<unknown>(
    "POST",
    `${workflowsBasePath(tenantId)}/instances`,
    {
      assetId: input.assetId,
      sources: input.sources,
      defaultSource: input.defaultSource,
    },
  );
  const deployment = WorkflowDeployment(raw);
  if (deployment instanceof type.errors) {
    throw new Error(`Invalid workflow deploy response: ${deployment.summary}`);
  }
  return deployment;
}

export async function deliverWorkflowSignal(
  transport: Transport,
  tenantId: string,
  deploymentId: string,
  input: DeliverSignalInput,
): Promise<void> {
  const body: {
    runId: string;
    signalName: string;
    signalId: string;
    payload?: unknown;
  } = {
    runId: input.runId,
    signalName: input.signalName,
    signalId: input.signalId,
  };
  if ("payload" in input) {
    body.payload = input.payload;
  }
  await transport.fetch(
    "POST",
    `${workflowsBasePath(tenantId)}/${deploymentId}/signals`,
    body,
  );
}
