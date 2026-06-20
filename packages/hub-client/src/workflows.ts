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

export const WorkflowRunTrigger = type({
  deploymentId: "string",
  address: "string",
  messageId: "string",
});
export type WorkflowRunTrigger = typeof WorkflowRunTrigger.infer;

const WorkflowRunList = type({
  runIds: "string[]",
});

export const WorkflowRunEvent = type({
  seq: "number",
  type: "string",
  body: "Record<string, unknown>",
});
export type WorkflowRunEvent = typeof WorkflowRunEvent.infer;

export const WorkflowRunEvents = type({
  runId: "string",
  events: WorkflowRunEvent.array(),
});
export type WorkflowRunEvents = typeof WorkflowRunEvents.infer;

export type TriggerRunAttachment = {
  mimeType: string;
  data: string;
  name?: string;
};

export type TriggerWorkflowRunInput = {
  content: string;
  attachments?: TriggerRunAttachment[];
};

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

export async function triggerWorkflowRun(
  transport: Transport,
  tenantId: string,
  deploymentId: string,
  input: TriggerWorkflowRunInput,
): Promise<WorkflowRunTrigger> {
  const body: TriggerWorkflowRunInput = { content: input.content };
  if ("attachments" in input) {
    body.attachments = input.attachments;
  }
  const raw = await transport.fetch<unknown>(
    "POST",
    `${workflowsBasePath(tenantId)}/${deploymentId}/mail`,
    body,
  );
  const trigger = WorkflowRunTrigger(raw);
  if (trigger instanceof type.errors) {
    throw new Error(
      `Invalid workflow run trigger response: ${trigger.summary}`,
    );
  }
  return trigger;
}

export async function listWorkflowRuns(
  transport: Transport,
  tenantId: string,
  deploymentId: string,
): Promise<string[]> {
  const raw = await transport.fetch<unknown>(
    "GET",
    `${workflowsBasePath(tenantId)}/${deploymentId}/runs`,
  );
  const list = WorkflowRunList(raw);
  if (list instanceof type.errors) {
    throw new Error(`Invalid workflow run list response: ${list.summary}`);
  }
  return list.runIds;
}

export async function readWorkflowRunEvents(
  transport: Transport,
  tenantId: string,
  deploymentId: string,
  runId: string,
): Promise<WorkflowRunEvents> {
  const raw = await transport.fetch<unknown>(
    "GET",
    `${workflowsBasePath(tenantId)}/${deploymentId}/runs/${runId}/events`,
  );
  const events = WorkflowRunEvents(raw);
  if (events instanceof type.errors) {
    throw new Error(`Invalid workflow run events response: ${events.summary}`);
  }
  return events;
}
