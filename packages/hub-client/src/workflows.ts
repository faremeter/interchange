import { type } from "arktype";

import { InferenceSource } from "@intx/types/runtime";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";

import type { Transport } from "./transport";
import { WorkflowRunEvents } from "./validators";

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
  runId: "string",
  address: "string",
  messageId: "string",
});
export type WorkflowRunTrigger = typeof WorkflowRunTrigger.infer;

const WorkflowRunList = type({
  runIds: "string[]",
});

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
  /** Where the definition's bytes come from at apply time. */
  source: WorkflowDefinitionSource;
  /** The `interchange.workflow` entry-module path the sidecar evaluates. */
  entry: string;
  /** The inference chain the deployment's step agents launch against. */
  sources: InferenceSource[];
  /** The default source id; the head of the pinned inference chain. */
  defaultSource: string;
  /**
   * A `name@range` pin selecting the definition package. Required for the
   * `registry` and asset-`tarball` source variants; omitted for asset-`source`,
   * whose member is selected by the source's `packageName`.
   */
  pin?: string;
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
    `${workflowsBasePath(tenantId)}/deployments`,
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
  const body: {
    source: WorkflowDefinitionSource;
    entry: string;
    sources: InferenceSource[];
    defaultSource: string;
    pin?: string;
  } = {
    source: input.source,
    entry: input.entry,
    sources: input.sources,
    defaultSource: input.defaultSource,
  };
  if (input.pin !== undefined) {
    body.pin = input.pin;
  }
  const raw = await transport.fetch<unknown>(
    "POST",
    `${workflowsBasePath(tenantId)}/deployments`,
    body,
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
  runId: string,
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
    `${workflowsBasePath(tenantId)}/${runId}/signals`,
    body,
  );
}

export async function triggerWorkflowRun(
  transport: Transport,
  tenantId: string,
  runId: string,
  input: TriggerWorkflowRunInput,
): Promise<WorkflowRunTrigger> {
  const body: TriggerWorkflowRunInput = { content: input.content };
  if ("attachments" in input) {
    body.attachments = input.attachments;
  }
  const raw = await transport.fetch<unknown>(
    "POST",
    `${workflowsBasePath(tenantId)}/${runId}/mail`,
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
  runId: string,
): Promise<string[]> {
  const raw = await transport.fetch<unknown>(
    "GET",
    `${workflowsBasePath(tenantId)}/${runId}/runs`,
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
  runId: string,
  eventRunId: string,
): Promise<WorkflowRunEvents> {
  const raw = await transport.fetch<unknown>(
    "GET",
    `${workflowsBasePath(tenantId)}/${runId}/runs/${eventRunId}/events`,
  );
  const events = WorkflowRunEvents(raw);
  if (events instanceof type.errors) {
    throw new Error(`Invalid workflow run events response: ${events.summary}`);
  }
  return events;
}
