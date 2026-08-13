// Shapes a folded run's routing record into the wire instance view -- one
// shaper, one resolver (`findRoutableById`) -- so a run renders as the instance
// it stands in for.

import type { WorkflowRunStatus } from "@intx/types";
import type { RoutableRecord } from "@intx/hub-sessions";

import { ts } from "../format";

// A workflow run's lifecycle enum differs from the run-view enum the wire
// contract speaks. A `deployed` (pre-trigger) or `running` run is live and maps
// to the wire status of the same name; its terminal states map onto the view
// vocabulary: a clean finish or an operator stop both read as `stopped`, a
// failure as `error`.
export function mapRunStatusToInstanceStatus(
  status: string,
): WorkflowRunStatus {
  switch (status) {
    case "deployed":
      return "deployed";
    case "running":
      return "running";
    case "completed":
      return "stopped";
    case "cancelled":
      return "stopped";
    case "failed":
      return "error";
    default:
      throw new Error(`unmapped workflow_run status "${status}"`);
  }
}

// The record's status in the run-view vocabulary: a run's status mapped onto
// it. Route status guards (a stopped endpoint is gone) through this so a
// terminal run 410s exactly as a stopped instance would.
export function instanceStatusOf(record: RoutableRecord): WorkflowRunStatus {
  return mapRunStatusToInstanceStatus(record.status);
}

export function formatInstanceView(
  record: RoutableRecord,
  definitionName: string,
  runtimeStatus?: string,
) {
  const status = instanceStatusOf(record);
  return {
    id: record.id,
    definitionId: record.definitionId,
    definitionName,
    tenantId: record.tenantId,
    address: record.address,
    status,
    publicKey: record.publicKey ?? null,
    kernelId: record.kernelId ?? null,
    sidecarId: record.sidecarId ?? null,
    createdAt: ts(record.createdAt),
    updatedAt: ts(record.updatedAt),
    endedAt: record.endedAt ? ts(record.endedAt) : null,
    ...(runtimeStatus !== undefined ? { runtimeStatus } : {}),
  };
}
