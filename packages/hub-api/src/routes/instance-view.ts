// Shapes a fold-normalized routing record into the wire instance view. Both a
// legacy agent instance and a folded workflow run flow through here, so a run
// renders identically to the instance it stands in for -- one shaper, one
// resolver (`findRoutableById`), no per-route branching on kind.

import { agentInstanceStatuses, type AgentInstanceStatus } from "@intx/types";
import type { RoutableRecord } from "@intx/hub-sessions";

import { ts } from "../format";

// A workflow run's lifecycle enum differs from the instance enum the wire
// contract speaks. A run is `running` while live; its terminal states map onto
// the instance vocabulary: a clean finish or an operator stop both read as
// `stopped`, a failure as `error`.
export function mapRunStatusToInstanceStatus(
  status: string,
): AgentInstanceStatus {
  switch (status) {
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

// A legacy instance's DB status enum is exactly the wire status set, so it
// passes through -- validated rather than asserted so an unexpected value
// fails loudly instead of leaking a bad status onto the wire.
function passthroughInstanceStatus(status: string): AgentInstanceStatus {
  const match = agentInstanceStatuses.find((s) => s === status);
  if (match === undefined) {
    throw new Error(`unknown agent_instance status "${status}"`);
  }
  return match;
}

// The record's status in the instance vocabulary: a run's mapped onto it, a
// legacy instance's passed through. Route status guards (a stopped endpoint is
// gone) through this so a terminal run 410s exactly as a stopped instance does.
export function instanceStatusOf(record: RoutableRecord): AgentInstanceStatus {
  return record.kind === "run"
    ? mapRunStatusToInstanceStatus(record.status)
    : passthroughInstanceStatus(record.status);
}

export function formatInstanceView(
  record: RoutableRecord,
  agentName: string,
  runtimeStatus?: string,
) {
  const status = instanceStatusOf(record);
  return {
    id: record.id,
    definitionId: record.definitionId,
    agentName,
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
