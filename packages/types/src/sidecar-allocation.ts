import type { WorkflowLifecycleResponse } from "./workflow-lifecycle";

export const sidecarAllocationStatuses = [
  "pending",
  "provisioning",
  "allocated",
  "replacing",
  "releasing",
  "destroy_failed",
  "released",
  "failed",
] as const;

export type SidecarAllocationStatus =
  (typeof sidecarAllocationStatuses)[number];

export type DispatchableSidecarAllocationStatus = Extract<
  SidecarAllocationStatus,
  "pending" | "provisioning" | "allocated" | "replacing"
>;

export function isSidecarAllocationDispatchable(
  status: SidecarAllocationStatus,
): status is DispatchableSidecarAllocationStatus {
  switch (status) {
    case "pending":
    case "provisioning":
    case "allocated":
    case "replacing":
      return true;
    case "releasing":
    case "destroy_failed":
    case "released":
    case "failed":
      return false;
    default: {
      const exhaustive: never = status;
      return exhaustive;
    }
  }
}

/** Allocation statuses with work or owed cleanup on the sidecar the row names. */
export const hostedSidecarAllocationStatuses = [
  "provisioning",
  "allocated",
  "replacing",
  "releasing",
  "destroy_failed",
] as const satisfies readonly SidecarAllocationStatus[];
export type HostedSidecarAllocationStatus =
  (typeof hostedSidecarAllocationStatuses)[number];

/** Probe statuses with work on the sidecar the row names. */
export const occupyingWorkflowProbeStatuses = [
  "provisioning",
  "probing",
] as const;

/** Probe statuses that name a sidecar; releasing ones no longer occupy it. */
export const hostedWorkflowProbeStatuses = [
  ...occupyingWorkflowProbeStatuses,
  "releasing",
] as const;
export type HostedWorkflowProbeStatus =
  (typeof hostedWorkflowProbeStatuses)[number];

export type SidecarDeploymentView = {
  readonly allocationId: string;
  readonly generation: number;
  readonly tenantId: string;
  readonly anchorRunId: string;
  readonly address?: string;
  readonly definitionId: string;
  readonly status: HostedSidecarAllocationStatus;
  /** The anchor run's status, not that of each run in the deployment. */
  readonly runStatus: (typeof WorkflowLifecycleResponse.infer)["status"];
  readonly cleanupConfirmed: boolean;
  readonly externalRef?: string;
};

export type SidecarProbeView = {
  /** The probe's id, which a deployment adopting the probe keeps. */
  readonly allocationId: string;
  readonly generation: number;
  readonly tenantId: string;
  readonly status: HostedWorkflowProbeStatus;
  readonly externalRef?: string;
};

/** One sidecar as the Hub's database records it. */
export type SidecarView = {
  readonly sidecarId: string;
  readonly retired: boolean;
  readonly adapterManifestDigest: string;
  /**
   * From this Hub's connection to the sidecar. Absent means unknown, not
   * disconnected: another Hub may hold the connection.
   */
  readonly capacity?: {
    readonly maxActiveDeployments: number;
    readonly activeDeployments: number;
  };
  readonly deployments: readonly SidecarDeploymentView[];
  readonly probes: readonly SidecarProbeView[];
};

export type RetireIdleSidecarResult =
  | { readonly kind: "retired" }
  | { readonly kind: "in_use" }
  | { readonly kind: "not_owned" };
