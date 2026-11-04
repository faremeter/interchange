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

/** Probe statuses that name a sidecar; releasing ones no longer occupy it. */
export const hostedWorkflowProbeStatuses = [
  "provisioning",
  "probing",
  "releasing",
] as const;
export type HostedWorkflowProbeStatus =
  (typeof hostedWorkflowProbeStatuses)[number];

export type RetireIdleSidecarResult =
  | { readonly kind: "retired" }
  | { readonly kind: "in_use" }
  | { readonly kind: "not_owned" };
