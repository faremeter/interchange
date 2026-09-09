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

export function isSidecarAllocationDispatchable(
  status: SidecarAllocationStatus,
): boolean {
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
