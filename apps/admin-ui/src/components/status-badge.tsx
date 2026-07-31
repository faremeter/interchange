import { Badge } from "@/components/ui/badge";

type BadgeVariant = "secondary" | "destructive" | "outline";

// A page's status vocabulary mapped to badge variants. A status the map does
// not list renders as the neutral "outline" badge -- the explicit fallback each
// page's badge already applied to statuses it did not recognize.
export type StatusVariants = Record<string, BadgeVariant>;

export function StatusBadge({
  status,
  variants,
}: {
  status: string;
  variants: StatusVariants;
}) {
  const variant = variants[status] ?? "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

// Per-domain presets, kept distinct rather than merged: the pages deliberately
// map different status vocabularies, and the cross-page differences -- the
// dashboard does not flag `deactivated` the way the principal pages do, and only
// the deployment badge colors `deployed` -- are a known product inconsistency
// this shared component must not silently reconcile.
export const RUN_STATUS_VARIANTS: StatusVariants = {
  running: "secondary",
  error: "destructive",
};

export const DEPLOYMENT_STATUS_VARIANTS: StatusVariants = {
  running: "secondary",
  deployed: "secondary",
  error: "destructive",
};

export const PRINCIPAL_STATUS_VARIANTS: StatusVariants = {
  active: "secondary",
  suspended: "destructive",
  deactivated: "destructive",
};

export const DASHBOARD_STATUS_VARIANTS: StatusVariants = {
  active: "secondary",
  deployed: "secondary",
  running: "secondary",
  error: "destructive",
  suspended: "destructive",
};
