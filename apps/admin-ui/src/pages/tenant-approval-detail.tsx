import { Link, useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { ArrowLeft } from "lucide-react";

import { ApprovalToolSnapshotView } from "@/components/approval-tool-snapshot-view";
import { approvalDetailQuery } from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";

function Row({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="grid grid-cols-[160px_1fr] border-b last:border-b-0">
      <dt className="border-r bg-muted/50 px-4 py-3 text-sm font-medium text-muted-foreground">
        {label}
      </dt>
      <dd className="px-4 py-3 text-sm">{children}</dd>
    </div>
  );
}

export function TenantApprovalDetailPage() {
  const { tenantId, approvalId } = useParams({
    from: "/authed/tenants/$tenantId/approvals/$approvalId",
  });
  const { data: approval, isLoading } = useQuery(
    approvalDetailQuery(tenantId, approvalId),
  );

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!approval) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  return (
    <div>
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/approvals"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Approvals
        </Link>
      </div>

      <div className="flex items-center gap-2">
        <h2 className="text-lg font-semibold">Approval</h2>
        <Badge variant="outline">{approval.status}</Badge>
      </div>

      <div className="mt-6">
        <h3 className="mb-2 text-sm font-semibold">Tool snapshot</h3>
        <ApprovalToolSnapshotView
          toolDefinition={approval.toolDefinition}
          toolArguments={approval.toolArguments}
        />
      </div>

      <div className="mt-6">
        <dl className="overflow-hidden rounded-lg border">
          <Row label="Scope">{approval.scope ?? "Not yet decided"}</Row>
          <Row label="Deployment">
            <span className="font-mono text-xs">{approval.deploymentId}</span>
          </Row>
          <Row label="Run">
            <span className="font-mono text-xs">{approval.runId}</span>
          </Row>
          <Row label="Agent Address">
            <span className="font-mono text-xs">{approval.agentAddress}</span>
          </Row>
          <Row label="Correlation">
            <span className="font-mono text-xs">{approval.correlationId}</span>
          </Row>
          <Row label="Timeout">
            {approval.timeoutAt
              ? new Date(approval.timeoutAt).toLocaleString()
              : "No deadline"}
          </Row>
          <Row label="Created">
            {new Date(approval.createdAt).toLocaleString()}
          </Row>
          <Row label="Resolved">
            {approval.resolvedAt
              ? new Date(approval.resolvedAt).toLocaleString()
              : "Not yet resolved"}
          </Row>
        </dl>
      </div>
    </div>
  );
}
