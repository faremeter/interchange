import { Link, useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { ArrowLeft } from "lucide-react";

import { runDetailQuery } from "@/lib/queries/tenants";
import { StatusBadge, RUN_STATUS_VARIANTS } from "@/components/status-badge";

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

export function TenantRunDetailPage() {
  const { tenantId, runId } = useParams({
    from: "/authed/tenants/$tenantId/workflows/runs/$runId",
  });

  const { data: run, isLoading } = useQuery(runDetailQuery(tenantId, runId));

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!run) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  return (
    <div>
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/workflows"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Workflows
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">{run.definitionName}</h2>
          <p className="mt-1 font-mono text-xs text-muted-foreground">
            {run.address}
          </p>
        </div>
      </div>

      <div className="mt-6">
        <dl className="overflow-hidden rounded-lg border">
          <Row label="Status">
            <StatusBadge status={run.status} variants={RUN_STATUS_VARIANTS} />
          </Row>
          <Row label="Run ID">
            <span className="font-mono text-xs">{run.id}</span>
          </Row>
          {run.kernelId && (
            <Row label="Kernel ID">
              <span className="font-mono text-xs">{run.kernelId}</span>
            </Row>
          )}
          {run.sidecarId && (
            <Row label="Sidecar ID">
              <span className="font-mono text-xs">{run.sidecarId}</span>
            </Row>
          )}
          <Row label="Created">{new Date(run.createdAt).toLocaleString()}</Row>
          {run.endedAt && (
            <Row label="Ended">{new Date(run.endedAt).toLocaleString()}</Row>
          )}
        </dl>
      </div>
    </div>
  );
}
