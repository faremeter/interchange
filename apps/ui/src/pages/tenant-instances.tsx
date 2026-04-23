import { useNavigate, useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "@/components/tenant-nav";
import {
  tenantInstancesQuery,
  type AgentInstanceResponse,
} from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

function StatusBadge({ status }: { status: AgentInstanceResponse["status"] }) {
  const variant =
    status === "running"
      ? "secondary"
      : status === "error"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

function InstanceRow({
  instance,
  tenantId,
}: {
  instance: AgentInstanceResponse;
  tenantId: string;
}) {
  const navigate = useNavigate();

  return (
    <TableRow
      className="cursor-pointer"
      onClick={() =>
        navigate({
          to: "/tenants/$tenantId/instances/$instanceId",
          params: { tenantId, instanceId: instance.id },
        })
      }
    >
      <TableCell>
        <span
          className="font-medium text-primary hover:underline"
          onClick={(e) => {
            e.stopPropagation();
            navigate({
              to: "/tenants/$tenantId/agents/$agentId",
              params: { tenantId, agentId: instance.agentId },
            });
          }}
        >
          {instance.agentName}
        </span>
      </TableCell>
      <TableCell>
        <StatusBadge status={instance.status} />
      </TableCell>
      <TableCell className="font-mono text-xs text-muted-foreground">
        {instance.address}
      </TableCell>
      <TableCell className="text-muted-foreground">
        {new Date(instance.createdAt).toLocaleString()}
      </TableCell>
    </TableRow>
  );
}

export function TenantInstancesPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: instances, isLoading } = useQuery(
    tenantInstancesQuery(tenantId),
  );

  return (
    <div>
      <TenantNav />

      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Instances</h2>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : !instances || instances.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">
          No instances found.
        </p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Agent</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Address</TableHead>
                <TableHead>Created</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {instances?.map((inst) => (
                <InstanceRow
                  key={inst.id}
                  instance={inst}
                  tenantId={tenantId}
                />
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
