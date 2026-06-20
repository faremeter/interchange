import { useNavigate, useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "@/components/tenant-nav";
import {
  tenantWorkflowsQuery,
  type WorkflowDefinitionResponse,
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

function WorkflowRow({
  workflow,
  tenantId,
}: {
  workflow: WorkflowDefinitionResponse;
  tenantId: string;
}) {
  const navigate = useNavigate();

  return (
    <TableRow
      className="cursor-pointer"
      onClick={() =>
        navigate({
          to: "/tenants/$tenantId/workflows/$workflowId",
          params: { tenantId, workflowId: workflow.id },
        })
      }
    >
      <TableCell>
        <div className="font-medium">
          {workflow.displayName ?? workflow.name}
        </div>
        <div className="text-xs text-muted-foreground">{workflow.name}</div>
      </TableCell>
      <TableCell>
        {workflow.origin.direct ? (
          <Badge variant="secondary">local</Badge>
        ) : (
          <Badge variant="outline">inherited</Badge>
        )}
      </TableCell>
      <TableCell className="text-muted-foreground">
        {new Date(workflow.createdAt).toLocaleDateString()}
      </TableCell>
    </TableRow>
  );
}

export function TenantWorkflowsPage() {
  const { tenantId } = useParams({
    from: "/authed/tenants/$tenantId/workflows",
  });
  const { data: workflows, isLoading } = useQuery(
    tenantWorkflowsQuery(tenantId),
  );

  return (
    <div>
      <TenantNav tenantId={tenantId} />

      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Workflow Definitions</h2>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : !workflows || workflows.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">
          No workflow definitions yet.
        </p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Origin</TableHead>
                <TableHead>Created</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {workflows.map((w) => (
                <WorkflowRow key={w.id} workflow={w} tenantId={tenantId} />
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
