import { useNavigate, useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "@/components/tenant-nav";
import { PaginatedListSentinel } from "@/components/paginated-list-sentinel";
import { usePaginatedList } from "@/lib/hooks/use-paginated-list";
import {
  tenantRunsInfiniteQuery,
  tenantWorkflowsQuery,
  type WorkflowDefinitionResponse,
  type WorkflowRunResponse,
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

function StatusBadge({ status }: { status: WorkflowRunResponse["status"] }) {
  const variant =
    status === "running"
      ? "secondary"
      : status === "error"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

function RunRow({
  run,
  tenantId,
}: {
  run: WorkflowRunResponse;
  tenantId: string;
}) {
  const navigate = useNavigate();

  return (
    <TableRow
      className="cursor-pointer"
      onClick={() =>
        void navigate({
          to: "/tenants/$tenantId/workflows/runs/$runId",
          params: { tenantId, runId: run.id },
        })
      }
    >
      <TableCell className="font-medium">{run.definitionName}</TableCell>
      <TableCell>
        <StatusBadge status={run.status} />
      </TableCell>
      <TableCell className="font-mono text-xs text-muted-foreground">
        {run.address}
      </TableCell>
      <TableCell className="text-muted-foreground">
        {new Date(run.createdAt).toLocaleString()}
      </TableCell>
    </TableRow>
  );
}

function DefinitionRow({
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
        void navigate({
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

  const {
    items: runs,
    isLoading: runsLoading,
    hasNextPage,
    isFetchingNextPage,
    fetchNextPage,
  } = usePaginatedList(tenantRunsInfiniteQuery(tenantId));

  const { data: definitions, isLoading: definitionsLoading } = useQuery(
    tenantWorkflowsQuery(tenantId),
  );

  return (
    <div>
      <TenantNav tenantId={tenantId} />

      <h2 className="text-lg font-semibold">Workflows</h2>

      <div className="mt-6">
        <h3 className="text-sm font-semibold text-muted-foreground">Running</h3>
        {runsLoading ? (
          <p className="mt-2 text-sm text-muted-foreground">Loading...</p>
        ) : runs.length === 0 ? (
          <p className="mt-2 text-sm text-muted-foreground">
            No running workflows.
          </p>
        ) : (
          <div className="mt-2 rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Definition</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Address</TableHead>
                  <TableHead>Created</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {runs.map((run) => (
                  <RunRow key={run.id} run={run} tenantId={tenantId} />
                ))}
              </TableBody>
            </Table>
            <PaginatedListSentinel
              hasNextPage={hasNextPage}
              isFetchingNextPage={isFetchingNextPage}
              fetchNextPage={fetchNextPage}
            />
          </div>
        )}
      </div>

      <div className="mt-8">
        <h3 className="text-sm font-semibold text-muted-foreground">
          Definitions
        </h3>
        {definitionsLoading ? (
          <p className="mt-2 text-sm text-muted-foreground">Loading...</p>
        ) : !definitions || definitions.length === 0 ? (
          <p className="mt-2 text-sm text-muted-foreground">
            No workflow definitions yet.
          </p>
        ) : (
          <div className="mt-2 rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Origin</TableHead>
                  <TableHead>Created</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {definitions.map((w) => (
                  <DefinitionRow key={w.id} workflow={w} tenantId={tenantId} />
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </div>
    </div>
  );
}
