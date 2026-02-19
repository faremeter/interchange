import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "@/components/tenant-nav";
import { tenantAgentsQuery } from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

function AgentStatusBadge({ status }: { status: string }) {
  const variant =
    status === "deployed"
      ? "secondary"
      : status === "error"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

export function TenantAgentsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: agents, isLoading } = useQuery(tenantAgentsQuery(tenantId));

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold">Agents</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : agents?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">No agents yet.</p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Version</TableHead>
                <TableHead>Created</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {agents?.map((a) => (
                <TableRow key={a.id}>
                  <TableCell>
                    <div className="font-medium">{a.name}</div>
                    {a.description && (
                      <div className="text-xs text-muted-foreground">
                        {a.description}
                      </div>
                    )}
                  </TableCell>
                  <TableCell>
                    <AgentStatusBadge status={a.status} />
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    v{a.currentVersion}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {new Date(a.createdAt).toLocaleDateString()}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
