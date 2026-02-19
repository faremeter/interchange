import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "@/components/tenant-nav";
import { tenantPrincipalsQuery } from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

function StatusBadge({ status }: { status: string }) {
  const variant =
    status === "active"
      ? "secondary"
      : status === "suspended" || status === "deactivated"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

export function TenantPrincipalsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: principals, isLoading } = useQuery(
    tenantPrincipalsQuery(tenantId),
  );

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold">Members</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : principals?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">No members yet.</p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Kind</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Roles</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {principals?.map((p) => (
                <TableRow key={p.id}>
                  <TableCell>
                    <div className="font-medium">{p.displayName}</div>
                    {p.email ? (
                      <div className="text-xs text-muted-foreground">
                        {p.email}
                      </div>
                    ) : null}
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline">{p.kind}</Badge>
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={p.status} />
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-1">
                      {p.roles.map((r) => (
                        <Badge key={r.id} variant="secondary">
                          {r.name}
                        </Badge>
                      ))}
                    </div>
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
