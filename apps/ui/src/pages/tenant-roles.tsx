import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "@/components/tenant-nav";
import { tenantRolesQuery } from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

export function TenantRolesPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: roles, isLoading } = useQuery(tenantRolesQuery(tenantId));

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold">Roles</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : roles?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">No roles yet.</p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Created</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {roles?.map((r) => (
                <TableRow key={r.id}>
                  <TableCell className="font-medium">{r.name}</TableCell>
                  <TableCell>
                    <Badge variant={r.isSystem ? "default" : "outline"}>
                      {r.isSystem ? "system" : "custom"}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {r.description ?? "-"}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {new Date(r.createdAt).toLocaleDateString()}
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
