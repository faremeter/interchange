import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "@/components/tenant-nav";
import { tenantGrantsQuery } from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

function EffectBadge({ effect }: { effect: string }) {
  const variant =
    effect === "allow"
      ? "secondary"
      : effect === "deny"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{effect}</Badge>;
}

export function TenantGrantsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: grants, isLoading } = useQuery(tenantGrantsQuery(tenantId));

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold">Grants</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : grants?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">No grants yet.</p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Resource</TableHead>
                <TableHead>Action</TableHead>
                <TableHead>Effect</TableHead>
                <TableHead>Source</TableHead>
                <TableHead>Target</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {grants?.map((g) => (
                <TableRow key={g.id}>
                  <TableCell className="font-mono text-xs">
                    {g.resource}
                  </TableCell>
                  <TableCell className="font-mono text-xs">
                    {g.action}
                  </TableCell>
                  <TableCell>
                    <EffectBadge effect={g.effect} />
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {g.source}
                  </TableCell>
                  <TableCell>
                    {g.roleName ? (
                      <Badge variant="secondary">{g.roleName}</Badge>
                    ) : g.roleId ? (
                      <span className="font-mono text-xs text-muted-foreground">
                        {g.roleId}
                      </span>
                    ) : null}
                    {g.principalName ? (
                      <Badge variant="outline">{g.principalName}</Badge>
                    ) : g.principalId ? (
                      <span className="font-mono text-xs text-muted-foreground">
                        {g.principalId}
                      </span>
                    ) : null}
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
