import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "@/components/tenant-nav";
import { tenantCapabilitiesQuery } from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

export function TenantCapabilitiesPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: capabilities, isLoading } = useQuery(
    tenantCapabilitiesQuery(tenantId),
  );

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold">Capabilities</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : capabilities?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">
          No capabilities registered.
        </p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Agent</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Pricing</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {capabilities?.map((cap) => (
                <TableRow key={cap.id}>
                  <TableCell className="font-medium">{cap.name}</TableCell>
                  <TableCell>
                    <Badge variant="secondary">{cap.agentName}</Badge>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {cap.description ?? "-"}
                  </TableCell>
                  <TableCell>
                    {cap.pricing?.base ? (
                      <span className="font-mono text-xs">
                        {cap.pricing.base.amount} {cap.pricing.base.currency}
                      </span>
                    ) : (
                      <span className="text-xs text-muted-foreground">
                        Free
                      </span>
                    )}
                    {cap.pricing?.negotiable ? (
                      <Badge variant="outline" className="ml-1">
                        negotiable
                      </Badge>
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
