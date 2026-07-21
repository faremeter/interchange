import { Link } from "@tanstack/react-router";

import {
  mePrincipalsInfiniteQuery,
  meInstancesInfiniteQuery,
} from "@/lib/queries/me";
import { PaginatedListSentinel } from "@/components/paginated-list-sentinel";
import { usePaginatedList } from "@/lib/hooks/use-paginated-list";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
    status === "active" || status === "deployed" || status === "running"
      ? "secondary"
      : status === "error" || status === "suspended"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

export function DashboardPage() {
  const {
    items: principals,
    isLoading: loadingPrincipals,
    hasNextPage: hasMorePrincipals,
    isFetchingNextPage: fetchingMorePrincipals,
    fetchNextPage: fetchMorePrincipals,
  } = usePaginatedList(mePrincipalsInfiniteQuery);
  const {
    items: instances,
    isLoading: loadingInstances,
    hasNextPage: hasMoreInstances,
    isFetchingNextPage: fetchingMoreInstances,
    fetchNextPage: fetchMoreInstances,
  } = usePaginatedList(meInstancesInfiniteQuery);

  return (
    <div className="space-y-8">
      <div>
        <h2 className="text-xl font-semibold">Your tenants</h2>
        <p className="text-sm text-muted-foreground">
          Organizations you belong to across the platform.
        </p>
      </div>

      {loadingPrincipals ? (
        <p className="text-sm text-muted-foreground">Loading...</p>
      ) : (
        <>
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {principals.map((p) => (
              <Link
                key={p.principalId}
                to="/tenants/$tenantId"
                params={{ tenantId: p.tenantId }}
              >
                <Card className="transition hover:border-primary/50 hover:shadow-sm">
                  <CardHeader className="pb-2">
                    <div className="flex items-center justify-between">
                      <CardTitle className="text-base">
                        {p.tenantName}
                      </CardTitle>
                      <StatusBadge status={p.status} />
                    </div>
                    <p className="text-xs text-muted-foreground">
                      {p.tenantSlug}
                    </p>
                  </CardHeader>
                  <CardContent className="pt-0">
                    <div className="flex flex-wrap gap-1">
                      {p.roles.map((r) => (
                        <Badge key={r.id} variant="outline">
                          {r.name}
                        </Badge>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              </Link>
            ))}
          </div>
          <PaginatedListSentinel
            hasNextPage={hasMorePrincipals}
            isFetchingNextPage={fetchingMorePrincipals}
            fetchNextPage={fetchMorePrincipals}
          />
        </>
      )}

      <div>
        <h2 className="text-xl font-semibold">Agents</h2>
        <p className="text-sm text-muted-foreground">
          Running agents across all your tenants.
        </p>
      </div>

      {loadingInstances ? (
        <p className="text-sm text-muted-foreground">Loading...</p>
      ) : instances.length === 0 ? (
        <p className="text-sm text-muted-foreground">No running agents.</p>
      ) : (
        <div className="rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Definition</TableHead>
                <TableHead>Tenant</TableHead>
                <TableHead>Address</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {instances.map((inst) => (
                <TableRow key={inst.id}>
                  <TableCell>
                    <Link
                      to="/tenants/$tenantId/instances/$instanceId"
                      params={{ tenantId: inst.tenantId, instanceId: inst.id }}
                      className="text-primary hover:underline"
                    >
                      {inst.agentName}
                    </Link>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {inst.tenantName}
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    {inst.address}
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={inst.status} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          <PaginatedListSentinel
            hasNextPage={hasMoreInstances}
            isFetchingNextPage={fetchingMoreInstances}
            fetchNextPage={fetchMoreInstances}
          />
        </div>
      )}
    </div>
  );
}
