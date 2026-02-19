import { Link } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { mePrincipalsQuery, meAgentsQuery } from "@/lib/queries/me";
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
    status === "active" || status === "deployed"
      ? "secondary"
      : status === "error" || status === "suspended"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

export function DashboardPage() {
  const { data: principals, isLoading: loadingPrincipals } =
    useQuery(mePrincipalsQuery);
  const { data: agents, isLoading: loadingAgents } = useQuery(meAgentsQuery);

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
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {principals?.map((p) => (
            <Link
              key={p.principalId}
              to="/tenants/$tenantId"
              params={{ tenantId: p.tenantId }}
            >
              <Card className="transition hover:border-primary/50 hover:shadow-sm">
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-base">{p.tenantName}</CardTitle>
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
      )}

      <div>
        <h2 className="text-xl font-semibold">Agents across tenants</h2>
        <p className="text-sm text-muted-foreground">
          All agents you have visibility into.
        </p>
      </div>

      {loadingAgents ? (
        <p className="text-sm text-muted-foreground">Loading...</p>
      ) : agents?.length === 0 ? (
        <p className="text-sm text-muted-foreground">No agents yet.</p>
      ) : (
        <div className="rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Tenant</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {agents?.map((a) => (
                <TableRow key={a.id}>
                  <TableCell>
                    <Link
                      to="/tenants/$tenantId/agents"
                      params={{ tenantId: a.tenantId }}
                      className="text-primary hover:underline"
                    >
                      {a.name}
                    </Link>
                    {a.description && (
                      <p className="text-xs text-muted-foreground">
                        {a.description}
                      </p>
                    )}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {a.tenantName}
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={a.status} />
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
