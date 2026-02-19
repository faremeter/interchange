import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "@/components/tenant-nav";
import {
  tenantDetailQuery,
  tenantPrincipalsQuery,
  tenantAgentsQuery,
  tenantRolesQuery,
} from "@/lib/queries/tenants";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export function TenantPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: tenant, isLoading } = useQuery(tenantDetailQuery(tenantId));
  const { data: principals } = useQuery(tenantPrincipalsQuery(tenantId));
  const { data: agents } = useQuery(tenantAgentsQuery(tenantId));
  const { data: roles } = useQuery(tenantRolesQuery(tenantId));

  if (isLoading) {
    return <p className="text-sm text-muted-foreground">Loading...</p>;
  }

  return (
    <div>
      <TenantNav />

      <div className="grid gap-4 sm:grid-cols-3">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Members
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <p className="text-2xl font-semibold">
              {principals?.filter((p) => p.kind === "user").length ?? 0}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Agents
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <p className="text-2xl font-semibold">{agents?.length ?? 0}</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Roles
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <p className="text-2xl font-semibold">{roles?.length ?? 0}</p>
          </CardContent>
        </Card>
      </div>

      {tenant && (
        <Card className="mt-6">
          <CardHeader>
            <CardTitle className="text-sm">Tenant details</CardTitle>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
              <dt className="text-muted-foreground">Slug</dt>
              <dd className="font-mono">{tenant.slug}</dd>
              <dt className="text-muted-foreground">Domain</dt>
              <dd className="font-mono">{tenant.domain}</dd>
              <dt className="text-muted-foreground">Created</dt>
              <dd>{new Date(tenant.createdAt).toLocaleDateString()}</dd>
            </dl>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
