import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "../components/tenant-nav";
import {
  tenantDetailQuery,
  tenantPrincipalsQuery,
  tenantAgentsQuery,
  tenantRolesQuery,
} from "../lib/queries/tenants";

export function TenantPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: tenant, isLoading } = useQuery(tenantDetailQuery(tenantId));
  const { data: principals } = useQuery(tenantPrincipalsQuery(tenantId));
  const { data: agents } = useQuery(tenantAgentsQuery(tenantId));
  const { data: roles } = useQuery(tenantRolesQuery(tenantId));

  if (isLoading) return <p className="text-sm text-gray-400">Loading...</p>;

  return (
    <div>
      <TenantNav />

      <div className="grid gap-6 sm:grid-cols-3">
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="text-sm text-gray-500">Members</p>
          <p className="text-2xl font-semibold text-gray-900">
            {principals?.filter((p) => p.kind === "user").length ?? 0}
          </p>
        </div>
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="text-sm text-gray-500">Agents</p>
          <p className="text-2xl font-semibold text-gray-900">
            {agents?.length ?? 0}
          </p>
        </div>
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="text-sm text-gray-500">Roles</p>
          <p className="text-2xl font-semibold text-gray-900">
            {roles?.length ?? 0}
          </p>
        </div>
      </div>

      {tenant && (
        <div className="mt-6 rounded-lg border border-gray-200 bg-white p-4">
          <h3 className="text-sm font-medium text-gray-600">Tenant details</h3>
          <dl className="mt-2 grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
            <dt className="text-gray-500">Slug</dt>
            <dd className="font-mono text-gray-900">{tenant.slug}</dd>
            <dt className="text-gray-500">Domain</dt>
            <dd className="font-mono text-gray-900">{tenant.domain}</dd>
            <dt className="text-gray-500">Created</dt>
            <dd className="text-gray-900">
              {new Date(tenant.createdAt).toLocaleDateString()}
            </dd>
          </dl>
        </div>
      )}
    </div>
  );
}
