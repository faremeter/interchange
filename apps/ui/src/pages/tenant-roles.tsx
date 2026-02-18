import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "../components/tenant-nav";
import { tenantRolesQuery } from "../lib/queries/tenants";

export function TenantRolesPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: roles, isLoading } = useQuery(tenantRolesQuery(tenantId));

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold text-gray-900">Roles</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-gray-400">Loading...</p>
      ) : roles?.length === 0 ? (
        <p className="mt-4 text-sm text-gray-400">No roles yet.</p>
      ) : (
        <div className="mt-4 overflow-hidden rounded-lg border border-gray-200 bg-white">
          <table className="w-full text-left text-sm">
            <thead className="border-b border-gray-100 bg-gray-50">
              <tr>
                <th className="px-4 py-2 font-medium text-gray-600">Name</th>
                <th className="px-4 py-2 font-medium text-gray-600">Type</th>
                <th className="px-4 py-2 font-medium text-gray-600">
                  Description
                </th>
                <th className="px-4 py-2 font-medium text-gray-600">Created</th>
              </tr>
            </thead>
            <tbody>
              {roles?.map((r) => (
                <tr
                  key={r.id}
                  className="border-b border-gray-50 last:border-0"
                >
                  <td className="px-4 py-2 font-medium text-gray-900">
                    {r.name}
                  </td>
                  <td className="px-4 py-2">
                    <span
                      className={`rounded px-2 py-0.5 text-xs font-medium ${
                        r.isSystem
                          ? "bg-purple-100 text-purple-700"
                          : "bg-gray-100 text-gray-600"
                      }`}
                    >
                      {r.isSystem ? "system" : "custom"}
                    </span>
                  </td>
                  <td className="px-4 py-2 text-gray-500">
                    {r.description ?? "-"}
                  </td>
                  <td className="px-4 py-2 text-gray-500">
                    {new Date(r.createdAt).toLocaleDateString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
