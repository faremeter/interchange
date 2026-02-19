import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "../components/tenant-nav";
import { tenantGrantsQuery } from "../lib/queries/tenants";

const EFFECT_COLORS: Record<string, string> = {
  allow: "bg-green-100 text-green-700",
  deny: "bg-red-100 text-red-700",
  ask: "bg-yellow-100 text-yellow-700",
};

export function TenantGrantsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: grants, isLoading } = useQuery(tenantGrantsQuery(tenantId));

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold text-gray-900">Grants</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-gray-400">Loading...</p>
      ) : grants?.length === 0 ? (
        <p className="mt-4 text-sm text-gray-400">No grants yet.</p>
      ) : (
        <div className="mt-4 overflow-hidden rounded-lg border border-gray-200 bg-white">
          <table className="w-full text-left text-sm">
            <thead className="border-b border-gray-100 bg-gray-50">
              <tr>
                <th className="px-4 py-2 font-medium text-gray-600">
                  Resource
                </th>
                <th className="px-4 py-2 font-medium text-gray-600">Action</th>
                <th className="px-4 py-2 font-medium text-gray-600">Effect</th>
                <th className="px-4 py-2 font-medium text-gray-600">Source</th>
                <th className="px-4 py-2 font-medium text-gray-600">Target</th>
              </tr>
            </thead>
            <tbody>
              {grants?.map((g) => (
                <tr
                  key={g.id}
                  className="border-b border-gray-50 last:border-0"
                >
                  <td className="px-4 py-2 font-mono text-xs text-gray-900">
                    {g.resource}
                  </td>
                  <td className="px-4 py-2 font-mono text-xs text-gray-900">
                    {g.action}
                  </td>
                  <td className="px-4 py-2">
                    <span
                      className={`rounded px-2 py-0.5 text-xs font-medium ${EFFECT_COLORS[g.effect] ?? "bg-gray-100"}`}
                    >
                      {g.effect}
                    </span>
                  </td>
                  <td className="px-4 py-2 text-gray-500">{g.source}</td>
                  <td className="px-4 py-2">
                    {g.roleName ? (
                      <span className="rounded bg-purple-50 px-1.5 py-0.5 text-xs text-purple-700">
                        {g.roleName}
                      </span>
                    ) : g.roleId ? (
                      <span className="font-mono text-xs text-purple-400">
                        {g.roleId}
                      </span>
                    ) : null}
                    {g.principalName ? (
                      <span className="rounded bg-blue-50 px-1.5 py-0.5 text-xs text-blue-700">
                        {g.principalName}
                      </span>
                    ) : g.principalId ? (
                      <span className="font-mono text-xs text-blue-400">
                        {g.principalId}
                      </span>
                    ) : null}
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
