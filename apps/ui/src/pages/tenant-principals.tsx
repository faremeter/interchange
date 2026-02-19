import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "../components/tenant-nav";
import { tenantPrincipalsQuery } from "../lib/queries/tenants";

const STATUS_COLORS: Record<string, string> = {
  active: "bg-green-100 text-green-700",
  invited: "bg-yellow-100 text-yellow-700",
  suspended: "bg-red-100 text-red-700",
  deactivated: "bg-gray-100 text-gray-500",
};

const KIND_LABELS: Record<string, string> = {
  user: "User",
  agent: "Agent",
};

export function TenantPrincipalsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: principals, isLoading } = useQuery(
    tenantPrincipalsQuery(tenantId),
  );

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold text-gray-900">Members</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-gray-400">Loading...</p>
      ) : principals?.length === 0 ? (
        <p className="mt-4 text-sm text-gray-400">No members yet.</p>
      ) : (
        <div className="mt-4 overflow-hidden rounded-lg border border-gray-200 bg-white">
          <table className="w-full text-left text-sm">
            <thead className="border-b border-gray-100 bg-gray-50">
              <tr>
                <th className="px-4 py-2 font-medium text-gray-600">Name</th>
                <th className="px-4 py-2 font-medium text-gray-600">Kind</th>
                <th className="px-4 py-2 font-medium text-gray-600">Status</th>
                <th className="px-4 py-2 font-medium text-gray-600">Roles</th>
              </tr>
            </thead>
            <tbody>
              {principals?.map((p) => (
                <tr
                  key={p.id}
                  className="border-b border-gray-50 last:border-0"
                >
                  <td className="px-4 py-2">
                    <div className="font-medium text-gray-900">
                      {p.displayName}
                    </div>
                    {p.email ? (
                      <div className="text-xs text-gray-400">{p.email}</div>
                    ) : null}
                  </td>
                  <td className="px-4 py-2">
                    <span className="rounded bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-600">
                      {KIND_LABELS[p.kind] ?? p.kind}
                    </span>
                  </td>
                  <td className="px-4 py-2">
                    <span
                      className={`rounded px-2 py-0.5 text-xs font-medium ${STATUS_COLORS[p.status] ?? "bg-gray-100"}`}
                    >
                      {p.status}
                    </span>
                  </td>
                  <td className="px-4 py-2">
                    <div className="flex flex-wrap gap-1">
                      {p.roles.map((r) => (
                        <span
                          key={r.id}
                          className="rounded bg-blue-50 px-1.5 py-0.5 text-xs text-blue-700"
                        >
                          {r.name}
                        </span>
                      ))}
                    </div>
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
