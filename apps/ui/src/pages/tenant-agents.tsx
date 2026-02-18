import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "../components/tenant-nav";
import { tenantAgentsQuery } from "../lib/queries/tenants";

const STATUS_COLORS: Record<string, string> = {
  deployed: "bg-green-100 text-green-700",
  stopped: "bg-gray-100 text-gray-500",
  updating: "bg-blue-100 text-blue-700",
  error: "bg-red-100 text-red-700",
};

export function TenantAgentsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: agents, isLoading } = useQuery(tenantAgentsQuery(tenantId));

  return (
    <div>
      <TenantNav />

      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900">Agents</h2>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-gray-400">Loading...</p>
      ) : agents?.length === 0 ? (
        <p className="mt-4 text-sm text-gray-400">No agents yet.</p>
      ) : (
        <div className="mt-4 overflow-hidden rounded-lg border border-gray-200 bg-white">
          <table className="w-full text-left text-sm">
            <thead className="border-b border-gray-100 bg-gray-50">
              <tr>
                <th className="px-4 py-2 font-medium text-gray-600">Name</th>
                <th className="px-4 py-2 font-medium text-gray-600">Status</th>
                <th className="px-4 py-2 font-medium text-gray-600">Version</th>
                <th className="px-4 py-2 font-medium text-gray-600">Created</th>
              </tr>
            </thead>
            <tbody>
              {agents?.map((a) => (
                <tr
                  key={a.id}
                  className="border-b border-gray-50 last:border-0"
                >
                  <td className="px-4 py-2">
                    <p className="font-medium text-gray-900">{a.name}</p>
                    {a.description && (
                      <p className="text-xs text-gray-400">{a.description}</p>
                    )}
                  </td>
                  <td className="px-4 py-2">
                    <span
                      className={`rounded px-2 py-0.5 text-xs font-medium ${STATUS_COLORS[a.status] ?? "bg-gray-100"}`}
                    >
                      {a.status}
                    </span>
                  </td>
                  <td className="px-4 py-2 font-mono text-xs text-gray-500">
                    v{a.currentVersion}
                  </td>
                  <td className="px-4 py-2 text-gray-500">
                    {new Date(a.createdAt).toLocaleDateString()}
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
