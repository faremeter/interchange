import { Link } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { mePrincipalsQuery, meAgentsQuery } from "../lib/queries/me";

const STATUS_COLORS: Record<string, string> = {
  active: "bg-green-100 text-green-700",
  invited: "bg-yellow-100 text-yellow-700",
  suspended: "bg-red-100 text-red-700",
  deactivated: "bg-gray-100 text-gray-500",
  deployed: "bg-green-100 text-green-700",
  stopped: "bg-gray-100 text-gray-500",
  updating: "bg-blue-100 text-blue-700",
  error: "bg-red-100 text-red-700",
};

function StatusBadge({ status }: { status: string }) {
  const color = STATUS_COLORS[status] ?? "bg-gray-100 text-gray-500";
  return (
    <span className={`rounded px-2 py-0.5 text-xs font-medium ${color}`}>
      {status}
    </span>
  );
}

export function DashboardPage() {
  const { data: principals, isLoading: loadingPrincipals } =
    useQuery(mePrincipalsQuery);
  const { data: agents, isLoading: loadingAgents } = useQuery(meAgentsQuery);

  return (
    <div className="space-y-8">
      <div>
        <h2 className="text-xl font-semibold text-gray-900">Your tenants</h2>
        <p className="text-sm text-gray-500">
          Organizations you belong to across the platform.
        </p>
      </div>

      {loadingPrincipals ? (
        <p className="text-sm text-gray-400">Loading...</p>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {principals?.map((p) => (
            <Link
              key={p.principalId}
              to="/tenants/$tenantId"
              params={{ tenantId: p.tenantId }}
              className="rounded-lg border border-gray-200 bg-white p-4 transition hover:border-blue-300 hover:shadow-sm"
            >
              <div className="flex items-center justify-between">
                <h3 className="font-medium text-gray-900">{p.tenantName}</h3>
                <StatusBadge status={p.status} />
              </div>
              <p className="mt-1 text-xs text-gray-500">{p.tenantSlug}</p>
              <div className="mt-2 flex flex-wrap gap-1">
                {p.roles.map((r) => (
                  <span
                    key={r.id}
                    className="rounded bg-gray-100 px-1.5 py-0.5 text-xs text-gray-600"
                  >
                    {r.name}
                  </span>
                ))}
              </div>
            </Link>
          ))}
        </div>
      )}

      <div>
        <h2 className="text-xl font-semibold text-gray-900">
          Agents across tenants
        </h2>
        <p className="text-sm text-gray-500">
          All agents you have visibility into.
        </p>
      </div>

      {loadingAgents ? (
        <p className="text-sm text-gray-400">Loading...</p>
      ) : agents?.length === 0 ? (
        <p className="text-sm text-gray-400">No agents yet.</p>
      ) : (
        <div className="overflow-hidden rounded-lg border border-gray-200 bg-white">
          <table className="w-full text-left text-sm">
            <thead className="border-b border-gray-100 bg-gray-50">
              <tr>
                <th className="px-4 py-2 font-medium text-gray-600">Name</th>
                <th className="px-4 py-2 font-medium text-gray-600">Tenant</th>
                <th className="px-4 py-2 font-medium text-gray-600">Status</th>
              </tr>
            </thead>
            <tbody>
              {agents?.map((a) => (
                <tr
                  key={a.id}
                  className="border-b border-gray-50 last:border-0"
                >
                  <td className="px-4 py-2">
                    <Link
                      to="/tenants/$tenantId/agents"
                      params={{ tenantId: a.tenantId }}
                      className="text-blue-600 hover:underline"
                    >
                      {a.name}
                    </Link>
                    {a.description && (
                      <p className="text-xs text-gray-400">{a.description}</p>
                    )}
                  </td>
                  <td className="px-4 py-2 text-gray-500">{a.tenantName}</td>
                  <td className="px-4 py-2">
                    <StatusBadge status={a.status} />
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
