import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "../components/tenant-nav";
import { tenantCapabilitiesQuery } from "../lib/queries/tenants";

export function TenantCapabilitiesPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: capabilities, isLoading } = useQuery(
    tenantCapabilitiesQuery(tenantId),
  );

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold text-gray-900">Capabilities</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-gray-400">Loading...</p>
      ) : capabilities?.length === 0 ? (
        <p className="mt-4 text-sm text-gray-400">
          No capabilities registered.
        </p>
      ) : (
        <div className="mt-4 overflow-hidden rounded-lg border border-gray-200 bg-white">
          <table className="w-full text-left text-sm">
            <thead className="border-b border-gray-100 bg-gray-50">
              <tr>
                <th className="px-4 py-2 font-medium text-gray-600">Name</th>
                <th className="px-4 py-2 font-medium text-gray-600">Agent</th>
                <th className="px-4 py-2 font-medium text-gray-600">
                  Description
                </th>
                <th className="px-4 py-2 font-medium text-gray-600">Pricing</th>
              </tr>
            </thead>
            <tbody>
              {capabilities?.map((cap) => (
                <tr
                  key={cap.id}
                  className="border-b border-gray-50 last:border-0"
                >
                  <td className="px-4 py-2 font-medium text-gray-900">
                    {cap.name}
                  </td>
                  <td className="px-4 py-2">
                    <span className="rounded bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-600">
                      {cap.agentName}
                    </span>
                  </td>
                  <td className="px-4 py-2 text-gray-500">
                    {cap.description ?? "-"}
                  </td>
                  <td className="px-4 py-2">
                    {cap.pricing?.base ? (
                      <span className="font-mono text-xs text-gray-700">
                        {cap.pricing.base.amount} {cap.pricing.base.currency}
                      </span>
                    ) : (
                      <span className="text-xs text-gray-400">Free</span>
                    )}
                    {cap.pricing?.negotiable ? (
                      <span className="ml-1 text-xs text-yellow-600">
                        (negotiable)
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
