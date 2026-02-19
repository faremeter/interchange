import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "../components/tenant-nav";
import { tenantCredentialsQuery } from "../lib/queries/tenants";

const TYPE_LABELS: Record<string, string> = {
  api_key: "API Key",
  oauth_token: "OAuth Token",
  certificate: "Certificate",
  other: "Other",
};

const TYPE_COLORS: Record<string, string> = {
  api_key: "bg-blue-50 text-blue-700",
  oauth_token: "bg-purple-50 text-purple-700",
  certificate: "bg-green-50 text-green-700",
  other: "bg-gray-100 text-gray-600",
};

export function TenantCredentialsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: credentials, isLoading } = useQuery(
    tenantCredentialsQuery(tenantId),
  );

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold text-gray-900">Credentials</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-gray-400">Loading...</p>
      ) : credentials?.length === 0 ? (
        <p className="mt-4 text-sm text-gray-400">No credentials stored.</p>
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
              {credentials?.map((cred) => (
                <tr
                  key={cred.id}
                  className="border-b border-gray-50 last:border-0"
                >
                  <td className="px-4 py-2 font-medium text-gray-900">
                    {cred.name}
                  </td>
                  <td className="px-4 py-2">
                    <span
                      className={`rounded px-2 py-0.5 text-xs font-medium ${TYPE_COLORS[cred.type] ?? "bg-gray-100"}`}
                    >
                      {TYPE_LABELS[cred.type] ?? cred.type}
                    </span>
                  </td>
                  <td className="px-4 py-2 text-gray-500">
                    {cred.description ?? "-"}
                  </td>
                  <td className="px-4 py-2 text-xs text-gray-400">
                    {new Date(cred.createdAt).toLocaleDateString()}
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
