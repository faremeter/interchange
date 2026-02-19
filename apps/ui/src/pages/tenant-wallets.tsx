import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "../components/tenant-nav";
import { tenantWalletsQuery } from "../lib/queries/tenants";

const BACKEND_LABELS: Record<string, string> = {
  crypto: "Crypto",
  fiat: "Fiat",
  credits: "Credits",
};

const BACKEND_COLORS: Record<string, string> = {
  crypto: "bg-orange-50 text-orange-700",
  fiat: "bg-green-50 text-green-700",
  credits: "bg-blue-50 text-blue-700",
};

export function TenantWalletsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: wallets, isLoading } = useQuery(tenantWalletsQuery(tenantId));

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold text-gray-900">Wallets</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-gray-400">Loading...</p>
      ) : wallets?.length === 0 ? (
        <p className="mt-4 text-sm text-gray-400">No wallets yet.</p>
      ) : (
        <div className="mt-4 overflow-hidden rounded-lg border border-gray-200 bg-white">
          <table className="w-full text-left text-sm">
            <thead className="border-b border-gray-100 bg-gray-50">
              <tr>
                <th className="px-4 py-2 font-medium text-gray-600">Name</th>
                <th className="px-4 py-2 font-medium text-gray-600">Type</th>
                <th className="px-4 py-2 font-medium text-gray-600">
                  Currency
                </th>
                <th className="px-4 py-2 font-medium text-gray-600">Balance</th>
                <th className="px-4 py-2 font-medium text-gray-600">Created</th>
              </tr>
            </thead>
            <tbody>
              {wallets?.map((w) => (
                <tr
                  key={w.id}
                  className="border-b border-gray-50 last:border-0"
                >
                  <td className="px-4 py-2 font-medium text-gray-900">
                    {w.name}
                  </td>
                  <td className="px-4 py-2">
                    <span
                      className={`rounded px-2 py-0.5 text-xs font-medium ${BACKEND_COLORS[w.backendType] ?? "bg-gray-100"}`}
                    >
                      {BACKEND_LABELS[w.backendType] ?? w.backendType}
                    </span>
                  </td>
                  <td className="px-4 py-2 font-mono text-xs text-gray-600">
                    {w.currency}
                  </td>
                  <td className="px-4 py-2 font-mono text-sm text-gray-900">
                    {w.balance}
                  </td>
                  <td className="px-4 py-2 text-xs text-gray-400">
                    {new Date(w.createdAt).toLocaleDateString()}
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
