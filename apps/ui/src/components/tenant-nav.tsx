import { Link, useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { tenantDetailQuery } from "../lib/queries/tenants";

const NAV_ITEMS = [
  { label: "Overview", path: "" },
  { label: "Agents", path: "/agents" },
  { label: "Members", path: "/principals" },
  { label: "Roles", path: "/roles" },
  { label: "Grants", path: "/grants" },
] as const;

export function TenantNav() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: tenant } = useQuery(tenantDetailQuery(tenantId));

  const currentPath = window.location.pathname;

  return (
    <div className="mb-6">
      <div className="flex items-center gap-2">
        <Link to="/" className="text-sm text-gray-400 hover:text-gray-600">
          Home
        </Link>
        <span className="text-gray-300">/</span>
        <span className="text-sm font-medium text-gray-900">
          {tenant?.name ?? "..."}
        </span>
      </div>
      <nav className="mt-3 flex gap-1 border-b border-gray-200">
        {NAV_ITEMS.map((item) => {
          const to = `/tenants/${tenantId}${item.path}`;
          const isActive = currentPath === to;
          return (
            <Link
              key={item.path}
              to={to}
              className={`border-b-2 px-3 py-2 text-sm font-medium transition ${
                isActive
                  ? "border-blue-600 text-blue-600"
                  : "border-transparent text-gray-500 hover:text-gray-700"
              }`}
            >
              {item.label}
            </Link>
          );
        })}
      </nav>
    </div>
  );
}
