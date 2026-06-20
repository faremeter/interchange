import { Link, useLocation } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { tenantDetailQuery } from "@/lib/queries/tenants";

const NAV_ITEMS = [
  { label: "Overview", path: "" },
  { label: "Agent Definitions", path: "/agents" },
  { label: "Agents", path: "/instances" },
  { label: "Workflows", path: "/workflows" },
  { label: "Members", path: "/principals" },
  { label: "Roles", path: "/roles" },
  { label: "Grants", path: "/grants" },
  { label: "Credentials", path: "/credentials" },
  { label: "Wallets", path: "/wallets" },
  { label: "Offerings", path: "/offerings" },
  { label: "Models", path: "/models" },
  { label: "Model Providers", path: "/model-providers" },
  { label: "Model Offerings", path: "/model-offerings" },
] as const;

export function TenantNav({ tenantId }: { tenantId: string }) {
  const { data: tenant } = useQuery(tenantDetailQuery(tenantId));

  const location = useLocation();
  const currentPath = location.pathname;

  return (
    <div className="mb-6">
      <div className="flex items-center gap-2">
        <Link
          to="/"
          className="text-sm text-muted-foreground hover:text-foreground"
        >
          Home
        </Link>
        <span className="text-muted-foreground/50">/</span>
        <span className="text-sm font-medium">{tenant?.name ?? "..."}</span>
      </div>
      <nav className="mt-3 flex gap-1 border-b">
        {NAV_ITEMS.map((item) => {
          const to = `/tenants/${tenantId}${item.path}`;
          const isActive =
            item.path === "" ? currentPath === to : currentPath.startsWith(to);
          return (
            <Link
              key={item.path}
              to={to}
              className={`border-b-2 px-3 py-2 text-sm font-medium transition ${
                isActive
                  ? "border-primary text-primary"
                  : "border-transparent text-muted-foreground hover:text-foreground"
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
