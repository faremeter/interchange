import {
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
  redirect,
} from "@tanstack/react-router";

import { Layout } from "./components/layout";
import { LoginPage } from "./pages/login";
import { DashboardPage } from "./pages/dashboard";
import { TenantPage } from "./pages/tenant";
import { TenantAgentsPage } from "./pages/tenant-agents";
import { TenantPrincipalsPage } from "./pages/tenant-principals";
import { TenantRolesPage } from "./pages/tenant-roles";
import { TenantGrantsPage } from "./pages/tenant-grants";
import { TenantCredentialsPage } from "./pages/tenant-credentials";
import { TenantWalletsPage } from "./pages/tenant-wallets";
import { TenantCapabilitiesPage } from "./pages/tenant-capabilities";

const rootRoute = createRootRoute({
  component: () => <Outlet />,
});

const loginRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/login",
  component: LoginPage,
});

const authedRoute = createRoute({
  getParentRoute: () => rootRoute,
  id: "authed",
  component: Layout,
  beforeLoad: async () => {
    const res = await fetch("/api/me");
    if (!res.ok) {
      throw redirect({ to: "/login" });
    }
  },
});

const dashboardRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/",
  component: DashboardPage,
});

const tenantRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId",
  component: TenantPage,
});

const tenantAgentsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/agents",
  component: TenantAgentsPage,
});

const tenantPrincipalsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/principals",
  component: TenantPrincipalsPage,
});

const tenantRolesRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/roles",
  component: TenantRolesPage,
});

const tenantGrantsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/grants",
  component: TenantGrantsPage,
});

const tenantCredentialsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/credentials",
  component: TenantCredentialsPage,
});

const tenantWalletsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/wallets",
  component: TenantWalletsPage,
});

const tenantCapabilitiesRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/capabilities",
  component: TenantCapabilitiesPage,
});

const routeTree = rootRoute.addChildren([
  loginRoute,
  authedRoute.addChildren([
    dashboardRoute,
    tenantRoute,
    tenantAgentsRoute,
    tenantPrincipalsRoute,
    tenantRolesRoute,
    tenantGrantsRoute,
    tenantCredentialsRoute,
    tenantWalletsRoute,
    tenantCapabilitiesRoute,
  ]),
]);

export const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
