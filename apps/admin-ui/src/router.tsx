import {
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
  redirect,
} from "@tanstack/react-router";

import { Layout } from "@/components/layout";
import { LoginPage } from "@/pages/login";
import { DashboardPage } from "@/pages/dashboard";
import { TenantPage } from "@/pages/tenant";
import { TenantAgentsPage } from "@/pages/tenant-agents";
import { TenantAgentDetailPage } from "@/pages/tenant-agent-detail";
import { TenantPrincipalsPage } from "@/pages/tenant-principals";
import { TenantPrincipalDetailPage } from "@/pages/tenant-principal-detail";
import { TenantRolesPage } from "@/pages/tenant-roles";
import { TenantRoleDetailPage } from "@/pages/tenant-role-detail";
import { TenantGrantsPage } from "@/pages/tenant-grants";
import { TenantGrantDetailPage } from "@/pages/tenant-grant-detail";
import { TenantCredentialsPage } from "@/pages/tenant-credentials";
import { TenantCredentialDetailPage } from "@/pages/tenant-credential-detail";
import { TenantWalletsPage } from "@/pages/tenant-wallets";
import { TenantWalletDetailPage } from "@/pages/tenant-wallet-detail";
import { TenantInstancesPage } from "@/pages/tenant-instances";
import { TenantInstanceDetailPage } from "@/pages/tenant-instance-detail";
import { TenantWorkflowsPage } from "@/pages/tenant-workflows";
import { TenantWorkflowDetailPage } from "@/pages/tenant-workflow-detail";
import { TenantOfferingsPage } from "@/pages/tenant-offerings";
import { TenantOfferingDetailPage } from "@/pages/tenant-offering-detail";
import { TenantModelsPage } from "@/pages/tenant-models";
import { TenantModelDetailPage } from "@/pages/tenant-model-detail";
import { TenantModelProvidersPage } from "@/pages/tenant-model-providers";
import { TenantModelProviderDetailPage } from "@/pages/tenant-model-provider-detail";
import { TenantModelOfferingsPage } from "@/pages/tenant-model-offerings";
import { TenantModelOfferingDetailPage } from "@/pages/tenant-model-offering-detail";

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

const tenantAgentDetailRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/agents/$agentId",
  component: TenantAgentDetailPage,
});

const tenantInstancesRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/instances",
  component: TenantInstancesPage,
});

const tenantInstanceDetailRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/instances/$instanceId",
  component: TenantInstanceDetailPage,
});

const tenantWorkflowsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/workflows",
  component: TenantWorkflowsPage,
});

const tenantWorkflowDetailRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/workflows/$workflowId",
  component: TenantWorkflowDetailPage,
});

const tenantPrincipalsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/principals",
  component: TenantPrincipalsPage,
});

const tenantPrincipalDetailRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/principals/$principalId",
  component: TenantPrincipalDetailPage,
});

const tenantRolesRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/roles",
  component: TenantRolesPage,
});

const tenantRoleDetailRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/roles/$roleId",
  component: TenantRoleDetailPage,
});

const tenantGrantsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/grants",
  component: TenantGrantsPage,
});

const tenantGrantDetailRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/grants/$grantId",
  component: TenantGrantDetailPage,
});

const tenantCredentialsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/credentials",
  component: TenantCredentialsPage,
});

const tenantCredentialDetailRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/credentials/$credentialId",
  component: TenantCredentialDetailPage,
});

const tenantWalletsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/wallets",
  component: TenantWalletsPage,
});

const tenantWalletDetailRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/wallets/$walletId",
  component: TenantWalletDetailPage,
});

const tenantOfferingsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/offerings",
  component: TenantOfferingsPage,
});

const tenantOfferingDetailRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/offerings/$offeringId",
  component: TenantOfferingDetailPage,
});

const tenantModelsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/models",
  component: TenantModelsPage,
});

const tenantModelDetailRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/models/$modelId",
  component: TenantModelDetailPage,
});

const tenantModelProvidersRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/model-providers",
  component: TenantModelProvidersPage,
});

const tenantModelProviderDetailRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/model-providers/$providerId",
  component: TenantModelProviderDetailPage,
});

const tenantModelOfferingsRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/model-offerings",
  component: TenantModelOfferingsPage,
});

const tenantModelOfferingDetailRoute = createRoute({
  getParentRoute: () => authedRoute,
  path: "/tenants/$tenantId/model-offerings/$offeringId",
  component: TenantModelOfferingDetailPage,
});

const routeTree = rootRoute.addChildren([
  loginRoute,
  authedRoute.addChildren([
    dashboardRoute,
    tenantRoute,
    tenantAgentsRoute,
    tenantAgentDetailRoute,
    tenantInstancesRoute,
    tenantInstanceDetailRoute,
    tenantWorkflowsRoute,
    tenantWorkflowDetailRoute,
    tenantPrincipalsRoute,
    tenantPrincipalDetailRoute,
    tenantRolesRoute,
    tenantRoleDetailRoute,
    tenantGrantsRoute,
    tenantGrantDetailRoute,
    tenantCredentialsRoute,
    tenantCredentialDetailRoute,
    tenantWalletsRoute,
    tenantWalletDetailRoute,
    tenantOfferingsRoute,
    tenantOfferingDetailRoute,
    tenantModelsRoute,
    tenantModelDetailRoute,
    tenantModelProvidersRoute,
    tenantModelProviderDetailRoute,
    tenantModelOfferingsRoute,
    tenantModelOfferingDetailRoute,
  ]),
]);

export const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
