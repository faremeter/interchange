import { describe, test, expect, mock } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { workflowDetailQuery } from "@/lib/queries/tenants";
import type { WorkflowAssetResponse } from "@/lib/queries/tenants";

// The page reads its route params and renders links through the router. Neither
// needs real routing here: the params are fixed and the links are plain anchors,
// so the page renders outside a RouterProvider. This mock must be registered
// before the page module (which imports the router at its top level) loads,
// hence the dynamic import below.
await mock.module("@tanstack/react-router", () => ({
  useParams: () => ({ tenantId: "tnt_1", workflowId: "wf_1" }),
  Link: ({ children }: { children: React.ReactNode }) => <a>{children}</a>,
}));

const { TenantWorkflowDetailPage } = await import("./tenant-workflow-detail");

function makeWorkflow(
  overrides: Partial<WorkflowAssetResponse> = {},
): WorkflowAssetResponse {
  return {
    id: "wf_1",
    tenantId: "tnt_1",
    kind: "workflow",
    name: "nightly-reconcile",
    displayName: "Nightly Reconcile",
    creatorPrincipalId: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    ...overrides,
  };
}

function renderPage(workflow: WorkflowAssetResponse) {
  const qc = new QueryClient();
  qc.setQueryData(workflowDetailQuery("tnt_1", "wf_1").queryKey, workflow);
  return renderToStaticMarkup(
    <QueryClientProvider client={qc}>
      <TenantWorkflowDetailPage />
    </QueryClientProvider>,
  );
}

describe("TenantWorkflowDetailPage launch form", () => {
  test("renders the source-kind picker and the launch action", () => {
    const html = renderPage(makeWorkflow());
    expect(html).toContain("Launch Workflow");
    expect(html).toContain("Source kind");
    expect(html).toContain("Entry module");
  });

  test("shows the asset-source fields by default", () => {
    const html = renderPage(makeWorkflow());
    expect(html).toContain("Asset ID");
    expect(html).toContain("Commit SHA");
    expect(html).toContain("Package name");
  });

  test("renders the inference source inputs", () => {
    const html = renderPage(makeWorkflow());
    expect(html).toContain("Source ID");
    expect(html).toContain("Provider");
    expect(html).toContain("Model");
    expect(html).toContain("Base URL");
    expect(html).toContain("Credential ID");
  });

  test("disables the launch button before the form is filled", () => {
    const html = renderPage(makeWorkflow());
    // The empty form fails the submit guard, so the launch button renders
    // disabled. crypto.randomUUID and the fields stay empty here.
    expect(html).toMatch(/<button[^>]*disabled[^>]*>Launch Workflow<\/button>/);
  });

  test("shows a loading state while the workflow query is pending", () => {
    const html = renderToStaticMarkup(
      <QueryClientProvider client={new QueryClient()}>
        <TenantWorkflowDetailPage />
      </QueryClientProvider>,
    );
    expect(html).toContain("Loading...");
  });
});
