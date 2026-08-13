import { describe, test, expect, mock } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { runDetailQuery } from "@/lib/queries/tenants";
import type { WorkflowRunResponse } from "@/lib/queries/tenants";

// The page reads its route params and renders a back-link through the router.
// Neither needs real routing here: the params are fixed and the link is a plain
// anchor, so the page can render outside a RouterProvider. This mock must be
// registered before the page module (which imports the router at its top level)
// is loaded, hence the dynamic import below. Bun's mock.module is process-wide
// and not reset between files, so only this file stubs the router; the sibling
// component tests render pure components that never import it.
await mock.module("@tanstack/react-router", () => ({
  useParams: () => ({ tenantId: "tnt_1", runId: "run_1" }),
  Link: ({ children }: { children: React.ReactNode }) => <a>{children}</a>,
}));

const { TenantRunDetailPage } = await import("./tenant-run-detail");

function makeRun(
  overrides: Partial<WorkflowRunResponse> = {},
): WorkflowRunResponse {
  return {
    id: "run_1",
    definitionId: "def_1",
    definitionName: "Nightly Reconcile",
    tenantId: "tnt_1",
    address: "agent://nightly-reconcile",
    status: "deployed",
    publicKey: null,
    kernelId: "kern_1",
    sidecarId: "side_1",
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    endedAt: null,
    ...overrides,
  };
}

function renderClient(qc: QueryClient) {
  return renderToStaticMarkup(
    <QueryClientProvider client={qc}>
      <TenantRunDetailPage />
    </QueryClientProvider>,
  );
}

function renderRun(run: WorkflowRunResponse) {
  const qc = new QueryClient();
  qc.setQueryData(runDetailQuery("tnt_1", "run_1").queryKey, run);
  return renderClient(qc);
}

describe("TenantRunDetailPage", () => {
  test("renders the run's identity and metadata", () => {
    const html = renderRun(makeRun());
    expect(html).toContain("Nightly Reconcile");
    expect(html).toContain("agent://nightly-reconcile");
    expect(html).toContain("Run ID");
    expect(html).toContain("run_1");
    expect(html).toContain("Kernel ID");
    expect(html).toContain("kern_1");
    expect(html).toContain("Sidecar ID");
    expect(html).toContain("side_1");
    expect(html).toContain("Created");
  });

  test("omits the kernel, sidecar, and ended rows when they are absent", () => {
    const html = renderRun(
      makeRun({ kernelId: null, sidecarId: null, endedAt: null }),
    );
    expect(html).toContain("Run ID");
    expect(html).not.toContain("Kernel ID");
    expect(html).not.toContain("Sidecar ID");
    expect(html).not.toContain("Ended");
  });

  test("renders the ended row once the run has an end time", () => {
    const html = renderRun(makeRun({ endedAt: "2026-01-02T00:00:00.000Z" }));
    expect(html).toContain("Ended");
  });

  test.each([
    ["running", "secondary"],
    ["error", "destructive"],
    ["deployed", "outline"],
    ["stopped", "outline"],
  ] as const)("renders the %s status as a %s badge", (status, variant) => {
    const html = renderRun(makeRun({ status }));
    // Tie the variant and the label to the same badge element: the escaped
    // class attribute between them carries no literal ">", so the tag does not
    // close until its text.
    expect(html).toMatch(
      new RegExp(`data-variant="${variant}"[^>]*>${status}<`),
    );
  });

  test("dresses a deployed anchor as a neutral live status, not an error", () => {
    const html = renderRun(makeRun({ status: "deployed" }));
    expect(html).toContain(">deployed<");
    // Deployed is not in RUN_STATUS_VARIANTS, so it falls through to the neutral
    // outline badge rather than the destructive treatment that would make a live
    // anchor read as a failed or stopped run.
    expect(html).toContain('data-variant="outline"');
    expect(html).not.toContain('data-variant="destructive"');
  });

  test("wires the activity timeline into the page", () => {
    const html = renderRun(makeRun());
    expect(html).toContain("Activity");
  });

  test("presents no stop or mail-history affordance", () => {
    const html = renderRun(makeRun());
    // The stop and mail-history controls hit routes that still answer 501 and
    // are deferred, so the page must present no such action.
    expect(html).not.toContain("<button");
    expect(html).not.toContain("Stop");
    expect(html).not.toContain("Mail");
  });

  test("shows a loading state while the run query is pending", () => {
    const html = renderClient(new QueryClient());
    expect(html).toContain("Loading...");
  });

  test("renders a not-found state when the run query resolves empty", () => {
    // A disabled query settles as not-loading with data undefined and no
    // fetch, which drives the page's empty-run branch.
    const qc = new QueryClient({
      defaultOptions: { queries: { enabled: false } },
    });
    const html = renderClient(qc);
    expect(html).toContain("Not found.");
  });
});
