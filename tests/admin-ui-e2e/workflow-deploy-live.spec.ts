import { test, expect, type Page } from "@playwright/test";

// A live, un-stubbed deploy. Unlike `workflow-deploy-picker.spec.ts`, which
// intercepts the workflow-detail data plane, this spec drives the real SPA
// against a real hub AND a real sidecar (both brought up by the harness), and
// deploys a real, seeded workflow-source asset through the source picker. No
// `/api` route is stubbed: the deploy round-trips through the hub's deploy
// route and the connected sidecar, proving the operator's picker deploy reaches
// the source-ref deploy end to end.
//
// The harness publishes the concrete, hub-minted inputs after seeding:
const BASE_URL = process.env["E2E_BASE_URL"];
const TENANT_ID = process.env["E2E_WORKFLOW_TENANT_ID"];
const ASSET_ID = process.env["E2E_WORKFLOW_ASSET_ID"];
const COMMIT_SHA = process.env["E2E_WORKFLOW_COMMIT_SHA"];
const ENTRY = process.env["E2E_WORKFLOW_ENTRY"];
const LOGIN_EMAIL = process.env["E2E_LOGIN_EMAIL"];
const LOGIN_PASSWORD = process.env["E2E_LOGIN_PASSWORD"];

function required(name: string, value: string | undefined): string {
  if (value === undefined || value === "") {
    throw new Error(`${name} is not set; global setup did not run`);
  }
  return value;
}

async function selectInferenceOffering(page: Page): Promise<void> {
  await page.locator("#source-offering").click();
  await page.getByRole("option").first().click();
}

// Read the tenant's deployments straight from the real hub (through the
// same-origin preview proxy, carrying the browser session cookie). Nothing is
// stubbed, so this is the hub's own view of what the deploy persisted.
async function readDeployments(
  page: Page,
  baseURL: string,
  tenantId: string,
): Promise<{ id: string; definitionAssetId: string; status: string }[]> {
  const res = await page.request.get(
    `${baseURL}/api/tenants/${tenantId}/workflows/deployments`,
  );
  expect(res.ok()).toBeTruthy();
  const body: unknown = await res.json();
  if (!Array.isArray(body)) {
    throw new Error(
      `deployments list was not an array: ${JSON.stringify(body)}`,
    );
  }
  const rows: { id: string; definitionAssetId: string; status: string }[] = [];
  for (const entry of body) {
    if (
      typeof entry === "object" &&
      entry !== null &&
      "id" in entry &&
      "definitionAssetId" in entry &&
      "status" in entry &&
      typeof entry.id === "string" &&
      typeof entry.definitionAssetId === "string" &&
      typeof entry.status === "string"
    ) {
      rows.push({
        id: entry.id,
        definitionAssetId: entry.definitionAssetId,
        status: entry.status,
      });
    }
  }
  return rows;
}

test("deploys a seeded workflow source through the picker to a real sidecar", async ({
  page,
}) => {
  const baseURL = required("E2E_BASE_URL", BASE_URL);
  const tenantId = required("E2E_WORKFLOW_TENANT_ID", TENANT_ID);
  const assetId = required("E2E_WORKFLOW_ASSET_ID", ASSET_ID);
  const commitSha = required("E2E_WORKFLOW_COMMIT_SHA", COMMIT_SHA);
  const entry = required("E2E_WORKFLOW_ENTRY", ENTRY);
  const email = required("E2E_LOGIN_EMAIL", LOGIN_EMAIL);
  const password = required("E2E_LOGIN_PASSWORD", LOGIN_PASSWORD);

  // Log in as the seeded operator (the default form mode is sign-in).
  await page.goto(baseURL);
  await expect(page).toHaveURL(`${baseURL}/login`);
  await page.getByLabel("Email").fill(email);
  await page.getByLabel("Password").fill(password);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(
    page.getByRole("heading", { name: "Your tenants" }),
  ).toBeVisible();

  // The seeded workflow has no deployments yet.
  expect(await readDeployments(page, baseURL, tenantId)).toHaveLength(0);

  // Navigate to the seeded workflow's detail page and drive the real picker.
  await page.goto(`${baseURL}/tenants/${tenantId}/workflows/${assetId}`);
  await expect(
    page.getByRole("heading", { name: "Launch Workflow" }),
  ).toBeVisible();

  // Asset source tree is the default kind; the asset id prefills with the
  // workflow's own id. Fill the commit and interchange.workflow entry, select
  // a resolved catalog offering, then launch.
  await expect(page.locator("#definition-asset-id")).toHaveValue(assetId);
  await page.locator("#definition-entry").fill(entry);
  await page.locator("#definition-commit").fill(commitSha);
  await selectInferenceOffering(page);

  await page.getByRole("button", { name: "Launch Workflow" }).click();

  // The deploy round-trips through the real hub and sidecar, so it is not
  // instant. The Deployments table renders a row whose status badge reads
  // "deployed" once the deploy returns — the source-ref deploy completed.
  const deploymentsTable = page
    .getByRole("heading", { name: "Deployments" })
    .locator("xpath=following-sibling::*[1]");
  await expect(deploymentsTable.getByText("deployed")).toBeVisible({
    timeout: 60_000,
  });

  // Cross-check against the hub's own view: a single deployment for this
  // definition, in the deployed state. This is the real persisted row, not a
  // stub — proof the picker deploy reached the hub route and the sidecar.
  const deployments = await readDeployments(page, baseURL, tenantId);
  const own = deployments.filter((d) => d.definitionAssetId === assetId);
  expect(own).toHaveLength(1);
  expect(own[0]?.status).toBe("deployed");
});
