import { test, expect, type Page } from "@playwright/test";

// The global setup publishes the vite-preview base URL here after the stack is
// up; the config cannot set `use.baseURL` because it evaluates before the
// dynamic preview port is known.
const configuredBaseURL = process.env["E2E_BASE_URL"];

const WORKFLOW_ID = "wf_e2e";
const TENANT_ID = "tnt_e2e";
const DEPLOYMENT_ID = "dep_e2e";

function requireBaseURL(): string {
  if (configuredBaseURL === undefined || configuredBaseURL === "") {
    throw new Error("E2E_BASE_URL is not set; global setup did not run");
  }
  return configuredBaseURL;
}

const workflowAsset = {
  id: WORKFLOW_ID,
  tenantId: TENANT_ID,
  kind: "workflow",
  name: "e2e-workflow",
  displayName: "E2E Workflow",
  creatorPrincipalId: null,
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
};

const deployment = {
  id: DEPLOYMENT_ID,
  tenantId: TENANT_ID,
  definitionAssetId: WORKFLOW_ID,
  status: "pending",
  createdAt: "2026-01-01T00:00:00.000Z",
};

const expectedSources = [
  {
    id: "anthropic:claude-sonnet-5",
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    credentialId: "sk-secret",
    model: "claude-sonnet-5",
  },
];

async function signUp(page: Page, email: string): Promise<void> {
  const baseURL = requireBaseURL();
  await page.goto(baseURL);
  await expect(page).toHaveURL(`${baseURL}/login`);

  await page.getByRole("button", { name: "Sign up" }).click();
  await page.getByLabel("Name").fill("Workflow Operator");
  await page.getByLabel("Email").fill(email);
  await page.getByLabel("Password").fill("password123");
  await page.getByRole("button", { name: "Create account" }).click();

  await expect(
    page.getByRole("heading", { name: "Your tenants" }),
  ).toBeVisible();
}

// Stub the workflow-detail data plane so the deploy form renders and the
// submit's POST body is captured. The admin-ui e2e harness starts no sidecar
// and the database is fresh, so a real deploy cannot complete; intercepting
// here drives the real SPA, router, and picker while asserting the exact
// request the picker builds and rendering the resulting success state.
async function stubWorkflowDetail(
  page: Page,
): Promise<{ body: () => unknown }> {
  let deployed = false;
  let captured: string | null = null;

  await page.route(`**/api/tenants/*/assets/${WORKFLOW_ID}`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(workflowAsset),
    }),
  );

  await page.route("**/api/tenants/*/workflows/deployments", async (route) => {
    if (route.request().method() === "POST") {
      captured = route.request().postData();
      deployed = true;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(deployment),
      });
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(deployed ? [deployment] : []),
    });
  });

  return {
    body: () => {
      if (captured === null) {
        throw new Error("deploy request body was not captured");
      }
      const parsed: unknown = JSON.parse(captured);
      return parsed;
    },
  };
}

async function fillInferenceSource(page: Page): Promise<void> {
  await page.locator("#source-id").fill("anthropic:claude-sonnet-5");
  await page.locator("#source-provider").fill("anthropic");
  await page.locator("#source-model").fill("claude-sonnet-5");
  await page.locator("#source-base-url").fill("https://api.anthropic.com");
  await page.locator("#source-api-key").fill("sk-secret");
}

test("deploys through the registry source picker with a pin", async ({
  page,
}) => {
  const baseURL = requireBaseURL();
  await signUp(page, "picker-registry@example.com");
  const deploy = await stubWorkflowDetail(page);

  await page.goto(`${baseURL}/tenants/${TENANT_ID}/workflows/${WORKFLOW_ID}`);
  await expect(
    page.getByRole("heading", { name: "Launch Workflow" }),
  ).toBeVisible();

  await page.locator("#definition-kind").click();
  await page.getByRole("option", { name: "Registry" }).click();

  await page.locator("#definition-entry").fill("./workflow.mjs");
  await page.locator("#definition-registry").fill("acme-registry");
  await page.locator("#definition-pin").fill("@acme/flow@^1.0.0");
  await fillInferenceSource(page);

  await page.getByRole("button", { name: "Launch Workflow" }).click();

  await expect(page.getByText(DEPLOYMENT_ID)).toBeVisible();
  expect(deploy.body()).toEqual({
    source: { kind: "registry", registry: "acme-registry" },
    entry: "./workflow.mjs",
    sources: expectedSources,
    defaultSource: "anthropic:claude-sonnet-5",
    pin: "@acme/flow@^1.0.0",
  });
});

test("deploys through the asset source picker without a pin", async ({
  page,
}) => {
  const baseURL = requireBaseURL();
  await signUp(page, "picker-source@example.com");
  const deploy = await stubWorkflowDetail(page);

  await page.goto(`${baseURL}/tenants/${TENANT_ID}/workflows/${WORKFLOW_ID}`);
  await expect(
    page.getByRole("heading", { name: "Launch Workflow" }),
  ).toBeVisible();

  // Asset source tree is the default kind, and the Asset ID prefills with the
  // workflow's own id, so only the commit, entry, and inference source need
  // filling. This variant selects its member by package name, so it sends no
  // pin.
  await page.locator("#definition-entry").fill("./workflow.mjs");
  await page.locator("#definition-commit").fill("abc123");
  await fillInferenceSource(page);

  await page.getByRole("button", { name: "Launch Workflow" }).click();

  await expect(page.getByText(DEPLOYMENT_ID)).toBeVisible();
  const body = deploy.body();
  expect(body).toEqual({
    source: {
      kind: "asset",
      assetId: WORKFLOW_ID,
      package: { format: "source", commitSha: "abc123" },
    },
    entry: "./workflow.mjs",
    sources: expectedSources,
    defaultSource: "anthropic:claude-sonnet-5",
  });
  expect(body).not.toHaveProperty("pin");
});

test("deploys through the asset tarball source picker with a pin", async ({
  page,
}) => {
  const baseURL = requireBaseURL();
  await signUp(page, "picker-tarball@example.com");
  const deploy = await stubWorkflowDetail(page);

  await page.goto(`${baseURL}/tenants/${TENANT_ID}/workflows/${WORKFLOW_ID}`);
  await expect(
    page.getByRole("heading", { name: "Launch Workflow" }),
  ).toBeVisible();

  await page.locator("#definition-kind").click();
  await page.getByRole("option", { name: "Asset tarball" }).click();

  // The tarball variant names a hub asset and selects the definition package
  // inside it by the install pin, so it sends an asset source with a `tarball`
  // format plus a pin.
  await page.locator("#definition-entry").fill("./workflow.mjs");
  await page.locator("#definition-asset-id").fill("ast_flow_tarball");
  await page.locator("#definition-pin").fill("@acme/flow@1.2.3");
  await fillInferenceSource(page);

  await page.getByRole("button", { name: "Launch Workflow" }).click();

  await expect(page.getByText(DEPLOYMENT_ID)).toBeVisible();
  expect(deploy.body()).toEqual({
    source: {
      kind: "asset",
      assetId: "ast_flow_tarball",
      package: { format: "tarball" },
    },
    entry: "./workflow.mjs",
    sources: expectedSources,
    defaultSource: "anthropic:claude-sonnet-5",
    pin: "@acme/flow@1.2.3",
  });
});
