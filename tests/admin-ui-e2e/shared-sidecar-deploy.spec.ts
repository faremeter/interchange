import fs from "node:fs";
import path from "node:path";

import { test, expect, type Page } from "@playwright/test";

// Runs when the harness hub shares sidecars by tenant:
//
//   E2E_SHARE_SIDECARS=tenant bunx playwright test shared-sidecar-deploy
//
// Two deployments of one tenant must run on a single sidecar process: the
// provisioner starts it for the first probe and answers every later ensure
// with that sidecar, and the Hub routes both deployments over its connection.
const SHARED = process.env["E2E_SHARE_SIDECARS"] === "tenant";
const BASE_URL = process.env["E2E_BASE_URL"];
const HUB_DATA_DIR = process.env["E2E_HUB_DATA_DIR"];
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

async function readDeployments(
  page: Page,
  baseURL: string,
  tenantId: string,
): Promise<{ id: string; status: string }[]> {
  const res = await page.request.get(
    `${baseURL}/api/tenants/${tenantId}/workflows/deployments`,
  );
  expect(res.ok()).toBeTruthy();
  const body: unknown = await res.json();
  if (!Array.isArray(body)) {
    throw new Error(`deployments list was not an array: ${String(body)}`);
  }
  return body.map((entry: unknown) => {
    if (
      typeof entry !== "object" ||
      entry === null ||
      !("id" in entry) ||
      !("status" in entry) ||
      typeof entry.id !== "string" ||
      typeof entry.status !== "string"
    ) {
      throw new Error(`malformed deployment: ${JSON.stringify(entry)}`);
    }
    return { id: entry.id, status: entry.status };
  });
}

async function readLiveness(
  page: Page,
  baseURL: string,
  tenantId: string,
  runId: string,
): Promise<unknown> {
  const res = await page.request.get(
    `${baseURL}/api/tenants/${tenantId}/workflows/runs/${runId}/health`,
  );
  expect(res.ok()).toBeTruthy();
  const body: unknown = await res.json();
  return typeof body === "object" && body !== null && "liveness" in body
    ? body.liveness
    : undefined;
}

test("two deployments of one tenant share a sidecar process", async ({
  page,
}) => {
  test.skip(!SHARED, "requires E2E_SHARE_SIDECARS=tenant");
  const baseURL = required("E2E_BASE_URL", BASE_URL);
  const hubDataDir = required("E2E_HUB_DATA_DIR", HUB_DATA_DIR);
  const tenantId = required("E2E_WORKFLOW_TENANT_ID", TENANT_ID);
  const assetId = required("E2E_WORKFLOW_ASSET_ID", ASSET_ID);

  await page.goto(baseURL);
  await page.getByLabel("Email").fill(required("E2E_LOGIN_EMAIL", LOGIN_EMAIL));
  await page
    .getByLabel("Password")
    .fill(required("E2E_LOGIN_PASSWORD", LOGIN_PASSWORD));
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(
    page.getByRole("heading", { name: "Your tenants" }),
  ).toBeVisible();

  await page.goto(`${baseURL}/tenants/${tenantId}/workflows/${assetId}`);
  await page
    .locator("#definition-entry")
    .fill(required("E2E_WORKFLOW_ENTRY", ENTRY));
  await page
    .locator("#definition-commit")
    .fill(required("E2E_WORKFLOW_COMMIT_SHA", COMMIT_SHA));
  await page.locator("#source-offering").click();
  await page
    .getByRole("option", { name: "Claude Sonnet 5 — Anthropic" })
    .click();

  // Deploy through the picker, then send the identical request again so the
  // second deployment differs from the first only in its allocation.
  const firstRequest = page.waitForRequest(
    (request) =>
      request.method() === "POST" &&
      request.url().endsWith(`/tenants/${tenantId}/workflows/deployments`),
  );
  await page.getByRole("button", { name: "Launch Workflow" }).click();
  const body: unknown = JSON.parse((await firstRequest).postData() ?? "null");
  const second = await page.request.post(
    `${baseURL}/api/tenants/${tenantId}/workflows/deployments`,
    { data: body },
  );
  expect(second.ok(), await second.text()).toBeTruthy();

  await expect
    .poll(
      async () =>
        (await readDeployments(page, baseURL, tenantId)).map(
          (deployment) => deployment.status,
        ),
      { timeout: 60_000 },
    )
    .toEqual(["deployed", "deployed"]);

  const deployments = await readDeployments(page, baseURL, tenantId);
  for (const deployment of deployments) {
    await expect
      .poll(() => readLiveness(page, baseURL, tenantId, deployment.id), {
        timeout: 60_000,
      })
      .toBe("ok");
  }
  expect(fs.readdirSync(path.join(hubDataDir, "local-sidecars"))).toHaveLength(
    1,
  );
});
