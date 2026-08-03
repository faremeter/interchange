import { test, expect } from "@playwright/test";

// The global setup publishes the vite-preview base URL here after the
// stack is up; the config cannot set `use.baseURL` because it evaluates
// before the dynamic preview port is known.
const baseURL = process.env["E2E_BASE_URL"];

test("signs up and loads the authenticated dashboard", async ({ page }) => {
  if (baseURL === undefined || baseURL === "") {
    throw new Error("E2E_BASE_URL is not set; global setup did not run");
  }

  // The gated route's beforeLoad hits /api/me and, unauthenticated,
  // redirects to /login.
  await page.goto(baseURL);
  await expect(page).toHaveURL(`${baseURL}/login`);

  // The fresh database has no user, so drive the sign-up flow: toggle
  // the form into create-account mode, which reveals the name field.
  await page.getByRole("button", { name: "Sign up" }).click();

  await page.getByLabel("Name").fill("Alice Admin");
  await page.getByLabel("Email").fill("alice@example.com");
  await page.getByLabel("Password").fill("password123");

  // autoSignIn (better-auth default) establishes the session cookie on
  // sign-up. The cookie is non-Secure because the hub serves over http
  // (127.0.0.1), so it survives the same-origin preview proxy without
  // TLS — load-bearing for this harness.
  await page.getByRole("button", { name: "Create account" }).click();

  // Landing on the dashboard proves the session cookie persisted through
  // the proxy and the authed route rendered its empty state.
  await expect(
    page.getByRole("heading", { name: "Your tenants" }),
  ).toBeVisible();
  await expect(page.getByText("No running workflows.")).toBeVisible();
});
