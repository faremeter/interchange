import { defineConfig } from "@playwright/test";

// The base URL is resolved inside globalSetup (it depends on a per-run
// dynamic preview port) and published to the spec via E2E_BASE_URL.
// Setting `use.baseURL` here would evaluate before globalSetup runs, so
// the spec reads the URL from the environment at runtime instead.
export default defineConfig({
  testDir: ".",
  globalSetup: "./harness/global-setup.ts",
  fullyParallel: false,
  workers: 1,
  reporter: "list",
  timeout: 60_000,
  use: { headless: true },
});
