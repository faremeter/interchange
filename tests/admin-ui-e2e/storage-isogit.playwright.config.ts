import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./storage-isogit",
  webServer: {
    command: "bun --conditions=intx-src storage-isogit/server.ts",
    url: "http://127.0.0.1:4174",
    reuseExistingServer: false,
    timeout: 30_000,
  },
  fullyParallel: false,
  workers: 1,
  reporter: "list",
  timeout: 60_000,
  use: { baseURL: "http://127.0.0.1:4174", headless: true },
});
