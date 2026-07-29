import path from "path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defaultClientConditions, defineConfig } from "vite";

// ADMIN_UI_HUB_ORIGIN drives only the preview proxy (the e2e harness
// points it at a per-run hub port). The dev server proxy stays
// hardcoded so a stale env var in a dev shell cannot silently redirect
// `make dev` away from the local hub.
const hubOrigin = process.env["ADMIN_UI_HUB_ORIGIN"] ?? "http://localhost:3000";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    // Resolve @intx/* to TypeScript source via the intx-src exports
    // condition; admin-ui is bundled from source and no dist exists in
    // the repo. Setting resolve.conditions replaces vite's defaults, so
    // spread them back in to keep vite's mode-aware development/production
    // resolution for every other dependency.
    conditions: ["intx-src", ...defaultClientConditions],
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:3000",
        changeOrigin: true,
        headers: { origin: "http://localhost:3000" },
      },
      "/ws": {
        target: "http://localhost:3000",
        ws: true,
      },
    },
  },
  preview: {
    proxy: {
      "/api": {
        target: hubOrigin,
        changeOrigin: true,
        headers: { origin: hubOrigin },
      },
    },
  },
});
