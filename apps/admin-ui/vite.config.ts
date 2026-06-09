import path from "path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    // TEMPORARY UPSTREAM FIX (CL-1534): env-driven port + hub target so the
    // GTM Workbench dev script can run admin-ui alongside web (5174) and proxy
    // to our hub (:4000) instead of the hardcoded Interchange dev port (:3000).
    // Upstream this as first-class VITE_HUB_URL / port env support.
    port: Number(process.env.ADMIN_UI_PORT ?? 5175),
    proxy: {
      "/api": {
        target: process.env.VITE_HUB_URL ?? "http://localhost:4000",
        changeOrigin: true,
        headers: { origin: process.env.VITE_HUB_URL ?? "http://localhost:4000" },
      },
      "/ws": {
        target: process.env.VITE_HUB_URL ?? "http://localhost:4000",
        ws: true,
      },
    },
  },
});
