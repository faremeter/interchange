import path from "path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defaultClientConditions, defineConfig } from "vite";

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
});
