import path from "node:path";
import { fileURLToPath } from "node:url";

const HOST = "127.0.0.1";
const PORT = 4174;
const HERE = path.dirname(fileURLToPath(import.meta.url));

const build = await Bun.build({
  entrypoints: [path.join(HERE, "harness.ts")],
  target: "browser",
  format: "esm",
  conditions: ["intx-src"],
});
if (!build.success) {
  throw new Error(
    build.logs
      .map((log) => (log instanceof Error ? log.message : String(log)))
      .join("\n"),
  );
}
const output = build.outputs[0];
if (output === undefined) {
  throw new Error("browser storage harness build produced no output");
}
const bundle = await output.text();

Bun.serve({
  hostname: HOST,
  port: PORT,
  fetch(request) {
    const pathname = new URL(request.url).pathname;
    if (pathname === "/harness.js") {
      return new Response(bundle, {
        headers: { "content-type": "text/javascript" },
      });
    }
    if (pathname === "/") {
      return new Response(
        '<!doctype html><html><body><script type="module" src="/harness.js"></script></body></html>',
        { headers: { "content-type": "text/html" } },
      );
    }
    return new Response(null, { status: 404 });
  },
});
