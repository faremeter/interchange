import { expect, test } from "bun:test";
import path from "node:path";

test("browser entry bundles without Node filesystem or path modules", async () => {
  const result = await Bun.build({
    entrypoints: [path.join(import.meta.dir, "browser.ts")],
    target: "browser",
    format: "esm",
    conditions: ["intx-src"],
  });
  if (!result.success) {
    throw new Error(
      result.logs
        .map((log) => (log instanceof Error ? log.message : String(log)))
        .join("\n"),
    );
  }
  const output = result.outputs[0];
  if (output === undefined) {
    throw new Error("Browser storage bundle produced no output");
  }
  const bundle = await output.text();
  expect(bundle).not.toContain('from "node:fs"');
  expect(bundle).not.toContain('from "node:path"');
});
