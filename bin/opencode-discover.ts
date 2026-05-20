import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

import { setup, getLogger } from "@intx/log";

import { capabilities } from "./opencode-discover/capabilities/index.ts";
import { models, probeModel } from "./opencode-discover/models.ts";

export const SCRIPT_VERSION = "1";

if (process.env.CI) {
  throw new Error("This script must not run in CI");
}

await setup({ dev: true });

const logger = getLogger(["opencode-discover"]);

const USAGE =
  "Usage: bun bin/opencode-discover.ts (--all | --only <capability>...) [--model <model>...] [--probe]";

const { values } = parseArgs({
  options: {
    only: { type: "string", multiple: true },
    model: { type: "string", multiple: true },
    all: { type: "boolean", default: false },
    probe: { type: "boolean", default: false },
    help: { type: "boolean", default: false },
  },
  strict: true,
});

if (values.help) {
  logger.info`${USAGE}`;
  process.exit(0);
}

const apiKey = process.env.OPENAI_API_KEY;
if (!apiKey || apiKey.length === 0) {
  throw new Error(
    "OPENAI_API_KEY is not set. Place it in .env at the repo root or export it before running.",
  );
}
const baseUrl = process.env.OPENAI_BASE_URL;
if (!baseUrl || baseUrl.length === 0) {
  throw new Error(
    "OPENAI_BASE_URL is not set. Place it in .env at the repo root or export it before running.",
  );
}

const requestedCaps = values.only ?? [];
const requestedModels = values.model ?? [];
const wantAll = values.all;
const wantProbe = values.probe;

if (!wantAll && !wantProbe && requestedCaps.length === 0) {
  throw new Error(`No work requested. ${USAGE}`);
}

const selectedCaps = wantAll ? Object.keys(capabilities) : requestedCaps;
for (const name of selectedCaps) {
  if (!(name in capabilities)) {
    const known = Object.keys(capabilities).join(", ");
    throw new Error(`Unknown capability "${name}". Known: ${known}`);
  }
}

const knownModelIds = new Set(models.map((m) => m.id));
const selectedModelIds =
  requestedModels.length > 0 ? requestedModels : models.map((m) => m.id);
for (const id of selectedModelIds) {
  if (!knownModelIds.has(id)) {
    throw new Error(
      `Unknown model "${id}". Known: ${[...knownModelIds].join(", ")}`,
    );
  }
}

const TASK_DIR = fileURLToPath(
  new URL(
    "../dispatch/intr-78-phase-2/1a-opencode_rig/",
    new URL(import.meta.url),
  ),
);

if (wantProbe) {
  logger.info`Probing ${String(selectedModelIds.length)} model(s)`;
  for (const modelId of selectedModelIds) {
    logger.info`Probing ${modelId}`;
    const result = await probeModel({ baseUrl, apiKey, model: modelId });
    const outPath = join(TASK_DIR, `probe-${modelId}.json`);
    await writeFile(outPath, JSON.stringify(result, null, 2) + "\n");
    logger.info`Wrote ${outPath} flags=${JSON.stringify(result.flags)} reasoningField=${result.reasoningEvidence?.fieldPath ?? "none"}`;
  }
}

for (const modelId of selectedModelIds) {
  for (const name of selectedCaps) {
    const capability = capabilities[name];
    if (!capability) {
      throw new Error(`Capability "${name}" disappeared from registry`);
    }
    logger.info`Running ${capability.name} for model=${modelId}`;
    const started = Date.now();
    await capability.build({
      apiKey,
      baseUrl,
      model: modelId,
      scriptVersion: SCRIPT_VERSION,
    });
    const elapsedMs = Date.now() - started;
    logger.info`Captured ${capability.name} for model=${modelId} in ${String(elapsedMs)} ms`;
  }
}
