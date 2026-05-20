import { parseArgs } from "node:util";

import { setup, getLogger } from "@intx/log";

import { capabilities } from "./gemini-discover/capabilities/index.ts";

export const SCRIPT_VERSION = "1";

if (process.env.CI) {
  throw new Error("This script must not run in CI");
}

await setup({ dev: true });

const logger = getLogger(["gemini-discover"]);

const USAGE =
  "Usage: bun bin/gemini-discover.ts (--all | --only <slice> [--only <slice>]...)";

const { values } = parseArgs({
  options: {
    only: { type: "string", multiple: true },
    all: { type: "boolean", default: false },
    help: { type: "boolean", default: false },
  },
  strict: true,
});

if (values.help) {
  logger.info`${USAGE}`;
  process.exit(0);
}

const apiKey = process.env.GOOGLE_API_KEY;
if (!apiKey || apiKey.length === 0) {
  throw new Error(
    "GOOGLE_API_KEY is not set. Place it in .env at the repo root or export it before running.",
  );
}

const requested = values.only ?? [];
const wantAll = values.all;

if (!wantAll && requested.length === 0) {
  throw new Error(`No slices requested. ${USAGE}`);
}

const selected = wantAll ? Object.keys(capabilities) : requested;

for (const name of selected) {
  if (!(name in capabilities)) {
    const known = Object.keys(capabilities).join(", ");
    throw new Error(`Unknown capability "${name}". Known: ${known}`);
  }
}

for (const name of selected) {
  const capability = capabilities[name];
  if (!capability) {
    throw new Error(`Capability "${name}" disappeared from registry`);
  }
  logger.info`Running capability ${capability.name} (model=${capability.model}, endpoint=${capability.endpoint})`;
  const started = Date.now();
  await capability.build({ apiKey, scriptVersion: SCRIPT_VERSION });
  const elapsedMs = Date.now() - started;
  logger.info`Captured ${capability.name} in ${String(elapsedMs)} ms`;
}
