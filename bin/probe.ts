#!/usr/bin/env bun
/* eslint-disable no-console */

// Exploratory capability probe for a single model not yet in the support
// matrix. Where bin/discover.ts replays the curated matrix, probe sweeps a new
// model: one live call per capability the provider can build, recording
// unsupported (cannot build), http-error (non-2xx), or a 2xx capture under a
// scratch tree for bin/classify-sessions.ts to classify offline. Captures never
// land in the committed sessions/ tree; baking a model copies the captured
// cells deliberately.

import { rmSync } from "node:fs";
import { resolve } from "node:path";
import {
  CapabilityNotBuildableError,
  INTENTS,
  type Capability,
} from "@intx/inference-discovery/catalog";
import {
  assertNotCI,
  requireEnvSet,
  runCapture,
  type ProviderPlugin,
} from "@intx/inference-discovery";
import {
  PLUGIN_REGISTRY,
  findPlugin,
  formatProviderHelp,
} from "./lib/discover-registry";
import { isKnownModel } from "@intx/inference-discovery-google-genai";
import { parseProbeArgs, MODEL_CLASSES } from "./lib/probe-args";

const ROOT = resolve(import.meta.dirname, "..");

function buildHelpText(): string {
  return `Usage: bin/probe --provider <name> --model <name> [options]

Sweeps a single model across the capabilities its provider can build,
making one live call per capability. Captures land under a scratch tree
(default tmp/probe/<provider>/<model>/); classify them with
bin/classify-sessions. Nothing is written to the committed sessions/
tree — baking an accepted model is a separate, deliberate step.

Options:
  --provider <name>       Required. Selects the provider plug-in.
  --model <name>          Required. The model to probe; need not be in the
                          support matrix.
  --model-class <class>   google-genai only: ${MODEL_CLASSES.join(" or ")}
                          (default text). Sets the request shape for a model
                          absent from the known sets. An image model must set
                          this.
  --only <capability>     Restrict to this capability. Repeatable. Defaults
                          to every capability the provider can build.
  --out <dir>             Scratch output directory. Defaults to
                          tmp/probe/<provider>/<model>/.
  --help, -h              Show this message.

Available providers:
${formatProviderHelp()}

CI guard:
  Probing makes real, paid network calls and must never run in CI. If the
  CI environment variable is set, the command aborts before any call.
`;
}

type CellVerdict =
  | { kind: "captured-2xx"; status: number }
  | { kind: "http-error"; status: number }
  | { kind: "unsupported" }
  | { kind: "call-failed"; message: string };

async function probeCapability(
  plugin: ProviderPlugin,
  model: string,
  cellDir: string,
  capability: Capability,
): Promise<CellVerdict> {
  // runCapture never cleans its output directory, so a re-probe would leave a
  // prior run's exchanges beside the new ones. Clear the cell first.
  rmSync(cellDir, { recursive: true, force: true });
  try {
    const result = await runCapture({
      plugin,
      model,
      capability,
      intent: INTENTS[capability],
      outDir: cellDir,
    });
    const ok = result.finalStatus >= 200 && result.finalStatus < 300;
    return ok
      ? { kind: "captured-2xx", status: result.finalStatus }
      : { kind: "http-error", status: result.finalStatus };
  } catch (error) {
    if (error instanceof CapabilityNotBuildableError) {
      return { kind: "unsupported" };
    }
    return {
      kind: "call-failed",
      message: error instanceof Error ? error.message : String(error),
    };
  }
}

function formatVerdict(verdict: CellVerdict): string {
  switch (verdict.kind) {
    case "captured-2xx":
      return `captured (HTTP ${String(verdict.status)}) — classify to confirm`;
    case "http-error":
      return `http-error (HTTP ${String(verdict.status)})`;
    case "unsupported":
      return "unsupported (provider cannot build this capability)";
    case "call-failed":
      return `call-failed: ${verdict.message}`;
  }
}

async function main(): Promise<number> {
  const parsed = parseProbeArgs(process.argv.slice(2));

  if (parsed.kind === "help") {
    console.log(buildHelpText());
    return 0;
  }
  if (parsed.kind === "error") {
    console.error(`error: ${parsed.message}`);
    console.error("");
    console.error("Run with --help for usage.");
    return 1;
  }

  assertNotCI();

  const { args } = parsed;
  const registered = findPlugin(args.provider);
  if (registered === undefined) {
    const known = PLUGIN_REGISTRY.map((entry) => entry.name).join(", ");
    console.error(`error: unknown provider '${args.provider}'`);
    console.error(`available providers: ${known}`);
    return 1;
  }

  const env = requireEnvSet(registered.requiredEnv);
  const plugin = registered.create(env, { modelClass: args.modelClass });
  const outDir =
    args.outDir !== undefined
      ? resolve(ROOT, args.outDir)
      : resolve(ROOT, "tmp", "probe", args.provider, args.model);

  // For google-genai the label must distinguish a declared class, a class known
  // from model-set membership, and a guessed class for an unknown model — a
  // deliberate default (probing must not require pre-registering a model), but
  // a wrong text guess on an image model would report image-output unsupported.
  let classLabel = "";
  if (args.provider === "google-genai") {
    if (args.modelClass !== undefined) {
      classLabel = ` model-class=${args.modelClass} (declared)`;
    } else if (isKnownModel(args.model)) {
      classLabel = " model-class=auto (from known model)";
    } else {
      classLabel = ` model-class=text (ASSUMED — ${args.model} unknown to google-genai; pass --model-class to confirm)`;
    }
  }
  console.error(
    `[probe] provider=${args.provider} model=${args.model}${classLabel} capabilities=${String(args.capabilities.length)} out=${outDir}`,
  );

  const counts = { captured: 0, httpError: 0, unsupported: 0, failed: 0 };
  for (const capability of args.capabilities) {
    const verdict = await probeCapability(
      plugin,
      args.model,
      resolve(outDir, capability),
      capability,
    );
    console.error(`[probe] ${capability.padEnd(38)} ${formatVerdict(verdict)}`);
    if (verdict.kind === "captured-2xx") counts.captured++;
    else if (verdict.kind === "http-error") counts.httpError++;
    else if (verdict.kind === "unsupported") counts.unsupported++;
    else counts.failed++;
  }

  console.error(
    `[probe] done captured=${String(counts.captured)} http-error=${String(counts.httpError)} unsupported=${String(counts.unsupported)} call-failed=${String(counts.failed)}`,
  );
  console.error(
    `[probe] classify the captures: bin/classify-sessions --dir ${outDir}`,
  );
  // A call-failed cell is a bug, not a discovery outcome; surface it in the
  // exit code. http-error and unsupported are expected results.
  return counts.failed > 0 ? 1 : 0;
}

const exitCode = await main();
process.exit(exitCode);
