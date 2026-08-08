#!/usr/bin/env bun
/* eslint-disable no-console */

// Exploratory capability probe for a single model that need not be in the
// support matrix yet. Where bin/discover.ts replays the curated matrix,
// bin/probe.ts sweeps a model the matrix does not list — the first step of
// bringing a new model in. For each capability the provider can build, it makes
// one live call and records the HTTP outcome it alone can see (the capture
// format does not persist the status line): unsupported when the provider
// cannot build the request at all, http-error on a non-2xx, or a 2xx capture
// written to a scratch tree for bin/classify-sessions.ts to classify offline.
//
// Captures land under a scratch directory, never the committed sessions/ tree:
// baking a model is a deliberate copy of the cells that classify as captured,
// plus their matrix rows, so the corpus stays curated by construction.

import { rmSync } from "node:fs";
import { resolve } from "node:path";
import {
  CAPABILITIES,
  CapabilityNotBuildableError,
  INTENTS,
  type Capability,
} from "@intx/inference-discovery/catalog";
import {
  assertNotCI,
  requireEnvSet,
  runCapture,
} from "@intx/inference-discovery";
import {
  PLUGIN_REGISTRY,
  findPlugin,
  type PluginCreateOptions,
} from "./lib/discover-registry";

const ROOT = resolve(import.meta.dirname, "..");

type ModelClass = NonNullable<PluginCreateOptions["modelClass"]>;
const MODEL_CLASSES: readonly ModelClass[] = ["text", "image"];

function isModelClass(value: string): value is ModelClass {
  return (MODEL_CLASSES as readonly string[]).includes(value);
}

function isCapability(value: string): value is Capability {
  return (CAPABILITIES as readonly string[]).includes(value);
}

interface ProbeArgs {
  provider: string;
  model: string;
  modelClass: ModelClass | undefined;
  capabilities: readonly Capability[];
  outDir: string;
}

type ParsedArgs =
  | { kind: "run"; args: ProbeArgs }
  | { kind: "help" }
  | { kind: "error"; message: string };

function buildHelpText(): string {
  const providers = PLUGIN_REGISTRY.map((entry) => {
    const envList = entry.requiredEnv.join(", ");
    return `  ${entry.name}\n    requires env: ${envList}`;
  }).join("\n");

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
  --model-class <class>   google-genai only: text (default) or image. Sets
                          the request shape for a model absent from the known
                          sets. An image model must set this.
  --only <capability>     Restrict to this capability. Repeatable. Defaults
                          to every capability the provider can build.
  --out <dir>             Scratch output directory. Defaults to
                          tmp/probe/<provider>/<model>/.
  --help, -h              Show this message.

Available providers:
${providers}

CI guard:
  Probing makes real, paid network calls and must never run in CI. If the
  CI environment variable is set, the command aborts before any call.
`;
}

function parseArgs(argv: readonly string[]): ParsedArgs {
  let provider: string | undefined;
  let model: string | undefined;
  let modelClass: ModelClass | undefined;
  let outDir: string | undefined;
  const only: Capability[] = [];

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--help" || arg === "-h") return { kind: "help" };
    const takeValue = (): string | undefined => {
      const value = argv[i + 1];
      if (value === undefined || value.startsWith("--")) {
        return undefined;
      }
      i++;
      return value;
    };
    switch (arg) {
      case "--provider": {
        const value = takeValue();
        if (value === undefined)
          return { kind: "error", message: `${arg} needs a value` };
        provider = value;
        break;
      }
      case "--model": {
        const value = takeValue();
        if (value === undefined)
          return { kind: "error", message: `${arg} needs a value` };
        model = value;
        break;
      }
      case "--model-class": {
        const value = takeValue();
        if (value === undefined || !isModelClass(value)) {
          return {
            kind: "error",
            message: `--model-class must be one of: ${MODEL_CLASSES.join(", ")}`,
          };
        }
        modelClass = value;
        break;
      }
      case "--out": {
        const value = takeValue();
        if (value === undefined)
          return { kind: "error", message: `${arg} needs a value` };
        outDir = value;
        break;
      }
      case "--only": {
        const value = takeValue();
        if (value === undefined)
          return { kind: "error", message: `${arg} needs a value` };
        if (!isCapability(value)) {
          return { kind: "error", message: `unknown capability '${value}'` };
        }
        only.push(value);
        break;
      }
      default:
        return { kind: "error", message: `unknown argument '${String(arg)}'` };
    }
  }

  if (provider === undefined)
    return { kind: "error", message: "--provider is required" };
  if (model === undefined)
    return { kind: "error", message: "--model is required" };

  const capabilities = only.length > 0 ? only : CAPABILITIES;
  const out = outDir ?? resolve(ROOT, "tmp", "probe", provider, model);

  return {
    kind: "run",
    args: { provider, model, modelClass, capabilities, outDir: out },
  };
}

type CellVerdict =
  | { kind: "captured-2xx"; status: number }
  | { kind: "http-error"; status: number }
  | { kind: "unsupported" }
  | { kind: "call-failed"; message: string };

async function probeCapability(
  args: ProbeArgs,
  plugin: Parameters<typeof runCapture>[0]["plugin"],
  capability: Capability,
): Promise<CellVerdict> {
  const outDir = resolve(args.outDir, capability);
  // runCapture never cleans its output directory, so a re-probe would leave a
  // prior run's exchanges beside the new ones. Clear the cell first.
  rmSync(outDir, { recursive: true, force: true });
  try {
    const result = await runCapture({
      plugin,
      model: args.model,
      capability,
      intent: INTENTS[capability],
      outDir,
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
  const parsed = parseArgs(process.argv.slice(2));

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

  const classLabel =
    args.provider === "google-genai"
      ? ` model-class=${args.modelClass ?? "text (default)"}`
      : "";
  console.error(
    `[probe] provider=${args.provider} model=${args.model}${classLabel} capabilities=${String(args.capabilities.length)} out=${args.outDir}`,
  );

  const counts = { captured: 0, httpError: 0, unsupported: 0, failed: 0 };
  for (const capability of args.capabilities) {
    const verdict = await probeCapability(args, plugin, capability);
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
    `[probe] classify the captures: bin/classify-sessions --dir ${args.outDir}`,
  );
  return 0;
}

const exitCode = await main();
process.exit(exitCode);
