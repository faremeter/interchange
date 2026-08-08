// Argument parsing for bin/probe.ts, split out so the parse logic — the
// index-mutating value reader, the model-class and capability validation, the
// google-only model-class rule — is unit-testable without running the CLI's
// paid capture loop. bin/probe.ts owns only the network side; this owns the
// contract between the command line and that side.

import {
  CAPABILITIES,
  type Capability,
} from "@intx/inference-discovery/catalog";
import type { PluginCreateOptions } from "./discover-registry";

export type ModelClass = NonNullable<PluginCreateOptions["modelClass"]>;
export const MODEL_CLASSES: readonly ModelClass[] = ["text", "image"];

// google-genai is the only provider whose request shape depends on a model
// class; for every other provider a class is meaningless and passing one is a
// mistake worth rejecting rather than silently ignoring.
const MODEL_CLASS_PROVIDER = "google-genai";

export interface ProbeArgs {
  provider: string;
  model: string;
  modelClass: ModelClass | undefined;
  capabilities: readonly Capability[];
  // The user's --out value, unresolved; the CLI supplies the scratch default
  // when this is undefined.
  outDir: string | undefined;
}

export type ParsedProbeArgs =
  | { kind: "run"; args: ProbeArgs }
  | { kind: "help" }
  | { kind: "error"; message: string };

function isModelClass(value: string): value is ModelClass {
  return (MODEL_CLASSES as readonly string[]).includes(value);
}

function isCapability(value: string): value is Capability {
  return (CAPABILITIES as readonly string[]).includes(value);
}

export function parseProbeArgs(argv: readonly string[]): ParsedProbeArgs {
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
      if (value === undefined || value.startsWith("--")) return undefined;
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
  if (modelClass !== undefined && provider !== MODEL_CLASS_PROVIDER) {
    return {
      kind: "error",
      message: `--model-class applies only to ${MODEL_CLASS_PROVIDER}, not '${provider}'`,
    };
  }

  const capabilities = only.length > 0 ? only : CAPABILITIES;
  return {
    kind: "run",
    args: { provider, model, modelClass, capabilities, outDir },
  };
}
