export interface ParsedCLIRun {
  kind: "run";
  provider: string;
  models: readonly string[];
  capabilities: readonly string[];
  all: boolean;
}

export interface ParsedCLIHelp {
  kind: "help";
  message: string;
}

export interface ParsedCLIError {
  kind: "error";
  message: string;
}

export type ParsedCLI = ParsedCLIRun | ParsedCLIHelp | ParsedCLIError;

export const HELP_TEXT = `Usage: discover --provider <name> [options]

Options:
  --provider <name>     Required. Selects the provider plug-in to invoke.
  --model <name>        Restrict to this model. Repeatable.
  --only <capability>   Restrict to this capability. Repeatable.
  --all                 Run every supported model x capability combination.
                        Mutually exclusive with --model and --only.
  --help, -h            Show this message.

When --all is omitted and no --model/--only flags are given, the runner
will fail; pass --all explicitly or narrow the scope with --model/--only.
`;

function takeValue(
  argv: readonly string[],
  index: number,
  flag: string,
): { value: string; nextIndex: number } | { error: string } {
  const next = argv[index + 1];
  if (next === undefined || next.startsWith("-")) {
    return { error: `Flag ${flag} requires a value` };
  }
  return { value: next, nextIndex: index + 2 };
}

export function parseCLI(argv: readonly string[]): ParsedCLI {
  let provider: string | undefined;
  const models: string[] = [];
  const capabilities: string[] = [];
  let all = false;

  let i = 0;
  while (i < argv.length) {
    const arg = argv[i];
    if (arg === undefined) {
      i++;
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      return { kind: "help", message: HELP_TEXT };
    }
    if (arg === "--all") {
      all = true;
      i++;
      continue;
    }
    if (arg === "--provider") {
      const result = takeValue(argv, i, "--provider");
      if ("error" in result) {
        return { kind: "error", message: result.error };
      }
      if (provider !== undefined) {
        return {
          kind: "error",
          message: "--provider may only be specified once",
        };
      }
      provider = result.value;
      i = result.nextIndex;
      continue;
    }
    if (arg === "--model") {
      const result = takeValue(argv, i, "--model");
      if ("error" in result) {
        return { kind: "error", message: result.error };
      }
      models.push(result.value);
      i = result.nextIndex;
      continue;
    }
    if (arg === "--only") {
      const result = takeValue(argv, i, "--only");
      if ("error" in result) {
        return { kind: "error", message: result.error };
      }
      capabilities.push(result.value);
      i = result.nextIndex;
      continue;
    }
    return { kind: "error", message: `Unknown argument: ${arg}` };
  }

  if (provider === undefined) {
    return {
      kind: "error",
      message: "--provider <name> is required",
    };
  }

  if (all && (models.length > 0 || capabilities.length > 0)) {
    return {
      kind: "error",
      message: "--all is mutually exclusive with --model and --only",
    };
  }

  if (!all && models.length === 0 && capabilities.length === 0) {
    return {
      kind: "error",
      message:
        "Specify --all or narrow the scope with --model/--only (at least one of either)",
    };
  }

  return {
    kind: "run",
    provider,
    models,
    capabilities,
    all,
  };
}
