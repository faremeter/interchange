// workflow-quickstart: run the workflow.
//
// `runLocal` is the in-process host. It supplies the whole
// `WorkflowRuntimeEnv` -- an in-memory event log, blob substrate,
// scheduler and signal channel -- and drives the same runtime body the
// deployed sidecar drives. What a caller has to supply is what the
// definition left as a string or a declaration:
//
//   authorize       -- required, no default. The deployed authorize
//                      refuses to answer when it cannot resolve a
//                      decision; the local surface refuses to invent one.
//   loopFns         -- what the deployed host loads from
//                      `interchange.loops`, resolved by export name.
//   actionResolver  -- what it loads from `interchange.actions`.
//   invokeStep      -- what turns a step's `AgentDefinition` into a
//                      running agent. Without it `runLocal` stubs every
//                      step and no tool is ever called.

import { mkdirSync } from "node:fs";
import { join } from "node:path";

import {
  defaultContextDir,
  optional,
  resolveAgentSource,
  resolveStdio,
  type SingleSourceMainOptions,
} from "@intx/example-agent-common";
import {
  runLocal,
  type ActionHandler,
  type LoopFn,
  type LoopFnRegistry,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";

import { publishTagline } from "./actions";
import { nextPass, stillTooLong } from "./loops";
import { createAgentStepInvoker } from "./step-invoker";
import { workflow } from "./workflow";

const EXAMPLE_NAME = "workflow-quickstart";

/** The tagline length the loop drives towards. */
export const MAX_WORDS = 6;

// Every `runLocal` call site declares its own authorize; the package
// deliberately ships no permit-everything helper, because "what may this
// run do" is the caller's decision and a shared default would make an
// authorization failure invisible until the workflow was deployed. An
// example that runs against a local in-memory substrate can say yes to
// everything; a real deployment resolves grants instead.
const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

// The two resolvers the deployed host builds by importing this package's
// `interchange.loops` and `interchange.actions` modules and looking up the
// ref as an export name. In-process there is no module to import, so the
// mapping is written out. Both fail loudly on an unknown ref, exactly as
// the loaders do -- a silently-missing loop predicate would run the loop
// to its cap and report exhaustion.
const loopFns: LoopFnRegistry = (ref: string): LoopFn => {
  if (ref === "stillTooLong") return stillTooLong;
  if (ref === "nextPass") return nextPass;
  throw new Error(`no loop fn exported as ${ref}`);
};

const actionResolver = (ref: string): ActionHandler => {
  if (ref === "publishTagline") return publishTagline;
  throw new Error(`no action handler exported as ${ref}`);
};

export type MainOptions = SingleSourceMainOptions & {
  /** Override where the `publish` action writes the accepted tagline. */
  outputPath?: string;
};

export async function main(
  argv: string[],
  env: NodeJS.ProcessEnv,
  opts: MainOptions = {},
): Promise<number> {
  const { stdout, stderr } = resolveStdio(opts);

  const tagline = argv.join(" ").trim();
  if (tagline === "") {
    stderr("usage: bun run start <tagline>\n");
    return 1;
  }

  const resolved = resolveAgentSource(opts, env, EXAMPLE_NAME, stderr);
  if (resolved === null) return 1;
  const { source, material } = resolved;

  const contextDir = opts.contextDir ?? defaultContextDir(EXAMPLE_NAME);
  mkdirSync(contextDir, { recursive: true });
  const outputPath = opts.outputPath ?? join(contextDir, "tagline.txt");

  const run = runLocal(workflow, {
    authorize: allowAll,
    // A manual workflow is launched with a payload. This one is the
    // loop's seed `RevisionPass`, and it is how `outputPath` -- a fact
    // about this machine, not about the workflow -- reaches the action
    // handler that needs it without being baked into the definition.
    triggerPayload: { tagline, maxWords: MAX_WORDS, outputPath },
    loopFns,
    actionResolver,
    invokeStep: createAgentStepInvoker({
      source,
      material,
      contextDir,
      authorize: allowAll,
      ...optional("deps", opts.deps),
    }),
  });

  const result = await run.complete;

  stdout(`run ${result.runId} ${result.terminalStatus}\n`);
  for (const stepId of workflow.stepOrder) {
    if (!(stepId in result.outputs)) continue;
    stdout(`  ${stepId}: ${JSON.stringify(result.outputs[stepId])}\n`);
  }

  if (result.terminalStatus !== "completed") {
    stderr(`workflow did not complete: ${result.terminalStatus}\n`);
    return 1;
  }
  return 0;
}

if (import.meta.main) {
  const code = await main(process.argv.slice(2), process.env);
  if (code !== 0) process.exit(code);
}
