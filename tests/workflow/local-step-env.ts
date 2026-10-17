// Shared setup for tests that run a definition with `runLocal` and the real
// step invoker against the `@intx/inference-testing` harness.

import { join } from "node:path";

import { createDefaultDirectorRegistry, type BaseEnv } from "@intx/agent";
import { noopAuditStore } from "@intx/agent/testing";
import type { Harness } from "@intx/inference-testing";
import { createIsogitStore } from "@intx/storage-isogit/node";
import type { InferenceSource } from "@intx/types/runtime-core";
import type { RunResult, WorkflowRun } from "@intx/workflow";

/**
 * The base env of one local step: isogit context storage and a workdir under
 * `baseDir/<name>`, the source, the default directors, and the harness's
 * fetch. Each step that runs at the same time needs its own `name`, because
 * the agent holds a process-wide lock on its workdir.
 */
export async function localStepEnvBase(args: {
  baseDir: string;
  name: string;
  source: InferenceSource;
  harness: Harness;
}): Promise<Omit<BaseEnv, "authorize">> {
  return {
    sources: [args.source],
    defaultSource: args.source.id,
    storage: await createIsogitStore(join(args.baseDir, args.name, "ctx")),
    workdir: join(args.baseDir, args.name, "workspace"),
    audit: noopAuditStore(),
    directors: createDefaultDirectorRegistry(),
    readCurrentMaterial: () => ({ secret: "test-secret" }),
    deps: args.harness.deps,
  };
}

/**
 * Drive the harness until `run` settles. `agent.send` parks until the
 * harness serves the response stream on its virtual clock, so the harness
 * has to keep running while the run is open.
 */
export async function driveUntilSettled(
  harness: Harness,
  run: WorkflowRun,
): Promise<RunResult> {
  let settled = false;
  const complete = run.complete.finally(() => {
    settled = true;
  });
  while (!settled) {
    await harness.run({ wallClockBudgetMs: 2000 });
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  return complete;
}
