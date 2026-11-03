import fs from "node:fs/promises";
import path from "node:path";

import {
  hashGrants,
  isErrnoNotFound,
  type CredentialsSnapshot,
} from "./credentials";
import type { WorkflowSupervisorBindings } from "./types";

function stepGrantsPath(baseDir: string, stepId: string): string {
  return path.join(baseDir, "seeded-step-grants", `${stepId}.json`);
}

/** Give a step the grants `seededSnapshot` then reads for it. */
export async function seedStepGrants(
  baseDir: string,
  stepId: string,
  grants: unknown[],
): Promise<void> {
  const file = stepGrantsPath(baseDir, stepId);
  await fs.mkdir(path.dirname(file), { recursive: true });
  await fs.writeFile(file, JSON.stringify(grants));
}

async function readSeededGrants(
  baseDir: string,
  stepId: string,
): Promise<readonly unknown[]> {
  let raw: string;
  try {
    raw = await fs.readFile(stepGrantsPath(baseDir, stepId), "utf8");
  } catch (cause) {
    if (isErrnoNotFound(cause)) return [];
    throw cause;
  }
  const grants: unknown = JSON.parse(raw);
  if (!Array.isArray(grants)) {
    throw new Error(`seeded grants of ${stepId} are not an array`);
  }
  return grants;
}

/** The snapshot of the grants a test seeded, none for an unseeded step. */
export async function seededSnapshot(
  baseDir: string,
  opts: {
    stepOrder: readonly string[];
  },
): Promise<CredentialsSnapshot> {
  const steps = [];
  for (const stepId of opts.stepOrder) {
    const grants = await readSeededGrants(baseDir, stepId);
    steps.push({
      stepId,
      grants,
      contentHash: await hashGrants(grants),
    });
  }
  return { steps };
}

/** A per-run grants source over the grants a test seeded. */
export function seededOnRunStart(
  baseDir: string,
): WorkflowSupervisorBindings["onRunStart"] {
  return ({ stepIds }) => seededSnapshot(baseDir, { stepOrder: stepIds });
}

/** A per-run grants source that gives every step no grants. */
export const noGrantsOnRunStart: WorkflowSupervisorBindings["onRunStart"] =
  async ({ stepIds }) => {
    const contentHash = await hashGrants([]);
    return {
      steps: stepIds.map((stepId) => ({
        stepId,
        grants: [],
        contentHash,
      })),
    };
  };
