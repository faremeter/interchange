import { afterEach, describe, expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import type { WorkflowProbeRequestFrame } from "@intx/types/sidecar";

import {
  createWorkflowProbeExecutor,
  defaultProbeChildSpawner,
  type MaterializeWorkflowClosure,
  type MaterializedWorkflowClosure,
  type ProbeChildHandle,
  type ProbeChildSpawner,
} from "./workflow-probe-handler";

// The workflow package the fixture entry modules import
// `@intx/workflow/definition` from. The fixture lays this out under the
// package's `node_modules/` the way a materialized closure would, so the
// entry's bare-specifier import resolves inside the spawned child.
const WORKFLOW_PACKAGE_DIR = path.resolve(
  import.meta.dir,
  "../../../packages/workflow",
);

const SPAWN_TEST_TIMEOUT_MS = 30_000;

const createdDirs: string[] = [];

afterEach(async () => {
  while (createdDirs.length > 0) {
    const dir = createdDirs.pop();
    if (dir === undefined) continue;
    await fs.rm(dir, { recursive: true, force: true });
  }
});

const MAIL_TRIGGER_ENTRY = `
import { defineWorkflow } from "@intx/workflow/definition";
export default defineWorkflow({
  id: "probe-fixture",
  trigger: { type: "mail", to: "probe@example.com" },
  steps: {
    wait: { kind: "sleep", id: "", durationMs: 5 },
  },
});
`;

const THROWING_ENTRY = `throw new Error("probe fixture boom");`;

async function layoutClosureFixture(entrySource: string): Promise<string> {
  const packageDir = await fs.mkdtemp(path.join(os.tmpdir(), "wf-probe-"));
  createdDirs.push(packageDir);

  const scopeDir = path.join(packageDir, "node_modules", "@intx");
  await fs.mkdir(scopeDir, { recursive: true });
  await fs.symlink(
    WORKFLOW_PACKAGE_DIR,
    path.join(scopeDir, "workflow"),
    "dir",
  );

  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify(
      {
        name: "@fixture/probe-workflow-package",
        version: "1.0.0",
        interchange: { workflow: "./workflow.js" },
      },
      null,
      2,
    ),
  );
  await fs.writeFile(path.join(packageDir, "workflow.js"), entrySource);
  return packageDir;
}

// A host-side materializer that lays out a fixture package dir (no
// network, no real closure fetch) so the test can exercise a REAL child
// spawn + real load/evaluate/walk/project against a known workflow.
function fixtureMaterializer(entrySource: string): MaterializeWorkflowClosure {
  return async (): Promise<MaterializedWorkflowClosure> => {
    const packageDir = await layoutClosureFixture(entrySource);
    return {
      packageDir,
      async cleanup() {
        await fs.rm(packageDir, { recursive: true, force: true });
      },
    };
  };
}

// Wrap the real spawner so the test captures the spawned pids and can
// assert the child was reaped once the probe settles.
function recordingSpawner(pids: number[]): ProbeChildSpawner {
  return (args): ProbeChildHandle => {
    const handle = defaultProbeChildSpawner(args);
    pids.push(handle.pid);
    return handle;
  };
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    // ESRCH: the process is gone. EPERM: it exists but is not ours (a
    // recycled pid owned by another user) -- treat as alive to avoid a
    // false "reaped" pass.
    if (
      err !== null &&
      typeof err === "object" &&
      "code" in err &&
      err.code === "EPERM"
    ) {
      return true;
    }
    return false;
  }
}

function probeFrame(): WorkflowProbeRequestFrame {
  return {
    type: "workflow.probe.request",
    requestId: "probe-req-1",
    source: { kind: "registry", registry: "npmjs" },
    closure: { schemaVersion: "1", topLevel: [], entries: [] },
    entry: "./workflow.js",
  };
}

describe("createWorkflowProbeExecutor", () => {
  test(
    "spawns a one-shot child that returns the inert projection, grants, and hash, and reaps it on success",
    async () => {
      const pids: number[] = [];
      const executor = createWorkflowProbeExecutor({
        materialize: fixtureMaterializer(MAIL_TRIGGER_ENTRY),
        spawnProbeChild: recordingSpawner(pids),
      });

      const result = await executor.probe(probeFrame());

      expect(result.projection.id).toBe("probe-fixture");
      expect(result.projection.stepOrder).toEqual(["wait"]);
      expect(result.grants).toEqual([
        "mail.address:probe@example.com",
        "mail.send:example.com",
      ]);
      expect(result.wireHash).toMatch(/^[0-9a-f]{64}$/);

      // A real child was spawned, and it is reaped by the time the probe
      // resolves: no leaked process survives the call.
      expect(pids).toHaveLength(1);
      for (const pid of pids) {
        expect(isProcessAlive(pid)).toBe(false);
      }
    },
    SPAWN_TEST_TIMEOUT_MS,
  );

  test(
    "rejects and reaps the child when the workflow entry throws during evaluation",
    async () => {
      const pids: number[] = [];
      const executor = createWorkflowProbeExecutor({
        materialize: fixtureMaterializer(THROWING_ENTRY),
        spawnProbeChild: recordingSpawner(pids),
      });

      await expect(executor.probe(probeFrame())).rejects.toThrow(
        /workflow probe evaluation failed/,
      );

      expect(pids).toHaveLength(1);
      for (const pid of pids) {
        expect(isProcessAlive(pid)).toBe(false);
      }
    },
    SPAWN_TEST_TIMEOUT_MS,
  );

  test(
    "always calls the materializer cleanup, on both success and failure",
    async () => {
      let cleanups = 0;
      const materialize: MaterializeWorkflowClosure = async () => {
        const packageDir = await layoutClosureFixture(MAIL_TRIGGER_ENTRY);
        return {
          packageDir,
          async cleanup() {
            cleanups += 1;
            await fs.rm(packageDir, { recursive: true, force: true });
          },
        };
      };

      const executor = createWorkflowProbeExecutor({
        materialize,
        spawnProbeChild: recordingSpawner([]),
      });

      await executor.probe(probeFrame());
      expect(cleanups).toBe(1);
    },
    SPAWN_TEST_TIMEOUT_MS,
  );
});
