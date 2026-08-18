import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, test, expect } from "bun:test";

import { type } from "arktype";
import { hashDefinition } from "@intx/workflow";
import { loadWorkflowDefinitionFromClosure } from "@intx/workflow-host";
import { PackageJSON } from "@intx/types/package-json";

import {
  buildWorkflowFixture,
  buildWorkflowCodebaseTree,
  WORKFLOW_FIXTURE_ENTRY,
  WORKFLOW_FIXTURE_ENTRY_FILE,
  WORKFLOW_FIXTURE_PACKAGE_JSON_FILE,
  WORKFLOW_FIXTURE_SIGNAL_NAME,
  WORKFLOW_RUN_GRANT_ACTION,
  WORKFLOW_RUN_GRANT_RESOURCE,
} from "./workflow-fixture";

describe("workflow fixture", () => {
  test("defineWorkflow yields the draft -> approve -> publish DAG", () => {
    const def = buildWorkflowFixture();
    expect(def.stepOrder).toEqual(["draft", "approval", "publish"]);

    const draft = def.steps["draft"];
    const approval = def.steps["approval"];
    const publish = def.steps["publish"];

    expect(draft?.kind).toBe("step");
    expect(approval?.kind).toBe("awaitSignal");
    expect(publish?.kind).toBe("step");

    if (approval?.kind !== "awaitSignal") throw new Error("unreachable");
    expect(approval.name).toBe(WORKFLOW_FIXTURE_SIGNAL_NAME);
    expect(approval.after).toEqual(["draft"]);
    expect(publish?.after).toEqual(["approval"]);

    // publish must consume the drafted content, not the approval node's
    // signal payload. The default-input convention would wire it to
    // `steps.approval.output` (the signal payload, `null` for a bare
    // approval); the fixture overrides that to read the draft.
    if (publish?.kind !== "step") throw new Error("unreachable");
    expect(publish.input).toEqual({ from: "steps.draft.output" });
  });

  test("step-agents are authored inline with no catalog reference", () => {
    const def = buildWorkflowFixture();
    const draft = def.steps["draft"];
    const publish = def.steps["publish"];
    if (draft?.kind !== "step" || publish?.kind !== "step") {
      throw new Error("unreachable");
    }
    expect(draft.agent.id).toBe("draft-agent");
    expect(draft.agent.systemPrompt.length).toBeGreaterThan(0);
    expect(draft.agent.inference.sources[0]?.provider).toBe("anthropic");
    expect(publish.agent.id).toBe("publish-agent");
    expect(publish.agent.systemPrompt.length).toBeGreaterThan(0);
  });

  test("hashDefinition produces a deterministic content hash", () => {
    const a = hashDefinition(buildWorkflowFixture());
    const b = hashDefinition(buildWorkflowFixture());
    expect(a.length).toBeGreaterThan(0);
    expect(a).toEqual(b);
  });

  test("the codebase package.json validates and declares the workflow entry", async () => {
    const tree = await buildWorkflowCodebaseTree();
    const manifest = tree.files[WORKFLOW_FIXTURE_PACKAGE_JSON_FILE];
    if (manifest === undefined) {
      throw new Error("codebase tree is missing its package.json");
    }
    const parsed: unknown = JSON.parse(manifest);
    const pkg = PackageJSON(parsed);
    expect(pkg instanceof type.errors).toBe(false);
    if (pkg instanceof type.errors) throw new Error("unreachable");
    expect(pkg.interchange?.workflow).toBe(WORKFLOW_FIXTURE_ENTRY);
  });

  test("the bundled entry is self-contained with no bare @intx import", async () => {
    const tree = await buildWorkflowCodebaseTree();
    const entry = tree.files[WORKFLOW_FIXTURE_ENTRY_FILE];
    if (entry === undefined) {
      throw new Error("codebase tree is missing its bundled entry");
    }
    expect(entry.includes("@intx/")).toBe(false);
  });

  test("the codebase tree loads via loadWorkflowDefinitionFromClosure", async () => {
    const tree = await buildWorkflowCodebaseTree();
    const packageDir = await mkdtemp(join(tmpdir(), "approval-flow-fixture-"));
    try {
      for (const [name, content] of Object.entries(tree.files)) {
        await writeFile(join(packageDir, name), content, "utf-8");
      }
      const loaded = await loadWorkflowDefinitionFromClosure({ packageDir });
      const expected = buildWorkflowFixture();
      expect(loaded.id).toBe(expected.id);
      expect(loaded.stepOrder).toEqual(expected.stepOrder);
    } finally {
      await rm(packageDir, { recursive: true, force: true });
    }
  });

  test("the planted signal grant matches the route's resource gate", () => {
    // The signal route resolves `idResource("workflow-run","deploymentId")`
    // to `workflow-run:<deploymentId>`; the planted wildcard resource must
    // glob-match any concrete deployment id, and the verb must be `manage`.
    expect(WORKFLOW_RUN_GRANT_RESOURCE.endsWith(":*")).toBe(true);
    expect(WORKFLOW_RUN_GRANT_RESOURCE.startsWith("workflow-run:")).toBe(true);
    expect(WORKFLOW_RUN_GRANT_ACTION).toBe("manage");
  });
});
