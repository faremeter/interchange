// The child-workflow fixture builder's tool-import hoisting invariant.
//
// `childWorkflowEntry` renders a self-contained entry module. When a step
// carries an inline tool, the rendered agent references `mailSendTool(...)`, so
// the module must import it -- and exactly when such a reference exists, at any
// nesting depth, since `bundleWorkflowEntry` would fail on an undefined
// `mailSendTool` or an unused import. `anyStepHasTool` and the render path are
// mutually recursive over the same spawn tree; a divergence would drop the
// import for a deeply-nested tool and surface only as a bundling failure inside
// an expensive deploy roundtrip. These cases pin the invariant directly and
// cheaply, including the depth-2 path no deploy roundtrip nests a tool into.

import { test, expect } from "bun:test";

import { childWorkflowEntry } from "./child-workflow";

const base = { workflowId: "wf_p", address: "p@d" };

function importCount(source: string): number {
  return (source.match(/import \{ mailSendTool \}/g) ?? []).length;
}

test("a toolless build renders no import and no reference", () => {
  const out = childWorkflowEntry({
    ...base,
    steps: [{ stepId: "s1", agentId: "a1", systemPrompt: "p" }],
    spawns: [],
  });
  expect(out.includes("mailSendTool")).toBe(false);
  expect(out.includes("tools: [],")).toBe(true);
});

test("a top-level tool renders the import exactly once and a reference", () => {
  const out = childWorkflowEntry({
    ...base,
    steps: [{ stepId: "s1", agentId: "a1", systemPrompt: "p", tool: "fs" }],
    spawns: [],
  });
  expect(importCount(out)).toBe(1);
  expect(out.includes('mailSendTool("fs")')).toBe(true);
});

test("a tool on a child step hoists the import to the module top", () => {
  const out = childWorkflowEntry({
    ...base,
    steps: [{ stepId: "s1", agentId: "a1", systemPrompt: "p" }],
    spawns: [
      {
        stepId: "spawn",
        after: ["s1"],
        child: {
          workflowId: "wf_c",
          address: "c@d",
          steps: [
            {
              stepId: "cs",
              agentId: "ca",
              systemPrompt: "cp",
              tool: "transport",
            },
          ],
        },
      },
    ],
  });
  expect(importCount(out)).toBe(1);
  expect(out.includes('mailSendTool("transport")')).toBe(true);
});

test("a tool on a grandchild step still hoists the import (depth 2)", () => {
  const out = childWorkflowEntry({
    ...base,
    steps: [{ stepId: "s1", agentId: "a1", systemPrompt: "p" }],
    spawns: [
      {
        stepId: "spawn",
        after: ["s1"],
        child: {
          workflowId: "wf_c",
          address: "c@d",
          steps: [{ stepId: "cs", agentId: "ca", systemPrompt: "cp" }],
          spawns: [
            {
              stepId: "spawn2",
              after: ["cs"],
              child: {
                workflowId: "wf_gc",
                address: "gc@d",
                steps: [
                  {
                    stepId: "gs",
                    agentId: "ga",
                    systemPrompt: "gp",
                    tool: "ask",
                  },
                ],
              },
            },
          ],
        },
      },
    ],
  });
  expect(importCount(out)).toBe(1);
  expect(out.includes('mailSendTool("ask")')).toBe(true);
});

test("a toolless build keeps its exact byte shape (no stray tool import)", () => {
  const out = childWorkflowEntry({
    ...base,
    steps: [{ stepId: "s1", agentId: "a1", systemPrompt: "p" }],
    spawns: [],
  });
  expect(
    out.includes(
      'import { defineAgent } from "@intx/agent";\n\nexport const workflow',
    ),
  ).toBe(true);
});
