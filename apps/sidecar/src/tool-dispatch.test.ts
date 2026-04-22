import { describe, test, expect } from "bun:test";
import type { DeployToolInfo } from "@interchange/harness";
import { buildToolDispatch } from "./session-manager";

function makeDeployTool(name: string, hasHandler: boolean): DeployToolInfo {
  return {
    definition: {
      name,
      description: `Tool ${name}`,
      inputSchema: { type: "object", properties: {} },
    },
    hasHandler,
  };
}

const signal = AbortSignal.timeout(5000);

describe("buildToolDispatch", () => {
  test("dispatches posix built-in tool", async () => {
    const dispatch = buildToolDispatch([makeDeployTool("read_file", false)]);
    const result = await dispatch.run(
      { id: "c1", name: "read_file", arguments: { path: "/dev/null" } },
      signal,
    );
    expect(result.callId).toBe("c1");
    expect(result.isError).not.toBe(true);
  });

  test("returns error for handler tool", async () => {
    const dispatch = buildToolDispatch([makeDeployTool("custom", true)]);
    const result = await dispatch.run(
      { id: "c2", name: "custom", arguments: {} },
      signal,
    );
    expect(result.callId).toBe("c2");
    expect(result.isError).toBe(true);
    expect(result.content).toContain("handler.ts");
    expect(result.content).toContain("not yet implemented");
  });

  test("returns error for deploy tool without handler or posix match", async () => {
    const dispatch = buildToolDispatch([makeDeployTool("exotic_tool", false)]);
    const result = await dispatch.run(
      { id: "c3", name: "exotic_tool", arguments: {} },
      signal,
    );
    expect(result.callId).toBe("c3");
    expect(result.isError).toBe(true);
    expect(result.content).toContain("declared in the deploy tree");
    expect(result.content).toContain("does not match a built-in tool");
  });

  test("returns error for completely unknown tool", async () => {
    const dispatch = buildToolDispatch([]);
    const result = await dispatch.run(
      { id: "c4", name: "nonexistent", arguments: {} },
      signal,
    );
    expect(result.callId).toBe("c4");
    expect(result.isError).toBe(true);
    expect(result.content).toContain("Unknown tool");
  });

  test("handler tools take priority over posix names", async () => {
    const dispatch = buildToolDispatch([makeDeployTool("read_file", true)]);
    const result = await dispatch.run(
      { id: "c5", name: "read_file", arguments: { path: "/dev/null" } },
      signal,
    );
    expect(result.isError).toBe(true);
    expect(result.content).toContain("handler.ts");
  });
});
