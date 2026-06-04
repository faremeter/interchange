// End-to-end audit-trail integration tests for the in-process agent.
//
// These pin the audit channel the agent wires through
// `createAgent(def, env)`: allowed tool calls land in commitAudit
// with `authz.effect === "allow"` and the tool's result attached;
// denied calls land with `authz.blocked === true` and an error
// result; authorize throwing produces a blocked record with
// `authz.effect === null`; the flush hooks at afterCheckpoint and
// onShutdown together flush exactly once when records exist and
// skip when they do not.
//
// The director is a hand-written test double that drives the
// reactor through a single tool execution and then shuts down,
// exercising the audit pipeline without needing a real LLM.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type } from "arktype";

import type { AuthzCallResult } from "@intx/inference";
import { createInboundMessage } from "@intx/mime";
import { createIsogitStore } from "@intx/storage-isogit";
import type { AuditRecord, ErrorRecord } from "@intx/types/audit";
import type {
  AuditStore,
  InferenceSource,
  ReactorCapabilities,
  ReactorDirector,
  ReactorInboundEvent,
  ReactorState,
  ToolCall,
  ToolResult,
} from "@intx/types/runtime";

import { createAgent } from "./agent";
import { defineAgent } from "./definition";
import { defineDirector } from "./director";
import { createDirectorRegistry } from "./director-registry";
import type { BaseEnv } from "./env";
import { defineTool } from "./tool";

// The audit pipeline needs an inference source to construct the
// reactor, but the test director never calls infer(); the URL is
// unreachable so any accidental infer() would fail fast rather than
// hang.
const SOURCE: InferenceSource = {
  id: "anthropic:audit-test",
  provider: "anthropic",
  baseURL: "http://localhost:1",
  apiKey: "test-key",
  model: "claude-test",
};

interface RecordingAuditStore extends AuditStore {
  getCommittedAudit(): AuditRecord[][];
  getCommittedErrors(): ErrorRecord[][];
}

function makeRecordingAuditStore(): RecordingAuditStore {
  const committedAudit: AuditRecord[][] = [];
  const committedErrors: ErrorRecord[][] = [];
  return {
    async commitAudit(records: AuditRecord[]): Promise<void> {
      committedAudit.push([...records]);
    },
    async commitErrors(records: ErrorRecord[]): Promise<void> {
      committedErrors.push([...records]);
    },
    async loadAudit(_sessionId: string): Promise<AuditRecord[]> {
      return committedAudit.flat();
    },
    getCommittedAudit() {
      return committedAudit;
    },
    getCommittedErrors() {
      return committedErrors;
    },
  };
}

function allowAll(): Promise<AuthzCallResult> {
  return Promise.resolve({
    effect: "allow" as const,
    matchingGrants: [],
    resolvedBy: null,
  });
}

function denyAll(): Promise<AuthzCallResult> {
  return Promise.resolve({
    effect: "deny" as const,
    matchingGrants: [],
    resolvedBy: null,
  });
}

// A tool factory that exposes a single tool whose `run` returns a
// fixed string result. The audit record carries the tool name,
// arguments observed by the authz extension, and the tool's
// returned `content`/`isError`, so a deterministic stub is enough.
function makeFixedResultTool(opts: {
  name: string;
  result: string;
}): ReturnType<typeof defineTool<BaseEnv>> {
  return defineTool<BaseEnv>({
    id: `@intx-test/audit/${opts.name}`,
    factory: () => ({
      definitions: [
        {
          name: opts.name,
          description: "test tool",
          inputSchema: { type: "object", properties: {} },
        },
      ],
      async run(call: ToolCall, _signal: AbortSignal): Promise<ToolResult> {
        return {
          callId: call.id,
          content: opts.result,
          isError: false,
        };
      },
    }),
  });
}

// Director that executes a single named tool on the inbound message
// and shuts the reactor down once the tool result arrives. The
// `checkpointBeforeDone` flag controls whether the director
// checkpoints before its terminal done() -- the audit pipeline
// flushes at afterCheckpoint when it does, and via onShutdown when
// it does not.
function makeToolExecDirector(opts: {
  toolName: string;
  args: Record<string, unknown>;
  checkpointBeforeDone: boolean;
}): ReactorDirector {
  return {
    async decide(
      event: ReactorInboundEvent,
      _state: ReactorState,
      caps: ReactorCapabilities,
    ) {
      if (event.type === "message.received") {
        return caps.executeTools([
          {
            id: `call-${opts.toolName}`,
            name: opts.toolName,
            arguments: opts.args,
          },
        ]);
      }
      if (event.type === "tool.done") {
        if (opts.checkpointBeforeDone) {
          return [caps.checkpoint("after-tool"), caps.done()];
        }
        return caps.done();
      }
      return caps.done();
    },
  };
}

function makeDirectorRegistry(director: ReactorDirector): BaseEnv["directors"] {
  const defined = defineDirector({
    id: "@intx-test/audit/probe",
    configSchema: type({}),
    factory: () => director,
  });
  return createDirectorRegistry({
    factories: [defined.factory],
    defaultId: defined.factory.id,
  });
}

async function buildEnv(opts: {
  workdir: string;
  audit: AuditStore;
  authorize: BaseEnv["authorize"];
  directors: BaseEnv["directors"];
}): Promise<BaseEnv> {
  const storage = await createIsogitStore(opts.workdir);
  return {
    source: SOURCE,
    storage,
    workdir: opts.workdir,
    audit: opts.audit,
    authorize: opts.authorize,
    directors: opts.directors,
  };
}

function inboundConversation(): ReturnType<typeof createInboundMessage> {
  return createInboundMessage({
    from: "user@local",
    to: "agent@local",
    content: "trigger",
    interchangeType: "conversation.message",
  });
}

async function waitForReactorDone(
  stream: AsyncIterable<{ type: string }>,
): Promise<void> {
  for await (const event of stream) {
    if (event.type === "reactor.done") return;
  }
}

describe("agent audit integration", () => {
  let workDir: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-audit-"));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  test("allowed tool call produces an audit record with allow effect and result", async () => {
    const audit = makeRecordingAuditStore();
    const tool = makeFixedResultTool({
      name: "test_tool",
      result: "mock-result",
    });
    const director = makeToolExecDirector({
      toolName: "test_tool",
      args: { key: "value" },
      checkpointBeforeDone: true,
    });

    const def = defineAgent({
      id: "audit-allow",
      systemPrompt: "test",
      tools: [tool],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    const env = await buildEnv({
      workdir: workDir,
      audit,
      authorize: () => allowAll(),
      directors: makeDirectorRegistry(director),
    });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    try {
      agent.deliver(inboundConversation());
      await waitForReactorDone(stream);
    } finally {
      await agent.close();
    }

    const records = audit.getCommittedAudit().flat();
    expect(records.length).toBe(1);
    const record = records[0];
    if (record === undefined) {
      throw new Error("expected one audit record");
    }
    expect(record.callId).toBe("call-test_tool");
    expect(record.tool).toBe("test_tool");
    expect(record.arguments).toEqual({ key: "value" });
    if (record.authz === null) {
      throw new Error("expected authz to be populated");
    }
    expect(record.authz.effect).toBe("allow");
    expect(record.authz.blocked).toBe(false);
    expect(record.result.content).toBe("mock-result");
    expect(record.result.isError).toBe(false);
    expect(record.sessionId.length).toBeGreaterThan(0);
  });

  test("denied tool call produces an audit record with blocked authz", async () => {
    const audit = makeRecordingAuditStore();
    const tool = makeFixedResultTool({
      name: "secret_tool",
      result: "should-not-reach",
    });
    const director = makeToolExecDirector({
      toolName: "secret_tool",
      args: { path: "/etc/shadow" },
      checkpointBeforeDone: true,
    });

    const def = defineAgent({
      id: "audit-deny",
      systemPrompt: "test",
      tools: [tool],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    const env = await buildEnv({
      workdir: workDir,
      audit,
      authorize: () => denyAll(),
      directors: makeDirectorRegistry(director),
    });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    try {
      agent.deliver(inboundConversation());
      await waitForReactorDone(stream);
    } finally {
      await agent.close();
    }

    const records = audit.getCommittedAudit().flat();
    expect(records.length).toBe(1);
    const record = records[0];
    if (record === undefined) {
      throw new Error("expected one audit record");
    }
    expect(record.callId).toBe("call-secret_tool");
    expect(record.tool).toBe("secret_tool");
    if (record.authz === null) {
      throw new Error("expected authz to be populated");
    }
    expect(record.authz.effect).toBe("deny");
    expect(record.authz.blocked).toBe(true);
    expect(record.result.isError).toBe(true);
  });

  test("audit records are flushed at shutdown when the director never checkpoints", async () => {
    const audit = makeRecordingAuditStore();
    const tool = makeFixedResultTool({
      name: "test_tool",
      result: "result-via-shutdown",
    });
    const director = makeToolExecDirector({
      toolName: "test_tool",
      args: {},
      checkpointBeforeDone: false,
    });

    const def = defineAgent({
      id: "audit-shutdown-flush",
      systemPrompt: "test",
      tools: [tool],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    const env = await buildEnv({
      workdir: workDir,
      audit,
      authorize: () => allowAll(),
      directors: makeDirectorRegistry(director),
    });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    try {
      agent.deliver(inboundConversation());
      await waitForReactorDone(stream);
    } finally {
      await agent.close();
    }

    const batches = audit.getCommittedAudit();
    expect(batches.length).toBe(1);
    expect(batches[0]?.length).toBe(1);
    expect(batches[0]?.[0]?.callId).toBe("call-test_tool");
  });

  test("a checkpoint flush followed by shutdown does not double-commit", async () => {
    const audit = makeRecordingAuditStore();
    const tool = makeFixedResultTool({
      name: "test_tool",
      result: "one-batch",
    });
    const director = makeToolExecDirector({
      toolName: "test_tool",
      args: { x: 1 },
      checkpointBeforeDone: true,
    });

    const def = defineAgent({
      id: "audit-no-double",
      systemPrompt: "test",
      tools: [tool],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    const env = await buildEnv({
      workdir: workDir,
      audit,
      authorize: () => allowAll(),
      directors: makeDirectorRegistry(director),
    });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    try {
      agent.deliver(inboundConversation());
      await waitForReactorDone(stream);
    } finally {
      await agent.close();
    }

    // afterCheckpoint flushes the single record; the onShutdown flush
    // finds the buffer empty and does not call commitAudit again.
    const batches = audit.getCommittedAudit();
    expect(batches.length).toBe(1);
    expect(batches[0]?.length).toBe(1);
  });

  test("authorize throwing produces a blocked audit record with null effect", async () => {
    const audit = makeRecordingAuditStore();
    const tool = makeFixedResultTool({
      name: "risky_tool",
      result: "should-not-reach",
    });
    const director = makeToolExecDirector({
      toolName: "risky_tool",
      args: { cmd: "rm -rf /" },
      checkpointBeforeDone: true,
    });

    const def = defineAgent({
      id: "audit-authz-throws",
      systemPrompt: "test",
      tools: [tool],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    const env = await buildEnv({
      workdir: workDir,
      audit,
      authorize: (_resource: string, _action: string) => {
        throw new Error("authz service unavailable");
      },
      directors: makeDirectorRegistry(director),
    });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    try {
      agent.deliver(inboundConversation());
      await waitForReactorDone(stream);
    } finally {
      await agent.close();
    }

    const records = audit.getCommittedAudit().flat();
    expect(records.length).toBe(1);
    const record = records[0];
    if (record === undefined) {
      throw new Error("expected one audit record");
    }
    expect(record.tool).toBe("risky_tool");
    if (record.authz === null) {
      throw new Error("expected authz to be populated");
    }
    expect(record.authz.blocked).toBe(true);
    expect(record.authz.effect).toBeNull();
    expect(record.result.isError).toBe(true);
  });
});
