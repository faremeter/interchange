// Audit/error flush wiring tests for the in-process agent.
//
// These tests pin the behaviour of `commitErrors` accumulation and the
// flush hooks the agent registers at the assembly's `afterCheckpoint`
// and `onShutdown` lifecycle boundaries. The wiring lives inside
// `createAgent`; the tests drive it through a custom `ReactorDirector`
// that responds to specific event types, so each test isolates one
// behavioural axis: error event shape, flush timing, multi-batch
// boundaries, no-op skip, and survives-commit-failure semantics.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type } from "arktype";

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
} from "@intx/types/runtime";

import { createAgent } from "./agent";
import { defineAgent } from "./definition";
import { defineDirector } from "./director";
import { createDirectorRegistry } from "./director-registry";
import type { BaseEnv } from "./env";
import { permissiveAuthorize } from "./testing/authorize-allow";

// An unreachable URL causes the inference call to fail with a network
// error, which the reactor surfaces as an `inference.error` event.
const UNREACHABLE_SOURCE: InferenceSource = {
  id: "anthropic:test-error",
  provider: "anthropic",
  baseURL: "http://localhost:1",
  apiKey: "test-key",
  model: "claude-test",
};

interface RecordingAuditStore extends AuditStore {
  getCommittedErrors(): ErrorRecord[][];
}

function makeRecordingAuditStore(): RecordingAuditStore {
  const committedErrors: ErrorRecord[][] = [];
  return {
    async commitAudit(_records: AuditRecord[]): Promise<void> {
      // No-op; these tests assert on the error channel only.
    },
    async commitErrors(records: ErrorRecord[]): Promise<void> {
      committedErrors.push([...records]);
    },
    async loadAudit(_sessionId: string): Promise<AuditRecord[]> {
      return [];
    },
    getCommittedErrors() {
      return committedErrors;
    },
  };
}

interface FailingAuditStore extends AuditStore {
  getCommittedErrors(): ErrorRecord[][];
}

// Audit store whose first `commitErrors` call throws and subsequent
// calls succeed. The original "errors survive a commitErrors failure"
// behaviour: records dropped from the agent's accumulator only on
// successful commit, so a transient failure does not silently lose
// the batch.
function makeFailFirstAuditStore(): FailingAuditStore {
  const committedErrors: ErrorRecord[][] = [];
  let shouldFail = true;
  return {
    async commitAudit(_records: AuditRecord[]): Promise<void> {
      // No-op.
    },
    async commitErrors(records: ErrorRecord[]): Promise<void> {
      if (shouldFail) {
        shouldFail = false;
        throw new Error("simulated storage failure");
      }
      committedErrors.push([...records]);
    },
    async loadAudit(_sessionId: string): Promise<AuditRecord[]> {
      return [];
    },
    getCommittedErrors() {
      return committedErrors;
    },
  };
}

// Director factory that closes over a caller-supplied `decide` to drive
// the reactor through targeted event shapes. The factory shape requires
// a configSchema (arktype) and returns a ReactorDirector; this helper
// hides that boilerplate.
function makeDirectorRegistry(
  decide: ReactorDirector["decide"],
): BaseEnv["directors"] {
  const defined = defineDirector({
    id: "@intx-test/flush/probe",
    configSchema: type({}),
    factory: () => ({ decide }),
  });
  return createDirectorRegistry({
    factories: [defined.factory],
    defaultId: defined.factory.id,
  });
}

async function buildAgentEnv(opts: {
  workdir: string;
  audit: AuditStore;
  directors: BaseEnv["directors"];
}): Promise<BaseEnv> {
  const storage = await createIsogitStore(opts.workdir);
  return {
    sources: [UNREACHABLE_SOURCE],
    defaultSource: UNREACHABLE_SOURCE.id,
    storage,
    workdir: opts.workdir,
    audit: opts.audit,
    authorize: permissiveAuthorize(),
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

// Drain agent.stream() until a `reactor.done` event is observed, then
// resolve. The agent's terminal event signals the reactor has settled
// and any pending audit-flush has had a chance to run via the
// assembly's onShutdown hook.
async function waitForReactorDone(
  stream: AsyncIterable<{ type: string }>,
): Promise<void> {
  for await (const event of stream) {
    if (event.type === "reactor.done") return;
  }
}

describe("agent error flushing", () => {
  let workDir: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-flush-"));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  test("accumulates inference.error and flushes it as an ErrorRecord", async () => {
    const audit = makeRecordingAuditStore();
    const directors = makeDirectorRegistry(
      async (
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) => {
        if (event.type === "message.received") return caps.infer();
        if (event.type === "inference.error") {
          return [caps.checkpoint("after-error"), caps.done()];
        }
        return caps.done();
      },
    );

    const def = defineAgent({
      id: "flush-inference-error",
      systemPrompt: "test",
      tools: [],
      capabilities: [],
      inference: {
        sources: [
          {
            provider: UNREACHABLE_SOURCE.provider,
            model: UNREACHABLE_SOURCE.model,
          },
        ],
      },
    });

    const env = await buildAgentEnv({ workdir: workDir, audit, directors });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    try {
      agent.deliver(inboundConversation());
      await waitForReactorDone(stream);
    } finally {
      await agent.close();
    }

    const batches = audit.getCommittedErrors();
    const record = batches.flat().find((r) => r.source === "inference");
    if (record === undefined) {
      throw new Error("expected an inference error record");
    }
    expect(record.source).toBe("inference");
    expect(record.fatal).toBe(false);
    expect(record.message.length).toBeGreaterThan(0);
    expect(record.sessionId.length).toBeGreaterThan(0);
  });

  test("accumulates fatal reactor.error and flushes it", async () => {
    const audit = makeRecordingAuditStore();
    const directors = makeDirectorRegistry(
      async (event: ReactorInboundEvent) => {
        if (event.type === "message.received") {
          throw new Error("director explosion");
        }
        return { type: "done" as const };
      },
    );

    const def = defineAgent({
      id: "flush-reactor-error",
      systemPrompt: "test",
      tools: [],
      capabilities: [],
      inference: {
        sources: [
          {
            provider: UNREACHABLE_SOURCE.provider,
            model: UNREACHABLE_SOURCE.model,
          },
        ],
      },
    });

    const env = await buildAgentEnv({ workdir: workDir, audit, directors });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    try {
      agent.deliver(inboundConversation());
      await waitForReactorDone(stream);
    } finally {
      await agent.close();
    }

    const record = audit
      .getCommittedErrors()
      .flat()
      .find((r) => r.source === "reactor");
    if (record === undefined) {
      throw new Error("expected a reactor error record");
    }
    expect(record.source).toBe("reactor");
    expect(record.category).toBe("reactor_error");
    expect(record.fatal).toBe(true);
    expect(record.message).toContain("director explosion");
  });

  test("does not call commitErrors when no errors occurred", async () => {
    const audit = makeRecordingAuditStore();
    const directors = makeDirectorRegistry(
      async (
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) => {
        if (event.type === "message.received") {
          return [caps.checkpoint("clean"), caps.done()];
        }
        return caps.done();
      },
    );

    const def = defineAgent({
      id: "flush-no-errors",
      systemPrompt: "test",
      tools: [],
      capabilities: [],
      inference: {
        sources: [
          {
            provider: UNREACHABLE_SOURCE.provider,
            model: UNREACHABLE_SOURCE.model,
          },
        ],
      },
    });

    const env = await buildAgentEnv({ workdir: workDir, audit, directors });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    try {
      agent.deliver(inboundConversation());
      await waitForReactorDone(stream);
    } finally {
      await agent.close();
    }

    expect(audit.getCommittedErrors().length).toBe(0);
  });

  test("retains the batch on a commitErrors failure and re-flushes on the next hook", async () => {
    // The agent's flush wiring spliced the accumulator only on
    // successful commit -- a transient storage failure left the
    // records in place for the next flush (afterCheckpoint or
    // onShutdown) to retry. This test pins that behaviour against the
    // failing-store stub: the first commitErrors throws, the second
    // (shutdown flush) sees the retained records and succeeds.
    const audit = makeFailFirstAuditStore();
    const directors = makeDirectorRegistry(
      async (
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) => {
        if (event.type === "message.received") return caps.infer();
        if (event.type === "inference.error") {
          return [caps.checkpoint("will-fail"), caps.done()];
        }
        return caps.done();
      },
    );

    const def = defineAgent({
      id: "flush-survive-failure",
      systemPrompt: "test",
      tools: [],
      capabilities: [],
      inference: {
        sources: [
          {
            provider: UNREACHABLE_SOURCE.provider,
            model: UNREACHABLE_SOURCE.model,
          },
        ],
      },
    });

    const env = await buildAgentEnv({ workdir: workDir, audit, directors });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    try {
      agent.deliver(inboundConversation());
      await waitForReactorDone(stream);
    } finally {
      await agent.close();
    }

    const batches = audit.getCommittedErrors();
    expect(batches.length).toBe(1);
    const record = batches[0]?.[0];
    if (record === undefined) {
      throw new Error("expected an error record on the second flush");
    }
    expect(record.source).toBe("inference");
  });

  test("records a non-fatal inference.error with fatal=false", async () => {
    // The reactor distinguishes fatal errors (director threw, no
    // recovery possible) from non-fatal ones (a single inference
    // attempt failed but the director caught it and kept going).
    // This test pins the non-fatal path: the unreachable URL surfaces
    // an `inference.error`, the director checkpoints + done, and the
    // recorded ErrorRecord carries `fatal: false` while the agent
    // still shuts down cleanly.
    const audit = makeRecordingAuditStore();
    const directors = makeDirectorRegistry(
      async (
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) => {
        if (event.type === "message.received") return caps.infer();
        if (event.type === "inference.error") {
          return [caps.checkpoint("after-error"), caps.done()];
        }
        return caps.done();
      },
    );

    const def = defineAgent({
      id: "flush-non-fatal",
      systemPrompt: "test",
      tools: [],
      capabilities: [],
      inference: {
        sources: [
          {
            provider: UNREACHABLE_SOURCE.provider,
            model: UNREACHABLE_SOURCE.model,
          },
        ],
      },
    });

    const env = await buildAgentEnv({ workdir: workDir, audit, directors });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    try {
      agent.deliver(inboundConversation());
      await waitForReactorDone(stream);
    } finally {
      await agent.close();
    }

    const inferenceErrors = audit
      .getCommittedErrors()
      .flat()
      .filter((r) => r.source === "inference");
    expect(inferenceErrors.length).toBeGreaterThanOrEqual(1);
    const record = inferenceErrors[0];
    if (record === undefined) {
      throw new Error("expected a non-fatal inference error record");
    }
    expect(record.fatal).toBe(false);
  });

  test("flushes accumulated errors at shutdown when no checkpoint runs", async () => {
    // A director that throws on `message.received` triggers a fatal
    // reactor.error without first issuing any `checkpoint` action.
    // The afterCheckpoint flush hook never fires; the only path for
    // the error to reach the audit store is the onShutdown drain.
    // This test pins that drain path independently of the
    // afterCheckpoint one.
    const audit = makeRecordingAuditStore();
    const directors = makeDirectorRegistry(
      async (event: ReactorInboundEvent) => {
        if (event.type === "message.received") {
          throw new Error("shutdown flush test");
        }
        return { type: "done" as const };
      },
    );

    const def = defineAgent({
      id: "flush-shutdown-only",
      systemPrompt: "test",
      tools: [],
      capabilities: [],
      inference: {
        sources: [
          {
            provider: UNREACHABLE_SOURCE.provider,
            model: UNREACHABLE_SOURCE.model,
          },
        ],
      },
    });

    const env = await buildAgentEnv({ workdir: workDir, audit, directors });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    try {
      agent.deliver(inboundConversation());
      await waitForReactorDone(stream);
    } finally {
      await agent.close();
    }

    const batches = audit.getCommittedErrors();
    expect(batches.length).toBe(1);
    expect(batches[0]?.[0]?.source).toBe("reactor");
  });
});
