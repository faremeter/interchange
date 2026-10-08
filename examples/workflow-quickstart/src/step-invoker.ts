// The step invoker: the seam between a workflow definition and a live
// agent.
//
// A `step` primitive carries an `AgentDefinition` -- a description, not an
// instance. The workflow runtime never instantiates it; it hands the
// definition to `env.invokeStep` and lets the host decide what running an
// agent means. `runLocal` supplies a stub invoker by default, which
// authorizes the step and returns `{ output: null }` without any
// inference: enough to exercise a DAG's routing, not enough for a step
// whose agent has a tool. So this example wires its own -- the deployed
// host's shape minus the parts only a sidecar can supply (mail-derived
// inputs, warm agents kept alive across turns, approval-gate resumption).

import { mkdirSync } from "node:fs";
import { join } from "node:path";

import {
  createAgent,
  createDefaultDirectorRegistry,
  createStaticCredentialResolver,
  type BaseEnv,
} from "@intx/agent";
import { noopAuditStore } from "@intx/agent/testing";
import { optional } from "@intx/example-agent-common";
import type { Dependencies } from "@intx/inference";
import { createIsogitStore } from "@intx/storage-isogit/node";
import type { InferenceSource } from "@intx/types/runtime";
import type {
  AuthorizeContext,
  StepInvoker,
  WorkflowAuthorizeFn,
} from "@intx/workflow";

export interface AgentStepInvokerArgs {
  /** The inference source every step agent runs against. */
  source: InferenceSource;
  /** Credential secrets keyed by `credentialId`, backing `source`. */
  material: Record<string, string>;
  /** Root under which each step invocation gets its own agent workdir. */
  contextDir: string;
  /** The same authorize the run was launched with. */
  authorize: WorkflowAuthorizeFn;
  /** Inference dependencies; the test injects the harness here. */
  deps?: Dependencies;
}

/**
 * Build a `StepInvoker` that runs each step's agent for one turn and
 * returns its reply as the step's output. Whatever an invoker returns
 * becomes the step's `output`, which downstream selectors and a loop's
 * `while`/`carry` read. This one returns `{ reply }`; the deployed host
 * returns `{ reply, turn }` so a consumer can walk the full turn.
 */
export function createAgentStepInvoker(
  args: AgentStepInvokerArgs,
): StepInvoker {
  return async ({ agent, input, authzContext, signal }) => {
    const stepName = stepInvocationName(authzContext);
    const workdir = join(args.contextDir, stepName);
    mkdirSync(workdir, { recursive: true });
    const storage = await createIsogitStore(workdir);

    const env: BaseEnv = {
      sources: [args.source],
      defaultSource: args.source.id,
      storage,
      workdir,
      audit: noopAuditStore(),
      // The agent layer is workflow-unaware: its `AuthorizeFn` has no
      // third argument for workflow vocabulary. Capturing the context in
      // a closure is how `{ stepId, attempt, runId }` reaches the
      // workflow-typed authorize on every tool and capability check the
      // step makes.
      authorize: (resource, action) =>
        args.authorize(resource, action, authzContext),
      directors: createDefaultDirectorRegistry(),
      readCurrentMaterial: createStaticCredentialResolver(args.material),
      ...optional("deps", args.deps),
    };

    const instance = await createAgent(agent, env);
    try {
      // A step's input is arbitrary JSON; the agent takes a string, so
      // the prompt is written against the JSON it sees.
      const result = await instance.send(JSON.stringify(input), { signal });
      if (result.type !== "reply") {
        // The other arm is a suspend on an approval gate. A host that
        // supports approvals returns `{ suspend: ... }` and the runtime
        // parks the step; this example has no approver, so it fails
        // loudly rather than report a reply it never got.
        throw new Error(
          `step ${stepName} suspended on correlationId ${result.correlationId}; this example has no approval path`,
        );
      }
      return { output: { reply: result.reply } };
    } finally {
      await instance.close();
    }
  };
}

/**
 * Name one step invocation, and with it the agent workdir that
 * invocation owns. Each loop iteration is its own child run, so `runId`
 * differs per pass: every pass gets a fresh agent with a fresh
 * conversation, which is what makes the passes independent. An absent
 * field means the runtime did not thread it -- a defect, not a default
 * to invent.
 */
function stepInvocationName(ctx: AuthorizeContext): string {
  const { runId, stepId } = ctx;
  if (runId === undefined || stepId === undefined) {
    throw new Error(
      "step invoker: the workflow runtime must supply runId and stepId on every step's AuthorizeContext",
    );
  }
  return `${runId}-${stepId}`;
}
