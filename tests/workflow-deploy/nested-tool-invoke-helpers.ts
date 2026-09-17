// Assertion helpers shared by the deployed tool-invoke round-trips that run a
// tool from a step BELOW the top rung: a loop body step, a `map` inner step
// inside a loop body, a nested inner loop's body step, a loop body inside a
// spawned child workflow, and an `onTrigger` section body step.
//
// Those round-trips read the same three things out of a finished run: the run
// grants a trigger must deliver, the text of every `tool_result` the agent saw,
// and the failure events a run log carries. Every test importing this module
// asserts on identical shapes, so the readers live here rather than as per-file
// copies that could drift apart while claiming to prove the same property at
// different nesting depths.

import type { GrantEffect, GrantWalkSnapshot } from "@intx/types";
import type { WireGrantRule } from "@intx/types/grant-wire";
import type { RepoId } from "@intx/hub-sessions";

import {
  listRunIds,
  type DeployFlowEnv,
  type InferenceRequest,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";

/**
 * Project a frozen grant-walk snapshot into the run's runtime `tool:`/`effect:`
 * grant rows, in the `run.grants` wire shape the trigger delivers. Mirrors the
 * production `deriveRunRuntimeGrantRows`/`runGrantToWire` tail (not exported
 * from `@intx/hub-api`): one row per distinct grant across steps, tool effect
 * taken from the step's `grantEffects` with `ask` winning over `allow`, effect
 * grants always `allow`. The rows are principal-agnostic (`principalId: null`),
 * matched by resource + action at the child's grant evaluator.
 */
export function deriveWireRunGrants(
  snapshot: GrantWalkSnapshot,
): WireGrantRule[] {
  const effectByResource = new Map<string, GrantEffect>();
  for (const step of snapshot.perStep) {
    const grantEffects = new Map<string, GrantEffect>(
      Object.entries(step.grantEffects),
    );
    for (const grant of step.grants) {
      if (grant.startsWith("tool:")) {
        const effect = grantEffects.get(grant);
        if (effect === undefined) {
          throw new Error(
            `deriveWireRunGrants: tool grant ${JSON.stringify(grant)} has no grantEffects entry`,
          );
        }
        const existing = effectByResource.get(grant);
        if (existing === "ask" || effect === "ask") {
          effectByResource.set(grant, "ask");
        } else if (existing === undefined) {
          effectByResource.set(grant, effect);
        }
      } else if (grant.startsWith("effect:") && !effectByResource.has(grant)) {
        effectByResource.set(grant, "allow");
      }
    }
  }
  return [...effectByResource].map(([resource, effect]) => ({
    id: `run-grant:${resource}`,
    resource,
    action: "invoke",
    effect,
    origin: "creator",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
  }));
}

/**
 * Flatten the text of every `tool_result` block in an inference request. The
 * Anthropic adapter serializes a result as a user-turn content block whose own
 * `content` is either a bare string or an array of `{ type: "text", text }`.
 *
 * The text is what a caller must assert on. `createToolRunner.run` converts a
 * handler throw into an error `ToolResult` rather than propagating it, so a
 * refusal at the authorize seam still appends a well-formed `tool_result` and
 * the agent answers it in an ordinary turn. Only the tool's own return value
 * distinguishes an execution from a refusal.
 */
export function toolResultTexts(req: InferenceRequest): string[] {
  const texts: string[] = [];
  for (const message of req.messages ?? []) {
    const content = message.content;
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      if (block.type !== "tool_result") continue;
      const inner = block.content;
      if (typeof inner === "string") {
        texts.push(inner);
        continue;
      }
      if (Array.isArray(inner)) {
        texts.push(
          inner
            .filter((b) => b.type === "text" && b.text !== undefined)
            .map((b) => b.text ?? "")
            .join(""),
        );
      }
    }
  }
  return texts;
}

/**
 * Every failure event in a run log, rendered with its full body. A step that
 * throws records the thrown message under `error.message`, so an assertion on
 * this list reports the underlying failure rather than a bare count mismatch.
 */
export function failureMessages(events: readonly WorkflowRunEvent[]): string[] {
  return events
    .filter((e) => e.type === "StepFailed" || e.type === "RunFailed")
    .map((e) => `${e.type} ${JSON.stringify(e.body)}`);
}

/**
 * The top-level container run id among the runs a deployment's run repo holds.
 * A loop's per-iteration body runs are keyed `<runId>__<loopStepId>__<index>`,
 * so the container is the only id carrying no `__` separator.
 */
export async function findContainerRunId(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  const ids = await listRunIds(env, workflowRunRepoId);
  return ids.find((id) => !id.includes("__"));
}
