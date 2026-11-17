// Default conversational director — reference ReactorDirector implementation.
//
// Inbound-event → action map (see INFERENCE.md § Director Decision Function
// for the director contract and action-validation rules):
//
//   message.received          → infer
//   inference.done (tools)    → checkpoint + execute_tools
//   tool.done                 → checkpoint + infer (re-infer with tool results)
//   inference.done (no tools) → checkpoint + reply (connector sends the message)
//   inference.error           → checkpoint + reply (error message to user)
//   abort                     → done
//   reactor.gate.cleared      → checkpoint + infer (resume after gate)
//   resume.execute_tools      → execute_tools (re-run a parked approved call)
//   resume.tool_result        → checkpoint + infer (parked call denied/timed out)
//
// The inference.done branch additionally runs the optional afterInferenceDone
// policy hook, whose continue/abort/halt decisions route independently of the
// event map above. See AfterInferenceDecision for that contract.
//
// The director never throws. Inference errors are surfaced to the user as a
// reply so the problem is visible, and the agent remains alive for retries.

import { getLogger } from "@intx/log";
import type {
  ReactorDirector,
  ReactorInboundEvent,
  ReactorState,
  ReactorCapabilities,
  ReactorAction,
  AssistantTurn,
  ToolCall,
  ToolDefinition,
} from "@intx/types/runtime";
import { formatSafetyRatingText } from "@intx/types/format-safety-rating";

const logger = getLogger(["interchange", "inference", "default-director"]);

/**
 * Decision returned by an `afterInferenceDone` policy hook.
 *
 *   continue — proceed with the director's normal post-inference logic
 *              (tool extraction, reply, or wait per the existing flow).
 *   abort    — terminate the agent. Routes to `[checkpoint, done]` and
 *              the reactor shuts down. Stronger than the
 *              `inference.error` branch, which only replies and stays
 *              alive — `abort` is for "session is over, do not accept
 *              further inputs."
 *   halt     — pause the current cycle without terminating. Routes to
 *              `[checkpoint, reply]`; the reply returns the reactor to
 *              waiting for the next inbound event, so it stays alive.
 *              There is no auto-resume; an external event (mail, gate
 *              clearance, etc.) must reach the reactor for the agent to
 *              make progress again.
 *
 * `reason` on a `halt` becomes the connector reply text verbatim, so
 * policy authors choose what is safe to surface to the user. On an
 * `abort` the reason is not surfaced: a terminal action cannot carry a
 * reply, since a reply invites continuation. Delivering an abort reason
 * to the user needs a dedicated terminal-notice path, which does not
 * exist today.
 */
export type AfterInferenceDecision =
  | { type: "continue" }
  | { type: "abort"; reason: string }
  | { type: "halt"; reason: string };

/**
 * Function shape for an after-inference-done policy hook.
 *
 * The hook fires only on `inference.done` (a successful cycle). Errored
 * cycles do not invoke it. `mode: "reactive"` does not change firing —
 * the hook gates the entire `inference.done` branch, including the
 * reactive-wait shortcut, so a budget check applies to reactive agents
 * the same way it does to conversational ones.
 *
 * The hook receives the post-cycle `ReactorState` (with `lastCycleSource`
 * and `lastCycleUsage` populated for the just-completed call) and the
 * assistant turn. Returns a decision (sync or async) that controls
 * whether the director continues, terminates the agent, or pauses the
 * cycle.
 *
 * Canonical use case: cost-aware gating. Read `state.lastCycleSource`
 * + `state.lastCycleUsage`, price the call against user-supplied rate
 * data, decide whether the budget is exhausted. Token caps, time caps,
 * wallet checks, and governance triggers fit the same shape; the
 * type stays policy-agnostic.
 *
 * "Downgrade to cheaper model" policies do NOT use this hook to return
 * a new source. Compose them via an external observer of
 * `lastCycleSource` / `lastCycleUsage` that calls `setSource` from
 * outside the director.
 *
 * The hook blocks the reactor's inference.done branch: keep its
 * latency low. The return type admits a Promise, but every await
 * inside the hook is wall-clock time the agent isn't making progress.
 * Small lookups (in-memory caches, fast DB reads) are fine; arbitrary
 * waits are not.
 *
 * Tool calls and `halt`: if the model emitted tool calls and the hook
 * returns `halt` (or `abort`), those tool calls are dropped — the
 * director never executes them. On resume, the model's next inference
 * sees an assistant turn with unanswered tool calls; depending on the
 * provider this is either a validation error or a confused model.
 * Policy authors that combine `halt` with tool-heavy agents need to
 * understand this.
 */
export type AfterInferenceHook = (
  state: ReactorState,
  turn: AssistantTurn,
) => AfterInferenceDecision | Promise<AfterInferenceDecision>;

export type DefaultDirectorPolicy = {
  /**
   * Controls the agent's behavior after inference completes.
   *
   *   "conversational" (default) — The standard agentic loop. After tools
   *     complete, re-infer so the model can reason about results, issue more
   *     tool calls, or compose a reply. When inference produces text without
   *     tool calls, send it as a connector reply.
   *
   *   "reactive" — The agent acts on each message by executing tools, then
   *     returns to the event loop to wait for the next inbound event. It does
   *     not re-infer after tools complete and does not send connector replies.
   *     Use this for agents that perform a single action per message.
   */
  mode?: "conversational" | "reactive";

  /**
   * Optional policy hook fired after every successful `inference.done`.
   * See `AfterInferenceHook` for the contract: firing boundary, return
   * shape, composition patterns, and policy-author caveats.
   *
   * If the hook throws or rejects, the director catches the error,
   * routes to `{ type: "abort", reason: "afterInferenceDone policy
   * threw: <message>" }`, and logs at error level. The director's
   * never-throws contract is preserved.
   */
  afterInferenceDone?: AfterInferenceHook;
};

function extractToolCalls(turn: AssistantTurn): ToolCall[] {
  const calls: ToolCall[] = [];
  for (const block of turn.content) {
    if (block.type === "tool_call") {
      calls.push({
        id: block.id,
        name: block.name,
        arguments: block.arguments,
      });
    }
  }
  return calls;
}

function extractTextContent(turn: AssistantTurn): string {
  // Text, refusal, and safety_rating blocks all carry human-readable
  // output the connector needs to surface. A refusal-only or
  // safety-only turn would otherwise route through the empty-response
  // branch below and never reach the reply path, leaving the human
  // waiting for an answer the model already declined or blocked.
  // Structural part kinds are preserved at the persistence layer;
  // the reply path only needs the words.
  const parts: string[] = [];
  for (const block of turn.content) {
    if (block.type === "text") {
      parts.push(block.text);
    } else if (block.type === "refusal") {
      parts.push(block.reason);
    } else if (block.type === "safety_rating") {
      parts.push(formatSafetyRatingText(block));
    }
  }
  return parts.join("\n").trim();
}

const ERROR_PREAMBLE: Record<string, string> = {
  credential_failure:
    "This agent could not complete your request due to a credential error",
  quota_exhausted:
    "This agent could not complete your request because the API quota has been exhausted",
  context_overflow:
    "This agent could not complete your request because the conversation exceeded the model's context limit",
  retryable:
    "This agent encountered a temporary error communicating with the inference provider",
  fatal:
    "This agent could not complete your request due to an unrecoverable inference error",
  aborted: "This agent's inference request was aborted",
};

function formatInferenceError(error: {
  category: string;
  message: string;
  statusCode?: number;
}): string {
  const preamble = ERROR_PREAMBLE[error.category] ?? ERROR_PREAMBLE["fatal"];
  const status =
    error.statusCode !== undefined ? ` [HTTP ${error.statusCode}]` : "";
  return `${preamble}${status}: ${error.message}`;
}

export class DefaultDirector implements ReactorDirector {
  private readonly systemPrompt: string;
  private readonly toolDefinitions: ToolDefinition[];
  private readonly policy: DefaultDirectorPolicy;

  // Outstanding call ids per tool batch, so the director re-infers once per
  // batch, only when every call of that batch is answered. A parked call stays
  // outstanding until its resume answers it, and one batch never waits on
  // another. Each call id maps to its batch's shared set.
  private readonly outstanding = new Map<string, Set<string>>();

  private openBatch(calls: ToolCall[]): void {
    const batch = new Set(calls.map((call) => call.id));
    for (const id of batch) this.outstanding.set(id, batch);
  }

  // A director built after a respawn holds no batches, but a parked call's
  // batch survives in history and in the pending operations: the calls of
  // the assistant turn that holds the call that no tool_result answers and
  // that a pending operation still parks, plus the call answered or resumed
  // now. Rebuilding it from there keeps a resumed call from re-inferring
  // while a sibling of its batch stays parked. An unanswered call with no
  // pending operation was running at the crash; nothing will answer it, so
  // the batch does not wait on it.
  // History alone cannot replace the in-memory set: a fresh batch lands all
  // of its results in one turn before its tool.done events arrive, so each of
  // those events would see a complete batch. The first turn that holds the
  // call is taken as its owner. The reactor checks that call ids are unique
  // in the assembled prompt (after the context transforms) at each
  // inference, not across the full history.
  private recoverBatch(
    callId: string,
    state: ReactorState,
  ): Set<string> | undefined {
    const owner = state.turns.find(
      (turn) =>
        turn.role === "assistant" &&
        turn.content.some(
          (block) => block.type === "tool_call" && block.id === callId,
        ),
    );
    if (owner === undefined) return undefined;
    const answered = new Set<string>();
    for (const turn of state.turns) {
      for (const block of turn.content) {
        if (block.type === "tool_result") answered.add(block.callId);
      }
    }
    const parked = new Set<string>();
    for (const op of state.pendingOperations) {
      if (op.suspendedCall !== undefined) parked.add(op.suspendedCall.id);
    }
    const batch = new Set<string>([callId]);
    for (const block of owner.content) {
      if (
        block.type === "tool_call" &&
        !answered.has(block.id) &&
        parked.has(block.id)
      ) {
        batch.add(block.id);
      }
    }
    for (const id of batch) this.outstanding.set(id, batch);
    return batch;
  }

  // Answers one call. Returns true when its batch has no call left. A call
  // that neither memory nor history places in a batch has nothing to wait on.
  private answer(callId: string, state: ReactorState): boolean {
    const batch =
      this.outstanding.get(callId) ?? this.recoverBatch(callId, state);
    if (batch === undefined) return true;
    this.outstanding.delete(callId);
    batch.delete(callId);
    return batch.size === 0;
  }

  constructor(
    systemPrompt: string,
    toolDefinitions: ToolDefinition[] = [],
    policy: DefaultDirectorPolicy = {},
  ) {
    this.systemPrompt = systemPrompt;
    this.toolDefinitions = toolDefinitions;
    this.policy = policy;
  }

  async decide(
    event: ReactorInboundEvent,
    state: ReactorState,
    capabilities: ReactorCapabilities,
  ): Promise<ReactorAction | ReactorAction[]> {
    switch (event.type) {
      case "message.received": {
        return capabilities.infer({
          systemPrompt: this.systemPrompt,
          tools: this.toolDefinitions,
        });
      }

      case "inference.done": {
        // The hook gates the entire inference.done branch (including
        // tool extraction and the reactive-mode wait shortcut). An
        // abort/halt from the policy drops any tool calls the model
        // emitted in this turn; see AfterInferenceHook TSDoc for the
        // implications.
        if (this.policy.afterInferenceDone !== undefined) {
          let decision: AfterInferenceDecision;
          try {
            decision = await this.policy.afterInferenceDone(state, event.turn);
          } catch (cause) {
            const message =
              cause instanceof Error ? cause.message : String(cause);
            logger.error`afterInferenceDone policy threw: ${message}`;
            decision = {
              type: "abort",
              reason: `afterInferenceDone policy threw: ${message}`,
            };
          }
          if (decision.type === "abort") {
            // A reply invites the next inbound message, but abort is
            // terminal — the reactor rejects reply paired with done. The
            // reason is therefore not surfaced on this path.
            return [
              capabilities.checkpoint("after-inference-abort"),
              capabilities.done(),
            ];
          }
          if (decision.type === "halt") {
            // A reply already returns the reactor to waiting for the next
            // inbound message, so no separate wait is needed (and the
            // reactor rejects reply paired with wait).
            return [
              capabilities.checkpoint("after-inference-halt"),
              capabilities.reply(decision.reason),
            ];
          }
          // decision.type === "continue" — fall through.
        }

        const toolCalls = extractToolCalls(event.turn);
        if (toolCalls.length > 0) {
          this.openBatch(toolCalls);
          return [
            capabilities.checkpoint("tool-execution"),
            capabilities.executeTools(toolCalls, true),
          ];
        }

        // No tool calls — the model is done reasoning for this turn.
        if (this.policy.mode === "reactive") {
          return [
            capabilities.checkpoint("inference-done"),
            capabilities.wait(),
          ];
        }

        // Conversational agent: send reply via the connector.
        const replyContent = extractTextContent(event.turn);
        if (replyContent.length > 0) {
          return [
            capabilities.checkpoint("inference-done"),
            capabilities.reply(replyContent),
          ];
        }

        // Empty response (no text, no tool calls) — checkpoint and wait for
        // the next inbound message. The reactor only shuts down on explicit
        // stop (abort), never because the model produced an empty turn.
        return [capabilities.checkpoint("inference-done"), capabilities.wait()];
      }

      case "resume.execute_tools": {
        // A resumed approval re-runs its parked tool call. The call is still
        // outstanding in its batch, and its tool.done answers it. A call this
        // director does not know gets its batch back from history, or a batch
        // of its own when history holds none.
        const unplaced = event.calls.filter(
          (call) =>
            !this.outstanding.has(call.id) &&
            this.recoverBatch(call.id, state) === undefined,
        );
        if (unplaced.length > 0) this.openBatch(unplaced);
        return capabilities.executeTools(event.calls, false, true);
      }

      case "resume.tool_result": {
        // A parked approval ended without running its tool (rejected or timed
        // out). The reactor appends the synthetic error result that answers the
        // parked call. Re-infer only when that answer completes its batch, so
        // the model sees every result of the batch at once.
        if (!this.answer(event.result.callId, state)) return [];
        return [
          capabilities.checkpoint("resume-tool-result"),
          capabilities.infer({
            systemPrompt: this.systemPrompt,
            tools: this.toolDefinitions,
          }),
        ];
      }

      case "tool.done": {
        if (!this.answer(event.result.callId, state)) {
          return [];
        }
        if (this.policy.mode === "reactive") {
          return [capabilities.checkpoint("tool-done"), capabilities.wait()];
        }
        // All tool results received — re-infer with complete context.
        return [
          capabilities.checkpoint("tool-done"),
          capabilities.infer({
            systemPrompt: this.systemPrompt,
            tools: this.toolDefinitions,
          }),
        ];
      }

      case "inference.error": {
        const statusDetail =
          event.error.statusCode !== undefined
            ? ` [HTTP ${event.error.statusCode}]`
            : "";

        logger.error`Inference error in default director: ${event.error.message}${statusDetail} (category: ${event.error.category})`;

        const userMessage = formatInferenceError(event.error);
        return [
          capabilities.checkpoint("inference-error"),
          capabilities.reply(userMessage),
        ];
      }

      case "reactor.gate.cleared": {
        return [
          capabilities.checkpoint("gate-cleared"),
          capabilities.infer({
            systemPrompt: this.systemPrompt,
            tools: this.toolDefinitions,
          }),
        ];
      }

      case "abort": {
        return capabilities.done();
      }
    }
  }
}

export function createDefaultDirector(
  systemPrompt: string,
  toolDefinitions: ToolDefinition[] = [],
  policy: DefaultDirectorPolicy = {},
): ReactorDirector {
  return new DefaultDirector(systemPrompt, toolDefinitions, policy);
}
