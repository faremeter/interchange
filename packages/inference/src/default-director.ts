// Default conversational director — reference ReactorDirector implementation.
//
// Implements the decision table from INFERENCE.md § Director Decision Function:
//
//   message.received          → infer
//   inference.done (tools)    → checkpoint + execute_tools
//   tool.done                 → checkpoint + infer (re-infer with tool results)
//   inference.done (no tools) → checkpoint + reply (connector sends the message)
//   inference.error           → checkpoint + reply (error message to user)
//   abort                     → done
//   reactor.gate.cleared      → checkpoint + infer (resume after gate)
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
  // Both regular text and refusal blocks carry human-readable model
  // output that the connector needs to surface — a refusal-only turn
  // (OpenAI strict-mode policy decline) would otherwise route through
  // the empty-response branch below and never reach the reply path,
  // leaving the human waiting for an answer the model already
  // declined to give. The structural "this was a refusal" signal is
  // preserved at the persistence layer (event-collector emits a
  // refusal turn-part); the reply path only needs the words.
  const parts: string[] = [];
  for (const block of turn.content) {
    if (block.type === "text") {
      parts.push(block.text);
    } else if (block.type === "refusal") {
      parts.push(block.reason);
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

  // Track outstanding tool results so we only re-infer once per batch.
  private pendingToolResults = 0;

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
          this.pendingToolResults = toolCalls.length;
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

      case "tool.done": {
        this.pendingToolResults--;
        if (this.pendingToolResults > 0) {
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
