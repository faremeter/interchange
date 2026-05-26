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

export type DirectorPolicy = {
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
  private readonly policy: DirectorPolicy;

  // Track outstanding tool results so we only re-infer once per batch.
  private pendingToolResults = 0;

  constructor(
    systemPrompt: string,
    toolDefinitions: ToolDefinition[] = [],
    policy: DirectorPolicy = {},
  ) {
    this.systemPrompt = systemPrompt;
    this.toolDefinitions = toolDefinitions;
    this.policy = policy;
  }

  async decide(
    event: ReactorInboundEvent,
    _state: ReactorState,
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
  policy: DirectorPolicy = {},
): ReactorDirector {
  return new DefaultDirector(systemPrompt, toolDefinitions, policy);
}
