// Default conversational plugin for the agent harness.
//
// Implements the decision table from INFERENCE.md § Plugin Decision Function:
//
//   message.received          → infer
//   inference.done (tools)    → checkpoint + execute_tools
//   tool.done                 → checkpoint + infer (re-infer with tool results)
//   inference.done (no tools) → checkpoint + reply (connector sends the message)
//   inference.error           → checkpoint + reply (error message to user)
//   abort                     → done
//   reactor.gate.cleared      → checkpoint + infer (resume after gate)
//
// The plugin never throws. Inference errors are surfaced to the user as a
// reply so the problem is visible, and the agent remains alive for retries.

import { getLogger } from "@interchange/log";
import type {
  ReactorPlugin,
  ReactorInboundEvent,
  ReactorState,
  ReactorCapabilities,
  ReactorAction,
  AssistantMessage,
  ToolCall,
  ToolDefinition,
} from "@interchange/types/runtime";
import type { PluginPolicy } from "./config";

const logger = getLogger(["interchange", "harness", "plugin"]);

function extractToolCalls(message: AssistantMessage): ToolCall[] {
  const calls: ToolCall[] = [];
  for (const block of message.content) {
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

function extractTextContent(message: AssistantMessage): string {
  const parts: string[] = [];
  for (const block of message.content) {
    if (block.type === "text") {
      parts.push(block.text);
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

export class DefaultPlugin implements ReactorPlugin {
  private readonly model: string;
  private readonly systemPrompt: string;
  private readonly toolDefinitions: ToolDefinition[];
  private readonly policy: PluginPolicy;

  // Track outstanding tool results so we only re-infer once per batch.
  private pendingToolResults = 0;

  constructor(
    model: string,
    systemPrompt: string,
    toolDefinitions: ToolDefinition[] = [],
    policy: PluginPolicy = {},
  ) {
    this.model = model;
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
        return capabilities.infer(this.model, {
          systemPrompt: this.systemPrompt,
          tools: this.toolDefinitions,
        });
      }

      case "inference.done": {
        const toolCalls = extractToolCalls(event.message);
        if (toolCalls.length > 0) {
          this.pendingToolResults = toolCalls.length;
          return [
            capabilities.checkpoint(),
            capabilities.executeTools(toolCalls, true),
          ];
        }

        // No tool calls — the model is done reasoning for this turn.
        if (this.policy.mode === "reactive") {
          return [capabilities.checkpoint(), capabilities.wait()];
        }

        // Conversational agent: send reply via the connector.
        const replyContent = extractTextContent(event.message);
        if (replyContent.length > 0) {
          return [capabilities.checkpoint(), capabilities.reply(replyContent)];
        }

        // Empty response (no text, no tool calls) — checkpoint and wait for
        // the next inbound message. The reactor only shuts down on explicit
        // stop (abort), never because the model produced an empty turn.
        return [capabilities.checkpoint(), capabilities.wait()];
      }

      case "tool.done": {
        this.pendingToolResults--;
        if (this.pendingToolResults > 0) {
          return [];
        }
        if (this.policy.mode === "reactive") {
          return [capabilities.checkpoint(), capabilities.wait()];
        }
        // All tool results received — re-infer with complete context.
        return [
          capabilities.checkpoint(),
          capabilities.infer(this.model, {
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

        logger.error`Inference error in default plugin: ${event.error.message}${statusDetail} (category: ${event.error.category})`;

        const userMessage = formatInferenceError(event.error);
        return [capabilities.checkpoint(), capabilities.reply(userMessage)];
      }

      case "reactor.gate.cleared": {
        return [
          capabilities.checkpoint(),
          capabilities.infer(this.model, {
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

export function createDefaultPlugin(
  model: string,
  systemPrompt: string,
  toolDefinitions: ToolDefinition[] = [],
  policy: PluginPolicy = {},
): ReactorPlugin {
  return new DefaultPlugin(model, systemPrompt, toolDefinitions, policy);
}
