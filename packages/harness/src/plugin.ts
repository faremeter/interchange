// Default conversational plugin for the agent harness.
//
// Implements the decision table from INFERENCE.md § Plugin Decision Function:
//
//   message.received          → infer
//   inference.done (tools)    → execute_tools
//   tool.done                 → infer (re-infer with tool results)
//   inference.done (no tools) → reply (connector sends the message)
//   inference.error           → log error, done
//   abort                     → done
//   reactor.gate.cleared      → infer (resume after gate)
//
// The plugin never throws. Inference errors are logged and the plugin returns
// done so the reactor shuts down cleanly rather than re-trying indefinitely.

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
          return capabilities.executeTools(toolCalls, true);
        }

        // No tool calls — the model is done reasoning for this turn.
        if (this.policy.mode === "reactive") {
          // Reactive agent: discard any text output and wait for the next
          // inbound event. The agent's job was to execute tools, not reply.
          return capabilities.wait();
        }

        // Conversational agent: send reply via the connector.
        const replyContent = extractTextContent(event.message);
        if (replyContent.length > 0) {
          return capabilities.reply(replyContent);
        }

        return capabilities.done();
      }

      case "tool.done": {
        this.pendingToolResults--;
        if (this.pendingToolResults > 0) {
          return [];
        }
        if (this.policy.mode === "reactive") {
          return capabilities.wait();
        }
        // All tool results received — re-infer with complete context.
        return capabilities.infer(this.model, {
          systemPrompt: this.systemPrompt,
          tools: this.toolDefinitions,
        });
      }

      case "inference.error": {
        const statusDetail =
          event.error.statusCode !== undefined
            ? ` [HTTP ${event.error.statusCode}]`
            : "";

        logger.error`Inference error in default plugin: ${event.error.message}${statusDetail} (category: ${event.error.category})`;
        return capabilities.done();
      }

      case "reactor.gate.cleared": {
        return capabilities.infer(this.model, {
          systemPrompt: this.systemPrompt,
          tools: this.toolDefinitions,
        });
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
