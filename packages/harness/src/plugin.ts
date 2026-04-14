// Default conversational plugin for the agent harness.
//
// Implements the decision table from INFERENCE.md § Plugin Decision Function:
//
//   message.received        → infer
//   inference.done (tools)  → execute_tools
//   tool.done               → infer (re-infer with tool results)
//   inference.done (no tools) → send reply via message.send, then wait (done)
//   inference.error         → log error, wait (done)
//   abort                   → done
//   reactor.gate.cleared    → infer (resume after gate)
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
} from "@interchange/types/runtime";

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

  // The address to reply to for the most recent inbound message.
  private pendingReplyTo: string | undefined = undefined;
  private pendingSubject: string | undefined = undefined;
  private pendingInReplyTo: string | undefined = undefined;

  // Track outstanding tool results so we only re-infer once per batch.
  private pendingToolResults = 0;

  constructor(model: string, systemPrompt: string) {
    this.model = model;
    this.systemPrompt = systemPrompt;
  }

  async decide(
    event: ReactorInboundEvent,
    _state: ReactorState,
    capabilities: ReactorCapabilities,
  ): Promise<ReactorAction | ReactorAction[]> {
    switch (event.type) {
      case "message.received": {
        const msg = event.message;

        // Record sender for the eventual reply.
        this.pendingReplyTo = msg.headers.from;
        this.pendingSubject = msg.headers.subject;
        this.pendingInReplyTo = msg.headers.messageId;

        return capabilities.infer(this.model, {
          systemPrompt: this.systemPrompt,
        });
      }

      case "inference.done": {
        const toolCalls = extractToolCalls(event.message);
        if (toolCalls.length > 0) {
          this.pendingToolResults = toolCalls.length;
          return capabilities.executeTools(toolCalls, true);
        }

        // No tool calls: send reply and signal completion.
        const replyContent = extractTextContent(event.message);
        if (replyContent.length > 0 && this.pendingReplyTo !== undefined) {
          const sendCall: ToolCall = {
            id: "harness-reply",
            name: "message.send",
            arguments: {
              to: this.pendingReplyTo,
              content: replyContent,
              type: "conversation.message",
              ...(this.pendingSubject !== undefined
                ? { subject: this.pendingSubject }
                : {}),
              ...(this.pendingInReplyTo !== undefined
                ? { inReplyTo: this.pendingInReplyTo }
                : {}),
            },
          };
          this.pendingReplyTo = undefined;
          this.pendingSubject = undefined;
          this.pendingInReplyTo = undefined;

          return [capabilities.executeTools([sendCall]), capabilities.done()];
        }

        return capabilities.done();
      }

      case "tool.done": {
        this.pendingToolResults--;
        if (this.pendingToolResults > 0) {
          return [];
        }
        // All tool results received — re-infer with complete context.
        return capabilities.infer(this.model, {
          systemPrompt: this.systemPrompt,
        });
      }

      case "inference.error": {
        // eslint-disable-next-line @typescript-eslint/no-unused-expressions
        logger.error`Inference error in default plugin: ${event.error.message} (category: ${event.error.category})`;
        return capabilities.done();
      }

      case "reactor.gate.cleared": {
        return capabilities.infer(this.model, {
          systemPrompt: this.systemPrompt,
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
): ReactorPlugin {
  return new DefaultPlugin(model, systemPrompt);
}
