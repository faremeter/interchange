// Plugin interface types and capabilities factory.
//
// The capabilities object is passed to the plugin on every decision call.
// It provides a type-safe API for constructing reactor actions without
// requiring the plugin to import or construct action literals directly.
//
// (INFERENCE.md § Reactor Plugin › Core Plugin)

import type {
  ReactorAction,
  ReactorCapabilities,
  GateType,
  ForkMode,
  InferenceOptions,
  ToolCall,
} from "@interchange/types/runtime";

/**
 * Builds a frozen capabilities object. The same instance is reused across
 * calls since all methods are pure constructors.
 */
export function createCapabilities(): ReactorCapabilities {
  return {
    infer(model: string, options?: InferenceOptions): ReactorAction {
      return {
        type: "infer",
        model,
        ...(options !== undefined ? { options } : {}),
      };
    },

    executeTools(
      calls: ToolCall[],
      parallel?: boolean,
      addToHistory?: boolean,
    ): ReactorAction {
      return {
        type: "execute_tools",
        calls,
        ...(parallel !== undefined ? { parallel } : {}),
        ...(addToHistory !== undefined ? { addToHistory } : {}),
      };
    },

    suspend(gate: {
      type: GateType;
      gateId: string;
      timeoutMs: number;
      correlationId?: string;
    }): ReactorAction {
      return { type: "suspend", gate };
    },

    fork(mode: ForkMode, forkId: string): ReactorAction {
      return { type: "fork", mode, forkId };
    },

    emit(
      eventType: `custom.${string}`,
      data: Record<string, unknown>,
    ): ReactorAction {
      return { type: "emit", eventType, data };
    },

    reply(content: string): ReactorAction {
      return { type: "reply", content };
    },

    checkpoint(reason?: string): ReactorAction {
      return {
        type: "checkpoint",
        message: reason !== undefined ? `checkpoint: ${reason}` : "checkpoint",
      };
    },

    wait(): ReactorAction {
      return { type: "wait" };
    },

    done(): ReactorAction {
      return { type: "done" };
    },
  };
}
