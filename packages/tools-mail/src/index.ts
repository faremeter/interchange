// Public surface for @intx/tools-mail.
//
// createMailTools resolves the bound agent's MessageTransport from the
// supplied RuntimeCapabilities once at handler-init and wires the mail
// handlers around it. The returned MailTools satisfies the
// ToolRunner contract the harness consumes.

import type {
  ToolDefinition,
  ToolRunner,
  ToolResult,
} from "@intx/types/runtime";
import type { RuntimeCapabilities } from "@intx/types/runtime-capabilities";

import { TOOL_DEFINITIONS } from "./definitions";
import { errorResult } from "./errors";
import {
  makeMailExpungeHandler,
  makeMailFlagHandler,
  makeMailReadHandler,
  makeMailReplyHandler,
  makeMailSearchHandler,
  makeMailSendHandler,
  makeMailWaitHandler,
  type ToolHandler,
} from "./handlers";

export { TOOL_DEFINITIONS } from "./definitions";
export type { MailToolName } from "./definitions";
export type { MailToolErrorCode } from "./errors";

export interface MailToolsOptions {
  capabilities: RuntimeCapabilities;
}

export interface MailTools extends ToolRunner {
  readonly definitions: ToolDefinition[];
  dispose(): Promise<void>;
}

export function createMailTools(opts: MailToolsOptions): MailTools {
  // Resolve the transport once at handler-init. The lifecycle contract is
  // "request once at handler-init, hold the handle for the deploy
  // lifetime"; the handler factories below close over the resolved
  // handle and do not re-consult capabilities.
  const transport = opts.capabilities.resolve("mail.transport");

  const handlers = new Map<string, ToolHandler>([
    ["mail_send", makeMailSendHandler(transport)],
    ["mail_reply", makeMailReplyHandler(transport)],
    ["mail_search", makeMailSearchHandler(transport)],
    ["mail_read", makeMailReadHandler(transport)],
    ["mail_wait", makeMailWaitHandler(transport)],
    ["mail_flag", makeMailFlagHandler(transport)],
    ["mail_expunge", makeMailExpungeHandler(transport)],
  ]);

  let disposed = false;

  return {
    definitions: TOOL_DEFINITIONS,
    async run(call, signal): Promise<ToolResult> {
      const handler = handlers.get(call.name);
      if (handler === undefined) {
        // This branch is unreachable in the sidecar composition: the
        // agent's `resolveTools` dispatches `call.name` to the owning
        // bundle by definition.name, so only names this runner
        // declared can ever reach `run`. It exists so callers that
        // use createMailTools as a standalone ToolRunner (rather than
        // through `defineMailTools`) get the package's native
        // object-shaped error.
        return errorResult(
          call.id,
          `Unknown tool: "${call.name}"`,
          "unknown_tool",
        );
      }
      try {
        return await handler(call, signal);
      } catch (err) {
        const message =
          err instanceof Error ? err.message : `unknown error: ${String(err)}`;
        return errorResult(call.id, message, "internal_error");
      }
    },
    async dispose() {
      if (disposed) return;
      disposed = true;
      // No-op today: the transport is owned by the host that
      // constructed it, and the only handler with active resources
      // (mail_wait subscribes via transport.watch and registers a
      // setTimeout / abort listener) releases them through the
      // per-call AbortSignal rather than through this dispose hook.
      // dispose exists for symmetry with createPosixTools and as a
      // seam for any future per-package resources; callers must not
      // rely on it to cancel in-flight tool calls.
    },
  };
}
