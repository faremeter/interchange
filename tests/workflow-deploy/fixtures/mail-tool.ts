// The `mail_send` tool as a real, type-checked `defineTool` source module.
//
// Reproduces the three variants of the ex-synthetic `@intx/tools-mail`
// bundle (`buildSyntheticToolsMailTarball` in the deploy-flow env) as a real
// module so a code-sourced workflow carries the tool in its own source
// closure. A fixture entry module imports `mailSendTool(variant)` and lists it
// in a step agent's `tools`; `bundleWorkflowEntry` inlines this module into the
// workflow bundle, so the tool evaluates in-child with the same filesystem /
// transport side effect the synthetic bundle produced.
//
// Three variants, selected by the `variant` parameter:
//   - "fs":        writes the `body` arg as a filename under `env.workdir`
//                  with the `to` arg as content. Declares no transport
//                  requirement, so a run that never drives the model to call
//                  the tool leaves the write inert.
//   - "transport": calls `env.transport.send(...)` -- the supervisor-backed
//                  transport the unified child wires for a step agent -- and
//                  writes the sentinel only after a successful receipt, so the
//                  sentinel is a load-bearing proof of the signed-outbound
//                  composition. Declares `requires: ["transport", "address"]`.
//   - "ask":       identical filesystem behaviour to "fs" but stamps
//                  `approval: "ask"` on the static declaration, so the
//                  deploy-time capability walk derives an `ask` floor and a
//                  call suspends for approval.

import fs from "node:fs";
import path from "node:path";

import { defineTool, type BaseEnv } from "@intx/agent";

/** The tool variant a fixture entry selects. */
export type MailToolVariant = "fs" | "transport" | "ask";

/**
 * The bundle id. The model-facing tool name mirrors the name the synthetic
 * tool-package loader synthesized (`<bundleId>:mail_send`) so a migrated test
 * keeps the same `tool:@intx/tools-mail/sidecar-bundle:mail_send` grant and
 * tool-name strings it asserted against the pinned bundle.
 */
export const MAIL_TOOL_BUNDLE_ID = "@intx/tools-mail/sidecar-bundle";

/** The model-facing tool name (bundle id + `mail_send`). */
export const MAIL_TOOL_NAME = `${MAIL_TOOL_BUNDLE_ID}:mail_send`;

/**
 * Env extension for the transport-backed variant. `transport` is opaque here
 * (typed `unknown`); the `send` shape is exercised at run time against the
 * supervisor-backed transport the child wires.
 */
export interface MailToolEnv extends BaseEnv {
  transport: {
    send: (
      message: { to: string; type: string; content: string },
      signal: AbortSignal,
    ) => Promise<{ messageId: string; status: string }>;
  };
  address: string;
}

function argString(
  args: Record<string, unknown>,
  key: string,
): string | undefined {
  const value = args[key];
  return typeof value === "string" ? value : undefined;
}

/**
 * Build the `mail_send` tool factory for the requested variant. The returned
 * `AnnotatedToolFactory` carries the static `mail_send` declaration the
 * capability walk reads (with the `ask` mark for the "ask" variant) and, at
 * run, reproduces the variant's filesystem / transport side effect.
 */
export function mailSendTool(variant: MailToolVariant) {
  const approvalMarked = variant === "ask";
  const staticDefinition = approvalMarked
    ? { name: MAIL_TOOL_NAME, approval: "ask" as const }
    : { name: MAIL_TOOL_NAME };
  const requires = variant === "transport" ? ["transport", "address"] : [];

  return defineTool<MailToolEnv>({
    id: MAIL_TOOL_BUNDLE_ID,
    requires,
    definitions: [staticDefinition],
    factory: (env) => ({
      definitions: [
        {
          name: MAIL_TOOL_NAME,
          description: "Send a mail message",
          inputSchema: {
            type: "object",
            properties: { to: { type: "string" }, body: { type: "string" } },
            required: ["to", "body"],
          },
        },
      ],
      run: async (call, signal) => {
        const args = call.arguments;
        const filename = argString(args, "body") ?? "tool-ran.txt";
        if (variant === "transport") {
          const to = argString(args, "to") ?? env.address;
          const receipt = await env.transport.send(
            {
              to,
              type: "conversation.message",
              content: "Reply produced by the unified-host step agent.",
            },
            signal,
          );
          await fs.promises.mkdir(env.workdir, { recursive: true });
          await fs.promises.writeFile(
            path.join(env.workdir, filename),
            JSON.stringify({
              messageId: receipt.messageId,
              status: receipt.status,
            }),
          );
          return { callId: call.id, content: `sent ${receipt.messageId}` };
        }
        const content = argString(args, "to") ?? "ok";
        await fs.promises.mkdir(env.workdir, { recursive: true });
        await fs.promises.writeFile(path.join(env.workdir, filename), content);
        return { callId: call.id, content: `wrote ${filename}` };
      },
    }),
  });
}
