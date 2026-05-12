import type { MailResponse } from "@interchange/types";

export type MailAddress = { name: string | null; email: string };

export type AgentActivity =
  | { type: "inferring" }
  | { type: "tool_call"; name: string }
  | { type: "tool_running"; name: string }
  | { type: "rate_limited"; retryAfterMs: number };

export type InstanceEvent =
  | {
      kind: "mail";
      id: string;
      role: "user" | "assistant";
      content: string;
      sender: MailAddress;
      recipients: MailAddress[];
      timestamp: string;
      attachments: MailResponse["attachments"];
      isError?: boolean;
    }
  | {
      kind: "turn";
      turnId: string;
      content: string;
      timestamp: string;
      isError?: boolean;
      errors?: { category: string; message: string }[];
      toolCalls?: ToolCallEvent[];
      toolErrors?: { name: string; content: string }[];
    };

export type ToolCallEvent = {
  name: string;
  arguments: Record<string, unknown>;
  result: string;
  isError: boolean;
};
