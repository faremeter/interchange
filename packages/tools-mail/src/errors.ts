import type { ToolResult } from "@intx/types/runtime";

export type MailToolErrorCode =
  | "invalid_arguments"
  | "invalid_query"
  | "invalid_mailbox"
  | "invalid_part"
  // The referenced message is gone, or its uid never named one.
  | "not_found"
  | "no_reply_address"
  | "search_failed"
  | "send_failed"
  // A message the tool observed to exist could not be read back.
  | "fetch_failed"
  | "flag_failed"
  | "expunge_failed"
  | "timeout"
  | "aborted"
  | "unknown_tool"
  // A defect in this package, not a condition the caller provoked.
  | "internal_error";

export function errorResult(
  callId: string,
  message: string,
  code: MailToolErrorCode,
): ToolResult {
  return { callId, content: { error: message, code }, isError: true };
}
