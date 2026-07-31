// Shared inbound-mail conversation-text extraction. Two call sites resolve a
// run's mail input to the text the agent actually sees, each at the layer that
// knows its bytes are inbound mail: the child's `resolveTriggerPayload` (turn
// 1, the trigger) reads the mail from the substrate, and the supervisor's
// dispatch loop (turn 2+, a mail delivered to a parked run as a signal) inlines
// it. Both must produce the SAME shape -- extracted body text, not the raw MIME
// envelope -- so the extractor lives here rather than in either caller.

import {
  extractPartByPath,
  parseHeaderSection,
  parseMimePart,
} from "@intx/mime";

/**
 * Extract the conversation body text from a raw inbound MIME message.
 *
 * Three on-wire shapes are handled, matching every producer the mail
 * bus accepts:
 *   1. The Interchange assembler's `multipart/signed` envelope whose
 *      first part is a `multipart/mixed` body carrying the text at part
 *      path `1.1`.
 *   2. A `multipart/signed` envelope wrapping a bare `text/plain` part
 *      (a sender that signs without the `multipart/mixed` wrapper); the
 *      text is at part path `1`.
 *   3. A flat top-level `text/plain` message (no multipart structure at
 *      all); the body is the bytes after the header section.
 *
 * The top-level `Content-Type` selects the shape: only a `multipart/*`
 * root walks into parts; anything else reads the single body directly.
 * This mirrors the conversation branch of mail-memory's `fetchFull`
 * while also tolerating the flat single-part case the in-process agent
 * accepts, so a non-standard inbound mail still delivers its text to
 * the agent rather than crashing the run. `messageId` is used only for
 * error attribution.
 */
export function extractConversationText(
  raw: Uint8Array,
  messageId: string,
): string {
  const { headers, bodyOffset } = parseHeaderSection(raw);
  const rootMime = (headers.get("content-type") ?? "")
    .split(";")[0]
    ?.trim()
    .toLowerCase();
  if (rootMime === undefined || !rootMime.startsWith("multipart/")) {
    // Flat single-part message: the body is everything after the
    // header section.
    return new TextDecoder("utf-8", { fatal: false }).decode(
      raw.subarray(bodyOffset),
    );
  }
  let part1: ReturnType<typeof parseMimePart>;
  try {
    part1 = parseMimePart(extractPartByPath(raw, "1"));
  } catch (cause) {
    throw new Error(
      `conversation-text: cannot parse inbound mail part 1 for messageId ${messageId}`,
      { cause },
    );
  }
  const part1Mime = (part1.contentType.split(";")[0] ?? "")
    .trim()
    .toLowerCase();
  const bodyBytes = part1Mime.startsWith("multipart/")
    ? parseMimePart(extractPartByPath(raw, "1.1")).body
    : part1.body;
  return new TextDecoder("utf-8", { fatal: false }).decode(bodyBytes);
}
