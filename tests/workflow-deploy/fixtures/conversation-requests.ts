import type {
  InferenceMessage,
  InferenceRequest,
} from "../../hub-agent/lib/deploy-flow-env";

export function messageText(message: InferenceMessage | undefined): string {
  const content = message?.content;
  if (content === undefined) return "";
  if (typeof content === "string") return content;
  return content.map((block) => block.text ?? "").join("");
}

/**
 * The one `step1` request for the turn whose inbound mail carries `marker`.
 * `step1`'s conversation opens with the mail carrying `openingMarker`, which
 * tells its requests apart from `step2`'s, whose input is `step1`'s last
 * reply.
 */
export function step1RequestFor(
  requests: readonly InferenceRequest[],
  openingMarker: string,
  marker: string,
): InferenceRequest {
  const matches = requests.filter(
    (request) =>
      messageText(request.messages?.[0]).includes(openingMarker) &&
      messageText(request.messages?.at(-1)).includes(marker),
  );
  const [request, ...rest] = matches;
  if (request === undefined || rest.length > 0) {
    throw new Error(
      `expected one step1 request for ${marker}, found ${String(matches.length)}`,
    );
  }
  return request;
}

/**
 * The adapter marks the newest user message as a prompt-cache breakpoint, so
 * the same message carries the marker on one request and not the next. The
 * marker is request framing, not conversation, so it is dropped before
 * comparing context across requests.
 */
export function withoutCacheMarkers(messages: InferenceMessage[]): unknown[] {
  return messages.map((message) =>
    typeof message.content === "string" || message.content === undefined
      ? message
      : {
          ...message,
          content: message.content.map((block) =>
            Object.fromEntries(
              Object.entries(block).filter(([key]) => key !== "cache_control"),
            ),
          ),
        },
  );
}
