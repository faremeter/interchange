// Tool-result size-cap transform.
//
// When a tool result's content exceeds `maxChars`, the full bytes are spilled
// via `contextStore.writeBlob` and the inline result is replaced with the
// first `maxChars` characters plus a marker pointing at the spill via a
// `tool-output:///{callId}` URI. The agent's `read_file` tool resolves the
// URI through its `BlobReader` capability.
//
// Within-cap results pass through unchanged (no blob is written) but still
// produce a `TransformRecord` so the manifest captures every invocation.

import type {
  ContextStore,
  StrategyContext,
  StrategyResult,
  ToolResult,
  ToolResultTransform,
} from "@intx/types/runtime";

const SIZE_CAP_VERSION = "1";
const SIZE_CAP_NAME = "size-cap";

export type SizeCapTransformOptions = {
  maxChars: number;
  contextStore: Pick<ContextStore, "writeBlob">;
};

/**
 * Create a `ToolResultTransform` that caps inline tool result content at
 * `maxChars` characters. Oversized results are spilled to the context store
 * via `writeBlob` and the inline content becomes a truncated marker
 * referencing the spill by `tool-output:///{callId}` URI.
 */
export function createSizeCapTransform(
  options: SizeCapTransformOptions,
): ToolResultTransform {
  const { maxChars, contextStore } = options;
  if (!Number.isFinite(maxChars) || maxChars <= 0) {
    throw new Error(
      `createSizeCapTransform: maxChars must be a positive finite number, got ${String(maxChars)}`,
    );
  }

  return {
    name: SIZE_CAP_NAME,
    version: SIZE_CAP_VERSION,
    async apply(
      input: { call: { id: string; name: string }; result: ToolResult },
      _ctx: StrategyContext,
    ): Promise<StrategyResult<ToolResult>> {
      const { call, result } = input;
      const text =
        typeof result.content === "string"
          ? result.content
          : JSON.stringify(result.content);

      if (text.length <= maxChars) {
        return {
          output: result,
          record: {
            strategy: SIZE_CAP_NAME,
            version: SIZE_CAP_VERSION,
            parameters: { maxChars },
            reason: "within-cap",
            decisions: { callId: call.id, length: text.length },
          },
        };
      }

      const omitted = text.length - maxChars;
      const kept = text.slice(0, maxChars);
      const spillURI = `tool-output:///${call.id}`;
      const marker =
        `${kept}\n[Tool output truncated: omitted ${String(omitted)} chars. ` +
        `Full output available at ${spillURI} -- use read_file with that URI to see the rest.]`;

      const bytes = new TextEncoder().encode(text);
      await contextStore.writeBlob(call.id, bytes, "text/plain");

      const output: ToolResult = {
        ...result,
        content: marker,
      };

      return {
        output,
        record: {
          strategy: SIZE_CAP_NAME,
          version: SIZE_CAP_VERSION,
          parameters: { maxChars },
          reason: "exceeded-cap",
          decisions: {
            callId: call.id,
            originalLength: text.length,
            kept: maxChars,
            spillKey: call.id,
            spillURI,
          },
        },
        blobs: [
          {
            key: call.id,
            bytes,
            contentType: "text/plain",
          },
        ],
      };
    },
  };
}
