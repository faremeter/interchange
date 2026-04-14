// Server-Sent Events byte stream parser.
//
// Converts a ReadableStream<Uint8Array> (the raw HTTP response body) into an
// AsyncIterable<string> of SSE data payloads. Each yielded string is the
// value of one `data:` field. Comments (`:`) and blank-line separators are
// consumed internally. The `[DONE]` sentinel (OpenAI convention) terminates
// the iteration.
//
// The parser buffers incomplete lines across chunk boundaries so split chunks
// are handled correctly regardless of where chunk boundaries fall.

const decoder = new TextDecoder();

export async function* parseSSE(
  stream: ReadableStream<Uint8Array>,
): AsyncIterable<string> {
  const reader = stream.getReader();
  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();

      if (done) {
        // Flush any remaining content in the buffer as a final line.
        if (buffer.length > 0) {
          const payload = extractDataPayload(buffer);
          if (payload !== null) {
            yield payload;
          }
        }
        break;
      }

      buffer += decoder.decode(value, { stream: true });

      // Process all complete lines (lines terminated by \n).
      // A line ending in \r\n counts as terminated at the \n.
      let newlineIndex: number;
      while ((newlineIndex = buffer.indexOf("\n")) !== -1) {
        const rawLine = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);

        // Strip trailing \r for CRLF line endings.
        const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;

        // Blank lines and comment lines are ignored.
        if (line === "" || line.startsWith(":")) {
          continue;
        }

        const payload = extractDataPayload(line);
        if (payload === null) {
          continue;
        }

        if (payload === "[DONE]") {
          return;
        }

        yield payload;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

function extractDataPayload(line: string): string | null {
  if (line.startsWith("data:")) {
    // The spec allows an optional space after the colon.
    const raw = line.slice(5);
    return raw.startsWith(" ") ? raw.slice(1) : raw;
  }
  return null;
}
