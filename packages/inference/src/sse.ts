// SSE byte-stream parser: ReadableStream<Uint8Array> → AsyncIterable<string>
// of `data:` payloads. Blank lines, `:` comments, and the `[DONE]` sentinel
// are consumed internally; incomplete lines buffer across chunk boundaries.

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

      // Process all complete lines; \r\n terminates at the \n.
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
