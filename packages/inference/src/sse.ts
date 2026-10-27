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

export async function* parseSSE(
  stream: ReadableStream<Uint8Array>,
): AsyncIterable<string> {
  const reader = stream.getReader();
  // One decoder per stream: a streaming decoder holds the incomplete UTF-8
  // sequence at the end of a read, so a shared one splices bytes across
  // concurrent streams.
  const decoder = new TextDecoder();
  // Fragments of the line still waiting for its newline, joined once when
  // it arrives so a large field spread over many reads is not rescanned
  // or recopied on every read.
  const fragments: string[] = [];

  try {
    while (true) {
      const { done, value } = await reader.read();

      if (done) {
        // Flush any remaining content in the buffer as a final line.
        const buffer = fragments.join("");
        if (buffer.length > 0) {
          const payload = extractDataPayload(buffer);
          if (payload !== null) {
            yield payload;
          }
        }
        break;
      }

      const text = decoder.decode(value, { stream: true });

      // Process all complete lines (lines terminated by \n).
      // A line ending in \r\n counts as terminated at the \n.
      let start = 0;
      let newlineIndex: number;
      while ((newlineIndex = text.indexOf("\n", start)) !== -1) {
        const tail = text.slice(start, newlineIndex);
        start = newlineIndex + 1;
        let rawLine = tail;
        if (fragments.length > 0) {
          fragments.push(tail);
          rawLine = fragments.join("");
          fragments.length = 0;
        }

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
      if (start < text.length) {
        fragments.push(text.slice(start));
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
