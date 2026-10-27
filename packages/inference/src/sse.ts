// Server-Sent Events byte stream parser.
//
// Converts a ReadableStream<Uint8Array> (the raw HTTP response body) into an
// AsyncIterable<string> of SSE data payloads. Each yielded string is the
// value of one `data:` field. Comments (`:`) and blank-line separators are
// consumed internally. The `[DONE]` sentinel (OpenAI convention) terminates
// the iteration.
//
// The parser buffers incomplete lines across chunk boundaries so split chunks
// are handled correctly regardless of where chunk boundaries fall. A final
// line without a terminator is parsed by the same rules at end of stream.

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
        fragments.push(decoder.decode());
        const payload = parseLine(fragments.join(""));
        if (payload !== null && payload !== DONE) {
          yield payload;
        }
        return;
      }

      const text = decoder.decode(value, { stream: true });
      let start = 0;
      let newlineIndex: number;
      while ((newlineIndex = text.indexOf("\n", start)) !== -1) {
        const tail = text.slice(start, newlineIndex);
        start = newlineIndex + 1;
        let line = tail;
        if (fragments.length > 0) {
          fragments.push(tail);
          line = fragments.join("");
          fragments.length = 0;
        }

        const payload = parseLine(line);
        if (payload === null) {
          continue;
        }
        if (payload === DONE) {
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

const DONE = "[DONE]";

// Returns the data payload of one line, or null for blank lines, comments,
// and non-data fields. A trailing \r from a CRLF ending is dropped.
function parseLine(rawLine: string): string | null {
  const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
  if (!line.startsWith("data:")) {
    return null;
  }
  // The spec allows an optional space after the colon.
  const raw = line.slice(5);
  return raw.startsWith(" ") ? raw.slice(1) : raw;
}
