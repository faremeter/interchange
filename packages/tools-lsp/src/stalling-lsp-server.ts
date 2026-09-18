// A server that completes the initialize handshake and then stops draining
// stdin -- the shape of a language server wedged in a synchronous operation.
// Raw framing, so no library holds stdin in flowing mode after the pause.
let buf = Buffer.alloc(0);

function reply(id: unknown) {
  const body = Buffer.from(
    JSON.stringify({
      jsonrpc: "2.0",
      id,
      result: { capabilities: { textDocumentSync: 1 } },
    }),
    "utf8",
  );
  process.stdout.write(`Content-Length: ${body.byteLength}\r\n\r\n`);
  process.stdout.write(body);
}

function onData(chunk: Buffer) {
  buf = Buffer.concat([buf, chunk]);
  for (;;) {
    const sep = buf.indexOf("\r\n\r\n");
    if (sep === -1) return;
    const header = buf.subarray(0, sep).toString("ascii");
    const m = /Content-Length: (\d+)/i.exec(header);
    if (m === null) return;
    const len = Number(m[1]);
    const start = sep + 4;
    if (buf.byteLength < start + len) return;
    const msg: unknown = JSON.parse(
      buf.subarray(start, start + len).toString("utf8"),
    );
    buf = buf.subarray(start + len);
    if (
      typeof msg === "object" &&
      msg !== null &&
      "method" in msg &&
      msg.method === "initialize" &&
      "id" in msg
    ) {
      reply(msg.id);
      // From here on the server never reads stdin again.
      process.stdin.removeListener("data", onData);
      process.stdin.pause();
      setInterval(() => undefined, 60_000);
      return;
    }
  }
}

process.stdin.on("data", onData);
