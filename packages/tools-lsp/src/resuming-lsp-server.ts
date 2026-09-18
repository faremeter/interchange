// A server that completes the initialize handshake, stops draining stdin,
// and works through the whole backlog when it is sent SIGUSR2 -- the shape of
// a language server wedged in a synchronous operation that later finishes.
// Raw framing, so no library holds stdin in flowing mode after the pause.
//
// It answers `intx/versionsReceived` with the document versions it has parsed
// so far. That answer rides the same stream as the notifications and is
// written only after them, so a test learns what the server saw by asking
// rather than by waiting for a duration to pass.
let buf = Buffer.alloc(0);
let paused = false;
const received: string[] = [];

function reply(id: unknown, result: unknown) {
  const body = Buffer.from(
    JSON.stringify({ jsonrpc: "2.0", id, result }),
    "utf8",
  );
  process.stdout.write(`Content-Length: ${body.byteLength}\r\n\r\n`);
  process.stdout.write(body);
}

function requireVersion(msg: object): number {
  if (!("params" in msg))
    throw new Error("document notification has no params");
  const params: unknown = msg.params;
  if (typeof params !== "object" || params === null) {
    throw new Error("document notification params is not an object");
  }
  if (!("textDocument" in params)) {
    throw new Error("document notification has no textDocument");
  }
  const textDocument: unknown = params.textDocument;
  if (typeof textDocument !== "object" || textDocument === null) {
    throw new Error("textDocument is not an object");
  }
  if (!("version" in textDocument)) {
    throw new Error("textDocument has no version");
  }
  const version: unknown = textDocument.version;
  if (typeof version !== "number") throw new Error("version is not a number");
  return version;
}

function handleMessage(msg: unknown) {
  if (typeof msg !== "object" || msg === null || !("method" in msg)) return;
  const method: unknown = msg.method;
  if (method === "initialize" && "id" in msg) {
    reply(msg.id, { capabilities: { textDocumentSync: 1 } });
    // From here on the server reads no stdin until SIGUSR2 arrives.
    paused = true;
    process.stdin.removeListener("data", onData);
    process.stdin.pause();
    return;
  }
  if (method === "intx/versionsReceived" && "id" in msg) {
    reply(msg.id, { received: [...received] });
    return;
  }
  if (method === "textDocument/didOpen") {
    received.push(`didOpen v${String(requireVersion(msg))}`);
    return;
  }
  if (method === "textDocument/didChange") {
    received.push(`didChange v${String(requireVersion(msg))}`);
  }
}

function drain() {
  while (!paused) {
    const sep = buf.indexOf("\r\n\r\n");
    if (sep === -1) return;
    const header = buf.subarray(0, sep).toString("ascii");
    const m = /Content-Length: (\d+)/i.exec(header);
    if (m === null) throw new Error(`no Content-Length in header: ${header}`);
    const len = Number(m[1]);
    const start = sep + 4;
    if (buf.byteLength < start + len) return;
    const msg: unknown = JSON.parse(
      buf.subarray(start, start + len).toString("utf8"),
    );
    buf = buf.subarray(start + len);
    handleMessage(msg);
  }
}

function onData(chunk: Buffer) {
  buf = Buffer.concat([buf, chunk]);
  drain();
}

process.on("SIGUSR2", () => {
  if (!paused) return;
  paused = false;
  process.stdin.on("data", onData);
  process.stdin.resume();
  drain();
});

process.stdin.on("data", onData);
setInterval(() => undefined, 60_000);
