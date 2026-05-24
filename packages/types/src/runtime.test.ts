import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import {
  ContentBlock,
  InferenceEvent,
  MediaSource,
  TransformRecord,
  type ContextTransform,
  type ToolResultTransform,
  type Compactor,
  type ReactorAction,
  type ReactorCapabilities,
  type BlobReader,
  type BlobSource,
  createBlobReader,
  parseToolOutputURI,
} from "./runtime";

// ---------------------------------------------------------------------------
// 1. TransformRecord validator
// ---------------------------------------------------------------------------

describe("TransformRecord validator", () => {
  test("accepts a well-formed record", () => {
    const result = TransformRecord({
      strategy: "size-cap",
      version: "1",
      parameters: { maxChars: 10_000 },
      reason: "exceeded-cap",
      decisions: { callId: "abc", originalBytes: 50_000, kept: 10_000 },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a record with empty parameter and decision maps", () => {
    const result = TransformRecord({
      strategy: "noop",
      version: "1",
      parameters: {},
      reason: "no-op",
      decisions: {},
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects records missing required fields", () => {
    const missingStrategy = TransformRecord({
      version: "1",
      parameters: {},
      reason: "x",
      decisions: {},
    });
    expect(missingStrategy instanceof type.errors).toBe(true);

    const missingVersion = TransformRecord({
      strategy: "size-cap",
      parameters: {},
      reason: "x",
      decisions: {},
    });
    expect(missingVersion instanceof type.errors).toBe(true);

    const missingParameters = TransformRecord({
      strategy: "size-cap",
      version: "1",
      reason: "x",
      decisions: {},
    });
    expect(missingParameters instanceof type.errors).toBe(true);

    const missingReason = TransformRecord({
      strategy: "size-cap",
      version: "1",
      parameters: {},
      decisions: {},
    });
    expect(missingReason instanceof type.errors).toBe(true);

    const missingDecisions = TransformRecord({
      strategy: "size-cap",
      version: "1",
      parameters: {},
      reason: "x",
    });
    expect(missingDecisions instanceof type.errors).toBe(true);
  });

  test("rejects records with wrong-typed fields", () => {
    const wrongStrategy = TransformRecord({
      strategy: 123,
      version: "1",
      parameters: {},
      reason: "x",
      decisions: {},
    });
    expect(wrongStrategy instanceof type.errors).toBe(true);

    const wrongParameters = TransformRecord({
      strategy: "size-cap",
      version: "1",
      parameters: "not-a-record",
      reason: "x",
      decisions: {},
    });
    expect(wrongParameters instanceof type.errors).toBe(true);

    const wrongReason = TransformRecord({
      strategy: "size-cap",
      version: "1",
      parameters: {},
      reason: 42,
      decisions: {},
    });
    expect(wrongReason instanceof type.errors).toBe(true);

    const wrongDecisions = TransformRecord({
      strategy: "size-cap",
      version: "1",
      parameters: {},
      reason: "x",
      decisions: "not-a-record",
    });
    expect(wrongDecisions instanceof type.errors).toBe(true);
  });

  test("rejects non-object inputs", () => {
    expect(TransformRecord(null) instanceof type.errors).toBe(true);
    expect(TransformRecord("string") instanceof type.errors).toBe(true);
    expect(TransformRecord(42) instanceof type.errors).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 2. compact action variant
// ---------------------------------------------------------------------------

describe("compact action", () => {
  test("ReactorCapabilities.compact constructs the expected action shape", () => {
    const caps: Pick<ReactorCapabilities, "compact"> = {
      compact(compactor: string, reason: string): ReactorAction {
        return { type: "compact", compactor, reason };
      },
    };

    const action = caps.compact("summarize-tail", "capacity");

    expect(action.type).toBe("compact");
    if (action.type !== "compact") throw new Error("unreachable");
    expect(action.compactor).toBe("summarize-tail");
    expect(action.reason).toBe("capacity");
  });

  test("compact action is structurally compatible with the ReactorAction union", () => {
    const action: ReactorAction = {
      type: "compact",
      compactor: "summarize-tail",
      reason: "overflow-recovery",
    };

    if (action.type === "compact") {
      expect(action.compactor).toBe("summarize-tail");
      expect(action.reason).toBe("overflow-recovery");
    } else {
      throw new Error("expected compact narrowing");
    }
  });
});

// ---------------------------------------------------------------------------
// 3. Type-relationship sanity
// ---------------------------------------------------------------------------

describe("ContextTransform vs ToolResultTransform assignability", () => {
  test("a ContextTransform is not assignable to a ToolResultTransform", () => {
    // A ContextTransform operates on ConversationTurn[]; a ToolResultTransform
    // operates on { call, result }. Their input types are disjoint, so a
    // value typed as one cannot satisfy the other. The // @ts-expect-error
    // below proves the compiler enforces this — if the assignment ever
    // becomes valid (e.g. the input/output type parameters are widened),
    // the tsc check will fail loudly here.
    const ctxTransform: ContextTransform = {
      name: "ctx",
      version: "1",
      async apply(turns, _ctx) {
        return {
          output: turns,
          record: {
            strategy: "ctx",
            version: "1",
            parameters: {},
            reason: "noop",
            decisions: {},
          },
        };
      },
    };

    // @ts-expect-error -- ContextTransform input is ConversationTurn[]; ToolResultTransform input is { call, result }
    ctxTransform satisfies ToolResultTransform;

    expect(ctxTransform.name).toBe("ctx");
  });

  test("a ToolResultTransform is not assignable to a ContextTransform", () => {
    const trTransform: ToolResultTransform = {
      name: "tr",
      version: "1",
      async apply(input, _ctx) {
        return {
          output: input.result,
          record: {
            strategy: "tr",
            version: "1",
            parameters: {},
            reason: "noop",
            decisions: {},
          },
        };
      },
    };

    // @ts-expect-error -- ToolResultTransform input is { call, result }; ContextTransform input is ConversationTurn[]
    trTransform satisfies ContextTransform;

    expect(trTransform.name).toBe("tr");
  });

  test("Compactor and ContextTransform share input/output types", () => {
    // Compactor and ContextTransform are both
    // ContextStrategy<ConversationTurn[], ConversationTurn[]>, so any
    // value typed as one is also typed as the other. The reactor
    // enforces their role distinction at the registration layer, not
    // through the type system.
    const compactor: Compactor = {
      name: "summarize-tail",
      version: "1",
      async apply(turns, _ctx) {
        return {
          output: turns,
          record: {
            strategy: "summarize-tail",
            version: "1",
            parameters: {},
            reason: "noop",
            decisions: {},
          },
        };
      },
    };

    const asContextTransform: ContextTransform = compactor;
    expect(asContextTransform.name).toBe("summarize-tail");
  });
});

// ---------------------------------------------------------------------------
// 4. BlobReader URI parsing and dispatch
// ---------------------------------------------------------------------------

describe("parseToolOutputURI", () => {
  test("extracts the callId from a well-formed three-slash URI", () => {
    expect(parseToolOutputURI("tool-output:///abc123")).toBe("abc123");
  });

  test("preserves case in the callId", () => {
    expect(parseToolOutputURI("tool-output:///AbC123")).toBe("AbC123");
  });

  test("rejects the two-slash form because hostnames are lowercased", () => {
    let thrown: Error | undefined;
    try {
      parseToolOutputURI("tool-output://abc123");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("authority must be empty");
  });

  test("rejects a non-tool-output scheme", () => {
    let thrown: Error | undefined;
    try {
      parseToolOutputURI("file:///abc123");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain('expected "tool-output:"');
  });

  test("rejects extra path segments", () => {
    let thrown: Error | undefined;
    try {
      parseToolOutputURI("tool-output:///abc/extra");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("single callId segment");
  });

  test("rejects an empty callId", () => {
    let thrown: Error | undefined;
    try {
      parseToolOutputURI("tool-output:///");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("missing callId");
  });

  test("rejects a URI with a query string", () => {
    let thrown: Error | undefined;
    try {
      parseToolOutputURI("tool-output:///abc?x=1");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("query string is not allowed");
  });

  test("rejects a URI with a fragment", () => {
    let thrown: Error | undefined;
    try {
      parseToolOutputURI("tool-output:///abc#x");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("fragment is not allowed");
  });

  test("rejects a non-URI string", () => {
    let thrown: Error | undefined;
    try {
      parseToolOutputURI("not a uri");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("invalid tool-output URI");
  });
});

describe("createBlobReader", () => {
  test("delegates to source.readBlob with the parsed callId", async () => {
    const seen: string[] = [];
    const source: BlobSource = {
      async readBlob(key) {
        seen.push(key);
        return new TextEncoder().encode(`bytes-for-${key}`);
      },
    };
    const reader: BlobReader = createBlobReader(source);

    const bytes = await reader.read("tool-output:///CallId123");
    expect(seen).toEqual(["CallId123"]);
    expect(new TextDecoder().decode(bytes)).toBe("bytes-for-CallId123");
  });

  test("propagates errors from source.readBlob", async () => {
    const source: BlobSource = {
      async readBlob() {
        throw new Error("Blob not found for key: missing");
      },
    };
    const reader = createBlobReader(source);

    let thrown: Error | undefined;
    try {
      await reader.read("tool-output:///missing");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("Blob not found");
  });

  test("throws on malformed URIs without touching the source", async () => {
    let touched = false;
    const source: BlobSource = {
      async readBlob() {
        touched = true;
        return new Uint8Array();
      },
    };
    const reader = createBlobReader(source);

    let thrown: Error | undefined;
    try {
      await reader.read("file:///abc");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("invalid tool-output URI scheme");
    expect(touched).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// MediaSource validator and ImageBlock shape
// ---------------------------------------------------------------------------

describe("MediaSource validator", () => {
  test("accepts a well-formed base64 source", () => {
    const result = MediaSource({
      kind: "base64",
      mimeType: "image/png",
      data: "aGVsbG8=",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a well-formed file-reference source", () => {
    const result = MediaSource({
      kind: "file-reference",
      mimeType: "application/pdf",
      reference: "file_abc123",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a base64 source missing mimeType", () => {
    const result = MediaSource({ kind: "base64", data: "aGVsbG8=" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a base64 source missing data", () => {
    const result = MediaSource({ kind: "base64", mimeType: "image/png" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a file-reference source missing mimeType", () => {
    const result = MediaSource({
      kind: "file-reference",
      reference: "file_abc",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a file-reference source missing reference", () => {
    const result = MediaSource({
      kind: "file-reference",
      mimeType: "image/png",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an unknown kind", () => {
    const result = MediaSource({
      kind: "url",
      mimeType: "image/png",
      url: "https://example.com/img.png",
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("ImageBlock shape on ContentBlock", () => {
  test("accepts an image block with a base64 source", () => {
    const result = ContentBlock({
      type: "image",
      source: { kind: "base64", mimeType: "image/png", data: "aGVsbG8=" },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts an image block with a file-reference source", () => {
    const result = ContentBlock({
      type: "image",
      source: {
        kind: "file-reference",
        mimeType: "application/pdf",
        reference: "file_abc",
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects the legacy flat shape (mimeType/data without source)", () => {
    const result = ContentBlock({
      type: "image",
      mimeType: "image/png",
      data: "aGVsbG8=",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an image block without a source", () => {
    const result = ContentBlock({ type: "image" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("accepts an image inside a tool_result content array", () => {
    const result = ContentBlock({
      type: "tool_result",
      callId: "call_abc",
      content: [
        { type: "text", text: "the screenshot:" },
        {
          type: "image",
          source: { kind: "base64", mimeType: "image/png", data: "aGVsbG8=" },
        },
      ],
    });
    expect(result instanceof type.errors).toBe(false);
  });
});

describe("AudioBlock / VideoBlock / DocumentBlock variants on ContentBlock", () => {
  test.each([
    ["audio", "audio/wav", "UklGRg=="],
    ["video", "video/mp4", "AAAAGGZ0eXA="],
    ["document", "application/pdf", "JVBERi0="],
  ])("accepts a %s block with a base64 source", (kind, mimeType, data) => {
    const result = ContentBlock({
      type: kind,
      source: { kind: "base64", mimeType, data },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test.each([
    ["audio", "audio/wav"],
    ["video", "video/mp4"],
    ["document", "application/pdf"],
  ])("accepts a %s block with a file-reference source", (kind, mimeType) => {
    const result = ContentBlock({
      type: kind,
      source: { kind: "file-reference", mimeType, reference: "file_abc" },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test.each(["audio", "video", "document"])(
    "rejects a %s block without a source",
    (kind) => {
      const result = ContentBlock({ type: kind });
      expect(result instanceof type.errors).toBe(true);
    },
  );

  test.each(["audio", "video", "document"])(
    "rejects a %s block with the legacy flat shape (mimeType/data, no source)",
    (kind) => {
      const result = ContentBlock({
        type: kind,
        mimeType: "application/octet-stream",
        data: "aGVsbG8=",
      });
      expect(result instanceof type.errors).toBe(true);
    },
  );

  test.each([
    ["audio", "audio/wav", "UklGRg=="],
    ["video", "video/mp4", "AAAAGGZ0eXA="],
    ["document", "application/pdf", "JVBERi0="],
  ])(
    "accepts a %s block (base64) inside a tool_result content array",
    (kind, mimeType, data) => {
      const result = ContentBlock({
        type: "tool_result",
        callId: "call_xyz",
        content: [
          { type: "text", text: "the payload:" },
          { type: kind, source: { kind: "base64", mimeType, data } },
        ],
      });
      expect(result instanceof type.errors).toBe(false);
    },
  );

  test.each([
    ["audio", "audio/wav", "file_audio"],
    ["video", "video/mp4", "file_video"],
    ["document", "application/pdf", "file_doc"],
  ])(
    "accepts a %s block (file-reference) inside a tool_result content array",
    (kind, mimeType, reference) => {
      const result = ContentBlock({
        type: "tool_result",
        callId: "call_xyz",
        content: [
          { type: "text", text: "the payload:" },
          {
            type: kind,
            source: { kind: "file-reference", mimeType, reference },
          },
        ],
      });
      expect(result instanceof type.errors).toBe(false);
    },
  );
});

describe("CitationBlock", () => {
  test("accepts a minimal citation with a uri source", () => {
    const result = ContentBlock({
      type: "citation",
      citedText: "the answer is 42",
      source: { uri: "https://example.com/article", title: "Article" },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a citation with a documentRef source", () => {
    const result = ContentBlock({
      type: "citation",
      citedText: "per the report",
      source: { documentRef: { index: 0 }, title: "Q3.pdf" },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a citation with both location and textOffset", () => {
    const result = ContentBlock({
      type: "citation",
      citedText: "Martinis.",
      source: { uri: "https://example.com/" },
      location: { kind: "char", start: 100, end: 109 },
      textOffset: { start: 99, end: 108 },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test.each(["page", "char", "content-block"] as const)(
    "accepts a citation with location kind=%s",
    (kind) => {
      const result = ContentBlock({
        type: "citation",
        citedText: "x",
        source: { uri: "https://example.com/" },
        location: { kind, start: 1, end: 2 },
      });
      expect(result instanceof type.errors).toBe(false);
    },
  );

  test("rejects a citation with an unknown location kind", () => {
    const result = ContentBlock({
      type: "citation",
      citedText: "x",
      source: { uri: "https://example.com/" },
      location: { kind: "span", start: 1, end: 2 },
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a citation missing citedText", () => {
    const result = ContentBlock({
      type: "citation",
      source: { uri: "https://example.com/" },
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a citation missing source", () => {
    const result = ContentBlock({ type: "citation", citedText: "x" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a citation block inside a tool_result content array", () => {
    // tool_result.content is deliberately narrow; citations annotate
    // model output, not tool output.
    const result = ContentBlock({
      type: "tool_result",
      callId: "call_abc",
      content: [
        {
          type: "citation",
          citedText: "x",
          source: { uri: "https://example.com/" },
        },
      ],
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("inference.citation event", () => {
  test("accepts an inference.citation event wrapping a CitationBlock", () => {
    const result = InferenceEvent({
      type: "inference.citation",
      seq: 7,
      data: {
        citation: {
          type: "citation",
          citedText: "the answer is 42",
          source: { uri: "https://example.com/" },
          location: { kind: "char", start: 0, end: 16 },
          textOffset: { start: 0, end: 16 },
        },
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects an inference.citation event missing the citation payload", () => {
    const result = InferenceEvent({
      type: "inference.citation",
      seq: 7,
      data: {},
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an inference.citation event with a malformed citation", () => {
    const result = InferenceEvent({
      type: "inference.citation",
      seq: 7,
      data: { citation: { type: "citation", citedText: "x" } },
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("CodeExecutionRequestBlock and CodeExecutionResultBlock", () => {
  test("accepts a minimal code_execution_request", () => {
    const result = ContentBlock({
      type: "code_execution_request",
      id: "srvtoolu_01",
      code: "print('hi')",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a code_execution_request with a language hint", () => {
    const result = ContentBlock({
      type: "code_execution_request",
      id: "srvtoolu_01",
      code: "print('hi')",
      language: "python",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a code_execution_request missing id", () => {
    const result = ContentBlock({
      type: "code_execution_request",
      code: "print('hi')",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a code_execution_request missing code", () => {
    const result = ContentBlock({
      type: "code_execution_request",
      id: "srvtoolu_01",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test.each(["ok", "error", "aborted", "timeout"] as const)(
    "accepts a code_execution_result with status=%s",
    (status) => {
      const result = ContentBlock({
        type: "code_execution_result",
        requestId: "srvtoolu_01",
        status,
      });
      expect(result instanceof type.errors).toBe(false);
    },
  );

  test("accepts a code_execution_result with all optional fields populated", () => {
    const result = ContentBlock({
      type: "code_execution_result",
      requestId: "srvtoolu_01",
      status: "ok",
      stdout: "144\n",
      stderr: "",
      returnCode: 0,
      providerOutcome: "OUTCOME_OK",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a code_execution_result with abortReason on an aborted run", () => {
    const result = ContentBlock({
      type: "code_execution_result",
      requestId: "srvtoolu_01",
      status: "aborted",
      abortReason: "execution time exceeded the 30s limit",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a code_execution_result missing requestId", () => {
    const result = ContentBlock({
      type: "code_execution_result",
      status: "ok",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a code_execution_result with an unknown status value", () => {
    const result = ContentBlock({
      type: "code_execution_result",
      requestId: "srvtoolu_01",
      status: "unknown",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a code_execution_request or _result inside tool_result content", () => {
    // tool_result.content is deliberately narrow; server-side code
    // execution has a distinct lifecycle from the user-tool round-trip.
    const requestInTool = ContentBlock({
      type: "tool_result",
      callId: "call_abc",
      content: [
        { type: "code_execution_request", id: "srvtoolu_01", code: "x" },
      ],
    });
    expect(requestInTool instanceof type.errors).toBe(true);

    const resultInTool = ContentBlock({
      type: "tool_result",
      callId: "call_abc",
      content: [
        {
          type: "code_execution_result",
          requestId: "srvtoolu_01",
          status: "ok",
        },
      ],
    });
    expect(resultInTool instanceof type.errors).toBe(true);
  });
});

describe("inference.code_execution.* events", () => {
  test("accepts a start event wrapping a CodeExecutionRequestBlock", () => {
    const result = InferenceEvent({
      type: "inference.code_execution.start",
      seq: 4,
      data: {
        request: {
          type: "code_execution_request",
          id: "srvtoolu_01",
          code: "print('hi')",
          language: "python",
        },
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a delta event with a code fragment", () => {
    const result = InferenceEvent({
      type: "inference.code_execution.delta",
      seq: 5,
      data: { requestId: "srvtoolu_01", codeFragment: "print(" },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a result event wrapping a CodeExecutionResultBlock", () => {
    const result = InferenceEvent({
      type: "inference.code_execution.result",
      seq: 6,
      data: {
        result: {
          type: "code_execution_result",
          requestId: "srvtoolu_01",
          status: "ok",
          stdout: "hi\n",
          returnCode: 0,
        },
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a code_execution event with a malformed payload", () => {
    const result = InferenceEvent({
      type: "inference.code_execution.start",
      seq: 4,
      data: { request: { type: "code_execution_request", id: "x" } },
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("RedactedThinkingBlock and inference.thinking.redacted event", () => {
  test("accepts a minimal redacted_thinking block", () => {
    const result = ContentBlock({
      type: "redacted_thinking",
      data: "EncryptedOpaqueBlobAAAA==",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a redacted_thinking block missing data", () => {
    const result = ContentBlock({ type: "redacted_thinking" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("accepts an inference.thinking.redacted event wrapping the block", () => {
    const result = InferenceEvent({
      type: "inference.thinking.redacted",
      seq: 3,
      data: {
        redactedThinking: {
          type: "redacted_thinking",
          data: "EncryptedOpaqueBlobAAAA==",
        },
        index: 1,
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects an inference.thinking.redacted event with a malformed payload", () => {
    const result = InferenceEvent({
      type: "inference.thinking.redacted",
      seq: 3,
      data: { redactedThinking: { type: "redacted_thinking" } },
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("optional index on delta event variants", () => {
  const partial = { text: "" };

  const cases: { name: string; type: string; data: Record<string, unknown> }[] =
    [
      {
        name: "inference.text.delta",
        type: "inference.text.delta",
        data: { token: "x", partial },
      },
      {
        name: "inference.thinking.delta",
        type: "inference.thinking.delta",
        data: { token: "x", partial },
      },
      {
        name: "inference.thinking.signature",
        type: "inference.thinking.signature",
        data: { signature: "sig_abc" },
      },
      {
        name: "inference.tool_call.start",
        type: "inference.tool_call.start",
        data: { callId: "call_1", name: "fn", partial },
      },
      {
        name: "inference.tool_call.delta",
        type: "inference.tool_call.delta",
        data: { callId: "call_1", argumentFragment: "{", partial },
      },
      {
        name: "inference.tool_call.end",
        type: "inference.tool_call.end",
        data: { callId: "call_1", name: "fn", arguments: {}, partial },
      },
    ];

  for (const c of cases) {
    test(`${c.name} accepts an optional index field`, () => {
      const result = InferenceEvent({
        type: c.type,
        seq: 1,
        data: { ...c.data, index: 3 },
      });
      expect(result instanceof type.errors).toBe(false);
    });
  }

  test("two text.delta events with different indices round-trip distinctly", () => {
    // Locks the per-block semantic: index 0 and index 1 are distinct
    // blocks, not aliases for "same buffer."
    const e0 = InferenceEvent({
      type: "inference.text.delta",
      seq: 1,
      data: { token: "hello", partial, index: 0 },
    });
    const e1 = InferenceEvent({
      type: "inference.text.delta",
      seq: 2,
      data: { token: "world", partial, index: 1 },
    });
    expect(e0 instanceof type.errors).toBe(false);
    expect(e1 instanceof type.errors).toBe(false);
    if (e0 instanceof type.errors || e1 instanceof type.errors) return;
    if (e0.type !== "inference.text.delta") {
      throw new Error("e0 narrowed incorrectly");
    }
    if (e1.type !== "inference.text.delta") {
      throw new Error("e1 narrowed incorrectly");
    }
    expect(e0.data.index).toBe(0);
    expect(e1.data.index).toBe(1);
  });

  test("rejects a malformed (non-number) index value", () => {
    const result = InferenceEvent({
      type: "inference.text.delta",
      seq: 1,
      data: { token: "x", partial, index: "two" },
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("inference.image_output event", () => {
  test("accepts an event wrapping an ImageBlock with a base64 source", () => {
    const result = InferenceEvent({
      type: "inference.image_output",
      seq: 8,
      data: {
        image: {
          type: "image",
          source: {
            kind: "base64",
            mimeType: "image/png",
            data: "iVBORw0KGgo=",
          },
        },
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts an event with an index field", () => {
    const result = InferenceEvent({
      type: "inference.image_output",
      seq: 8,
      data: {
        image: {
          type: "image",
          source: {
            kind: "base64",
            mimeType: "image/png",
            data: "iVBORw0KGgo=",
          },
        },
        index: 1,
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts an event wrapping an ImageBlock with a file-reference", () => {
    const result = InferenceEvent({
      type: "inference.image_output",
      seq: 8,
      data: {
        image: {
          type: "image",
          source: {
            kind: "file-reference",
            mimeType: "image/png",
            reference: "file_generated",
          },
        },
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects an event missing the image payload", () => {
    const result = InferenceEvent({
      type: "inference.image_output",
      seq: 8,
      data: {},
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an event with a malformed image block", () => {
    const result = InferenceEvent({
      type: "inference.image_output",
      seq: 8,
      data: {
        image: {
          type: "image",
          // Missing `source` field.
        },
      },
    });
    expect(result instanceof type.errors).toBe(true);
  });
});
