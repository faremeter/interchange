// Fake LSP server for testing. Spawned as a child process by tests.
// Speaks enough of the LSP protocol to exercise client behaviors:
// initialize handshake, didOpen -> publishDiagnostics, pull diagnostics,
// dynamic capability registration, and shutdown.

import {
  createMessageConnection,
  StreamMessageReader,
  StreamMessageWriter,
} from "vscode-jsonrpc/node";

const connection = createMessageConnection(
  new StreamMessageReader(process.stdin),
  new StreamMessageWriter(process.stdout),
);

let initialized = false;

const openDocuments = new Map<string, { version: number; text: string }>();

connection.onRequest("initialize", (_params: Record<string, unknown>) => {
  return {
    capabilities: {
      textDocumentSync: 1,
      diagnosticProvider: {
        interFileDependencies: true,
        workspaceDiagnostics: true,
      },
    },
  };
});

connection.onNotification("initialized", () => {
  initialized = true;
});

connection.onNotification(
  "workspace/didChangeConfiguration",
  (_params: { settings: unknown }) => {
    // Accept configuration silently.
  },
);

connection.onNotification(
  "textDocument/didOpen",
  (params: {
    textDocument: {
      uri: string;
      languageId: string;
      version: number;
      text: string;
    };
  }) => {
    const { uri, version, text } = params.textDocument;
    openDocuments.set(uri, { version, text });

    // Simulate publishDiagnostics after a short delay
    setTimeout(() => {
      const diagnostics = generateDiagnostics(uri, text);
      void connection.sendNotification("textDocument/publishDiagnostics", {
        uri,
        version,
        diagnostics,
      });
    }, 50);
  },
);

connection.onNotification(
  "textDocument/didChange",
  (params: {
    textDocument: { uri: string; version: number };
    contentChanges: { text: string }[];
  }) => {
    const { uri, version } = params.textDocument;
    const lastChange = params.contentChanges[params.contentChanges.length - 1];
    if (lastChange !== undefined) {
      openDocuments.set(uri, { version, text: lastChange.text });

      setTimeout(() => {
        const diagnostics = generateDiagnostics(uri, lastChange.text);
        void connection.sendNotification("textDocument/publishDiagnostics", {
          uri,
          version,
          diagnostics,
        });
      }, 50);
    }
  },
);

connection.onNotification(
  "workspace/didChangeWatchedFiles",
  (_params: unknown) => {
    // Accept silently.
  },
);

connection.onRequest(
  "textDocument/diagnostic",
  (params: { textDocument: { uri: string } }) => {
    const doc = openDocuments.get(params.textDocument.uri);
    if (doc === undefined) {
      return { kind: "full", items: [] };
    }
    return {
      kind: "full",
      items: generateDiagnostics(params.textDocument.uri, doc.text),
    };
  },
);

connection.onRequest("workspace/diagnostic", () => {
  const items: { uri: string; kind: string; items: unknown[] }[] = [];
  for (const [uri, doc] of openDocuments) {
    items.push({
      uri,
      kind: "full",
      items: generateDiagnostics(uri, doc.text),
    });
  }
  return { items };
});

connection.onRequest(
  "textDocument/hover",
  (params: {
    textDocument: { uri: string };
    position: { line: number; character: number };
  }) => {
    const doc = openDocuments.get(params.textDocument.uri);
    if (doc === undefined) return null;
    return {
      contents: {
        kind: "markdown",
        value: `Hover at ${params.position.line}:${params.position.character}`,
      },
      range: {
        start: params.position,
        end: {
          line: params.position.line,
          character: params.position.character + 1,
        },
      },
    };
  },
);

connection.onRequest(
  "textDocument/definition",
  (params: {
    textDocument: { uri: string };
    position: { line: number; character: number };
  }) => {
    return [
      {
        uri: params.textDocument.uri,
        range: {
          start: { line: 0, character: 0 },
          end: { line: 0, character: 1 },
        },
      },
    ];
  },
);

connection.onRequest(
  "textDocument/references",
  (_params: {
    textDocument: { uri: string };
    position: { line: number; character: number };
  }) => {
    return [];
  },
);

connection.onRequest("shutdown", () => {
  return null;
});

connection.onNotification("exit", () => {
  process.exit(0);
});

function generateDiagnostics(
  _uri: string,
  text: string,
): {
  range: {
    start: { line: number; character: number };
    end: { line: number; character: number };
  };
  severity: number;
  message: string;
}[] {
  const diagnostics: {
    range: {
      start: { line: number; character: number };
      end: { line: number; character: number };
    };
    severity: number;
    message: string;
  }[] = [];

  // Generate a diagnostic for each line containing "ERROR_MARKER"
  const lines = text.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (line !== undefined && line.includes("ERROR_MARKER")) {
      diagnostics.push({
        range: {
          start: { line: i, character: 0 },
          end: { line: i, character: line.length },
        },
        severity: 1,
        message: `Error on line ${i + 1}: found ERROR_MARKER`,
      });
    }
  }

  return diagnostics;
}

void initialized;
connection.listen();
