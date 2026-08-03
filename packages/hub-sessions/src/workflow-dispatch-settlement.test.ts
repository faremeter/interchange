import { describe, expect, test } from "bun:test";

import type { CommittedReads } from "./repo-store";
import {
  listAcceptedWorkflowDispatches,
  listConsumedWorkflowDispatches,
  listReceivedWorkflowSignals,
} from "./workflow-dispatch-settlement";

function committedReads(files: ReadonlyMap<string, string>): CommittedReads {
  return {
    async listDir(path) {
      if (path === "addresses") {
        return [
          { name: "workflow%40tenant", oid: "tree-address", type: "tree" },
        ];
      }
      if (path === "addresses/workflow%40tenant/consumed") {
        return [...files.keys()].map((name, index) => ({
          name,
          oid: `blob-${String(index)}`,
          type: "blob" as const,
        }));
      }
      return [];
    },
    async readBlobByOid(oid) {
      const index = Number(oid.slice("blob-".length));
      const value = [...files.values()][index];
      if (value === undefined) throw new Error(`unknown oid ${oid}`);
      return new TextEncoder().encode(value);
    },
  };
}

describe("listConsumedWorkflowDispatches", () => {
  test("projects retained claim-check entries into settlement keys", async () => {
    const reads = committedReads(
      new Map([
        [
          "message-1.json",
          JSON.stringify({
            messageId: "message-1",
            address: "workflow@tenant",
            receivedAt: 1,
          }),
        ],
      ]),
    );

    await expect(listConsumedWorkflowDispatches(reads)).resolves.toEqual([
      { messageId: "message-1", address: "workflow@tenant" },
    ]);
  });

  test("projects an explicit supervisor rejection instead of settling it", async () => {
    const reads = committedReads(
      new Map([
        [
          "message-1.json",
          JSON.stringify({
            messageId: "message-1",
            address: "workflow@tenant",
            rejection: {
              code: "workflow_run_terminal",
              message: "run is terminal",
            },
          }),
        ],
      ]),
    );

    await expect(listConsumedWorkflowDispatches(reads)).resolves.toEqual([
      {
        messageId: "message-1",
        address: "workflow@tenant",
        rejection: {
          code: "workflow_run_terminal",
          message: "run is terminal",
        },
      },
    ]);
  });

  test("rejects malformed consumed envelopes instead of settling a guess", async () => {
    const reads = committedReads(
      new Map([["message-1.json", JSON.stringify({ messageId: "message-1" })]]),
    );

    await expect(listConsumedWorkflowDispatches(reads)).rejects.toThrow(
      "workflow_dispatch_consumed_invalid",
    );
  });
});

describe("listReceivedWorkflowSignals", () => {
  test("reads only the selected live run", async () => {
    const blobs = new Map([
      [
        "signal",
        JSON.stringify({
          type: "SignalReceived",
          signalId: "signal-1",
          signalName: "continue",
        }),
      ],
      ["started", JSON.stringify({ type: "RunStarted" })],
    ]);
    const visited: string[] = [];
    const reads: CommittedReads = {
      async listDir(path) {
        visited.push(path);
        if (path === "runs" || path.includes("unrelated-child")) {
          throw new Error(`unexpected run enumeration: ${path}`);
        }
        if (path === "runs/workflow@tenant") {
          return [{ name: "events", oid: "events", type: "tree" }];
        }
        if (path === "runs/workflow@tenant/events") {
          return [
            { name: "1.json", oid: "started", type: "blob" },
            { name: "2.json", oid: "signal", type: "blob" },
          ];
        }
        return [];
      },
      async readBlobByOid(oid) {
        const value = blobs.get(oid);
        if (value === undefined) throw new Error(`unknown oid ${oid}`);
        return new TextEncoder().encode(value);
      },
    };

    await expect(
      listReceivedWorkflowSignals(reads, "workflow@tenant"),
    ).resolves.toEqual([{ runId: "workflow@tenant", signalId: "signal-1" }]);
    expect(visited).toEqual([
      "runs/workflow@tenant",
      "runs/workflow@tenant/events",
    ]);
  });

  test("reads signals from the selected sealed run", async () => {
    const combined = [
      JSON.stringify({ type: "RunStarted" }),
      JSON.stringify({
        type: "SignalReceived",
        signalId: "signal-sealed",
        signalName: "continue",
      }),
    ].join("\n");
    const reads: CommittedReads = {
      async listDir(path) {
        if (path === "runs/workflow@tenant") {
          return [{ name: "events.jsonl", oid: "sealed-events", type: "blob" }];
        }
        throw new Error(`unexpected directory read: ${path}`);
      },
      async readBlobByOid(oid) {
        if (oid !== "sealed-events") throw new Error(`unknown oid ${oid}`);
        return new TextEncoder().encode(combined);
      },
    };

    await expect(
      listReceivedWorkflowSignals(reads, "workflow@tenant"),
    ).resolves.toEqual([
      { runId: "workflow@tenant", signalId: "signal-sealed" },
    ]);
  });

  test("returns no signals for a missing selected run", async () => {
    const reads: CommittedReads = {
      async listDir() {
        return [];
      },
      async readBlobByOid(oid) {
        throw new Error(`unexpected blob read: ${oid}`);
      },
    };

    await expect(
      listReceivedWorkflowSignals(reads, "missing@tenant"),
    ).resolves.toEqual([]);
  });

  test("rejects a malformed signal in the selected run", async () => {
    const reads: CommittedReads = {
      async listDir(path) {
        if (path === "runs/workflow@tenant") {
          return [{ name: "events", oid: "events", type: "tree" }];
        }
        if (path === "runs/workflow@tenant/events") {
          return [{ name: "1.json", oid: "malformed", type: "blob" }];
        }
        return [];
      },
      async readBlobByOid(oid) {
        if (oid !== "malformed") throw new Error(`unknown oid ${oid}`);
        return new TextEncoder().encode(
          JSON.stringify({ type: "SignalReceived" }),
        );
      },
    };

    await expect(
      listReceivedWorkflowSignals(reads, "workflow@tenant"),
    ).rejects.toThrow("workflow_dispatch_signal_invalid");
  });
});

describe("listAcceptedWorkflowDispatches", () => {
  test("projects the firing mail and later signals in one run-scoped pass", async () => {
    const blobs = new Map([
      [
        "started",
        JSON.stringify({
          type: "RunStarted",
          consumedMessageId: "mail-1",
        }),
      ],
      [
        "signal",
        JSON.stringify({
          type: "SignalReceived",
          signalId: "signal-1",
        }),
      ],
    ]);
    const reads: CommittedReads = {
      async listDir(path) {
        if (path === "runs/workflow@tenant") {
          return [{ name: "events", oid: "events", type: "tree" }];
        }
        if (path === "runs/workflow@tenant/events") {
          return [
            { name: "0.json", oid: "started", type: "blob" },
            { name: "1.json", oid: "signal", type: "blob" },
          ];
        }
        return [];
      },
      async readBlobByOid(oid) {
        const value = blobs.get(oid);
        if (value === undefined) throw new Error(`unknown oid ${oid}`);
        return new TextEncoder().encode(value);
      },
    };

    await expect(
      listAcceptedWorkflowDispatches(reads, "workflow@tenant"),
    ).resolves.toEqual([
      { runId: "workflow@tenant", kind: "mail", messageId: "mail-1" },
      {
        runId: "workflow@tenant",
        kind: "signal",
        messageId: "signal-1",
      },
    ]);
  });

  test("stops the newest-first scan once every unsettled id is found", async () => {
    const visitedOids: string[] = [];
    const reads: CommittedReads = {
      async listDir(path) {
        if (path === "runs/workflow@tenant") {
          return [{ name: "events", oid: "events", type: "tree" }];
        }
        if (path === "runs/workflow@tenant/events") {
          return [
            { name: "0.json", oid: "old-start", type: "blob" },
            { name: "10.json", oid: "latest-signal", type: "blob" },
            { name: "2.json", oid: "middle", type: "blob" },
          ];
        }
        return [];
      },
      async readBlobByOid(oid) {
        visitedOids.push(oid);
        if (oid !== "latest-signal") {
          throw new Error(`unexpected old event read: ${oid}`);
        }
        return new TextEncoder().encode(
          JSON.stringify({ type: "SignalReceived", signalId: "signal-10" }),
        );
      },
    };

    await expect(
      listAcceptedWorkflowDispatches(
        reads,
        "workflow@tenant",
        new Set(["signal-10"]),
      ),
    ).resolves.toEqual([
      {
        runId: "workflow@tenant",
        kind: "signal",
        messageId: "signal-10",
      },
    ]);
    expect(visitedOids).toEqual(["latest-signal"]);
  });
});
