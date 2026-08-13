import type { AuditRecord } from "@intx/types/audit";
import type { ConversationTurn } from "@intx/types/runtime";
import { createBrowserIsogitStorage } from "@intx/storage-isogit/browser";

const turns: ConversationTurn[] = [
  {
    role: "user",
    content: [{ type: "text", text: "stored for this browser session" }],
    timestamp: 1,
  },
];

const auditRecord: AuditRecord = {
  callId: "browser-call-1",
  tool: "browser-smoke",
  arguments: {},
  authz: null,
  result: { content: "ok", isError: false },
  timestamp: "2026-08-10T00:00:00.000Z",
  sessionId: "browser-session-1",
  seq: 0,
};

type WriteResult = {
  sourceCommitSha: string;
  deployCommitSha: string;
  packCount: number;
  looseObjectCount: number;
};

type ReadResult = {
  firstTurnText: string | null;
  auditCallIds: string[];
  sourceCommitHashes: string[];
  targetCommitSha: string;
};

type StorageIsogitSmoke = {
  writeAndFlush(databaseName: string): Promise<WriteResult>;
  readInSession(databaseName: string): Promise<ReadResult>;
  repositoryExists(databaseName: string, dir: string): Promise<boolean>;
};

let activeStorage: ReturnType<typeof createBrowserIsogitStorage> | undefined;
let activeFilesystemName: string | undefined;

declare global {
  var storageIsogitSmoke: StorageIsogitSmoke;
}

async function writeAndFlush(databaseName: string): Promise<WriteResult> {
  const storage = createBrowserIsogitStorage(databaseName);
  activeStorage = storage;
  activeFilesystemName = databaseName;
  const source = await storage.createIsogitStore("source");

  await source.writeTurns(turns);
  const committed = await source.commit({ message: "browser checkpoint" });
  await source.commitAudit([auditRecord]);

  const { pack, commitSha } = await storage.createDeployPack(
    "/source",
    "refs/heads/main",
  );
  await storage.initAgentRepo("/target");
  await storage.applyPack(
    "/target",
    pack,
    "refs/heads/deploy",
    commitSha,
    "browser-smoke-transfer",
  );

  const gc = await storage.runGC("/target", { retention: "keep-history" });
  await storage.runtime.flush();

  return {
    sourceCommitSha: committed.hash,
    deployCommitSha: commitSha,
    packCount: gc.after.packCount,
    looseObjectCount: gc.after.looseObjectCount,
  };
}

async function readInSession(databaseName: string): Promise<ReadResult> {
  if (activeStorage === undefined || activeFilesystemName !== databaseName) {
    throw new Error(`No active browser filesystem named ${databaseName}`);
  }
  const storage = activeStorage;
  const source = await storage.createIsogitStore("/source");
  const snapshot = await source.load();
  const firstBlock = snapshot.turns[0]?.content[0];
  const audit = await source.loadAudit("browser-session-1");
  const sourceLog = await source.log();
  const targetPack = await storage.createDeployPack(
    "/target",
    "refs/heads/deploy",
  );

  return {
    firstTurnText: firstBlock?.type === "text" ? firstBlock.text : null,
    auditCallIds: audit.map((record) => record.callId),
    sourceCommitHashes: sourceLog.map((entry) => entry.hash),
    targetCommitSha: targetPack.commitSha,
  };
}

async function repositoryExists(
  databaseName: string,
  dir: string,
): Promise<boolean> {
  const { fs } = createBrowserIsogitStorage(databaseName);
  try {
    const stat = await fs.promises.stat(`${dir}/.git`);
    return stat.isDirectory();
  } catch (cause) {
    if (
      typeof cause === "object" &&
      cause !== null &&
      Reflect.get(cause, "code") === "ENOENT"
    ) {
      return false;
    }
    throw cause;
  }
}

globalThis.storageIsogitSmoke = {
  writeAndFlush,
  readInSession,
  repositoryExists,
};
