import { type } from "arktype";

export const FileEntry = type({
  path: "string",
  type: "'file' | 'directory'",
  "size?": "number | null",
  "modifiedAt?": "string | null",
});

export const FileContent = type({
  path: "string",
  content: "string",
  "encoding?": "'utf-8' | 'base64'",
});

export const HistoryEntry = type({
  ref: "string",
  message: "string",
  author: "string",
  timestamp: "string",
  "filesChanged?": "number",
});

export const CommitDetail = type({
  ref: "string",
  message: "string",
  author: "string",
  timestamp: "string",
  changes: type({
    path: "string",
    status: "'added' | 'modified' | 'deleted'",
    "additions?": "number",
    "deletions?": "number",
  }).array(),
});

export const BranchInfo = type({
  name: "string",
  "isCurrent?": "boolean",
  "lastCommitRef?": "string | null",
  "lastCommitMessage?": "string | null",
  "lastCommitAt?": "string | null",
});
