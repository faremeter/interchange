/* eslint-disable no-console */
import fs from "node:fs";
import { parseArgs } from "node:util";

import { IsogitStore } from "@interchange/storage-isogit";
import type { AuditRecord } from "@interchange/types/audit";

const USAGE = "Usage: audit --dir <agent-repo> --session <sessionId> [--json]";

const { values } = parseArgs({
  options: {
    dir: { type: "string" },
    session: { type: "string" },
    json: { type: "boolean", default: false },
    help: { type: "boolean", default: false },
  },
  strict: true,
});

if (values.help) {
  console.log(USAGE);
  process.exit(0);
}

if (values.dir === undefined || values.session === undefined) {
  console.error(USAGE);
  process.exit(1);
}

const dir = values.dir;
const sessionId = values.session;

if (!fs.existsSync(`${dir}/.git`)) {
  console.error(`Not an agent repository: ${dir}`);
  process.exit(1);
}

const store = new IsogitStore(dir);

let records: AuditRecord[];
try {
  records = await store.loadAudit(sessionId);
} catch (cause) {
  console.error(
    `Failed to load audit records: ${cause instanceof Error ? cause.message : String(cause)}`,
  );
  process.exit(1);
}

if (records.length === 0) {
  console.error(`No audit records found for session ${sessionId}`);
  process.exit(1);
}

if (values.json) {
  console.log(JSON.stringify(records, null, 2));
  process.exit(0);
}

console.log(`Session ${sessionId} — ${records.length} tool invocations\n`);

for (const record of records) {
  const authzLabel = formatAuthz(record);
  const resultLabel = record.result.isError ? "error" : "ok";

  console.log(
    `[${record.seq}] ${record.tool} — ${authzLabel} — ${resultLabel}`,
  );
  console.log(`    ${record.timestamp}`);

  const preview = formatPreview(record.arguments, 120);
  if (preview.length > 0) {
    console.log(`    > ${preview}`);
  }

  const resultPreview = formatPreview(record.result.content, 120);
  if (resultPreview.length > 0) {
    console.log(`    < ${resultPreview}`);
  }

  console.log();
}

function formatAuthz(record: AuditRecord): string {
  if (record.authz === null) return "no authz";
  if (record.authz.blocked) {
    const reason = record.authz.blockReason ?? "blocked";
    return reason;
  }
  if (record.authz.effect === null) return "no matching grants";
  return record.authz.effect;
}

function formatPreview(value: unknown, maxLen: number): string {
  let text: string;
  if (typeof value === "string") {
    text = value;
  } else if (typeof value === "object" && value !== null) {
    if (Object.keys(value).length === 0) return "";
    text = JSON.stringify(value);
  } else {
    return "";
  }
  text = text.replace(/\n/g, "\\n");
  if (text.length > maxLen) {
    return text.slice(0, maxLen) + "...";
  }
  return text;
}
