import { type } from "arktype";

import {
  ConnectorThreadState,
  ConversationTurn,
  TokenUsage,
} from "./runtime-core";

// An agent step's state as exported from a workflow run, and as a later
// deployment imports it. It is plain client-owned data: nothing links the
// exporting and importing runs, and a client may edit it in between. Pending
// operations are deliberately absent -- their correlation ids only mean
// something inside the run that registered them.
export const StepStateSnapshot = type({
  version: type("1").describe("Snapshot format version."),
  turns: ConversationTurn.array().describe(
    "The step agent's conversation history, oldest first. An import carries no system turn: the imported step keeps the system prompt its deployment approved.",
  ),
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null").describe(
    "Mail thread the agent replies on, or null when no thread is active. An import must carry null: the imported step starts without a thread, and a single-step agent starts one from the sender of its first mail.",
  ),
  "mailboxUidNext?": type("1 <= number.integer <= 4294967295").describe(
    "The UID the exporting deployment's mailbox assigns next, so every mailbox UID the turns name is below it. A deployment that imports the snapshot numbers its own mail from at least this UID, so those UIDs never name one of its messages. An import accepts at most 2147483648. Absent when the exporting deployment has no mailbox.",
  ),
});
export type StepStateSnapshot = typeof StepStateSnapshot.infer;

// Deploy-time step state: a snapshot per agent step, keyed by the id of the
// step it seeds. Imported state is committed to the run's history as JSON,
// which has no encoding for a non-finite number: `JSON.parse` reads an
// out-of-range literal such as `1e999` as `Infinity`, and serializing it
// writes `null`. Such a number is rejected here rather than silently
// changing in the seed. Serializing the seed also recurses once per level
// of nesting, so client JSON nested deep enough (a tool call's arguments)
// would overflow the call stack; no real conversation nests near the cap.
const MAX_STEP_STATE_DEPTH = 64;

// A step's state is stored under a directory named for its id, and common
// filesystems cap a directory name at 255 bytes, so no longer key names a
// step whose state could be stored. It is checked before the snapshots are:
// every issue beneath a key repeats it in its path, so an unbounded key makes
// validating them grow with the key times the issues.
const MAX_STEP_STATE_KEY_LENGTH = 255;

// Mailbox UIDs are 32-bit, and nothing renumbers a mailbox that runs out of
// them. An import numbers the new mailbox from the largest `mailboxUidNext`
// it carries, so this bound leaves it at least 2^31 UIDs, the same order of
// headroom as a mailbox numbering from 1.
const MAX_IMPORTED_MAILBOX_UID_NEXT = 2 ** 31;

// A key the snapshot does not declare, at any depth, is refused rather than
// stored: export returns a seed as it is stored, so the key would come back
// out of it.
export const StepStateImport = type({
  "[string]": StepStateSnapshot.onDeepUndeclaredKey("reject"),
}).narrow((stepState, ctx) => {
  const problem = findUnstorableStepState(stepState);
  // `actual` replaces the default description of the rejected value,
  // which would print the value and overflow on the same nesting.
  if (problem !== null) return ctx.reject(problem);
  for (const [stepId, snapshot] of Object.entries(stepState)) {
    // A thread's recipients are whatever the client wrote, so an imported
    // thread would let the importer choose where the step's replies go.
    if (snapshot.connectorState !== null) {
      return ctx.reject({
        expected:
          "null, since an imported step starts without a reply thread and replies only to senders that mailed its own deployment",
        actual: "a reply thread",
        relativePath: [stepId, "connectorState"],
      });
    }
    // No agent writes a system turn, so one can only come from the client,
    // and a provider would read it as instructions the deployment never
    // approved.
    const systemTurn = snapshot.turns.findIndex(
      (turn) => turn.role === "system",
    );
    if (systemTurn !== -1) {
      return ctx.reject({
        expected:
          "user or assistant, since an imported step keeps the system prompt its deployment approved",
        actual: "a system turn",
        relativePath: [stepId, "turns", systemTurn, "role"],
      });
    }
    const uidNext = snapshot.mailboxUidNext;
    if (uidNext !== undefined && uidNext > MAX_IMPORTED_MAILBOX_UID_NEXT) {
      return ctx.reject({
        expected: `a mailboxUidNext of at most ${String(MAX_IMPORTED_MAILBOX_UID_NEXT)}`,
        actual: String(uidNext),
        relativePath: [stepId, "mailboxUidNext"],
      });
    }
  }
  return true;
});
export type StepStateImport = typeof StepStateImport.infer;

// Walks with an explicit stack, so it cannot overflow on the nesting it
// exists to reject. The stack holds one frame per container rather than one
// entry per value, so a wide array costs no allocation per element.
export function findUnstorableStepState(
  value: unknown,
): { expected: string; actual: string } | null {
  if (typeof value === "object" && value !== null) {
    for (const stepId of Object.keys(value)) {
      if (stepId.length > MAX_STEP_STATE_KEY_LENGTH) {
        return {
          expected: `keyed by step ids of at most ${String(MAX_STEP_STATE_KEY_LENGTH)} characters`,
          actual: "a longer key",
        };
      }
    }
  }
  // `depth` is the depth of the frame's own values.
  const frames: { values: readonly unknown[]; next: number; depth: number }[] =
    [{ values: [value], next: 0, depth: 0 }];
  for (let frame = frames.at(-1); frame !== undefined; frame = frames.at(-1)) {
    if (frame.next === frame.values.length) {
      frames.pop();
      continue;
    }
    const current = frame.values[frame.next];
    frame.next += 1;
    if (typeof current === "number" && !Number.isFinite(current)) {
      return {
        expected: "state whose numbers are all finite",
        actual: String(current),
      };
    }
    if (typeof current === "object" && current !== null) {
      if (frame.depth >= MAX_STEP_STATE_DEPTH) {
        return {
          expected: `state nested at most ${String(MAX_STEP_STATE_DEPTH)} levels deep`,
          actual: "nested deeper",
        };
      }
      frames.push({
        values: Array.isArray(current) ? current : Object.values(current),
        next: 0,
        depth: frame.depth + 1,
      });
    }
  }
  return null;
}
