// Action types and validation for the agent reactor.
//
// The reactor validates the set of actions returned by the director before
// executing any of them. Invalid combinations produce a reactor.error rather
// than partial execution — the reactor does not guess intent.
//
// Validation rules (INFERENCE.md § Action Validation):
// - At most one `infer` action.
// - At most one `done` action.
// - At most one `reply` action.
// - `infer` + `done` together is invalid.
// - `reply` + `infer` together is invalid.
// - `reply` + `execute_tools` together is invalid.
// - `reply` + `done` together is invalid.
// - `reply` + `suspend` together is invalid.
// - `wait` + `infer` together is invalid.
// - `wait` + `execute_tools` together is invalid.
// - `wait` + `suspend` together is invalid.
// - `wait` + `reply` together is invalid.
// - `wait` + `done` together is invalid.
// - `suspend` cannot appear alongside `infer` or `execute_tools`.
// - `fork` is composable — may appear alongside any other action.
// - At most one `checkpoint` action; composable with any other action.
// - At most one `wait` action.
// - Multiple `execute_tools` are merged into a single parallel batch.
// - `emit` is always valid and composable.
// - At most one `compact` action; composable with `checkpoint`, `emit`, and
//   `fork`. Not composable with `infer`, `execute_tools`, `reply`, `suspend`,
//   `wait`, or `done`. Context-overflow recovery runs compaction in its own
//   cycle and re-infers on the next director invocation.

import type { ReactorAction, ToolCall } from "@interchange/types/runtime";

export type ValidationResult =
  | { ok: true; normalized: ReactorAction[] }
  | { ok: false; error: string };

/**
 * Validate and normalize a set of actions returned by the director.
 *
 * On success, returns the normalized action list with multiple `execute_tools`
 * collapsed into a single batched action. On failure, returns a diagnostic
 * error string.
 */
export function validateActions(
  actions: ReactorAction | ReactorAction[],
): ValidationResult {
  const list = Array.isArray(actions) ? actions : [actions];

  // An empty action list means "no-op, keep waiting for the next event."
  // This is valid — the reactor loop continues to waitForEvent().
  if (list.length === 0) {
    return { ok: true, normalized: [] };
  }

  const inferActions = list.filter((a) => a.type === "infer");
  const doneActions = list.filter((a) => a.type === "done");
  const replyActions = list.filter((a) => a.type === "reply");
  const suspendActions = list.filter((a) => a.type === "suspend");
  const executeActions = list.filter(
    (a): a is Extract<ReactorAction, { type: "execute_tools" }> =>
      a.type === "execute_tools",
  );
  const waitActions = list.filter((a) => a.type === "wait");
  const forkActions = list.filter((a) => a.type === "fork");
  const emitActions = list.filter((a) => a.type === "emit");
  const checkpointActions = list.filter((a) => a.type === "checkpoint");
  const compactActions = list.filter((a) => a.type === "compact");

  if (checkpointActions.length > 1) {
    return {
      ok: false,
      error: "Multiple checkpoint actions are not allowed",
    };
  }

  if (inferActions.length > 1) {
    return { ok: false, error: "Multiple infer actions are not allowed" };
  }

  if (doneActions.length > 1) {
    return { ok: false, error: "Multiple done actions are not allowed" };
  }

  if (inferActions.length > 0 && doneActions.length > 0) {
    return { ok: false, error: "infer and done cannot appear together" };
  }

  if (replyActions.length > 1) {
    return { ok: false, error: "Multiple reply actions are not allowed" };
  }

  if (replyActions.length > 0 && inferActions.length > 0) {
    return { ok: false, error: "reply and infer cannot appear together" };
  }

  if (replyActions.length > 0 && executeActions.length > 0) {
    return {
      ok: false,
      error: "reply and execute_tools cannot appear together",
    };
  }

  if (replyActions.length > 0 && doneActions.length > 0) {
    return { ok: false, error: "reply and done cannot appear together" };
  }

  if (replyActions.length > 0 && suspendActions.length > 0) {
    return { ok: false, error: "reply and suspend cannot appear together" };
  }

  if (suspendActions.length > 0) {
    if (inferActions.length > 0) {
      return {
        ok: false,
        error: "suspend cannot appear alongside infer",
      };
    }
    if (executeActions.length > 0) {
      return {
        ok: false,
        error: "suspend cannot appear alongside execute_tools",
      };
    }
    if (suspendActions.length > 1) {
      return { ok: false, error: "Multiple suspend actions are not allowed" };
    }
  }

  if (waitActions.length > 1) {
    return { ok: false, error: "Multiple wait actions are not allowed" };
  }

  if (waitActions.length > 0) {
    if (inferActions.length > 0) {
      return { ok: false, error: "wait and infer cannot appear together" };
    }
    if (executeActions.length > 0) {
      return {
        ok: false,
        error: "wait and execute_tools cannot appear together",
      };
    }
    if (suspendActions.length > 0) {
      return { ok: false, error: "wait and suspend cannot appear together" };
    }
    if (replyActions.length > 0) {
      return { ok: false, error: "wait and reply cannot appear together" };
    }
    if (doneActions.length > 0) {
      return { ok: false, error: "wait and done cannot appear together" };
    }
  }

  if (compactActions.length > 1) {
    return { ok: false, error: "Multiple compact actions are not allowed" };
  }

  if (compactActions.length > 0) {
    if (inferActions.length > 0) {
      return { ok: false, error: "compact and infer cannot appear together" };
    }
    if (executeActions.length > 0) {
      return {
        ok: false,
        error: "compact and execute_tools cannot appear together",
      };
    }
    if (replyActions.length > 0) {
      return { ok: false, error: "compact and reply cannot appear together" };
    }
    if (suspendActions.length > 0) {
      return { ok: false, error: "compact and suspend cannot appear together" };
    }
    if (waitActions.length > 0) {
      return { ok: false, error: "compact and wait cannot appear together" };
    }
    if (doneActions.length > 0) {
      return { ok: false, error: "compact and done cannot appear together" };
    }
  }

  // Verify fork actions have unique IDs.
  const forkIds = forkActions.map(
    (a) => (a as Extract<ReactorAction, { type: "fork" }>).forkId,
  );
  const uniqueForkIds = new Set(forkIds);
  if (forkIds.length !== uniqueForkIds.size) {
    return { ok: false, error: "Duplicate fork IDs in action list" };
  }

  // Build normalized list: collapse execute_tools into one parallel batch.
  const normalized: ReactorAction[] = [];

  for (const a of checkpointActions) {
    normalized.push(a);
  }

  for (const a of emitActions) {
    normalized.push(a);
  }

  for (const a of forkActions) {
    normalized.push(a);
  }

  for (const a of compactActions) {
    normalized.push(a);
  }

  if (executeActions.length > 0) {
    const merged: ToolCall[] = executeActions.flatMap((a) => a.calls);
    const allAddToHistory = executeActions.every(
      (a) => a.addToHistory !== false,
    );
    normalized.push({
      type: "execute_tools",
      calls: merged,
      parallel: true,
      ...(!allAddToHistory ? { addToHistory: false } : {}),
    });
  }

  for (const a of replyActions) {
    normalized.push(a);
  }

  for (const a of inferActions) {
    normalized.push(a);
  }

  for (const a of suspendActions) {
    normalized.push(a);
  }

  for (const a of waitActions) {
    normalized.push(a);
  }

  for (const a of doneActions) {
    normalized.push(a);
  }

  return { ok: true, normalized };
}
