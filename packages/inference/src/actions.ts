// Action types and validation for the agent reactor.
//
// The reactor validates the set of actions returned by the plugin before
// executing any of them. Invalid combinations produce a reactor.error rather
// than partial execution — the reactor does not guess intent.
//
// Validation rules (INFERENCE.md § Action Validation):
// - At most one `infer` action.
// - At most one `done` action.
// - `infer` + `done` together is invalid.
// - `suspend` cannot appear alongside `infer` or `execute_tools`.
// - `fork` is composable — may appear alongside any other action.
// - `checkpoint` is composable — may appear alongside any other action.
// - Multiple `execute_tools` are merged into a single parallel batch.
// - `emit` is always valid and composable.

import type { ReactorAction, ToolCall } from "@interchange/types/runtime";

export type ValidationResult =
  | { ok: true; normalized: ReactorAction[] }
  | { ok: false; error: string };

/**
 * Validate and normalize a set of actions returned by the plugin.
 *
 * On success, returns the normalized action list with multiple `execute_tools`
 * collapsed into a single batched action. On failure, returns a diagnostic
 * error string.
 */
export function validateActions(
  actions: ReactorAction | ReactorAction[],
): ValidationResult {
  const list = Array.isArray(actions) ? actions : [actions];

  if (list.length === 0) {
    return { ok: false, error: "Plugin returned an empty action list" };
  }

  const inferActions = list.filter((a) => a.type === "infer");
  const doneActions = list.filter((a) => a.type === "done");
  const suspendActions = list.filter((a) => a.type === "suspend");
  const executeActions = list.filter(
    (a): a is Extract<ReactorAction, { type: "execute_tools" }> =>
      a.type === "execute_tools",
  );
  const forkActions = list.filter((a) => a.type === "fork");
  const emitActions = list.filter((a) => a.type === "emit");
  const checkpointActions = list.filter((a) => a.type === "checkpoint");

  if (inferActions.length > 1) {
    return { ok: false, error: "Multiple infer actions are not allowed" };
  }

  if (doneActions.length > 1) {
    return { ok: false, error: "Multiple done actions are not allowed" };
  }

  if (inferActions.length > 0 && doneActions.length > 0) {
    return { ok: false, error: "infer and done cannot appear together" };
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

  if (executeActions.length > 0) {
    const merged: ToolCall[] = executeActions.flatMap((a) => a.calls);
    normalized.push({
      type: "execute_tools",
      calls: merged,
      parallel: true,
    });
  }

  for (const a of inferActions) {
    normalized.push(a);
  }

  for (const a of suspendActions) {
    normalized.push(a);
  }

  for (const a of doneActions) {
    normalized.push(a);
  }

  return { ok: true, normalized };
}
