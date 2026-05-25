import type { Clock } from "./clock";

/**
 * Return shape of a tool handler registered via `scenario.onTool(name, fn)`.
 *
 * Three variants are accepted:
 *
 * 1. Sync `R` — the handler returns a plain (non-promise, non-delayed) value;
 *    the harness dispatches it to the caller-supplied callback in the same
 *    tick.
 * 2. `{ result, virtualDelayMs }` — the handler asks the harness to defer
 *    dispatch by `virtualDelayMs` virtual milliseconds via
 *    `clock.schedule(clock.now() + virtualDelayMs, dispatch)`.
 * 3. `Promise<R | { result, virtualDelayMs }>` — the handler returns a
 *    promise (async work). The harness tracks the promise in its in-flight
 *    set (which blocks quiescence), awaits resolution, and then applies the
 *    sync-or-delayed dispatch rules to the resolved value.
 *
 * `undefined` is NOT a valid resolved value; treating it as one would let a
 * forgotten `return` silently dispatch nothing to the reactor. The
 * orchestration throws if any branch resolves to `undefined`.
 */
export type ToolHandlerReturn<R> =
  | R
  | { result: R; virtualDelayMs: number }
  | Promise<R | { result: R; virtualDelayMs: number }>;

/**
 * A scenario-registered handler for a single tool name. Invoked by the
 * harness when the test dispatches a tool call (today via the explicit
 * `scenario.invokeTool` helper; future slices may autodetect tool-call
 * frames in served wire bytes). The handler classifies its result via
 * `ToolHandlerReturn`; see that type for the three accepted shapes.
 */
export type ToolHandler = (args: unknown) => ToolHandlerReturn<unknown>;

/**
 * Internal: detect whether a returned (or resolved) value is the
 * `{ result, virtualDelayMs }` envelope rather than a bare result. We accept
 * the envelope only when both fields are present and `virtualDelayMs` is a
 * finite non-negative number; any other object shape falls through to the
 * "treat as sync result" branch, including objects that happen to have a
 * `result` key for other reasons.
 */
/**
 * Returns true iff `value` would be unwrapped by `ToolHandlerRegistry`
 * as a delayed envelope. Exported so session capture and replay can
 * reject results that collide with this shape — recording would
 * mis-classify them as test-harness constructs, and replay would
 * unwrap them and serve the inner `result` to the reactor.
 *
 * Both this package's recording and replay paths reject envelope-
 * shaped results before they reach the registry; the symmetry is
 * load-bearing.
 */
export function isDelayedEnvelope(
  value: unknown,
): value is { result: unknown; virtualDelayMs: number } {
  if (value === null || typeof value !== "object") return false;
  if (!("result" in value) || !("virtualDelayMs" in value)) return false;
  const delay: unknown = Reflect.get(value, "virtualDelayMs");
  if (typeof delay !== "number" || !Number.isFinite(delay) || delay < 0) {
    return false;
  }
  return true;
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  if (value === null || typeof value !== "object") return false;
  if (!("then" in value)) return false;
  const then: unknown = Reflect.get(value, "then");
  return typeof then === "function";
}

/**
 * Callback the harness invokes with the tool handler's resolved result.
 * Called exactly once per `invokeTool` — in the same tick for a sync
 * return, at the scheduled virtual deadline for a delayed envelope, or
 * after promise resolution for an async handler.
 */
export type DispatchToolResult = (result: unknown) => void;

export type ToolHandlerRegistry = {
  register(name: string, handler: ToolHandler): void;
  has(name: string): boolean;
  /**
   * Invoke the handler registered for `name`. The orchestrator synchronously
   * calls the handler, classifies the return shape, and:
   *
   * - sync result: calls `dispatch(result)` in the same tick;
   * - delayed envelope: schedules dispatch via `clock.schedule(now + d, ...)`;
   * - promise: registers it with `trackInFlight`, awaits resolution, and
   *   then applies sync-or-delayed rules to the resolved value.
   *
   * Throws synchronously if no handler is registered for `name` or if the
   * handler returned `undefined` synchronously. Async resolution to
   * `undefined` rejects the in-flight promise so `harness.run()` surfaces
   * the error.
   */
  invoke(name: string, args: unknown, dispatch: DispatchToolResult): void;
};

export type CreateToolHandlerRegistryOpts = {
  clock: Clock;
  /**
   * Called when a handler returns a promise. The registry passes the
   * promise here so the harness can block quiescence on it; the harness is
   * responsible for tracking add/remove lifecycle and re-throw semantics.
   */
  trackInFlight: (promise: Promise<void>) => void;
};

export function createToolHandlerRegistry(
  opts: CreateToolHandlerRegistryOpts,
): ToolHandlerRegistry {
  const { clock, trackInFlight } = opts;
  const handlers = new Map<string, ToolHandler>();

  const dispatchValue = (
    value: unknown,
    dispatch: DispatchToolResult,
  ): void => {
    if (value === undefined) {
      throw new Error(
        "Tool handler resolved to `undefined`; return a concrete result or a `{ result, virtualDelayMs }` envelope",
      );
    }
    if (isDelayedEnvelope(value)) {
      const { result, virtualDelayMs } = value;
      if (virtualDelayMs === 0) {
        dispatch(result);
        return;
      }
      clock.schedule(clock.now() + virtualDelayMs, () => {
        dispatch(result);
      });
      return;
    }
    dispatch(value);
  };

  const register = (name: string, handler: ToolHandler): void => {
    if (typeof name !== "string" || name.length === 0) {
      throw new Error("scenario.onTool: name must be a non-empty string");
    }
    if (typeof handler !== "function") {
      throw new Error("scenario.onTool: handler must be a function");
    }
    if (handlers.has(name)) {
      throw new Error(
        `scenario.onTool: a handler is already registered for tool "${name}"`,
      );
    }
    handlers.set(name, handler);
  };

  const has = (name: string): boolean => handlers.has(name);

  const invoke = (
    name: string,
    args: unknown,
    dispatch: DispatchToolResult,
  ): void => {
    const handler = handlers.get(name);
    if (handler === undefined) {
      throw new Error(
        `scenario.invokeTool: no handler registered for tool "${name}"`,
      );
    }
    const ret: unknown = handler(args);
    if (isPromiseLike(ret)) {
      const tracking = Promise.resolve(ret).then((resolved) => {
        dispatchValue(resolved, dispatch);
      });
      trackInFlight(tracking);
      return;
    }
    dispatchValue(ret, dispatch);
  };

  return { register, has, invoke };
}
