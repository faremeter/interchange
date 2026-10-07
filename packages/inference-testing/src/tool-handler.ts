import type { Clock } from "./clock";

/**
 * Return shape of a tool handler registered via `scenario.onTool(name, fn)`.
 * Three variants are accepted:
 *
 * 1. Sync `R` — dispatch in the same tick.
 * 2. `{ result, virtualDelayMs }` — dispatch at `clock.now() + virtualDelayMs`.
 * 3. `Promise<R | { result, virtualDelayMs }>` — the harness tracks the
 *    promise in its in-flight set (blocking quiescence), awaits resolution,
 *    then applies the sync-or-delayed rules.
 *
 * `undefined` is NOT a valid resolved value; it would silently dispatch
 * nothing to the reactor. The orchestration throws on `undefined`.
 */
export type ToolHandlerReturn<R> =
  | R
  | { result: R; virtualDelayMs: number }
  | Promise<R | { result: R; virtualDelayMs: number }>;

/**
 * A scenario-registered handler for a single tool name. Invoked when the
 * test dispatches a tool call (via `scenario.invokeTool`, or auto-dispatch
 * through `harness.runInference`). Classifies its result via
 * `ToolHandlerReturn`.
 */
export type ToolHandler = (args: unknown) => ToolHandlerReturn<unknown>;

/**
 * True iff `value` would be unwrapped by `ToolHandlerRegistry` as a delayed
 * envelope: both `result` and a finite non-negative `virtualDelayMs` must
 * be present; any other shape falls through to the sync-result branch.
 *
 * Exported so session capture and replay can reject results that collide
 * with this shape: recording would mis-classify them as harness constructs,
 * and replay would unwrap them and serve the inner `result` to the reactor.
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
 * Callback the harness invokes with the tool handler's resolved result —
 * once per `invokeTool`, in the same tick (sync), at the scheduled virtual
 * deadline (delayed envelope), or after promise resolution (async).
 */
export type DispatchToolResult = (result: unknown) => void;

export type ToolHandlerRegistry = {
  register(name: string, handler: ToolHandler): void;
  has(name: string): boolean;
  /**
   * Invoke the handler registered for `name`. Classifies the return shape:
   * sync results dispatch in the same tick; delayed envelopes dispatch via
   * `clock.schedule(now + d, ...)`; promises register with `trackInFlight`,
   * await resolution, then apply sync-or-delayed rules.
   *
   * Throws synchronously if no handler is registered for `name` or the
   * handler returned `undefined` synchronously. Async resolution to
   * `undefined` rejects the in-flight promise so `harness.run()` surfaces it.
   */
  invoke(name: string, args: unknown, dispatch: DispatchToolResult): void;
};

export type CreateToolHandlerRegistryOpts = {
  clock: Clock;
  /**
   * Called when a handler returns a promise. The registry passes the
   * promise here so the harness can block quiescence on it.
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
