import { describe, test, expect } from "bun:test";

import { getLogger } from "@intx/log";

import {
  killChildHandle,
  waitDeadline,
  defaultSetTimer,
} from "./child-termination";
import type { SubprocessHandle } from "./types";

const logger = getLogger(["workflow-host", "supervisor", "child-termination"]);

function emptyControlReader(): AsyncIterableIterator<string> {
  return (async function* () {
    /* no control frames in these unit tests */
  })();
}

function emptyEventReader(): AsyncIterableIterator<Uint8Array> {
  return (async function* () {
    /* no event frames in these unit tests */
  })();
}

/**
 * A `SubprocessHandle` whose only live surface is `kill` (recording the
 * signals it receives) and `exited`. When `sigtermExits` is false the child
 * ignores SIGTERM and settles `exited` only on SIGKILL -- the wedged-child
 * shape the escalation branch exists for. The stream fields are inert
 * stubs; `killChildHandle` never touches them.
 */
function makeHandle(opts: { sigtermExits: boolean }): {
  handle: SubprocessHandle;
  killSignals: string[];
} {
  const killSignals: string[] = [];
  let resolveExit: ((code: number) => void) | undefined;
  const exited = new Promise<number>((resolve) => {
    resolveExit = resolve;
  });
  const handle: SubprocessHandle = {
    pid: 1234,
    controlWriter: { write: () => Promise.resolve() },
    controlReader: { read: emptyControlReader },
    eventReader: { read: emptyEventReader },
    kill: (signal?: number | string) => {
      const sig = typeof signal === "string" ? signal : String(signal ?? "");
      killSignals.push(sig);
      if (opts.sigtermExits || sig === "SIGKILL") {
        resolveExit?.(0);
      }
    },
    exited,
  };
  return { handle, killSignals };
}

describe("killChildHandle", () => {
  test("a child that exits on SIGTERM is not escalated to SIGKILL", async () => {
    const { handle, killSignals } = makeHandle({ sigtermExits: true });
    let cleared = 0;

    await killChildHandle(handle, 5_000, {
      logger,
      // A child that exits before the deadline means the timer callback
      // never fires; the deadline handle is still cleared exactly once.
      setTimer: () => Symbol("timer"),
      clearTimer: () => {
        cleared += 1;
      },
    });

    expect(killSignals).toEqual(["SIGTERM"]);
    expect(cleared).toBe(1);
  });

  test("a child that ignores SIGTERM is escalated to SIGKILL when the deadline wins", async () => {
    const { handle, killSignals } = makeHandle({ sigtermExits: false });
    let cleared = 0;
    const scheduled: (() => void)[] = [];

    // waitDeadline calls setTimer synchronously, before killChildHandle's
    // first await, so the callback is captured by the time this call returns
    // its pending promise. Firing it makes the deadline win the race.
    const pending = killChildHandle(handle, 5_000, {
      logger,
      setTimer: (cb) => {
        scheduled.push(cb);
        return Symbol("timer");
      },
      clearTimer: () => {
        cleared += 1;
      },
    });
    expect(scheduled).toHaveLength(1);
    scheduled[0]?.();
    await pending;

    expect(killSignals).toEqual(["SIGTERM", "SIGKILL"]);
    expect(cleared).toBe(1);
  });

  test("omitting the timer deps falls back to real timers and still escalates", async () => {
    const { handle, killSignals } = makeHandle({ sigtermExits: false });

    // No setTimer/clearTimer keys -- the omit-key path the recycle call site
    // takes when its context timers are undefined. A tiny real timeout drives
    // the escalation without a fake timer.
    await killChildHandle(handle, 5, { logger });

    expect(killSignals).toEqual(["SIGTERM", "SIGKILL"]);
  });
});

describe("waitDeadline", () => {
  test("resolves via the injected timer and surfaces the timer handle", async () => {
    const scheduled: (() => void)[] = [];
    const sentinel = Symbol("timer");
    const { promise, handle } = waitDeadline((cb) => {
      scheduled.push(cb);
      return sentinel;
    }, 1_000);

    expect(handle).toBe(sentinel);
    let resolved = false;
    void promise.then(() => {
      resolved = true;
    });
    expect(resolved).toBe(false);
    scheduled[0]?.();
    await promise;
    expect(resolved).toBe(true);
  });

  test("defaultSetTimer schedules the callback via the real event loop", async () => {
    let fired = false;
    defaultSetTimer(() => {
      fired = true;
    }, 1);
    await new Promise((r) => setTimeout(r, 15));
    expect(fired).toBe(true);
  });
});
