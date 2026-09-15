// The level check is the whole point of the bridge: an abort that already
// fired must still reach the reaction, because every caller builds its bridge
// after an await and can therefore arrive late.

import { describe, test, expect } from "bun:test";

import { bridgeAbort } from "./abort-bridge";

describe("bridgeAbort", () => {
  test("reacts to an abort that has already fired", () => {
    const ctrl = new AbortController();
    ctrl.abort();
    let reacted = 0;

    bridgeAbort(ctrl.signal, () => {
      reacted += 1;
    });

    expect(reacted).toBe(1);
  });

  test("reacts to an abort that fires later", () => {
    const ctrl = new AbortController();
    let reacted = 0;

    bridgeAbort(ctrl.signal, () => {
      reacted += 1;
    });
    expect(reacted).toBe(0);

    ctrl.abort();
    expect(reacted).toBe(1);
  });

  test("reacts once, however many times abort is called", () => {
    const ctrl = new AbortController();
    let reacted = 0;

    bridgeAbort(ctrl.signal, () => {
      reacted += 1;
    });
    ctrl.abort();
    ctrl.abort();

    expect(reacted).toBe(1);
  });

  test("detaching stops a later abort from reaching the reaction", () => {
    const ctrl = new AbortController();
    let reacted = 0;

    const detach = bridgeAbort(ctrl.signal, () => {
      reacted += 1;
    });
    detach();
    ctrl.abort();

    expect(reacted).toBe(0);
  });

  test("detaching after an already-fired abort is safe", () => {
    const ctrl = new AbortController();
    ctrl.abort();
    let reacted = 0;

    const detach = bridgeAbort(ctrl.signal, () => {
      reacted += 1;
    });
    // Nothing was attached, so this has nothing to undo -- and must not
    // pretend the reaction did not happen.
    detach();

    expect(reacted).toBe(1);
  });
});
