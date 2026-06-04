import { describe, test, expect } from "bun:test";

import { noopAuditStore } from "./audit-noop";

describe("noopAuditStore", () => {
  test("commitAudit resolves to void", async () => {
    const store = noopAuditStore();
    expect(await store.commitAudit([])).toBeUndefined();
  });

  test("commitErrors resolves to void", async () => {
    const store = noopAuditStore();
    expect(await store.commitErrors([])).toBeUndefined();
  });

  test("loadAudit returns an empty array", async () => {
    const store = noopAuditStore();
    expect(await store.loadAudit("sess")).toEqual([]);
  });

  test("each call returns a fresh object", () => {
    expect(noopAuditStore()).not.toBe(noopAuditStore());
  });
});
