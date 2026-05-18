import { describe, test, expect } from "bun:test";
import { resolve } from "node:path";

import { acquireContextDirLock, AgentInUseError } from "./lock";

describe("acquireContextDirLock", () => {
  test("returns a lock whose path is the resolved absolute path", () => {
    const lock = acquireContextDirLock("/tmp/agent-lock-1");
    try {
      expect(lock.path).toBe(resolve("/tmp/agent-lock-1"));
    } finally {
      lock.release();
    }
  });

  test("rejects a second acquisition of the same directory", () => {
    const lock = acquireContextDirLock("/tmp/agent-lock-2");
    try {
      expect(() => acquireContextDirLock("/tmp/agent-lock-2")).toThrow(
        AgentInUseError,
      );
    } finally {
      lock.release();
    }
  });

  test("re-acquires the same directory after release", () => {
    const lock1 = acquireContextDirLock("/tmp/agent-lock-3");
    lock1.release();
    const lock2 = acquireContextDirLock("/tmp/agent-lock-3");
    try {
      expect(lock2.path).toBe(resolve("/tmp/agent-lock-3"));
    } finally {
      lock2.release();
    }
  });

  test("collides on lexically distinct but equivalent paths", () => {
    const lock = acquireContextDirLock("/tmp/agent-lock-4/../agent-lock-4/foo");
    try {
      expect(() => acquireContextDirLock("/tmp/agent-lock-4/foo")).toThrow(
        AgentInUseError,
      );
    } finally {
      lock.release();
    }
  });

  test("collides on a relative path that resolves to the same absolute path", () => {
    const absolute = resolve("relative-lock-test-dir");
    const lock = acquireContextDirLock("relative-lock-test-dir");
    try {
      expect(lock.path).toBe(absolute);
      expect(() => acquireContextDirLock(absolute)).toThrow(AgentInUseError);
    } finally {
      lock.release();
    }
  });

  test("does not collide between distinct directories", () => {
    const lock1 = acquireContextDirLock("/tmp/agent-lock-5a");
    const lock2 = acquireContextDirLock("/tmp/agent-lock-5b");
    try {
      expect(lock1.path).not.toBe(lock2.path);
    } finally {
      lock1.release();
      lock2.release();
    }
  });

  test("release is idempotent", () => {
    const lock = acquireContextDirLock("/tmp/agent-lock-6");
    lock.release();
    lock.release();
    const reacquired = acquireContextDirLock("/tmp/agent-lock-6");
    reacquired.release();
  });

  test("AgentInUseError exposes contextDir as the resolved path", () => {
    const lock = acquireContextDirLock("/tmp/agent-lock-7");
    try {
      acquireContextDirLock("/tmp/agent-lock-7");
      throw new Error("should have thrown AgentInUseError");
    } catch (err) {
      expect(err).toBeInstanceOf(AgentInUseError);
      if (err instanceof AgentInUseError) {
        expect(err.contextDir).toBe(resolve("/tmp/agent-lock-7"));
      }
    } finally {
      lock.release();
    }
  });
});
