import { describe, test, expect } from "bun:test";
import type { MessageTransport } from "@intx/types/runtime";

import { createHarnessRuntimeCapabilities } from "./runtime-capabilities";

// Minimal stand-in for MessageTransport. The factory passes the handle
// through; it does not invoke any methods on it.
// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test-only stand-in; factory never calls these methods
const stubTransport = {} as unknown as MessageTransport;

describe("createHarnessRuntimeCapabilities", () => {
  test("resolve('mail.transport') returns the supplied transport reference", () => {
    const capabilities = createHarnessRuntimeCapabilities({
      transport: stubTransport,
    });

    expect(capabilities.resolve("mail.transport")).toBe(stubTransport);
  });
});
