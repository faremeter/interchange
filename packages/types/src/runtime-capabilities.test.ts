import { describe, test, expect } from "bun:test";
import type { MessageTransport } from "./runtime";
import {
  createRuntimeCapabilities,
  type RuntimeCapabilityMap,
} from "./runtime-capabilities";

// A minimal stand-in for MessageTransport. The resolver does not invoke any
// transport methods — these tests assert handle-passing semantics, not
// transport behavior.
// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test-only stand-in; resolver never calls these methods
const stubTransport = {} as unknown as MessageTransport;

describe("createRuntimeCapabilities", () => {
  test("resolve returns the same reference the host provided", () => {
    const capabilities = createRuntimeCapabilities({
      "mail.transport": stubTransport,
    });

    expect(capabilities.resolve("mail.transport")).toBe(stubTransport);
  });

  test("resolve throws naming the missing key and identifies it as not provided", () => {
    const capabilities = createRuntimeCapabilities({});

    expect(() => capabilities.resolve("mail.transport")).toThrow(
      /"mail\.transport".*not provided by the host/,
    );
  });

  test("resolve throws distinctly when the host wires undefined to a non-nullable key", () => {
    // The host explicitly passed the key but set its value to undefined.
    // exactOptionalPropertyTypes prevents this construction through the
    // typed entry point, so the test casts to exercise the runtime
    // defensive check that catches a host that subverts the type system
    // (e.g. through an `as any` or an external JSON source that wasn't
    // validated). The resolver must surface this as a distinct error
    // from "key not provided" so the host can tell the two failure
    // modes apart.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- exercising the runtime guard that catches type-system subversion
    const subverted = {
      "mail.transport": undefined,
    } as unknown as Partial<RuntimeCapabilityMap>;
    const capabilities = createRuntimeCapabilities(subverted);

    expect(() => capabilities.resolve("mail.transport")).toThrow(
      /"mail\.transport".*provided as undefined/,
    );
  });

  test("mutating the input map after construction does not affect resolution", () => {
    const values: Partial<RuntimeCapabilityMap> = {
      "mail.transport": stubTransport,
    };
    const capabilities = createRuntimeCapabilities(values);

    delete values["mail.transport"];

    expect(capabilities.resolve("mail.transport")).toBe(stubTransport);
  });
});
