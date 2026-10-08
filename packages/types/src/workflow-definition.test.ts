import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import { workflowDefinitionEnvelopeSchema } from "./workflow-definition";

describe("workflowDefinitionEnvelopeSchema", () => {
  // What declaring `grantRequirements` on the envelope buys is VALIDATION,
  // not field survival. `.onUndeclaredKey("ignore")` is passthrough, so an
  // undeclared field would survive the read regardless; only the declaration
  // makes a malformed requirement fail at the deploy boundary. These tests
  // assert that validation property. The rejection test fails if the
  // `grantRequirements?` line is removed from the schema; a survival test
  // would pass either way and prove nothing.
  test("rejects a declared grantRequirement carrying an unknown source", () => {
    const validated = workflowDefinitionEnvelopeSchema({
      id: "my-workflow",
      triggers: [{ type: "manual" }],
      steps: { first: { kind: "step", id: "first" } },
      stepOrder: ["first"],
      grantRequirements: [
        { resource: "tool:search", action: "invoke", source: "stranger" },
      ],
    });
    expect(validated instanceof type.errors).toBe(true);
  });

  test("accepts and preserves a well-formed grantRequirement", () => {
    const blob = {
      id: "my-workflow",
      triggers: [{ type: "manual" }],
      steps: { first: { kind: "step", id: "first" } },
      stepOrder: ["first"],
      grantRequirements: [
        {
          resource: "credential:openai",
          action: "use",
          source: "creator" as const,
        },
        {
          resource: "tool:search",
          action: "invoke",
          effect: "ask" as const,
          source: "invoker" as const,
        },
      ],
    };
    const validated = workflowDefinitionEnvelopeSchema(blob);
    if (validated instanceof type.errors) {
      throw new Error(`unexpected validation error: ${validated.summary}`);
    }
    expect(validated.grantRequirements).toEqual(blob.grantRequirements);
  });

  // Same property for credentialBindings: declaring it on the envelope makes a
  // malformed binding fail at the deploy boundary. This is the path launch-time
  // resolution reads bindings back from, so a bad locator/authority must not
  // pass through unchecked. The rejection test fails if the
  // `credentialBindings?` line is removed from the schema.
  test("rejects a declared credentialBinding carrying an unknown authority", () => {
    const validated = workflowDefinitionEnvelopeSchema({
      id: "my-workflow",
      triggers: [{ type: "manual" }],
      steps: { first: { kind: "step", id: "first" } },
      stepOrder: ["first"],
      credentialBindings: [
        {
          package: "@acme/tools",
          handle: "gh",
          provider: "github",
          locator: "stranger",
        },
      ],
    });
    expect(validated instanceof type.errors).toBe(true);
  });

  test("accepts and preserves a well-formed credentialBinding", () => {
    const blob = {
      id: "my-workflow",
      triggers: [{ type: "manual" }],
      steps: { first: { kind: "step", id: "first" } },
      stepOrder: ["first"],
      credentialBindings: [
        {
          package: "@acme/tools",
          handle: "gh",
          provider: "github",
          locator: "tenant" as const,
        },
      ],
    };
    const validated = workflowDefinitionEnvelopeSchema(blob);
    if (validated instanceof type.errors) {
      throw new Error(`unexpected validation error: ${validated.summary}`);
    }
    expect(validated.credentialBindings).toEqual(blob.credentialBindings);
  });

  // Same property for inboundMailPolicy: declaring it on the envelope makes a
  // malformed policy fail at the deploy boundary. The policy keys on exactly
  // the four author-controllable outcomes, so an unknown outcome key (a typo or
  // a non-controllable outcome such as `clean`) must be rejected here rather
  // than ride through to later admission resolution. The rejection test fails
  // if the `inboundMailPolicy?` line is removed from the schema.
  test("rejects a declared inboundMailPolicy carrying an unknown outcome key", () => {
    const validated = workflowDefinitionEnvelopeSchema({
      id: "my-workflow",
      triggers: [{ type: "mail", to: "wf@acme.test" }],
      steps: { first: { kind: "step", id: "first" } },
      stepOrder: ["first"],
      inboundMailPolicy: { clean: "admit" },
    });
    expect(validated instanceof type.errors).toBe(true);
  });

  test("rejects a declared inboundMailPolicy carrying a non reject/admit value", () => {
    const validated = workflowDefinitionEnvelopeSchema({
      id: "my-workflow",
      triggers: [{ type: "mail", to: "wf@acme.test" }],
      steps: { first: { kind: "step", id: "first" } },
      stepOrder: ["first"],
      inboundMailPolicy: { missing: "quarantine" },
    });
    expect(validated instanceof type.errors).toBe(true);
  });

  test("accepts and preserves a well-formed sparse inboundMailPolicy", () => {
    const blob = {
      id: "my-workflow",
      triggers: [{ type: "mail", to: "wf@acme.test" }],
      steps: { first: { kind: "step", id: "first" } },
      stepOrder: ["first"],
      inboundMailPolicy: {
        untrustedFrom: "admit" as const,
        missing: "reject" as const,
      },
    };
    const validated = workflowDefinitionEnvelopeSchema(blob);
    if (validated instanceof type.errors) {
      throw new Error(`unexpected validation error: ${validated.summary}`);
    }
    expect(validated.inboundMailPolicy).toEqual(blob.inboundMailPolicy);
    // The two unset outcomes stay absent -- the envelope does not populate a
    // default for an outcome the author omitted.
    expect(validated.inboundMailPolicy).not.toHaveProperty("invalid");
    expect(validated.inboundMailPolicy).not.toHaveProperty("unknown");
  });
});

// The static workflow.json envelope push path is retired: a workflow asset is
// now a codebase, and a bare envelope tree is rejected at the push boundary
// before any steps/state shape check runs. The structural guard that a
// definition's `steps` and `state` are JSON objects (not arrays) now lives in
// `workflowDefinitionEnvelopeSchema`, which the codebase ambiguity check and the
// hydrate-time definition loaders both reuse. These regressions pin that guard
// at the schema so a loosened narrow surfaces here.
describe("workflow-definition steps/state-as-array rejection (regression)", () => {
  test("rejects a definition whose steps field is a JSON array", () => {
    const result = workflowDefinitionEnvelopeSchema({
      id: "wf-1",
      triggers: [],
      steps: [{ name: "step1" }, { name: "step2" }],
      stepOrder: ["step1", "step2"],
    });
    expect(result instanceof type.errors).toBe(true);
    if (!(result instanceof type.errors)) throw new Error("unreachable");
    expect(result.summary).toMatch(/array|object/);
  });

  test("rejects a definition whose state field is a JSON array", () => {
    const result = workflowDefinitionEnvelopeSchema({
      id: "wf-1",
      triggers: [],
      steps: { s1: { name: "s1" } },
      stepOrder: ["s1"],
      state: [],
    });
    expect(result instanceof type.errors).toBe(true);
    if (!(result instanceof type.errors)) throw new Error("unreachable");
    expect(result.summary).toMatch(/array|object/);
  });
});
