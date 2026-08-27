import { type } from "arktype";

export const SidecarCapabilityRule = type({
  capability: "string > 0",
  effect: "'require' | 'block'",
});
export type SidecarCapabilityRule = typeof SidecarCapabilityRule.infer;

export const SidecarCapabilityDeclaration = type({
  capability: "string > 0",
  state: "'available' | 'blocked'",
});
export type SidecarCapabilityDeclaration =
  typeof SidecarCapabilityDeclaration.infer;

export const SidecarCapabilityPolicy = type({
  "capabilities?": SidecarCapabilityRule.array(),
});
export type SidecarCapabilityPolicy = typeof SidecarCapabilityPolicy.infer;
